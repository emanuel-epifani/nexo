import * as net from 'net';

// --- PROTOCOL DEFINITIONS ---

enum FrameType {
  REQUEST = 0x01,
  RESPONSE = 0x02,
  PUSH = 0x03,
  ERROR = 0x04,
  PING = 0x05,
  PONG = 0x06,
}

enum ResponseStatus {
  OK = 0x00,
  ERR = 0x01,
  NULL = 0x02,
  DATA = 0x03,
  Q_DATA = 0x04,
}

export enum Opcode {
  // KV commands (0x02-0x0F)
  KV_SET = 0x02,
  KV_GET = 0x03,
  KV_DEL = 0x04,

  // Queue commands (0x10-0x1F)
  Q_DECLARE = 0x10,
  Q_PUSH = 0x11,
  Q_CONSUME = 0x12,
  Q_ACK = 0x13,

  // Topic commands (0x20-0x2F)
  PUB = 0x21,
  SUB = 0x22,

  // Stream commands (0x30-0x3F)
  S_ADD = 0x31,
  S_READ = 0x32,
}

export interface QueueConfig {
  visibilityTimeoutMs?: number;
  maxRetries?: number;
  ttlMs?: number;
  delayMs?: number;
}

export interface PushOptions {
  priority?: number;
  delayMs?: number;
}

export class NexoQueue {
  constructor(private client: NexoClient, public readonly name: string) { }

  async push(data: any, options: PushOptions = {}): Promise<void> {
    let valBuf: Buffer;
    if (Buffer.isBuffer(data)) {
      valBuf = data;
    } else if (typeof data === 'string') {
      valBuf = Buffer.from(data, 'utf8');
    } else {
      valBuf = Buffer.from(JSON.stringify(data), 'utf8');
    }

    const qLen = Buffer.byteLength(this.name, 'utf8');
    const priority = options.priority || 0;
    const delay = BigInt(options.delayMs || 0);
    const payloadSize = 1 + 8 + 4 + qLen + valBuf.length;
    const payload = Buffer.allocUnsafe(payloadSize);
    payload[0] = priority;
    payload.writeBigUInt64BE(delay, 1);
    payload.writeUInt32BE(qLen, 9);
    payload.write(this.name, 13, 'utf8');
    valBuf.copy(payload, 13 + qLen);

    // Use internal send from client
    const res = await (this.client as any).send(Opcode.Q_PUSH, payload);
    if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
  }

  subscribe(callback: (data: any) => Promise<void> | void): { stop: () => void } {
    const qLen = Buffer.byteLength(this.name, 'utf8');
    const payload = Buffer.allocUnsafe(4 + qLen);
    payload.writeUInt32BE(qLen, 0);
    payload.write(this.name, 4, 'utf8');

    let active = true;

    const loop = async () => {
      while (this.client.connected && active) {
        try {
          const res = await (this.client as any).send(Opcode.Q_CONSUME, payload);
          if (!active) break;
          if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
          if (res.status === ResponseStatus.Q_DATA) {
            const idHex = res.data.subarray(0, 16).toString('hex');
            const dataBuf = res.data.subarray(16);
            const rawStr = dataBuf.toString('utf8');

            let data: any;
            try {
              data = JSON.parse(rawStr);
            } catch {
              data = rawStr;
            }

            try {
              await callback(data);
              await this.ack(idHex);
            } catch (e) {
              if (active) console.error(`Error in subscribe callback for queue ${this.name}:`, e);
            }
          }
        } catch (err) {
          if (!this.client.connected || !active) break;
          console.error(`Subscription error for queue ${this.name}:`, err);
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    };

    loop();

    return {
      stop: () => { active = false; }
    };
  }

  async ack(id: string): Promise<void> {
    const qLen = Buffer.byteLength(this.name, 'utf8');
    const idBuf = Buffer.from(id, 'hex');
    const payload = Buffer.allocUnsafe(16 + 4 + qLen);
    idBuf.copy(payload, 0);
    payload.writeUInt32BE(qLen, 16);
    payload.write(this.name, 20, 'utf8');
    const res = await (this.client as any).send(Opcode.Q_ACK, payload);
    if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
  }
}

// --- RING BUFFER DECODER (Zero-Copy, Pre-allocated) ---

class RingDecoder {
  private buf: Buffer;
  private head = 0;  // Read position
  private tail = 0;  // Write position

  constructor(size = 512 * 1024) {
    this.buf = Buffer.allocUnsafe(size);
  }

  push(chunk: Buffer): void {
    const needed = chunk.length;
    const available = this.buf.length - this.tail;

    if (available < needed) {
      const used = this.tail - this.head;
      if (used > 0) {
        this.buf.copy(this.buf, 0, this.head, this.tail);
      }
      this.tail = used;
      this.head = 0;
    }

    chunk.copy(this.buf, this.tail);
    this.tail += needed;
  }

  nextFrame(): { type: number; id: number; payload: Buffer } | null {
    const available = this.tail - this.head;
    if (available < 9) return null;

    const type = this.buf[this.head];
    const id = this.buf.readUInt32BE(this.head + 1);
    const payloadLen = this.buf.readUInt32BE(this.head + 5);
    const totalLen = 9 + payloadLen;

    if (available < totalLen) return null;

    const payload = this.buf.subarray(this.head + 9, this.head + totalLen);
    this.head += totalLen;

    if (this.head === this.tail) {
      this.head = 0;
      this.tail = 0;
    }

    return { type, id, payload };
  }
}

// --- CLIENT IMPLEMENTATION ---

export interface NexoOptions {
  host?: string;
  port?: number;
}

type PendingHandler = { resolve: (v: { status: ResponseStatus; data: Buffer }) => void; reject: (e: Error) => void };

export class NexoClient {
  private socket: net.Socket;
  private isConnected = false;
  private host: string;
  private port: number;
  private decoder = new RingDecoder();
  private queues = new Map<string, NexoQueue>();

  // ID management: u32 wrap-around
  private nextId = 1;

  // Map for pending requests: ID -> Handler
  private pending = new Map<number, PendingHandler>();

  private writeBuf: Buffer;
  private writeOffset = 0;
  private flushScheduled = false;

  constructor(options: NexoOptions = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 8080;
    this.socket = new net.Socket();
    this.socket.setNoDelay(true);
    this.writeBuf = Buffer.allocUnsafe(64 * 1024);
    this.setupSocketListeners();
  }

  public get connected(): boolean {
    return this.isConnected;
  }

  static async connect(options: NexoOptions = {}): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.connect();
    return client;
  }

  async registerQueue(name: string, config: QueueConfig = {}): Promise<NexoQueue> {
    if (this.queues.has(name)) return this.queues.get(name)!;

    const visibility = BigInt(config.visibilityTimeoutMs ?? 30000);
    const maxRetries = config.maxRetries ?? 5;
    const ttl = BigInt(config.ttlMs ?? 604800000);
    const delay = BigInt(config.delayMs ?? 0);
    const qLen = Buffer.byteLength(name, 'utf8');

    const payload = Buffer.allocUnsafe(8 + 4 + 8 + 8 + 4 + qLen);
    payload.writeBigUInt64BE(visibility, 0);
    payload.writeUInt32BE(maxRetries, 8);
    payload.writeBigUInt64BE(ttl, 12);
    payload.writeBigUInt64BE(delay, 20);
    payload.writeUInt32BE(qLen, 28);
    payload.write(name, 32, 'utf8');

    const res = await this.send(Opcode.Q_DECLARE, payload);
    if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());

    const queue = new NexoQueue(this, name);
    this.queues.set(name, queue);
    return queue;
  }

  private setupSocketListeners() {
    this.socket.on('data', (chunk) => {
      this.decoder.push(chunk);
      let frame;
      while ((frame = this.decoder.nextFrame())) {
        if (frame.type === FrameType.RESPONSE) {
          const handler = this.pending.get(frame.id);
          if (handler) {
            const status = frame.payload[0] as ResponseStatus;
            const data = Buffer.from(frame.payload.subarray(1));
            handler.resolve({ status, data });
            this.pending.delete(frame.id);
          }
        }
      }
    });

    this.socket.on('error', (err) => {
      this.isConnected = false;
      for (const handler of this.pending.values()) {
        handler.reject(err as Error);
      }
      this.pending.clear();
    });

    this.socket.on('close', () => {
      this.isConnected = false;
      // Rigetta tutte le richieste pendenti per evitare hang
      for (const handler of this.pending.values()) {
        handler.reject(new Error('Connection closed'));
      }
      this.pending.clear();
    });
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;
    return new Promise((resolve, reject) => {
      this.socket.connect(this.port, this.host, () => {
        this.isConnected = true;
        resolve();
      });
      this.socket.once('error', (err) => {
        if (!this.isConnected) reject(err);
      });
    });
  }

  private flushBatch = (): void => {
    this.flushScheduled = false;
    if (this.writeOffset === 0) return;
    this.socket.write(this.writeBuf.subarray(0, this.writeOffset));
    this.writeOffset = 0;
  };

  private send(opcode: number, payloadBody: Buffer): Promise<{ status: ResponseStatus; data: Buffer }> {
    if (!this.isConnected) return Promise.reject(new Error('Client not connected'));

    // Wrap ID as u32
    const id = this.nextId;
    this.nextId = (this.nextId + 1) >>> 0;
    if (this.nextId === 0) this.nextId = 1;

    const payloadLen = 1 + payloadBody.length;
    const totalSize = 9 + payloadLen;

    if (this.writeOffset + totalSize > this.writeBuf.length) {
      if (this.writeOffset > 0) {
        this.socket.write(this.writeBuf.subarray(0, this.writeOffset));
        this.writeOffset = 0;
      }
      if (totalSize > this.writeBuf.length) {
        const msg = Buffer.allocUnsafe(totalSize);
        msg[0] = FrameType.REQUEST;
        msg.writeUInt32BE(id, 1);
        msg.writeUInt32BE(payloadLen, 5);
        msg[9] = opcode;
        payloadBody.copy(msg, 10);
        this.socket.write(msg);
        return new Promise((resolve, reject) => {
          this.pending.set(id, { resolve, reject });
        });
      }
    }

    const buf = this.writeBuf;
    let off = this.writeOffset;
    buf[off] = FrameType.REQUEST;
    buf.writeUInt32BE(id, off + 1);
    buf.writeUInt32BE(payloadLen, off + 5);
    buf[off + 9] = opcode;
    payloadBody.copy(buf, off + 10);
    this.writeOffset = off + totalSize;

    if (!this.flushScheduled) {
      this.flushScheduled = true;
      setImmediate(this.flushBatch);
    }

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  disconnect(): void {
    if (this.writeOffset > 0) {
      this.socket.write(this.writeBuf.subarray(0, this.writeOffset));
      this.writeOffset = 0;
    }
    this.socket.end();
    this.socket.destroy();
    this.isConnected = false;
  }

  public readonly kv = {
    set: (key: string, value: string | Buffer, ttlSeconds = 0): Promise<void> => {
      const keyLen = Buffer.byteLength(key, 'utf8');
      const valBuf = typeof value === 'string' ? Buffer.from(value, 'utf8') : value;
      const payloadSize = 8 + 4 + keyLen + valBuf.length;
      const payload = Buffer.allocUnsafe(payloadSize);
      payload.writeBigUInt64BE(BigInt(ttlSeconds), 0);
      payload.writeUInt32BE(keyLen, 8);
      payload.write(key, 12, 'utf8');
      valBuf.copy(payload, 12 + keyLen);
      return this.send(Opcode.KV_SET, payload).then((res) => {
        if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
      });
    },

    get: (key: string): Promise<Buffer | null> => {
      const keyLen = Buffer.byteLength(key, 'utf8');
      const payload = Buffer.allocUnsafe(4 + keyLen);
      payload.writeUInt32BE(keyLen, 0);
      payload.write(key, 4, 'utf8');
      return this.send(Opcode.KV_GET, payload).then((res) => {
        if (res.status === ResponseStatus.NULL) return null;
        if (res.status === ResponseStatus.DATA) return res.data;
        if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
        return null;
      });
    },

    del: (key: string): Promise<void> => {
      const keyLen = Buffer.byteLength(key, 'utf8');
      const payload = Buffer.allocUnsafe(4 + keyLen);
      payload.writeUInt32BE(keyLen, 0);
      payload.write(key, 4, 'utf8');
      return this.send(Opcode.KV_DEL, payload).then((res) => {
        if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
      });
    },
  };
}

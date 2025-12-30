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
  DEBUG_ECHO = 0x00,
  KV_SET = 0x02,
  KV_GET = 0x03,
  KV_DEL = 0x04,
  Q_DECLARE = 0x10,
  Q_PUSH = 0x11,
  Q_CONSUME = 0x12,
  Q_ACK = 0x13,
  PUB = 0x21,
  SUB = 0x22,
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

/**
 * DATA CODEC: Centralized serialization logic.
 */
class DataCodec {
  static serialize(data: any): Buffer {
    if (Buffer.isBuffer(data)) return data;
    if (typeof data === 'string') return Buffer.from(data, 'utf8');
    if (data === undefined || data === null) return Buffer.alloc(0);
    return Buffer.from(JSON.stringify(data), 'utf8');
  }

  static deserialize(buf: Buffer): any {
    if (buf.length === 0) return null;
    const str = buf.toString('utf8');
    try { return JSON.parse(str); } catch { return str; }
  }
}

/**
 * PROTOCOL READER: Zero-copy wrapper for server responses.
 */
class ProtocolReader {
  private offset = 0;
  constructor(public readonly buffer: Buffer) { }

  readU8(): number { return this.buffer[this.offset++]; }
  readU32(): number {
    const v = this.buffer.readUInt32BE(this.offset);
    this.offset += 4;
    return v;
  }
  readU64(): bigint {
    const v = this.buffer.readBigUInt64BE(this.offset);
    this.offset += 8;
    return v;
  }
  readUUID(): string {
    const hex = this.buffer.subarray(this.offset, this.offset + 16).toString('hex');
    this.offset += 16;
    return hex;
  }
  readString(): string {
    const len = this.readU32();
    const s = this.buffer.subarray(this.offset, this.offset + len).toString('utf8');
    this.offset += len;
    return s;
  }
  readData(): any { return DataCodec.deserialize(this.buffer.subarray(this.offset)); }
}

/**
 * OPTIMIZED REQUEST BUILDER: Fluid API that writes directly to the client's shared buffer.
 * Reusable to avoid GC pressure.
 */
class RequestBuilder {
  private _ops: { type: number, val: any, size: number }[] = [];
  private _payloadSize = 1; // +1 for opcode
  private _opcode: Opcode = Opcode.DEBUG_ECHO;

  constructor(private client: NexoClient) { }

  reset(opcode: Opcode): this {
    this._opcode = opcode;
    this._ops.length = 0;
    this._payloadSize = 1;
    return this;
  }

  writeU8(v: number): this {
    this._ops.push({ type: 1, val: v, size: 1 });
    this._payloadSize += 1;
    return this;
  }

  writeU32(v: number): this {
    this._ops.push({ type: 2, val: v, size: 4 });
    this._payloadSize += 4;
    return this;
  }

  writeU64(v: number | bigint): this {
    this._ops.push({ type: 3, val: BigInt(v), size: 8 });
    this._payloadSize += 8;
    return this;
  }

  writeUUID(id: string): this {
    const buf = Buffer.from(id, 'hex');
    this._ops.push({ type: 4, val: buf, size: 16 });
    this._payloadSize += 16;
    return this;
  }

  writeString(s: string): this {
    const len = Buffer.byteLength(s, 'utf8');
    this._ops.push({ type: 5, val: s, size: 4 + len });
    this._payloadSize += 4 + len;
    return this;
  }

  writeData(d: any): this {
    const buf = DataCodec.serialize(d);
    this._ops.push({ type: 4, val: buf, size: buf.length });
    this._payloadSize += buf.length;
    return this;
  }

  async send(): Promise<{ status: ResponseStatus; reader: ProtocolReader }> {
    const res = await (this.client as any).dispatch(this._opcode, this._payloadSize, this._ops);
    return { status: res.status, reader: new ProtocolReader(res.data) };
  }
}

export class NexoQueue {
  constructor(private client: NexoClient, public readonly name: string) { }

  async push(data: any, options: PushOptions = {}): Promise<void> {
    await this.client.request(Opcode.Q_PUSH)
      .writeU8(options.priority || 0)
      .writeU64(options.delayMs || 0)
      .writeString(this.name)
      .writeData(data)
      .send();
  }

  subscribe(callback: (data: any) => Promise<void> | void): { stop: () => void } {
    let active = true;
    const loop = async () => {
      while (this.client.connected && active) {
        try {
          const res = await this.client.request(Opcode.Q_CONSUME).writeString(this.name).send();
          if (!active) break;
          if (res.status === ResponseStatus.Q_DATA) {
            const idHex = res.reader.readUUID();
            const data = res.reader.readData();
            try {
              await callback(data);
              await this.ack(idHex);
            } catch (e) {
              if (active) console.error(`Callback error:`, e);
            }
          }
        } catch (err) {
          if (!this.client.connected || !active) break;
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    };
    loop();
    return { stop: () => { active = false; } };
  }

  async ack(id: string): Promise<void> {
    await this.client.request(Opcode.Q_ACK).writeUUID(id).writeString(this.name).send();
  }
}

// --- RING BUFFER DECODER ---
class RingDecoder {
  private buf: Buffer;
  private head = 0;
  private tail = 0;
  constructor(size = 512 * 1024) { this.buf = Buffer.allocUnsafe(size); }

  push(chunk: Buffer): void {
    const needed = chunk.length;
    if (this.buf.length - this.tail < needed) {
      const used = this.tail - this.head;
      if (used > 0) this.buf.copy(this.buf, 0, this.head, this.tail);
      this.tail = used; this.head = 0;
    }
    chunk.copy(this.buf, this.tail);
    this.tail += needed;
  }

  nextFrame(): { type: number; id: number; payload: Buffer } | null {
    if (this.tail - this.head < 9) return null;
    const type = this.buf[this.head];
    const id = this.buf.readUInt32BE(this.head + 1);
    const payloadLen = this.buf.readUInt32BE(this.head + 5);
    if (this.tail - this.head < 9 + payloadLen) return null;
    const payload = this.buf.subarray(this.head + 9, this.head + 9 + payloadLen);
    this.head += 9 + payloadLen;
    if (this.head === this.tail) { this.head = 0; this.tail = 0; }
    return { type, id, payload };
  }
}

// --- CLIENT IMPLEMENTATION ---
export class NexoClient {
  private socket: net.Socket;
  private isConnected = false;
  private decoder = new RingDecoder();
  private nextId = 1;
  private pending = new Map<number, { resolve: any, reject: any }>();
  private writeBuf = Buffer.allocUnsafe(64 * 1024);
  private writeOffset = 0;
  private flushScheduled = false;
  private sharedBuilder: RequestBuilder;
  private queues = new Map<string, NexoQueue>();

  constructor(private options: { host?: string, port?: number } = {}) {
    this.socket = new net.Socket();
    this.socket.setNoDelay(true);
    this.sharedBuilder = new RequestBuilder(this);
    this.setupListeners();
  }

  public get connected() { return this.isConnected; }

  static async connect(opts?: any) {
    const c = new NexoClient(opts);
    await c.connect();
    return c;
  }

  request(opcode: Opcode) { return this.sharedBuilder.reset(opcode); }

  async registerQueue(name: string, config: QueueConfig = {}): Promise<NexoQueue> {
    if (this.queues.has(name)) return this.queues.get(name)!;
    await this.request(Opcode.Q_DECLARE)
      .writeU64(config.visibilityTimeoutMs ?? 30000).writeU32(config.maxRetries ?? 5)
      .writeU64(config.ttlMs ?? 604800000).writeU64(config.delayMs ?? 0)
      .writeString(name).send();
    const q = new NexoQueue(this, name);
    this.queues.set(name, q);
    return q;
  }

  public readonly kv = {
    set: (key: string, val: any, ttl = 0) => this.request(Opcode.KV_SET).writeU64(ttl).writeString(key).writeData(val).send().then(() => { }),
    get: (key: string) => this.request(Opcode.KV_GET).writeString(key).send().then(r => r.status === ResponseStatus.NULL ? null : r.reader.readData()),
    del: (key: string) => this.request(Opcode.KV_DEL).writeString(key).send().then(() => { }),
  };

  /**
   * Namespace for protocol validation and debugging.
   */
  public readonly debug = {
    echo: async (data: any): Promise<any> => {
      const res = await this.request(Opcode.DEBUG_ECHO)
        .writeData(data)
        .send();
      return res.reader.readData();
    }
  };

  private setupListeners() {
    this.socket.on('data', (chunk) => {
      this.decoder.push(chunk);
      let frame;
      while ((frame = this.decoder.nextFrame())) {
        if (frame.type === FrameType.RESPONSE) {
          const h = this.pending.get(frame.id);
          if (h) {
            this.pending.delete(frame.id);
            h.resolve({ status: frame.payload[0], data: frame.payload.subarray(1) });
          }
        }
      }
    });
    const cleanup = (err: any) => {
      this.isConnected = false;
      this.pending.forEach(h => h.reject(err || new Error('Closed')));
      this.pending.clear();
    };
    this.socket.on('error', cleanup);
    this.socket.on('close', cleanup);
  }

  async connect(): Promise<void> {
    return new Promise((res, rej) => {
      this.socket.connect(this.options.port || 8080, this.options.host || '127.0.0.1', () => {
        this.isConnected = true; res();
      });
      this.socket.once('error', rej);
    });
  }

  private flush = () => {
    this.flushScheduled = false;
    if (this.writeOffset === 0) return;
    this.socket.write(this.writeBuf.subarray(0, this.writeOffset));
    this.writeOffset = 0;
  };

  private dispatch(opcode: number, payloadLen: number, ops: any[]): Promise<any> {
    const total = 9 + payloadLen;
    if (this.writeOffset + total > this.writeBuf.length) this.flush();

    const id = this.nextId++;
    if (this.nextId === 0) this.nextId = 1;

    let off = this.writeOffset;
    const buf = this.writeBuf;
    buf[off] = FrameType.REQUEST;
    buf.writeUInt32BE(id, off + 1);
    buf.writeUInt32BE(payloadLen, off + 5);
    buf[off + 9] = opcode;
    off += 10;

    for (let i = 0; i < ops.length; i++) {
      const op = ops[i];
      switch (op.type) {
        case 1: buf[off] = op.val; off += 1; break;
        case 2: buf.writeUInt32BE(op.val, off); off += 4; break;
        case 3: buf.writeBigUInt64BE(op.val, off); off += 8; break;
        case 4: (op.val as Buffer).copy(buf, off); off += op.size; break;
        case 5:
          const sLen = op.size - 4;
          buf.writeUInt32BE(sLen, off);
          buf.write(op.val, off + 4, 'utf8');
          off += op.size;
          break;
      }
    }
    this.writeOffset = off;
    if (!this.flushScheduled) { this.flushScheduled = true; setImmediate(this.flush); }

    return new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
  }

  disconnect() { this.flush(); this.socket.destroy(); this.isConnected = false; }
}

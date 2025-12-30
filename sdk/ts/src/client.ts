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
  // Debug commands
  DEBUG_ECHO = 0x00,

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

/**
 * DATA CODEC: Handles all serialization/deserialization of payloads.
 * Centralizing this allows for future optimizations like MessagePack or Protobuf.
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
    try {
      return JSON.parse(str);
    } catch {
      return str;
    }
  }
}

/**
 * PROTOCOL READER: Sequential reader for parsing server responses byte by byte.
 * Avoids manual offset management in business logic.
 */
class ProtocolReader {
  private offset = 0;
  constructor(public readonly buffer: Buffer) { }

  readU8(): number {
    return this.buffer[this.offset++];
  }

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

  readData(): any {
    return DataCodec.deserialize(this.buffer.subarray(this.offset));
  }

  // Returns everything from current offset to end
  readRemaining(): Buffer {
    return this.buffer.subarray(this.offset);
  }
}

/**
 * REQUEST BUILDER: Fluid API for internal use to construct binary requests.
 * Ensures correct protocol layout and handles single-allocation optimization.
 */
class RequestBuilder {
  private _frameType: number = FrameType.REQUEST;
  private _chunks: { type: string; value: any; size: number }[] = [];

  constructor(private client: NexoClient, private opcode: Opcode) { }

  header(config: { type: number }): this {
    this._frameType = config.type;
    return this;
  }

  writeU8(v: number): this {
    this._chunks.push({ type: 'u8', value: v, size: 1 });
    return this;
  }

  writeU32(v: number): this {
    this._chunks.push({ type: 'u32', value: v, size: 4 });
    return this;
  }

  writeU64(v: number | bigint): this {
    this._chunks.push({ type: 'u64', value: BigInt(v), size: 8 });
    return this;
  }

  writeUUID(id: string): this {
    const buf = Buffer.from(id, 'hex');
    if (buf.length !== 16) throw new Error('Invalid UUID length');
    this._chunks.push({ type: 'raw', value: buf, size: 16 });
    return this;
  }

  writeString(s: string): this {
    const buf = Buffer.from(s, 'utf8');
    this.writeU32(buf.length);
    this._chunks.push({ type: 'raw', value: buf, size: buf.length });
    return this;
  }

  writeData(d: any): this {
    const buf = DataCodec.serialize(d);
    this._chunks.push({ type: 'raw', value: buf, size: buf.length });
    return this;
  }

  /**
   * Performs the actual assembly and dispatch.
   * Centralizes all performance optimizations like pre-allocation.
   */
  async send(): Promise<{ status: ResponseStatus; reader: ProtocolReader }> {
    const payloadBodyLen = this._chunks.reduce((acc, c) => acc + c.size, 0);
    const payloadLen = 1 + payloadBodyLen; // +1 for opcode
    const totalSize = 9 + payloadLen;

    // TODO OPTIMIZATION: Use a BufferPool to reuse large buffers and avoid GC pressure
    const buf = Buffer.allocUnsafe(totalSize);

    // 1. Header (9 bytes)
    buf[0] = this._frameType;
    const id = (this.client as any).nextId++;
    if ((this.client as any).nextId === 0) (this.client as any).nextId = 1;
    buf.writeUInt32BE(id, 1);
    buf.writeUInt32BE(payloadLen, 5);

    // 2. Payload starts with Opcode
    buf[9] = this.opcode;

    // 3. Chunks
    let offset = 10;
    for (const chunk of this._chunks) {
      switch (chunk.type) {
        case 'u8': buf[offset] = chunk.value; break;
        case 'u32': buf.writeUInt32BE(chunk.value, offset); break;
        case 'u64': buf.writeBigUInt64BE(chunk.value, offset); break;
        case 'raw': chunk.value.copy(buf, offset); break;
      }
      offset += chunk.size;
    }

    const res = await (this.client as any).dispatchRaw(id, buf);
    return {
      status: res.status,
      reader: new ProtocolReader(res.data)
    };
  }
}

export class NexoQueue {
  constructor(private client: NexoClient, public readonly name: string) { }

  async push(data: any, options: PushOptions = {}): Promise<void> {
    const res = await this.client.request(Opcode.Q_PUSH)
      .writeU8(options.priority || 0)
      .writeU64(options.delayMs || 0)
      .writeString(this.name)
      .writeData(data)
      .send();

    if (res.status === ResponseStatus.ERR) throw new Error(res.reader.readString());
  }

  subscribe(callback: (data: any) => Promise<void> | void): { stop: () => void } {
    let active = true;

    const loop = async () => {
      while (this.client.connected && active) {
        try {
          const res = await this.client.request(Opcode.Q_CONSUME)
            .writeString(this.name)
            .send();

          if (!active) break;
          if (res.status === ResponseStatus.ERR) throw new Error(res.reader.readString());

          if (res.status === ResponseStatus.Q_DATA) {
            const idHex = res.reader.readUUID();
            const data = res.reader.readData();

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
    return { stop: () => { active = false; } };
  }

  async ack(id: string): Promise<void> {
    const res = await this.client.request(Opcode.Q_ACK)
      .writeUUID(id)
      .writeString(this.name)
      .send();

    if (res.status === ResponseStatus.ERR) throw new Error(res.reader.readString());
  }
}

// --- RING BUFFER DECODER ---

class RingDecoder {
  private buf: Buffer;
  private head = 0;
  private tail = 0;

  constructor(size = 512 * 1024) {
    this.buf = Buffer.allocUnsafe(size);
  }

  push(chunk: Buffer): void {
    const needed = chunk.length;
    const available = this.buf.length - this.tail;

    if (available < needed) {
      const used = this.tail - this.head;
      if (used > 0) this.buf.copy(this.buf, 0, this.head, this.tail);
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
  private nextId = 1;
  private pending = new Map<number, PendingHandler>();

  constructor(options: NexoOptions = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 8080;
    this.socket = new net.Socket();
    this.socket.setNoDelay(true);
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

  /**
   * Starts a new fluid request.
   */
  request(opcode: Opcode): RequestBuilder {
    return new RequestBuilder(this, opcode);
  }

  async registerQueue(name: string, config: QueueConfig = {}): Promise<NexoQueue> {
    if (this.queues.has(name)) return this.queues.get(name)!;

    const res = await this.request(Opcode.Q_DECLARE)
      .writeU64(config.visibilityTimeoutMs ?? 30000)
      .writeU32(config.maxRetries ?? 5)
      .writeU64(config.ttlMs ?? 604800000)
      .writeU64(config.delayMs ?? 0)
      .writeString(name)
      .send();

    if (res.status === ResponseStatus.ERR) throw new Error(res.reader.readString());

    const queue = new NexoQueue(this, name);
    this.queues.set(name, queue);
    return queue;
  }

  public readonly kv = {
    set: async (key: string, value: any, ttlSeconds = 0): Promise<void> => {
      const res = await this.request(Opcode.KV_SET)
        .writeU64(ttlSeconds)
        .writeString(key)
        .writeData(value)
        .send();
      if (res.status === ResponseStatus.ERR) throw new Error(res.reader.readString());
    },

    get: async (key: string): Promise<any> => {
      const res = await this.request(Opcode.KV_GET)
        .writeString(key)
        .send();
      if (res.status === ResponseStatus.NULL) return null;
      if (res.status === ResponseStatus.DATA) return res.reader.readData();
      if (res.status === ResponseStatus.ERR) throw new Error(res.reader.readString());
      return null;
    },

    del: async (key: string): Promise<void> => {
      const res = await this.request(Opcode.KV_DEL)
        .writeString(key)
        .send();
      if (res.status === ResponseStatus.ERR) throw new Error(res.reader.readString());
    },
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
      this.pending.forEach(h => h.reject(err));
      this.pending.clear();
    });

    this.socket.on('close', () => {
      this.isConnected = false;
      this.pending.forEach(h => h.reject(new Error('Connection closed')));
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

  /**
   * Internal method called by RequestBuilder to dispatch the final buffer.
   */
  private async dispatchRaw(id: number, buffer: Buffer): Promise<{ status: ResponseStatus; data: Buffer }> {
    if (!this.isConnected) throw new Error('Client not connected');

    // TODO OPTIMIZATION: Implement OS-level batching (write merging) here
    // Currently we write immediately for simplicity and testability.
    this.socket.write(buffer);

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  disconnect(): void {
    this.socket.end();
    this.socket.destroy();
    this.isConnected = false;
  }
}

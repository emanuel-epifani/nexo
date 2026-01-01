import * as net from 'net';
import { logger } from './utils/logger';

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
  // Store KV: 0x02 - 0x05
  KV_SET = 0x02,
  KV_GET = 0x03,
  KV_DEL = 0x04,
  // Future: KV_INCR = 0x05
  // Future Store Hash: 0x06 - 0x09
  // Future Store List: 0x0A - 0x0D
  // Future Store Set: 0x0E - 0x0F
  // Queue: 0x10 - 0x1F
  Q_DECLARE = 0x10,
  Q_PUSH = 0x11,
  Q_CONSUME = 0x12,
  Q_ACK = 0x13,
  // Topic: 0x20 - 0x2F
  PUB = 0x21,
  SUB = 0x22,
  UNSUB = 0x23,
  // Stream: 0x30 - 0x3F
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

export interface PublishOptions {
  retain?: boolean;
}

export interface NexoOptions {
  host?: string;
  port?: number;
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
 * RING BUFFER DECODER: Efficient frame extraction.
 */
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

/**
 * NEXO CONNECTION: The "Engine" - handles all low-level networking and buffering.
 */
class NexoConnection {
  private socket: net.Socket;
  public isConnected = false;
  private decoder = new RingDecoder();
  private nextId = 1;
  private pending = new Map<number, { resolve: any, reject: any }>();
  private writeBuf = Buffer.allocUnsafe(64 * 1024);
  private writeOffset = 0;
  private flushScheduled = false;

  public onPush?: (payload: Buffer) => void;

  constructor(private host: string, private port: number) {
    this.socket = new net.Socket();
    this.socket.setNoDelay(true);
    this.setupListeners();
  }

  async connect(): Promise<void> {
    return new Promise((res, rej) => {
      this.socket.connect(this.port, this.host, () => {
        this.isConnected = true; res();
      });
      this.socket.once('error', rej);
    });
  }

  private setupListeners() {
    this.socket.on('data', (chunk) => {
      this.decoder.push(chunk);
      let frame;
      while ((frame = this.decoder.nextFrame())) {
        switch (frame.type) {
          case FrameType.RESPONSE: {
            const h = this.pending.get(frame.id);
            if (h) {
              this.pending.delete(frame.id);
              h.resolve({ status: frame.payload[0], data: frame.payload.subarray(1) });
            }
            break;
          }
          case FrameType.PUSH: {
            if (this.onPush) {
              this.onPush(frame.payload);
            }
            break;
          }
          case FrameType.PING:
            // TODO: Auto-Pong if needed
            break;
          case FrameType.ERROR:
             logger.error('Received Protocol Error frame');
             break;
          default:
             logger.warn(`Unknown frame type: ${frame.type}`);
        }
      }
    });
    const cleanup = (err: any) => {
      this.isConnected = false;
      this.pending.forEach(h => h.reject(err || new Error('Connection closed')));
      this.pending.clear();
    };
    this.socket.on('error', cleanup);
    this.socket.on('close', cleanup);
  }

  private flush = () => {
    this.flushScheduled = false;
    if (this.writeOffset === 0) return;
    this.socket.write(this.writeBuf.subarray(0, this.writeOffset));
    this.writeOffset = 0;
  };

  async dispatch(opcode: number, payloadLen: number, ops: any[]): Promise<{ status: ResponseStatus, data: Buffer }> {
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

/**
 * REQUEST BUILDER: Internal utility for fluent request construction.
 */
class RequestBuilder {
  private _ops: { type: number, val: any, size: number }[] = [];
  private _payloadSize = 1;
  private _opcode: Opcode = Opcode.DEBUG_ECHO;

  constructor(private conn: NexoConnection) { }

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
    const res = await this.conn.dispatch(this._opcode, this._payloadSize, this._ops);
    if (res.status === ResponseStatus.ERR) {
      const err = new ProtocolReader(res.data).readString();
      logger.error(`<- ERROR ${Opcode[this._opcode]} (${err})`);
      throw new Error(err);
    }
    return { status: res.status, reader: new ProtocolReader(res.data) };
  }
}

// ========================================
// STORE BROKER - Data Structures
// ========================================

/**
 * NEXO KV: Key-Value operations (store.kv.set/get/del)
 */
export class NexoKV<T = any> {
  constructor(private builder: RequestBuilder) { }

  async set(key: string, value: T, ttlSeconds = 0): Promise<void> {
    await this.builder.reset(Opcode.KV_SET)
      .writeU64(ttlSeconds)
      .writeString(key)
      .writeData(value)
      .send();
  }

  async get(key: string): Promise<T | null> {
    const res = await this.builder.reset(Opcode.KV_GET)
      .writeString(key)
      .send();
    return res.status === ResponseStatus.NULL ? null : res.reader.readData() as T;
  }

  async del(key: string): Promise<void> {
    await this.builder.reset(Opcode.KV_DEL)
      .writeString(key)
      .send();
  }

  // Future: incr(key: string, delta: number): Promise<number>
}

/**
 * NEXO HASH: Hash operations (store.hash("key").set/get)
 * Placeholder for future implementation
 */
export class NexoHash<T = any> {
  constructor(private builder: RequestBuilder, public readonly key: string) { }

  // Future implementation
  // async set(field: string, value: T): Promise<void>
  // async get(field: string): Promise<T | null>
  // async del(field: string): Promise<void>
  // async getAll(): Promise<Record<string, T>>
  // async incr(field: string, delta: number): Promise<number>
}

/**
 * NEXO LIST: List operations (store.list("key").push/pop)
 * Placeholder for future implementation
 */
export class NexoList<T = any> {
  constructor(private builder: RequestBuilder, public readonly key: string) { }

  // Future implementation
  // async push(value: T): Promise<number>
  // async pop(): Promise<T | null>
  // async shift(): Promise<T | null>
  // async range(start: number, end: number): Promise<T[]>
}

/**
 * NEXO SET: Set operations (store.set("key").add/has)
 * Placeholder for future implementation
 */
export class NexoSet<T = any> {
  constructor(private builder: RequestBuilder, public readonly key: string) { }

  // Future implementation
  // async add(member: T): Promise<boolean>
  // async has(member: T): Promise<boolean>
  // async remove(member: T): Promise<boolean>
  // async members(): Promise<T[]>
}

/**
 * NEXO STORE: Container for all data structure operations
 * Access via nexo.store.kv, nexo.store.hash(), etc.
 */
export class NexoStore {
  /** Key-Value operations */
  public readonly kv: NexoKV;

  constructor(private builder: RequestBuilder) {
    this.kv = new NexoKV(builder);
  }

  /** Hash operations for a specific key */
  hash<T = any>(key: string): NexoHash<T> {
    return new NexoHash<T>(this.builder, key);
  }

  /** List operations for a specific key */
  list<T = any>(key: string): NexoList<T> {
    return new NexoList<T>(this.builder, key);
  }

  /** Set operations for a specific key */
  set<T = any>(key: string): NexoSet<T> {
    return new NexoSet<T>(this.builder, key);
  }
}

// ========================================
// QUEUE BROKER
// ========================================

/**
 * NEXO QUEUE: Resource handle for Queue operations.
 */
export class NexoQueue<T = any> {
  private isDeclared = false;
  private declarePromise: Promise<void> | null = null;

  constructor(
    private builder: RequestBuilder,
    public readonly name: string,
    private config?: QueueConfig
  ) { }

  private async ensureDeclared(): Promise<void> {
    if (this.isDeclared) return;
    if (this.declarePromise) return this.declarePromise;

    if (!this.config) {
      this.isDeclared = true;
      return;
    }

    this.declarePromise = (async () => {
      try {
        await this.declare(this.config!);
      } finally {
        this.declarePromise = null;
      }
    })();

    return this.declarePromise;
  }

  async declare(config: QueueConfig = {}): Promise<this> {
    await this.builder.reset(Opcode.Q_DECLARE)
      .writeU64(config.visibilityTimeoutMs ?? 30000)
      .writeU32(config.maxRetries ?? 5)
      .writeU64(config.ttlMs ?? 604800000)
      .writeU64(config.delayMs ?? 0)
      .writeString(this.name)
      .send();
    this.isDeclared = true;
    return this;
  }

  async push(data: T, options: PushOptions = {}): Promise<void> {
    await this.ensureDeclared();
    await this.builder.reset(Opcode.Q_PUSH)
      .writeU8(options.priority || 0)
      .writeU64(options.delayMs || 0)
      .writeString(this.name)
      .writeData(data)
      .send();
  }

  subscribe(callback: (data: T) => Promise<void> | void): { stop: () => void } {
    let active = true;
    const loop = async () => {
      await this.ensureDeclared();
      while ((this.builder as any).conn.isConnected && active) {
        try {
          const res = await this.builder.reset(Opcode.Q_CONSUME).writeString(this.name).send();
          if (!active) break;
          if (res.status === ResponseStatus.Q_DATA) {
            const idHex = res.reader.readUUID();
            const data = res.reader.readData() as T;
            try {
              await callback(data);
              await this.ack(idHex);
            } catch (e) {
              if (active) logger.error(`Callback error in queue ${this.name}:`, e);
            }
          }
        } catch (err) {
          if (!(this.builder as any).conn.isConnected || !active) break;
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    };
    loop();
    return { stop: () => { active = false; } };
  }

  private async ack(id: string): Promise<void> {
    await this.builder.reset(Opcode.Q_ACK).writeUUID(id).writeString(this.name).send();
  }
}

// ========================================
// PUB-SUB BROKER
// ========================================

/**
 * NEXO PUBSUB: Publish/Subscribe operations
 */
export class NexoPubSub {
  private handlers = new Map<string, Array<(data: any) => void>>();

  constructor(private builder: RequestBuilder, conn: NexoConnection) {
    conn.onPush = (payload) => {
      // Parse PUSH payload: [TopicLen:4][Topic][Data]
      const reader = new ProtocolReader(payload);
      const topic = reader.readString();
      const data = reader.readData();
      
      this.dispatch(topic, data);
    };
  }

  async publish(topic: string, data: any, options?: PublishOptions): Promise<void> {
    let flags = 0;
    if (options?.retain) flags |= 0x01;

    await this.builder.reset(Opcode.PUB)
      .writeU8(flags)
      .writeString(topic)
      .writeData(data)
      .send();
  }

  async subscribe<T = any>(topic: string, callback: (data: T) => void): Promise<void> {
    // Register local handler immediately to catch retained messages that arrive before SUB-ACK
    if (!this.handlers.has(topic)) {
      this.handlers.set(topic, []);
    }
    this.handlers.get(topic)!.push(callback);

    // Send SUB command to server if this is the first handler (or we want to ensure subscription)
    // Note: In a real robust client, we might track 'isSubscribed' state separate from handlers.
    // Here we send SUB only if we just created the handler array (length was 0 before push? No, we just checked has).
    // Actually, simple check: if length is 1, it's the first one.
    if (this.handlers.get(topic)!.length === 1) {
        await this.builder.reset(Opcode.SUB).writeString(topic).send();
    }
  }

  async unsubscribe(topic: string): Promise<void> {
    if (this.handlers.has(topic)) {
      this.handlers.delete(topic);
      await this.builder.reset(Opcode.UNSUB).writeString(topic).send();
    }
  }

  private dispatch(topic: string, data: any) {
    // 1. Exact match
    const handlers = this.handlers.get(topic);
    if (handlers) {
        handlers.forEach(cb => {
            try { cb(data); } catch(e) { console.error("Topic handler error", e); }
        });
    }

    // 2. Wildcard matching
    for (const [pattern, cbs] of this.handlers.entries()) {
        if (pattern === topic) continue; // Already handled
        if (this.matches(pattern, topic)) {
             cbs.forEach(cb => {
                try { cb(data); } catch(e) { console.error("Topic handler error", e); }
            });
        }
    }
  }

  private matches(pattern: string, topic: string): boolean {
      const pParts = pattern.split('/');
      const tParts = topic.split('/');
      
      for(let i=0; i<pParts.length; i++) {
          const p = pParts[i];
          if (p === '#') return true;
          if (i >= tParts.length) return false;
          if (p !== '+' && p !== tParts[i]) return false;
      }
      return pParts.length === tParts.length;
  }
}


// ========================================
// NEXO CLIENT - Main Entry Point
// ========================================

/**
 * NEXO CLIENT: The public-facing SDK entrypoint.
 * 
 * @example
 * ```typescript
 * const nexo = await NexoClient.connect();
 * 
 * // Store operations
 * await nexo.store.kv.set("key", "value");
 * const value = await nexo.store.kv.get("key");
 * 
 * // Queue operations
 * const orders = nexo.queue("orders");
 * await orders.push({ item: "book" });
 * orders.subscribe(order => console.log(order));
 * 
 * // Topic operations
 * nexo.topic.subscribe("chat/room/1", msg => console.log(msg));
 * nexo.topic.publish("chat/room/1", "Hello World", { retain: true });
 * ```
 */
export class NexoClient {
  private conn: NexoConnection;
  private builder: RequestBuilder;
  private queues = new Map<string, NexoQueue<any>>();

  /** Store broker - access data structures (kv, hash, list, set) */
  public readonly store: NexoStore;

  /** PubSub broker - pub/sub operations */
  public readonly pubsub: NexoPubSub;

  constructor(options: NexoOptions = {}) {
    this.conn = new NexoConnection(options.host || '127.0.0.1', options.port || 8080);
    this.builder = new RequestBuilder(this.conn);
    this.store = new NexoStore(this.builder);
    this.pubsub = new NexoPubSub(this.builder, this.conn);
  }

  static async connect(options?: NexoOptions): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.conn.connect();
    return client;
  }

  public get connected(): boolean {
    return this.conn.isConnected;
  }

  public disconnect(): void {
    this.conn.disconnect();
  }

  /** Queue broker - get or create a queue by name */
  public queue<T = any>(name: string, config?: QueueConfig): NexoQueue<T> {
    let q = this.queues.get(name);
    if (!q) {
      q = new NexoQueue<T>(this.builder, name, config);
      this.queues.set(name, q);
    }
    return q as NexoQueue<T>;
  }

  /** @internal Debug utilities */
  public get debug() {
    return {
      echo: async (data: any): Promise<any> => {
        const res = await this.builder.reset(Opcode.DEBUG_ECHO).writeData(data).send();
        return res.reader.readData();
      }
    };
  }
}

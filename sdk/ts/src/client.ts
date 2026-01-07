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
}

export enum Opcode {
  DEBUG_ECHO = 0x00,
  // Store KV: 0x02 - 0x05
  KV_SET = 0x02,
  KV_GET = 0x03,
  KV_DEL = 0x04,
  // Future: KV_INCR = 0x05
  // Future Store Hash: 0x06 - 0x09
  // Queue: 0x10 - 0x1F
  Q_CREATE = 0x10,
  Q_PUSH = 0x11,
  Q_CONSUME = 0x12,
  Q_ACK = 0x13,
  // Topic: 0x20 - 0x2F
  PUB = 0x21,
  SUB = 0x22,
  UNSUB = 0x23,
  // Stream: 0x30 - 0x3F
  S_CREATE = 0x30,
  S_PUB = 0x31,
  S_FETCH = 0x32,
  S_JOIN = 0x33,
  S_COMMIT = 0x34,
}

enum DataType {
  RAW = 0x00,
  STRING = 0x01,
  JSON = 0x02,
}

export interface QueueConfig {
  visibilityTimeoutMs?: number;
  maxRetries?: number;
  ttlMs?: number;
  delayMs?: number;
  passive?: boolean;
}

export interface QueueSubscribeOptions {
  batchSize?: number;  // Default 50
  waitMs?: number;     // Default 20000 (20 seconds)
  concurrency?: number; // Default 1 (Serial)
}

export interface StreamConfig {
  // Reserved for future options (e.g., retention)
}

export interface StreamSubscribeOptions {
  batchSize?: number; // Default 100
}

export interface PushOptions {
  priority?: number;
  delayMs?: number;
}

export interface PublishOptions {
  retain?: boolean;
}

export interface StreamPublishOptions {
  key?: string;
}

export interface NexoOptions {
  host?: string;
  port?: number;
  requestTimeoutMs?: number;
}

class DataCodec {
  static serialize(data: any): Buffer {
    let type = DataType.RAW;
    let payload: Buffer;

    if (Buffer.isBuffer(data)) {
      type = DataType.RAW;
      payload = data;
    } else if (typeof data === 'string') {
      type = DataType.STRING;
      payload = Buffer.from(data, 'utf8');
    } else {
      type = DataType.JSON;
      // null/undefined → JSON "null" string, not empty buffer
      payload = Buffer.from(JSON.stringify(data ?? null), 'utf8');
    }

    // [Type:1][Payload...]
    const buf = Buffer.allocUnsafe(1 + payload.length);
    buf[0] = type;
    payload.copy(buf, 1);
    return buf;
  }

  static deserialize(buf: Buffer): any {
    if (buf.length === 0) return null;

    const type = buf[0];
    const content = buf.subarray(1);

    switch (type) {
      case DataType.JSON:
        if (content.length === 0) return null;
        return JSON.parse(content.toString('utf8'));
      case DataType.STRING:
        return content.toString('utf8');
      case DataType.RAW:
      default:
        return content;
    }
  }
}

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
  readBuffer(len: number): Buffer {
    const b = this.buffer.subarray(this.offset, this.offset + len);
    this.offset += len;
    return b;
  }
  readData(): any { return DataCodec.deserialize(this.buffer.subarray(this.offset)); }
}

class RingDecoder {
  private buf: Buffer;
  private head = 0;
  private tail = 0;
  private readonly MAX_FRAME_SIZE = 512 * 1024 * 1024; // 512MB Hard Limit

  constructor(size = 512 * 1024) { this.buf = Buffer.allocUnsafe(size); }

  push(chunk: Buffer): void {
    const needed = chunk.length;
    const used = this.tail - this.head;
    const freeSpace = this.buf.length - this.tail;

    // 1. If there is enough space at the end, write directly
    if (freeSpace >= needed) {
      chunk.copy(this.buf, this.tail);
      this.tail += needed;
      return;
    }

    // 2. If compacting would free enough space
    if (this.buf.length - used >= needed) {
      this.buf.copy(this.buf, 0, this.head, this.tail);
      this.tail = used;
      this.head = 0;
      chunk.copy(this.buf, this.tail);
      this.tail += needed;
      return;
    }

    // 3. Not enough space even after compact -> RESIZE
    let newSize = this.buf.length * 2;
    while (newSize - used < needed) {
      newSize *= 2;
      if (newSize > this.MAX_FRAME_SIZE) {
        throw new Error(`Frame too large: limit is ${this.MAX_FRAME_SIZE / 1024 / 1024}MB`);
      }
    }

    const newBuf = Buffer.allocUnsafe(newSize);
    this.buf.copy(newBuf, 0, this.head, this.tail);

    this.buf = newBuf;
    this.tail = used;
    this.head = 0;

    chunk.copy(this.buf, this.tail);
    this.tail += needed;
  }

  nextFrame(): { type: number; id: number; payload: Buffer } | null {
    if (this.tail - this.head < 9) return null;
    const type = this.buf[this.head];
    const id = this.buf.readUInt32BE(this.head + 1);
    const payloadLen = this.buf.readUInt32BE(this.head + 5);
    if (this.tail - this.head < 9 + payloadLen) return null;
    // CRITICAL: Use slice() to create a COPY of the payload
    const payload = this.buf.slice(this.head + 9, this.head + 9 + payloadLen);
    this.head += 9 + payloadLen;
    if (this.head === this.tail) { this.head = 0; this.tail = 0; }
    return { type, id, payload };
  }
}

class NexoConnection {
  private socket: net.Socket;
  public isConnected = false;
  private decoder = new RingDecoder();
  private nextId = 1;
  private pending = new Map<number, { resolve: any, reject: any, ts: number }>();
  private timeoutTimer?: NodeJS.Timeout;
  private readonly requestTimeoutMs: number;

  // Micro-batching: collect buffers and flush on nextTick
  private writeQueue: Buffer[] = [];
  private flushScheduled = false;

  public onPush?: (payload: Buffer) => void;

  constructor(private host: string, private port: number, options: NexoOptions = {}) {
    this.socket = new net.Socket();
    this.socket.setNoDelay(true);
    this.requestTimeoutMs = options.requestTimeoutMs ?? 30000;
    this.setupListeners();
    this.startTimeoutLoop();
  }

  async connect(): Promise<void> {
    return new Promise((res, rej) => {
      this.socket.connect(this.port, this.host, () => {
        this.isConnected = true; res();
      });
      this.socket.once('error', rej);
    });
  }

  private startTimeoutLoop() {
    this.timeoutTimer = setInterval(() => {
      const now = Date.now();
      for (const [id, req] of this.pending) {
        if (now - req.ts > this.requestTimeoutMs) {
          this.pending.delete(id);
          req.reject(new Error(`Request timeout after ${this.requestTimeoutMs}ms`));
        }
      }
    }, 1000);
    this.timeoutTimer.unref();
  }

  private setupListeners() {
    this.socket.on('data', (chunk) => {
      try {
        this.decoder.push(chunk);
        let frame;
        while ((frame = this.decoder.nextFrame())) {
          switch (frame.type) {
            case FrameType.RESPONSE: {
              const h = this.pending.get(frame.id);
              if (h) {
                this.pending.delete(frame.id);
                h.resolve({ status: frame.payload[0], data: frame.payload.slice(1) });
              }
              break;
            }
            case FrameType.PUSH: {
              if (this.onPush) this.onPush(frame.payload);
              break;
            }
            case FrameType.PING:
              break;
            case FrameType.ERROR:
              logger.error('Received Protocol Error frame');
              break;
            default:
              logger.warn(`Unknown frame type: ${frame.type}`);
          }
        }
      } catch (err) {
        logger.error('Decoder error', err);
        this.socket.destroy();
      }
    });

    const cleanup = (err: any) => {
      this.isConnected = false;
      this.pending.forEach(h => h.reject(err || new Error('Connection closed')));
      this.pending.clear();
      if (this.timeoutTimer) clearInterval(this.timeoutTimer);
    };
    this.socket.on('error', cleanup);
    this.socket.on('close', cleanup);
  }

  private flush = () => {
    this.flushScheduled = false;
    if (this.writeQueue.length === 0) return;

    const combined = Buffer.concat(this.writeQueue);
    this.writeQueue.length = 0;
    this.socket.write(combined);
  };

  dispatch(
    opcode: number,
    payloadLen: number,
    ops: { type: number, val: any, size: number }[]
  ): Promise<{ status: ResponseStatus, data: Buffer }> {
    const id = this.nextId++;
    if (this.nextId === 0) this.nextId = 1;

    const buf = Buffer.allocUnsafe(9 + payloadLen);

    // Header: [Type:1][ID:4][PayloadLen:4]
    buf[0] = FrameType.REQUEST;
    buf.writeUInt32BE(id, 1);
    buf.writeUInt32BE(payloadLen, 5);

    // Payload: [Opcode:1][...fields]
    buf[9] = opcode;
    let off = 10;
    for (const op of ops) {
      switch (op.type) {
        case 1: buf[off++] = op.val; break;
        case 2: buf.writeUInt32BE(op.val, off); off += 4; break;
        case 3: buf.writeBigUInt64BE(op.val, off); off += 8; break;
        case 4: (op.val as Buffer).copy(buf, off); off += op.size; break;
        case 5:
          buf.writeUInt32BE(op.val.length, off);
          (op.val as Buffer).copy(buf, off + 4);
          off += 4 + op.val.length;
          break;
      }
    }

    // Queue buffer and schedule flush on nextTick (~0.1ms vs setImmediate ~4ms)
    this.writeQueue.push(buf);
    if (!this.flushScheduled) {
      this.flushScheduled = true;
      process.nextTick(this.flush);
    }

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject, ts: Date.now() });
    });
  }

  disconnect() {
    this.flush();
    this.socket.destroy();
    this.isConnected = false;
    if (this.timeoutTimer) clearInterval(this.timeoutTimer);
  }
}

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
    const encoded = Buffer.from(s, 'utf8');
    this._ops.push({ type: 5, val: encoded, size: 4 + encoded.length });
    this._payloadSize += 4 + encoded.length;
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
      // Only log errors that are not expected protocol validations
      if (!err.includes('FENCED') && !err.includes('not found')) {
        logger.error(`<- ERROR ${Opcode[this._opcode]} (${err})`);
      }
      throw new Error(err);
    }
    return { status: res.status, reader: new ProtocolReader(res.data) };
  }
}

// ========================================
// STORE BROKER - Data Structures
// ========================================

export class NexoKV {
  constructor(private builder: RequestBuilder) { }

  async set(key: string, value: any, ttlSeconds = 0): Promise<void> {
    await this.builder.reset(Opcode.KV_SET)
      .writeU64(ttlSeconds)
      .writeString(key)
      .writeData(value)
      .send();
  }

  async get<T = any>(key: string): Promise<T | null> {
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
}

export class NexoHash<T = any> {
  constructor(private builder: RequestBuilder, public readonly key: string) { }
}

export class NexoStore {
  public readonly kv: NexoKV;

  constructor(private builder: RequestBuilder) {
    this.kv = new NexoKV(builder);
  }

  hash<T = any>(key: string): NexoHash<T> {
    return new NexoHash<T>(this.builder, key);
  }
}

// ========================================
// QUEUE BROKER
// ========================================

export class NexoQueue<T = any> {
  private isDeclared = false;
  private declarePromise: Promise<void> | null = null;

  constructor(
    private builder: RequestBuilder,
    public readonly name: string,
    private config?: QueueConfig
  ) { }

  async create(config: QueueConfig = {}): Promise<this> {
    const flags = config.passive ? 0x01 : 0x00;

    await this.builder.reset(Opcode.Q_CREATE)
      .writeU8(flags)
      .writeU64(config.visibilityTimeoutMs ?? 0)
      .writeU32(config.maxRetries ?? 0)
      .writeU64(config.ttlMs ?? 0)
      .writeU64(config.delayMs ?? 0)
      .writeString(this.name)
      .send();
    return this;
  }

  async push(data: T, options: PushOptions = {}): Promise<void> {
    await this.builder.reset(Opcode.Q_PUSH)
      .writeU8(options.priority || 0)
      .writeU64(options.delayMs || 0)
      .writeString(this.name)
      .writeData(data)
      .send();
  }

  async subscribe(callback: (data: T) => Promise<void> | void, options: QueueSubscribeOptions = {}): Promise<{ stop: () => void }> {
    const batchSize = options.batchSize ?? 50;
    const waitMs = options.waitMs ?? 20000;
    const concurrency = options.concurrency ?? 1;

    // 1. Check if queue exists (Fail-Fast)
    // We expect declare_queue in passive mode to throw ERR if queue not found.
    // .send() throws if ERR is returned.
    await this.create({ passive: true });

    let active = true;

    const loop = async () => {
      while (active && (this.builder as any).conn.isConnected) {
        try {
          // Request batch of messages
          const res = await this.builder.reset(Opcode.Q_CONSUME)
            .writeU32(batchSize)
            .writeU64(waitMs)
            .writeString(this.name)
            .send();

          if (!active) return;

          // Parse response: [Count:4][Msg1][Msg2]...
          // Each Msg: [UUID:16][PayloadLen:4][Payload]
          const count = res.reader.readU32();

          if (count === 0) {
            // Timeout with no messages, continue polling
            continue;
          }

          // 1. Deserializza tutto subito (CPU Bound)
          const messages: { id: string; data: T }[] = [];
          for (let i = 0; i < count; i++) {
            const idHex = res.reader.readUUID();
            const payloadLen = res.reader.readU32();
            const payloadBuf = res.reader.readBuffer(payloadLen);
            const data = DataCodec.deserialize(payloadBuf) as T;
            messages.push({ id: idHex, data });
          }

          // 2. Processa i messaggi (IO Bound)
          if (concurrency === 1) {
            // --- MODALITÀ SERIALE (Default) ---
            // Garantisce l'ordine di completamento
            for (const msg of messages) {
              if (!active) break;
              try {
                await callback(msg.data);
                await this.ack(msg.id);
              } catch (e) {
                logger.error(`Callback error in queue ${this.name}:`, e);
              }
            }
          } else {
            // --- MODALITÀ CONCORRENTE ---
            // Massimizza il throughput
            const tasks = messages.map(msg => async () => {
              if (!active) return;
              try {
                await callback(msg.data);
                await this.ack(msg.id);
              } catch (e) {
                logger.error(`Callback error in queue ${this.name}:`, e);
              }
            });

            // Esegui a blocchi (Chunking) per rispettare il limite di concorrenza
            for (let i = 0; i < tasks.length; i += concurrency) {
              if (!active) break;
              const chunk = tasks.slice(i, i + concurrency);
              await Promise.all(chunk.map(t => t()));
            }
          }

        } catch (e) {
          if (!active || !(this.builder as any).conn.isConnected) {
            // Fail-Fast: Connection lost or stopped, exit loop immediately
            return;
          }
          logger.error(`Queue consume error in ${this.name}:`, e);
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    };

    // Start the loop
    loop();

    return {
      stop: () => {
        active = false;
      }
    };
  }

  private async ack(id: string): Promise<void> {
    await this.builder.reset(Opcode.Q_ACK).writeUUID(id).writeString(this.name).send();
  }
}

class NexoPubSub {
  private handlers = new Map<string, Array<(data: any) => void>>();

  constructor(private builder: RequestBuilder, conn: NexoConnection) {
    conn.onPush = (payload) => {
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
    if (!this.handlers.has(topic)) {
      this.handlers.set(topic, []);
    }
    this.handlers.get(topic)!.push(callback);

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
    const handlers = this.handlers.get(topic);
    if (handlers) {
      handlers.forEach(cb => {
        try { cb(data); } catch (e) { console.error("Topic handler error", e); }
      });
    }

    for (const [pattern, cbs] of this.handlers.entries()) {
      if (pattern === topic) continue;
      if (this.matches(pattern, topic)) {
        cbs.forEach(cb => {
          try { cb(data); } catch (e) { console.error("Topic handler error", e); }
        });
      }
    }
  }

  private matches(pattern: string, topic: string): boolean {
    const pParts = pattern.split('/');
    const tParts = topic.split('/');

    for (let i = 0; i < pParts.length; i++) {
      const p = pParts[i];
      if (p === '#') return true;
      if (i >= tParts.length) return false;
      if (p !== '+' && p !== tParts[i]) return false;
    }
    return pParts.length === tParts.length;
  }
}

// ========================================
// STREAM BROKER (Simplified - No Partitions)
// ========================================

export class NexoStream<T = any> {
  private nextOffset = BigInt(0);
  private active = false;

  constructor(
    private builder: RequestBuilder,
    public readonly name: string,
    public readonly consumerGroup?: string
  ) { }

  async create(_config: StreamConfig = {}): Promise<this> {
    await this.builder.reset(Opcode.S_CREATE)
      .writeString(this.name)
      .send();
    return this;
  }

  async publish(data: T, options?: StreamPublishOptions): Promise<void> {
    await this.builder.reset(Opcode.S_PUB)
      .writeString(options?.key ?? "")
      .writeString(this.name)
      .writeData(data)
      .send();
  }

  async subscribe(callback: (data: T) => Promise<void> | void, options: StreamSubscribeOptions = {}): Promise<{ stop: () => void }> {
    if (!this.consumerGroup) {
      throw new Error("Consumer Group is required for subscription. Use nexo.stream(name, group) to create a consumer handle.");
    }

    // Stop any previous subscription
    this.active = false;
    await new Promise(r => setTimeout(r, 10)); // Let previous loop exit
    this.active = true;

    const batchSize = options.batchSize || 100;

    // Join group and get starting offset
    const res = await this.builder.reset(Opcode.S_JOIN)
      .writeString(this.consumerGroup)
      .writeString(this.name)
      .send();

    // New simplified response: just [StartOffset:8]
    this.nextOffset = res.reader.readU64();

    // Start single polling loop
    this.pollLoop(callback, batchSize);

    return {
      stop: () => { this.active = false; }
    };
  }

  private async pollLoop(callback: (data: T) => Promise<void> | void, batchSize: number) {
    while ((this.builder as any).conn.isConnected && this.active) {
      try {
        const res = await this.builder.reset(Opcode.S_FETCH)
          .writeString(this.name)
          .writeU64(this.nextOffset)
          .writeU32(batchSize)
          .send();

        const numMsgs = res.reader.readU32();

        if (numMsgs === 0) {
          await new Promise(r => setTimeout(r, 100));
          continue;
        }

        let lastMsgOffset = this.nextOffset;
        let batchError = false;

        for (let i = 0; i < numMsgs; i++) {
          const msgOffset = res.reader.readU64();

          // Only update lastMsgOffset if we succeed later
          // lastMsgOffset = msgOffset; <-- REMOVED

          res.reader.readU64(); // timestamp (skip)
          const keyLen = res.reader.readU32();
          if (keyLen > 0) res.reader.readBuffer(keyLen); // skip key

          const payloadLen = res.reader.readU32();
          const payloadBuf = res.reader.readBuffer(payloadLen);
          const data = DataCodec.deserialize(payloadBuf);

          try {
            await callback(data);
            lastMsgOffset = msgOffset; // Success: mark this offset as done
          } catch (e) {
            logger.error(`Stream callback error at offset ${msgOffset}. Retrying in 1s and set ${this.nextOffset} as offset on remote broker.`, e);

            // Strategy: Stop, Commit progress up to here, Backoff, Retry
            this.nextOffset = msgOffset; // Set next fetch to start from this failed message
            batchError = true;

            if (this.consumerGroup) {
              await this.builder.reset(Opcode.S_COMMIT)
                .writeString(this.consumerGroup)
                .writeString(this.name)
                .writeU64(this.nextOffset)
                .send();
            }

            await new Promise(r => setTimeout(r, 1000));
            break; // Exit batch loop
          }
        }

        // Commit after processing batch (only if no error occurred)
        if (!batchError && this.consumerGroup) {
          const commitOffset = lastMsgOffset + BigInt(1);
          this.nextOffset = commitOffset;

          await this.builder.reset(Opcode.S_COMMIT)
            .writeString(this.consumerGroup)
            .writeString(this.name)
            .writeU64(commitOffset)
            .send();
        }

      } catch (e) {
        if (!this.active || !(this.builder as any).conn.isConnected) {
          // Fail-Fast: Connection lost or stopped, exit loop immediately
          return;
        }
        logger.error("Stream poll error", e);
        await new Promise(r => setTimeout(r, 1000));
      }
    }
  }
}

// ========================================
// TOPIC BROKER
// ========================================

export class NexoTopic<T = any> {
  constructor(
    private broker: NexoPubSub,
    public readonly name: string
  ) { }

  async publish(data: T, options?: PublishOptions): Promise<void> {
    return this.broker.publish(this.name, data, options);
  }

  async subscribe(callback: (data: T) => void): Promise<void> {
    return this.broker.subscribe<T>(this.name, callback);
  }

  async unsubscribe(): Promise<void> {
    return this.broker.unsubscribe(this.name);
  }
}

// ========================================
// NEXO CLIENT - Main Entry Point
// ========================================

export class NexoClient {
  private conn: NexoConnection;
  private builder: RequestBuilder;
  private queues = new Map<string, NexoQueue<any>>();
  private streams = new Map<string, NexoStream<any>>(); //KAFKA style
  private topics = new Map<string, NexoTopic<any>>(); //MQTT style

  /** Store broker - access data structures (kv, hash) */
  public readonly store: NexoStore;

  /** PubSub broker - pub/sub operations */
  private readonly pubsubBroker: NexoPubSub;

  constructor(options: NexoOptions = {}) {
    this.conn = new NexoConnection(options.host || '127.0.0.1', options.port || 8080, options);
    this.builder = new RequestBuilder(this.conn);
    this.store = new NexoStore(this.builder);
    this.pubsubBroker = new NexoPubSub(this.builder, this.conn);
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

  /** Stream broker - get or create a stream handle */
  public stream<T = any>(name: string, consumerGroup?: string): NexoStream<T> {
    const key = consumerGroup ? `${name}:${consumerGroup}` : name;
    let s = this.streams.get(key);
    if (!s) {
      s = new NexoStream<T>(this.builder, name, consumerGroup);
      this.streams.set(key, s);
    }
    return s as NexoStream<T>;
  }

  /** PubSub broker - get or create a typed topic handle */
  public pubsub<T = any>(name: string): NexoTopic<T> {
    let t = this.topics.get(name);
    if (!t) {
      t = new NexoTopic<T>(this.pubsubBroker, name);
      this.topics.set(name, t);
    }
    return t as NexoTopic<T>;
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

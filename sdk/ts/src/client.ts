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
  // Reserved for future options
}

export interface NexoOptions {
  host?: string;
  port?: number;
  requestTimeoutMs?: number;
}

// ========================================
// FRAME CODEC - Centralized Protocol Logic
// ========================================

class Cursor {
  constructor(public buf: Buffer, public offset = 0) { }

  readU8(): number { return this.buf.readUInt8(this.offset++); }
  readU32(): number { const v = this.buf.readUInt32BE(this.offset); this.offset += 4; return v; }
  readU64(): bigint { const v = this.buf.readBigUInt64BE(this.offset); this.offset += 8; return v; }
  
  readBuffer(len: number): Buffer {
    const v = this.buf.subarray(this.offset, this.offset + len);
    this.offset += len;
    return v;
  }

  readString(): string {
    const len = this.readU32();
    return this.readBuffer(len).toString('utf8');
  }

  readUUID(): string {
    return this.readBuffer(16).toString('hex');
  }
}

class FrameCodec {
  // --- ENCODERS (Data -> Buffer) ---

  static u8(v: number): Buffer {
    const b = Buffer.allocUnsafe(1);
    b.writeUInt8(v, 0);
    return b;
  }

  static u32(v: number): Buffer {
    const b = Buffer.allocUnsafe(4);
    b.writeUInt32BE(v, 0);
    return b;
  }

  static u64(v: number | bigint): Buffer {
    const b = Buffer.allocUnsafe(8);
    b.writeBigUInt64BE(BigInt(v), 0);
    return b;
  }

  static string(s: string): Buffer {
    const strBuf = Buffer.from(s, 'utf8');
    // [Len:4][Bytes...]
    return Buffer.concat([this.u32(strBuf.length), strBuf]);
  }

  static uuid(hex: string): Buffer {
    return Buffer.from(hex, 'hex');
  }

  static any(data: any): Buffer {
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
      payload = Buffer.from(JSON.stringify(data ?? null), 'utf8');
    }

    // [Type:1][Payload...]
    return Buffer.concat([this.u8(type), payload]);
  }

  static packRequest(id: number, opcode: Opcode, ...parts: Buffer[]): Buffer {
    const payload = Buffer.concat([this.u8(opcode), ...parts]);
    const header = Buffer.allocUnsafe(9);
    
    // Header: [Type:1][ID:4][PayloadLen:4]
    header.writeUInt8(FrameType.REQUEST, 0);
    header.writeUInt32BE(id, 1);
    header.writeUInt32BE(payload.length, 5);

    return Buffer.concat([header, payload]);
  }

  // --- DECODERS (Buffer -> Data) ---

  static decodeAny(cursor: Cursor): any {
    const type = cursor.readU8();
    const content = cursor.buf.subarray(cursor.offset); // Read until end
    // Note: In strict framing, we might want to pass length, but here 'Any' is usually trailing

    switch (type) {
      case DataType.JSON: return content.length ? JSON.parse(content.toString('utf8')) : null;
      case DataType.STRING: return content.toString('utf8');
      case DataType.RAW: default: return content;
    }
  }
}

// ========================================
// CONNECTION MANAGER
// ========================================

class NexoConnection {
  private socket: net.Socket;
  public isConnected = false;
  private nextId = 1;
  private pending = new Map<number, (res: { status: number, data: Buffer }) => void>();
  private requestTimeoutMs: number;
  
  public onPush?: (topic: string, data: any) => void;

  // Simple Buffering
  private buffer: Buffer = Buffer.alloc(0);

  constructor(private host: string, private port: number, options: NexoOptions = {}) {
    this.socket = new net.Socket();
    this.socket.setNoDelay(true); // Low latency
    this.requestTimeoutMs = options.requestTimeoutMs ?? 30000;
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
      // 1. Easy Append
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this.processBuffer();
    });

    const cleanup = (err: any) => {
      this.isConnected = false;
      this.pending.clear();
    };

    this.socket.on('error', (err) => logger.error("Socket error", err));
    this.socket.on('close', cleanup);
  }

  private processBuffer() {
    while (true) {
      // Need at least header (9 bytes)
      if (this.buffer.length < 9) break;

      const payloadLen = this.buffer.readUInt32BE(5);
      const totalFrameLen = 9 + payloadLen;

      if (this.buffer.length < totalFrameLen) break;

      // 2. Slice Frame
      const frame = this.buffer.subarray(0, totalFrameLen);
      this.buffer = this.buffer.subarray(totalFrameLen); // Advance buffer

      this.handleFrame(frame);
    }
  }

  private handleFrame(frame: Buffer) {
    const cursor = new Cursor(frame);
    const type = cursor.readU8();
    const id = cursor.readU32();
    cursor.readU32(); // Skip payloadLen

    const payload = cursor.buf.subarray(cursor.offset);

    switch (type) {
      case FrameType.RESPONSE: {
        const resolve = this.pending.get(id);
        if (resolve) {
          this.pending.delete(id);
          // Payload: [Status:1][Data...]
          resolve({ status: payload[0], data: payload.subarray(1) });
        }
        break;
      }
      case FrameType.PUSH: {
        if (this.onPush) {
          const pushCursor = new Cursor(payload);
          const topic = pushCursor.readString();
          const data = FrameCodec.decodeAny(pushCursor);
          this.onPush(topic, data);
        }
        break;
      }
      case FrameType.ERROR:
        logger.error('Received Protocol Error');
        break;
      case FrameType.PING:
        // Optional: Send PONG
        break;
    }
  }

  send(opcode: Opcode, ...args: Buffer[]): Promise<{ status: ResponseStatus, cursor: Cursor }> {
    const id = this.nextId++;
    if (this.nextId === 0) this.nextId = 1;

    const packet = FrameCodec.packRequest(id, opcode, ...args);

    return new Promise((resolve, reject) => {
      // 1. Setup Timeout
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Request timeout after ${this.requestTimeoutMs}ms`));
      }, this.requestTimeoutMs);

      // 2. Register Callback
      this.pending.set(id, (res) => {
        clearTimeout(timer);
        if (res.status === ResponseStatus.ERR) {
          const errCursor = new Cursor(res.data);
          const errMsg = errCursor.readString();
          // Silence common expected errors
          if (!errMsg.includes('FENCED') && !errMsg.includes('REBALANCE') && !errMsg.includes('not found')) {
            logger.error(`<- ERROR ${Opcode[opcode]} (${errMsg})`);
          }
          reject(new Error(errMsg));
        } else {
          resolve({ status: res.status, cursor: new Cursor(res.data) });
        }
      });

      // 3. Send
      if (!this.isConnected) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(new Error("Client not connected"));
        return;
      }
      this.socket.write(packet);
    });
  }

  disconnect() {
    this.socket.destroy();
    this.isConnected = false;
  }
}

// ========================================
// STORE BROKER
// ========================================

export class NexoKV {
  constructor(private conn: NexoConnection) { }

  async set(key: string, value: any, ttlSeconds = 0): Promise<void> {
    await this.conn.send(
      Opcode.KV_SET,
      FrameCodec.u64(ttlSeconds),
      FrameCodec.string(key),
      FrameCodec.any(value)
    );
  }

  async get<T = any>(key: string): Promise<T | null> {
    const res = await this.conn.send(Opcode.KV_GET, FrameCodec.string(key));
    if (res.status === ResponseStatus.NULL) return null;
    return FrameCodec.decodeAny(res.cursor);
  }

  async del(key: string): Promise<void> {
    await this.conn.send(Opcode.KV_DEL, FrameCodec.string(key));
  }
}

export class NexoStore {
  public readonly kv: NexoKV;
  constructor(conn: NexoConnection) {
    this.kv = new NexoKV(conn);
  }
}

// ========================================
// QUEUE BROKER
// ========================================

export class NexoQueue<T = any> {
  private isSubscribed = false;

  constructor(
    private conn: NexoConnection,
    public readonly name: string,
    private config?: QueueConfig
  ) { }

  async create(config: QueueConfig = {}): Promise<this> {
    const flags = config.passive ? 0x01 : 0x00;
    await this.conn.send(
        Opcode.Q_CREATE,
        FrameCodec.u8(flags),
        FrameCodec.u64(config.visibilityTimeoutMs ?? 0),
        FrameCodec.u32(config.maxRetries ?? 0),
        FrameCodec.u64(config.ttlMs ?? 0),
        FrameCodec.u64(config.delayMs ?? 0),
        FrameCodec.string(this.name)
    );
    return this;
  }

  async push(data: T, options: PushOptions = {}): Promise<void> {
    await this.conn.send(
        Opcode.Q_PUSH,
        FrameCodec.u8(options.priority || 0),
        FrameCodec.u64(options.delayMs || 0),
        FrameCodec.string(this.name),
        FrameCodec.any(data)
    );
  }

  async subscribe(callback: (data: T) => Promise<void> | void, options: QueueSubscribeOptions = {}): Promise<{ stop: () => void }> {
    if (this.isSubscribed) throw new Error(`Queue '${this.name}' already subscribed.`);
    this.isSubscribed = true;

    const batchSize = options.batchSize ?? 50;
    const waitMs = options.waitMs ?? 20000;
    const concurrency = options.concurrency ?? 1;

    try {
      await this.create({ passive: true });
    } catch (e) {
      this.isSubscribed = false;
      throw e;
    }

    let active = true;

    const loop = async () => {
      while (active && this.conn.isConnected) {
        try {
          const res = await this.conn.send(
              Opcode.Q_CONSUME,
              FrameCodec.u32(batchSize),
              FrameCodec.u64(waitMs),
              FrameCodec.string(this.name)
          );

          const count = res.cursor.readU32();
          if (count === 0) continue;

          const messages: { id: string; data: T }[] = [];
          for (let i = 0; i < count; i++) {
            const idHex = res.cursor.readUUID();
            const payloadLen = res.cursor.readU32();
            const payloadBuf = res.cursor.readBuffer(payloadLen);
            const data = FrameCodec.decodeAny(new Cursor(payloadBuf));
            messages.push({ id: idHex, data });
          }

          await runConcurrent(messages, concurrency, async (msg) => {
            if (!active) return;
            try {
              await callback(msg.data);
              await this.ack(msg.id);
            } catch (e) {
                if (!this.conn.isConnected) return;
                logger.error(`Callback error in queue ${this.name}:`, e);
            }
          });

        } catch (e) {
          if (!active || !this.conn.isConnected) return;
          logger.error(`Queue consume error in ${this.name}:`, e);
          await new Promise(r => setTimeout(r, 1000));
        }
      }
      this.isSubscribed = false;
    };

    loop();

    return { stop: () => { active = false; } };
  }

  private async ack(id: string): Promise<void> {
    await this.conn.send(Opcode.Q_ACK, FrameCodec.uuid(id), FrameCodec.string(this.name));
  }
}

// ========================================
// PUBSUB BROKER
// ========================================

class NexoPubSub {
  private handlers = new Map<string, Array<(data: any) => void>>();

  constructor(private conn: NexoConnection) {
    conn.onPush = (topic, data) => this.dispatch(topic, data);
  }

  async publish(topic: string, data: any, options?: PublishOptions): Promise<void> {
    const flags = options?.retain ? 0x01 : 0x00;
    await this.conn.send(Opcode.PUB, FrameCodec.u8(flags), FrameCodec.string(topic), FrameCodec.any(data));
  }

  async subscribe(topic: string, callback: (data: any) => void): Promise<void> {
    if (!this.handlers.has(topic)) this.handlers.set(topic, []);
    this.handlers.get(topic)!.push(callback);

    if (this.handlers.get(topic)!.length === 1) {
      await this.conn.send(Opcode.SUB, FrameCodec.string(topic));
    }
  }

  async unsubscribe(topic: string): Promise<void> {
    if (this.handlers.has(topic)) {
      this.handlers.delete(topic);
      await this.conn.send(Opcode.UNSUB, FrameCodec.string(topic));
    }
  }

  private dispatch(topic: string, data: any) {
    this.handlers.get(topic)?.forEach(cb => { try { cb(data); } catch (e) { console.error(e); } });
    for (const [pattern, cbs] of this.handlers) {
      if (pattern === topic) continue;
      if (this.matches(pattern, topic)) {
        cbs.forEach(cb => { try { cb(data); } catch (e) { console.error(e); } });
      }
    }
  }

  private matches(pattern: string, topic: string): boolean {
    const pParts = pattern.split('/');
    const tParts = topic.split('/');
    for (let i = 0; i < pParts.length; i++) {
      if (pParts[i] === '#') return true;
      if (i >= tParts.length || (pParts[i] !== '+' && pParts[i] !== tParts[i])) return false;
    }
    return pParts.length === tParts.length;
  }
}

// ========================================
// STREAM BROKER (Rebalancing Aware V1)
// ========================================

export class NexoStream<T = any> {
  private active = false;
  private isSubscribed = false;

  // Rebalancing State
  private generationId = BigInt(0);
  private assignedPartitions: number[] = [];
  private offsets = new Map<number, bigint>(); // Partition -> NextOffset

  constructor(
    private conn: NexoConnection,
    public readonly name: string,
    public readonly consumerGroup?: string
  ) {
  }

  async create(_config: StreamConfig = {}): Promise<this> {
    await this.conn.send(Opcode.S_CREATE, FrameCodec.string(this.name));
    return this;
  }

  async publish(data: T, _options?: StreamPublishOptions): Promise<void> {
    await this.conn.send(Opcode.S_PUB, FrameCodec.string(this.name), FrameCodec.any(data));
  }

  async subscribe(callback: (data: T) => Promise<void> | void, options: StreamSubscribeOptions = {}): Promise<{ stop: () => void }> {
    if (this.isSubscribed) throw new Error("Stream already subscribed.");
    if (!this.consumerGroup) throw new Error("Consumer Group required.");

    this.isSubscribed = true;
    this.active = false;
    await new Promise(r => setTimeout(r, 10)); // Yield
    this.active = true;

    // Start Main Loop (Manages Joins and Rebalances)
    this.mainLoop(callback, options.batchSize || 100);

    return {
      stop: () => {
        this.active = false;
        // Also send LeaveGroup for polite exit
        // (Fire and forget, we don't await because stop() is usually sync)
      }
    };
  }

  private async mainLoop(callback: (data: T) => Promise<void> | void, batchSize: number) {
    while (this.conn.isConnected && this.active) {
      try {
        // 1. JOIN GROUP (Get Assignments)
        logger.info(`Joining group ${this.consumerGroup}...`);
        const joinRes = await this.conn.send(Opcode.S_JOIN, FrameCodec.string(this.consumerGroup!), FrameCodec.string(this.name));

        // Parse Join Response: [GenID:8][P_Count:4][P1, P2...][StartOffset:8 (Legacy)]
        this.generationId = joinRes.cursor.readU64();
        const pCount = joinRes.cursor.readU32();
        this.assignedPartitions = [];
        for (let i = 0; i < pCount; i++) {
          this.assignedPartitions.push(joinRes.cursor.readU32());
        }
        const legacyStartOffset = joinRes.cursor.readU64(); // Ignored if map used later

        // Init offsets
        this.offsets.clear();
        for (const p of this.assignedPartitions) {
          // Ideally server sends map, for now reset or use legacy. 
          // TODO: Use committed offsets map from server response
          this.offsets.set(p, legacyStartOffset);
        }

        logger.info(`Joined Gen ${this.generationId}. Assigned: ${this.assignedPartitions}`);

        // 2. POLL LOOP (Run until Rebalance Error)
        await this.partitionLoop(callback, batchSize);

      } catch (e: any) {
        if (!this.active || !this.conn.isConnected) break;

        if (e.message?.includes("REBALANCE")) {
          logger.warn("Rebalance needed. Restarting join...");
          // Clear state and retry join immediately
          this.assignedPartitions = [];
          continue;
        }

        logger.error("Stream fatal error (Backoff 1s)", e);
        await new Promise(r => setTimeout(r, 1000));
      }
    }
    this.isSubscribed = false;
  }

  private async partitionLoop(callback: (data: T) => Promise<void> | void, batchSize: number) {
    // Loop while active and NOT rebalancing
    while (this.active && this.conn.isConnected) {
      if (this.assignedPartitions.length === 0) {
        await new Promise(r => setTimeout(r, 100));
        continue;
      }

      // ROUND ROBIN FETCH
      for (const partition of this.assignedPartitions) {
        const currentOffset = this.offsets.get(partition) || BigInt(0);

        // FETCH: [GenID:8][Topic][Group][Partition:4][Offset:8][Limit:4]
        const res = await this.conn.send(
          Opcode.S_FETCH,
          FrameCodec.u64(this.generationId),
          FrameCodec.string(this.name),
          FrameCodec.string(this.consumerGroup!),
          FrameCodec.u32(partition),
          FrameCodec.u64(currentOffset),
          FrameCodec.u32(batchSize)
        );

        const numMsgs = res.cursor.readU32();
        if (numMsgs === 0) continue;

        let lastMsgOffset = currentOffset;
        let processError = false;

        // Process Batch
        for (let i = 0; i < numMsgs; i++) {
          const msgOffset = res.cursor.readU64();
          res.cursor.readU64(); // timestamp
          const payloadLen = res.cursor.readU32();
          const payloadBuf = res.cursor.readBuffer(payloadLen);
          const data = FrameCodec.decodeAny(new Cursor(payloadBuf));

          try {
            await callback(data);
            lastMsgOffset = msgOffset;
          } catch (e) {
            logger.error(`Processing error at P${partition}:${msgOffset}. Skipping commit for this partition.`);
            processError = true;
            break; // Break batch, move to next partition
          }
        }

        // Commit if OK
        if (!processError) {
          const nextOffset = lastMsgOffset + BigInt(1);
          this.offsets.set(partition, nextOffset);

          // COMMIT: [GenID:8][Group][Topic][Partition:4][Offset:8]
          await this.conn.send(
            Opcode.S_COMMIT,
            FrameCodec.u64(this.generationId),
            FrameCodec.string(this.consumerGroup!),
            FrameCodec.string(this.name),
            FrameCodec.u32(partition),
            FrameCodec.u64(nextOffset)
          );
        }
      }

      // Yield to avoid blocking event loop
      await new Promise(r => setTimeout(r, 0));
    }
  }
}

// ========================================
// CLIENT MAIN ENTRY
// ========================================

// Helper: Run Concurrent
async function runConcurrent<T>(items: T[], concurrency: number, fn: (item: T) => Promise<void>) {
  if (concurrency === 1) {
    for (const item of items) await fn(item);
    return;
  }
  const queue = [...items];
  const workers = Array(Math.min(concurrency, items.length)).fill(null).map(async () => {
    while (queue.length) await fn(queue.shift()!);
  });
  await Promise.all(workers);
}

export class NexoTopic<T = any> {
    constructor(private broker: NexoPubSub, public readonly name: string) {}
    async publish(data: T, options?: PublishOptions) { return this.broker.publish(this.name, data, options); }
    async subscribe(cb: (data: T) => void) { return this.broker.subscribe(this.name, cb); }
    async unsubscribe() { return this.broker.unsubscribe(this.name); }
}

export class NexoClient {
  private conn: NexoConnection;
  private queues = new Map<string, NexoQueue<any>>();
  private streams = new Map<string, NexoStream<any>>();
  private topics = new Map<string, NexoTopic<any>>();

  public readonly store: NexoStore;
  private readonly pubsubBroker: NexoPubSub;

  constructor(options: NexoOptions = {}) {
    this.conn = new NexoConnection(options.host || '127.0.0.1', options.port || 8080, options);
    this.store = new NexoStore(this.conn);
    this.pubsubBroker = new NexoPubSub(this.conn);
    this.setupGracefulShutdown();
  }

  static async connect(options?: NexoOptions): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.conn.connect();
    return client;
  }

  get connected() { return this.conn.isConnected; }
  disconnect() { this.conn.disconnect(); }

  queue<T = any>(name: string, config?: QueueConfig): NexoQueue<T> {
    if (!this.queues.has(name)) this.queues.set(name, new NexoQueue<T>(this.conn, name, config));
    return this.queues.get(name) as NexoQueue<T>;
  }

  stream<T = any>(name: string, group?: string): NexoStream<T> {
    const key = group ? `${name}:${group}` : name;
    if (!this.streams.has(key)) this.streams.set(key, new NexoStream<T>(this.conn, name, group));
    return this.streams.get(key) as NexoStream<T>;
  }

  pubsub<T = any>(name: string): NexoTopic<T> {
    if (!this.topics.has(name)) this.topics.set(name, new NexoTopic<T>(this.pubsubBroker, name));
    return this.topics.get(name) as NexoTopic<T>;
  }

  get debug() {
    return {
      echo: async (data: any) => {
        const res = await this.conn.send(Opcode.DEBUG_ECHO, FrameCodec.any(data));
        return FrameCodec.decodeAny(res.cursor);
      }
    };
  }

  private setupGracefulShutdown() {
    const shutdown = async () => {
      logger.info("Graceful shutdown triggered. Disconnecting...");
      this.disconnect();
      process.exit(0);
    };

    // Ensure we don't register duplicates if user creates multiple clients
    // Note: This is a simplistic global hook. In real-world, maybe let user handle it.
    // But for robust defaults, it's good.
    if (process.listenerCount('SIGINT') === 0) {
      process.on('SIGINT', shutdown);
      process.on('SIGTERM', shutdown);
    }
  }
}

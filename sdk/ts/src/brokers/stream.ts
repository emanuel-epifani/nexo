import { NexoConnection } from '../connection';
import { Cursor } from '../codec';
import { Logger } from '../utils/logger';
import { DEFAULT_CONFIG } from '../config';
import { ConnectionClosedError } from '../errors';

enum StreamOpcode {
  S_CREATE = 0x30,
  S_PUB = 0x31,
  S_FETCH = 0x32,
  S_JOIN = 0x33,
  S_ACK = 0x34,
  S_EXISTS = 0x35,
  S_DELETE = 0x36,
  S_NACK = 0x37,
  S_SEEK = 0x38,
  S_LEAVE = 0x39,
}

export interface RetentionOptions {
  maxAgeMs: number;
  maxBytes: number;
}

export interface StreamCreateOptions {
  retention?: RetentionOptions;
}

export interface StreamSubscribeOptions {
  batchSize?: number; // Default 100
  waitMs?: number; // Default 500
}

export interface StreamMessage<T> {
  seq: bigint;
  data: T;
}

const StreamCommands = {
  create: (conn: NexoConnection, name: string, options: StreamCreateOptions) =>
    conn.send(StreamOpcode.S_CREATE, w => w
      .string(name)
      .string(JSON.stringify(options || {}))
    ),

  exists: async (conn: NexoConnection, name: string) => {
    try {
      const res = await conn.send(StreamOpcode.S_EXISTS, w => w.string(name));
      return res.status === 0x00;
    } catch {
      return false;
    }
  },

  delete: (conn: NexoConnection, name: string) =>
    conn.send(StreamOpcode.S_DELETE, w => w.string(name)),

  publish: (conn: NexoConnection, name: string, data: any) =>
    conn.send(StreamOpcode.S_PUB, w => w
      .string(name)
      .any(data)
    ),

  join: async (conn: NexoConnection, stream: string, group: string) => {
    const res = await conn.send(StreamOpcode.S_JOIN, w => w
      .string(group)
      .string(stream)
    );
    const ackFloor = res.cursor.readU64();
    return { ackFloor };
  },

  fetch: async <T>(
    conn: NexoConnection,
    stream: string,
    group: string,
    batchSize: number,
    waitMs: number
  ): Promise<StreamMessage<T>[]> => {
    const res = await conn.send(StreamOpcode.S_FETCH, w => w
      .string(stream)
      .string(group)
      .u32(batchSize)
      .u32(waitMs)
    );

    const count = res.cursor.readU32();
    const messages: StreamMessage<T>[] = [];
    for (let i = 0; i < count; i++) {
      const seq = res.cursor.readU64();
      res.cursor.readU64(); // skip timestamp
      const payloadLen = res.cursor.readU32();
      const payloadBuf = res.cursor.readBuffer(payloadLen);
      const data = new Cursor(payloadBuf).decodeAny();
      messages.push({ seq, data });
    }
    return messages;
  },

  ack: (conn: NexoConnection, stream: string, group: string, seq: bigint) =>
    conn.sendFireAndForget(StreamOpcode.S_ACK, w => w
      .string(stream)
      .string(group)
      .u64(seq)
    ),

  nack: (conn: NexoConnection, stream: string, group: string, seq: bigint) =>
    conn.sendFireAndForget(StreamOpcode.S_NACK, w => w
      .string(stream)
      .string(group)
      .u64(seq)
    ),

  seek: (conn: NexoConnection, stream: string, group: string, target: 'beginning' | 'end') =>
    conn.send(StreamOpcode.S_SEEK, w => w
      .string(stream)
      .string(group)
      .u8(target === 'beginning' ? 0 : 1)
    ),

  leave: (conn: NexoConnection, stream: string, group: string) =>
    conn.send(StreamOpcode.S_LEAVE, w => w
      .string(stream)
      .string(group)
    ),
};

class StreamSubscription<T> {
  private active = false;
  private loopDone: Promise<void> = Promise.resolve();

  constructor(
    private conn: NexoConnection,
    private streamName: string,
    private group: string,
    private logger: Logger
  ) { }

  async start(
    callback: (data: T) => Promise<any> | any,
    options: StreamSubscribeOptions
  ): Promise<void> {
    this.active = true;
    const batchSize = options.batchSize ?? DEFAULT_CONFIG.stream.batchSize;
    const waitMs = options.waitMs ?? DEFAULT_CONFIG.stream.waitMs;

    this.loopDone = this.runConsumerLoop(callback, batchSize, waitMs).catch(err => {
      this.logger.error(`[${this.streamName}:${this.group}] Consumer crashed`, err);
      this.active = false;
    });
  }

  async stop(): Promise<void> {
    this.active = false;
    try {
      await StreamCommands.leave(this.conn, this.streamName, this.group);
    } catch { /* connection may already be closed */ }
    await this.loopDone;
  }

  private async runConsumerLoop(callback: (data: T) => Promise<any> | any, batchSize: number, waitMs: number) {
    while (this.active) {
      if (!this.conn.isConnected) {
        await this.backoff(DEFAULT_CONFIG.connection.backoff.short);
        continue;
      }

      try {
        // Join the group (idempotent — re-joining just returns ack_floor)
        await StreamCommands.join(this.conn, this.streamName, this.group);
        await this.pollLoop(callback, batchSize, waitMs);
      } catch (e: any) {
        if (!this.active) break;

        if (!this.conn.isConnected || e instanceof ConnectionClosedError || e.code === 'ECONNRESET') {
          await this.backoff(DEFAULT_CONFIG.connection.backoff.short);
          continue;
        }

        this.logger.error(`[${this.streamName}:${this.group}] Error. Retrying in ${DEFAULT_CONFIG.connection.backoff.long}ms...`, e);
        await this.backoff(DEFAULT_CONFIG.connection.backoff.long);
      }
    }
  }

  private async pollLoop(callback: (data: T) => Promise<any> | any, batchSize: number, waitMs: number): Promise<void> {
    while (this.active && this.conn.isConnected) {
      const messages = await StreamCommands.fetch<T>(
        this.conn,
        this.streamName,
        this.group,
        batchSize,
        waitMs
      );

      if (messages.length === 0) {
        continue;
      }

      for (let i = 0; i < messages.length; i++) {
        const msg = messages[i];
        if (!this.active) {
          for (let j = i; j < messages.length; j++) {
            StreamCommands.nack(this.conn, this.streamName, this.group, messages[j].seq);
          }
          return;
        }

        try {
          await callback(msg.data);
          StreamCommands.ack(this.conn, this.streamName, this.group, msg.seq);
        } catch (e: any) {
          this.logger.error(`[${this.streamName}:${this.group}] Processing error at seq=${msg.seq}. Nacking.`);
          StreamCommands.nack(this.conn, this.streamName, this.group, msg.seq);
        }
      }
    }
  }

  private backoff(ms: number): Promise<void> {
    return new Promise(r => setTimeout(r, ms));
  }
}

export class NexoStream<T = any> {
  constructor(
    private conn: NexoConnection,
    public readonly name: string,
    private logger: Logger
  ) { }

  async create(options: StreamCreateOptions = {}): Promise<this> {
    await StreamCommands.create(this.conn, this.name, options);
    return this;
  }

  async exists(): Promise<boolean> {
    return StreamCommands.exists(this.conn, this.name);
  }

  async delete(): Promise<void> {
    await StreamCommands.delete(this.conn, this.name);
  }

  async publish(data: T): Promise<void> {
    await StreamCommands.publish(this.conn, this.name, data);
  }

  async subscribe(
    group: string,
    callback: (data: T) => Promise<any> | any,
    options: StreamSubscribeOptions = {}
  ): Promise<{ stop: () => Promise<void> }> {
    if (!group) throw new Error("Consumer Group is required for subscription");

    // Fail Fast: Check existence first
    if (!(await this.exists())) {
      throw new Error(`Stream '${this.name}' not found`);
    }

    const subscription = new StreamSubscription<T>(this.conn, this.name, group, this.logger);

    // Start the subscription loop (non-blocking)
    await subscription.start(callback, options);

    return {
      stop: () => subscription.stop(),
    };
  }

  /**
   * Seek to beginning or end of the stream for a consumer group.
   */
  async seek(group: string, target: 'beginning' | 'end'): Promise<void> {
    await StreamCommands.seek(this.conn, this.name, group, target);
  }
}

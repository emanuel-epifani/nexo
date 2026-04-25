import { NexoConnection } from '../connection';
import { Cursor } from '../codec';
import { Logger } from '../utils/logger';
import { DEFAULT_CONFIG } from '../config';
import { ConnectionClosedError, NotConnectedError } from '../errors';

enum StreamOpcode {
  S_CREATE = 0x30,
  S_PUB = 0x31,
  S_FETCH = 0x32,
  S_JOIN = 0x33,
  S_ACK = 0x34,
  S_EXISTS = 0x35,
  S_DELETE = 0x36,
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

interface StreamConsumerSession {
  consumerId: string;
  generation: bigint;
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
    const generation = res.cursor.readU64();
    const consumerId = res.cursor.readString();
    return { ackFloor, generation, consumerId };
  },

  fetch: async <T>(
    conn: NexoConnection,
    stream: string,
    group: string,
    consumerId: string,
    generation: bigint,
    batchSize: number,
    waitMs: number
  ): Promise<StreamMessage<T>[]> => {
    const res = await conn.send(StreamOpcode.S_FETCH, w => w
      .string(stream)
      .string(group)
      .string(consumerId)
      .u64(generation)
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

  ack: (conn: NexoConnection, stream: string, group: string, consumerId: string, generation: bigint, seq: bigint) =>
    conn.sendFireAndForget(StreamOpcode.S_ACK, w => w
      .string(stream)
      .string(group)
      .string(consumerId)
      .u64(generation)
      .u64(seq)
    ),

  seek: (conn: NexoConnection, stream: string, group: string, target: 'beginning' | 'end') =>
    conn.send(StreamOpcode.S_SEEK, w => w
      .string(stream)
      .string(group)
      .u8(target === 'beginning' ? 0 : 1)
    ),

  leave: (conn: NexoConnection, stream: string, group: string, consumerId: string, generation: bigint) =>
    conn.send(StreamOpcode.S_LEAVE, w => w
      .string(stream)
      .string(group)
      .string(consumerId)
      .u64(generation)
    ),
};

class StreamSubscription<T> {
  private active = false;
  private loopDone: Promise<void> = Promise.resolve();
  private consumer: StreamConsumerSession | null = null;

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

    // First join is synchronous: fail fast on permanent server errors (e.g. topic not found)
    await this.ensureJoined();

    this.loopDone = this.runConsumerLoop(callback, batchSize, waitMs).catch(err => {
      this.logger.error(`[${this.streamName}:${this.group}] Consumer crashed`, err);
      this.active = false;
    });
  }

  private async ensureJoined(): Promise<void> {
    if (!this.conn.isConnected) {
      throw new NotConnectedError();
    }
    const joined = await StreamCommands.join(this.conn, this.streamName, this.group);
    this.consumer = {
      consumerId: joined.consumerId,
      generation: joined.generation,
    };
  }

  async stop(): Promise<void> {
    this.active = false;
    try {
      if (this.consumer) {
        await StreamCommands.leave(this.conn, this.streamName, this.group, this.consumer.consumerId, this.consumer.generation);
      }
    } catch { /* connection may already be closed */ }
    await this.loopDone;
  }

  private async runConsumerLoop(callback: (data: T) => Promise<any> | any, batchSize: number, waitMs: number) {
    while (this.active) {
      if (!this.conn.isConnected) {
        this.consumer = null;
        await this.backoff(DEFAULT_CONFIG.connection.backoff.short);
        continue;
      }

      try {
        if (!this.consumer) {
          await this.ensureJoined();
        }

        await this.pollLoop(callback, batchSize, waitMs);
      } catch (e: any) {
        if (!this.active) break;

        if (!this.conn.isConnected || e instanceof ConnectionClosedError || e.code === 'ECONNRESET') {
          this.consumer = null;
          await this.backoff(DEFAULT_CONFIG.connection.backoff.short);
          continue;
        }

        if (this.isRecoverableMembershipError(e)) {
          this.consumer = null;
          continue;
        }

        this.logger.error(`[${this.streamName}:${this.group}] Error. Retrying in ${DEFAULT_CONFIG.connection.backoff.long}ms...`, e);
        await this.backoff(DEFAULT_CONFIG.connection.backoff.long);
      }
    }
  }

  private async pollLoop(callback: (data: T) => Promise<any> | any, batchSize: number, waitMs: number): Promise<void> {
    const consumer = this.consumer;
    if (!consumer) {
      throw new Error('Missing consumer session');
    }

    while (this.active && this.conn.isConnected) {
      const messages = await StreamCommands.fetch<T>(
        this.conn,
        this.streamName,
        this.group,
        consumer.consumerId,
        consumer.generation,
        batchSize,
        waitMs
      );

      if (messages.length === 0) {
        continue;
      }

      for (let i = 0; i < messages.length; i++) {
        const msg = messages[i];
        if (!this.active) {
          return;
        }

        try {
          await callback(msg.data);
          StreamCommands.ack(this.conn, this.streamName, this.group, consumer.consumerId, consumer.generation, msg.seq);
        } catch (e: any) {
          this.logger.error(`[${this.streamName}:${this.group}] Processing error at seq=${msg.seq}. Waiting for timeout-based retry.`, e);
        }
      }
    }
  }

  private isRecoverableMembershipError(error: any): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return message.includes('FENCED') || message.includes('NOT_MEMBER');
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

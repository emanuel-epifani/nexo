import { NexoConnection } from '../connection';
import { Logger } from '../utils/logger';
import { DEFAULT_CONFIG } from '../config';
import { ConnectionClosedError, RequestTimeoutError, RequestCancelledError } from '../errors';
import { runConcurrent } from '../utils/concurrent';
import { Subscription } from '../subscription';
import { FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES, FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT, FLAG_QUEUE_Q_PUSH_HAS_PRIORITY, QueueOpcode } from '../protocol';

const CONSUME_TIMEOUT_MARGIN_MS = 5000;

const QueueCommands = {
  create: (conn: NexoConnection, name: string, config: QueueConfig) => {
    const hasVto = config?.visibilityTimeoutMs !== undefined;
    const hasRetries = config?.maxDeliveries !== undefined;
    const flags = (hasVto ? FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT : 0x00) | (hasRetries ? FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES : 0x00);
    return conn.send(QueueOpcode.Q_CREATE, w => {
      w.string(name).u8(flags);
      if (hasVto) w.u64(config!.visibilityTimeoutMs!);
      if (hasRetries) w.u32(config!.maxDeliveries!);
    });
  },

  exists: async (conn: NexoConnection, name: string) => {
    try {
      const res = await conn.send(QueueOpcode.Q_EXISTS, w => w.string(name));
      return res.cursor.readU8() === 1;
    } catch {
      return false;
    }
  },

  delete: (conn: NexoConnection, name: string) =>
    conn.send(QueueOpcode.Q_DELETE, w => w.string(name)),

  push: (conn: NexoConnection, name: string, data: any, options: QueuePushOptions) => {
    const hasPriority = options?.priority !== undefined;
    const flags = hasPriority ? FLAG_QUEUE_Q_PUSH_HAS_PRIORITY : 0x00;
    return conn.send(QueueOpcode.Q_PUSH, w => {
      w.string(name).u32(1).u8(flags);
      if (hasPriority) w.u8(options!.priority!);
      w.anyWithLen(data);
    });
  },

  pushBatch: (conn: NexoConnection, name: string, items: { data: any, options?: QueuePushOptions }[]) => {
    return conn.send(QueueOpcode.Q_PUSH, w => {
      w.string(name).u32(items.length);
      for (const item of items) {
        const hasPriority = item.options?.priority !== undefined;
        const flags = hasPriority ? FLAG_QUEUE_Q_PUSH_HAS_PRIORITY : 0x00;
        w.u8(flags);
        if (hasPriority) w.u8(item.options!.priority!);
        w.anyWithLen(item.data);
      }
    });
  },

  consume: async <T>(conn: NexoConnection, name: string, batchSize: number, waitMs: number, signal?: AbortSignal): Promise<{ id: string, deliveryToken: bigint, data: T }[]> => {
    const res = await conn.send(QueueOpcode.Q_CONSUME, w => {
      w.string(name).u32(batchSize).u32(waitMs);
    }, { timeoutMs: waitMs + CONSUME_TIMEOUT_MARGIN_MS, signal });

    const count = res.cursor.readU32();
    if (count === 0) return [];

    const messages: { id: string; deliveryToken: bigint; data: T }[] = [];
    for (let i = 0; i < count; i++) {
      const idHex = res.cursor.readUUID();
      const deliveryToken = res.cursor.readU64();
      const payloadLen = res.cursor.readU32();
      const data = res.cursor.decodeAnyFromBuffer(payloadLen);
      messages.push({ id: idHex, deliveryToken, data });
    }
    return messages;
  },

  ack: (conn: NexoConnection, name: string, id: string, deliveryToken: bigint) =>
    conn.sendFireAndForget(QueueOpcode.Q_ACK, w => w.uuid(id).u64(deliveryToken).string(name)),

  nack: (conn: NexoConnection, name: string, id: string, deliveryToken: bigint, reason: string) =>
    conn.sendFireAndForget(QueueOpcode.Q_NACK, w => w
      .uuid(id)
      .u64(deliveryToken)
      .string(name)
      .string(reason)
    ),

  // DLQ Commands
  peekDLQ: async <T>(conn: NexoConnection, name: string, limit: number, offset: number): Promise<{ total: number, items: { id: string, data: T, attempts: number, failureReason: string }[] }> => {
    const res = await conn.send(QueueOpcode.Q_PEEK_DLQ, w => w
      .string(name)
      .u32(limit)
      .u32(offset)
    );

    const total = res.cursor.readU32();
    const count = res.cursor.readU32();

    const items: { id: string; data: T; attempts: number; failureReason: string }[] = [];
    for (let i = 0; i < count; i++) {
      const idHex = res.cursor.readUUID();
      const payloadLen = res.cursor.readU32();
      const data = res.cursor.decodeAnyFromBuffer(payloadLen);
      const attempts = res.cursor.readU32();
      const failureReason = res.cursor.readString();
      items.push({ id: idHex, data, attempts, failureReason });
    }
    return { total, items };
  },

  moveToQueue: async (conn: NexoConnection, name: string, messageId: string): Promise<boolean> => {
    const res = await conn.send(QueueOpcode.Q_MOVE_TO_QUEUE, w => w
      .string(name)
      .uuid(messageId)
    );
    return res.cursor.readU8() === 1;
  },

  deleteDLQ: async (conn: NexoConnection, name: string, messageId: string): Promise<boolean> => {
    const res = await conn.send(QueueOpcode.Q_DELETE_DLQ, w => w
      .string(name)
      .uuid(messageId)
    );
    return res.cursor.readU8() === 1;
  },

  purgeDLQ: async (conn: NexoConnection, name: string): Promise<number> => {
    const res = await conn.send(QueueOpcode.Q_PURGE_DLQ, w => w.string(name));
    return res.cursor.readU32();
  },
};

export interface QueueConfig {
  visibilityTimeoutMs?: number;
  maxDeliveries?: number;
}

export interface QueueSubscribeOptions {
  batchSize?: number;
  waitMs?: number;
  concurrency?: number;
}

export interface QueuePushOptions {
  priority?: number;
}

/**
 * Dead Letter Queue (DLQ) management for a queue.
 * Provides methods to inspect, replay, delete, and purge failed messages.
 */
export class NexoDLQ<T = any> {
  constructor(
    private conn: NexoConnection,
    private queueName: string,
    private logger: Logger
  ) { }

  /**
   * Peek messages in the DLQ without consuming them.
   * @param limit Maximum number of messages to return (default 10)
   * @param offset Pagination offset (default: 0)
   * @returns Object containing total count and array of messages
   */
  async peek(limit: number = DEFAULT_CONFIG.queue.peek.limit, offset: number = DEFAULT_CONFIG.queue.peek.offset): Promise<{ total: number, items: { id: string; data: T; attempts: number; failureReason: string }[] }> {
    this.logger.debug(`[DLQ:${this.queueName}] Peeking ${limit} messages at offset ${offset}`);
    return QueueCommands.peekDLQ<T>(this.conn, this.queueName, limit, offset);
  }

  /**
   * Move a message from DLQ back to the main queue (replay/retry).
   * The message will be reset with attempts = 0 and become available for consumption.
   * @param messageId ID of the message to move
   * @returns true if the message was moved, false if not found
   */
  async moveToQueue(messageId: string): Promise<boolean> {
    this.logger.debug(`[DLQ:${this.queueName}] Moving message ${messageId} to main queue`);
    return QueueCommands.moveToQueue(this.conn, this.queueName, messageId);
  }

  /**
   * Delete a specific message from the DLQ.
   * @param messageId ID of the message to delete
   * @returns true if the message was deleted, false if not found
   */
  async delete(messageId: string): Promise<boolean> {
    this.logger.debug(`[DLQ:${this.queueName}] Deleting message ${messageId}`);
    return QueueCommands.deleteDLQ(this.conn, this.queueName, messageId);
  }

  /**
   * Purge all messages from the DLQ.
   * @returns Number of messages purged
   */
  async purge(): Promise<number> {
    this.logger.debug(`[DLQ:${this.queueName}] Purging all messages`);
    return QueueCommands.purgeDLQ(this.conn, this.queueName);
  }
}

class QueueSubscription<T> {
  active = false;
  private loopPromise: Promise<void> = Promise.resolve();
  private abortController: AbortController | null = null;

  constructor(
    private readonly conn: NexoConnection,
    private readonly queueName: string,
    private readonly logger: Logger,
    private readonly callback: (data: T) => Promise<any> | any,
    private readonly batchSize: number,
    private readonly waitMs: number,
    private readonly concurrency: number,
    private readonly stopTimeoutMs: number,
  ) { }

  start(): void {
    this.active = true;
    this.loopPromise = this.loop().catch(err => {
      this.logger.error(`[CRITICAL] Queue loop crashed for ${this.queueName}`, err);
    });
  }

  async stop(): Promise<void> {
    this.active = false;
    this.abortController?.abort();
    try {
      await Promise.race([
        this.loopPromise,
        new Promise<void>((_, reject) =>
          setTimeout(() => reject(new Error(`stop() drain timeout after ${this.stopTimeoutMs}ms`)), this.stopTimeoutMs)
        ),
      ]);
    } catch (e: any) {
      this.logger.warn(`[Queue:${this.queueName}] ${e.message}`);
    }
  }

  private async loop(): Promise<void> {
    while (this.active) {
      if (!this.conn.isConnected) {
        await new Promise(r => setTimeout(r, DEFAULT_CONFIG.connection.backoff.short));
        continue;
      }

      try {
        if (!this.conn.isConnected) continue;

        this.abortController = new AbortController();
        const messages = await QueueCommands.consume<T>(this.conn, this.queueName, this.batchSize, this.waitMs, this.abortController.signal);
        this.abortController = null;

        if (messages.length === 0) continue;

        await runConcurrent(messages, this.concurrency, async (msg) => {
          if (!this.active) return;
          try {
            await this.callback(msg.data);
            QueueCommands.ack(this.conn, this.queueName, msg.id, msg.deliveryToken);
          } catch (e: any) {
            if (!this.conn.isConnected) return;
            const reason = e instanceof Error ? e.message : String(e);
            this.logger.error(`[Queue:${this.queueName}] Consumer error, sending NACK. Reason: ${reason}`);
            QueueCommands.nack(this.conn, this.queueName, msg.id, msg.deliveryToken, reason);
          }
        });

      } catch (e: any) {
        if (!this.active) break;
        if (e instanceof RequestCancelledError) break;
        if (!this.conn.isConnected || e instanceof ConnectionClosedError || e instanceof RequestTimeoutError || e.code === 'ECONNRESET') {
          await new Promise(r => setTimeout(r, DEFAULT_CONFIG.connection.backoff.short));
          continue;
        }
        this.logger.error(`[Queue:${this.queueName}] Consumer stopping:`, e.message);
        break;
      }
    }
  }
}

export class NexoQueue<T = any> {
  private _dlq: NexoDLQ<T>;

  constructor(
    private conn: NexoConnection,
    public readonly name: string,
    private logger: Logger
  ) {
    this._dlq = new NexoDLQ<T>(conn, name, logger);
  }

  /**
   * Access the Dead Letter Queue (DLQ) for this queue.
   * Use this to inspect, replay, delete, or purge failed messages.
   */
  get dlq(): NexoDLQ<T> {
    return this._dlq;
  }

  async create(config: QueueConfig = {}): Promise<this> {
    await QueueCommands.create(this.conn, this.name, config);
    return this;
  }

  async exists(): Promise<boolean> {
    return QueueCommands.exists(this.conn, this.name);
  }

  async delete(): Promise<void> {
    await QueueCommands.delete(this.conn, this.name);
  }

  async push(data: T, options: QueuePushOptions = {}): Promise<void> {
    await QueueCommands.push(this.conn, this.name, data, options);
  }

  async pushBatch(items: { data: T, options?: QueuePushOptions }[]): Promise<void> {
    if (items.length === 0) return;
    await QueueCommands.pushBatch(this.conn, this.name, items);
  }

  /**
   * Start a consume loop for this queue. Each call spawns an independent loop;
   * calling subscribe() N times produces N parallel consumers sharing the queue,
   * with messages split between them by the server.
   */
  async subscribe(callback: (data: T) => Promise<any> | any, options: QueueSubscribeOptions = {}): Promise<Subscription> {
    const batchSize = options.batchSize ?? DEFAULT_CONFIG.queue.batchSize;
    const waitMs = options.waitMs ?? DEFAULT_CONFIG.queue.waitMs;
    const concurrency = options.concurrency ?? DEFAULT_CONFIG.queue.concurrency;

    if (batchSize < 1) throw new Error(`batchSize must be >= 1, got ${batchSize}`);
    if (concurrency < 1) throw new Error(`concurrency must be >= 1, got ${concurrency}`);

    const sub = new QueueSubscription<T>(this.conn, this.name, this.logger, callback, batchSize, waitMs, concurrency, DEFAULT_CONFIG.queue.stopTimeoutMs);
    sub.start();

    return new Subscription(
      () => sub.stop(),
      () => sub.active,
    );
  }

}

import { NexoConnection } from '../transport/tcp/connection';
import { Logger } from '../utils/logger';
import { DEFAULT_CONFIG } from '../config';
import { ConnectionClosedError, ProtocolError, RequestTimeoutError, RequestCancelledError } from '../errors';
import { runConcurrent } from '../utils/concurrent';
import { Subscription } from '../subscription';
import { ProvisionOutcome, ProvisionResult } from '../provisioning';
import {
  FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES,
  FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT,
  FLAG_QUEUE_Q_PUSH_HAS_PRIORITY,
  ProvisionStatus as WireProvisionStatus,
  QueueOpcode,
} from '../protocol/generated';
import { Cursor } from '../protocol/codec';

const CONSUME_TIMEOUT_MARGIN_MS = 5000;

export interface QueueCreateOptions {
  visibilityTimeoutMs?: number;
  maxDeliveries?: number;
}

export interface QueueConfiguration {
  visibilityTimeoutMs: number;
  maxDeliveries: number;
}

export interface QueueDefinition {
  name: string;
  config: QueueConfiguration;
}

export interface QueueSubscribeOptions {
  batchSize?: number;
  waitMs?: number;
  concurrency?: number;
}

export interface QueuePushOptions {
  priority?: number;
}

export interface QueueMessageMeta {
  id: string;
}

export type QueueHandler<T> = (data: T, meta: QueueMessageMeta) => unknown | Promise<unknown>;

export interface DlqPeekOptions {
  limit?: number;
  offset?: number;
}

function readQueueDefinition(cursor: Cursor): QueueDefinition {
  return {
    name: cursor.readString(),
    config: {
      visibilityTimeoutMs: Number(cursor.readU64()),
      maxDeliveries: cursor.readU32(),
    },
  };
}

function readProvisionStatus(cursor: Cursor): ProvisionOutcome {
  const status = cursor.readU8();
  if (status === WireProvisionStatus.CREATED) return 'created';
  if (status === WireProvisionStatus.UNCHANGED) return 'unchanged';
  throw new ProtocolError(`Unknown queue provision status: ${status}`);
}

const QueueCommands = {
  create: async (conn: NexoConnection, name: string, options: QueueCreateOptions): Promise<ProvisionResult<QueueDefinition>> => {
    const hasVto = options.visibilityTimeoutMs !== undefined;
    const hasDeliveries = options.maxDeliveries !== undefined;
    const flags = (hasVto ? FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT : 0x00) | (hasDeliveries ? FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES : 0x00);
    const res = await conn.send(QueueOpcode.Q_CREATE, w => {
      w.string(name).u8(flags);
      if (hasVto) w.u64(options.visibilityTimeoutMs!);
      if (hasDeliveries) w.u32(options.maxDeliveries!);
    });
    return {
      status: readProvisionStatus(res.cursor),
      definition: readQueueDefinition(res.cursor),
    };
  },

  describe: async (conn: NexoConnection, name: string): Promise<QueueDefinition> => {
    const res = await conn.send(QueueOpcode.Q_DESCRIBE, w => w.string(name));
    return readQueueDefinition(res.cursor);
  },

  exists: async (conn: NexoConnection, name: string): Promise<boolean> => {
    const res = await conn.send(QueueOpcode.Q_EXISTS, w => w.string(name));
    return res.cursor.readU8() === 1;
  },

  delete: (conn: NexoConnection, name: string) =>
    conn.send(QueueOpcode.Q_DELETE, w => w.string(name)),

  push: (conn: NexoConnection, name: string, data: any, options: QueuePushOptions) => {
    const hasPriority = options.priority !== undefined;
    const flags = hasPriority ? FLAG_QUEUE_Q_PUSH_HAS_PRIORITY : 0x00;
    return conn.send(QueueOpcode.Q_PUSH, w => {
      w.string(name).u32(1).u8(flags);
      if (hasPriority) w.u8(options.priority!);
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
   * @param options Pagination options
   * @returns Object containing total count and array of messages
   */
  async peek(options: DlqPeekOptions = {}): Promise<{ total: number, items: { id: string; data: T; attempts: number; failureReason: string }[] }> {
    const limit = options.limit ?? DEFAULT_CONFIG.queue.peek.limit;
    const offset = options.offset ?? DEFAULT_CONFIG.queue.peek.offset;
    this.logger.debug(`[DLQ:${this.queueName}] Peeking ${limit} messages at offset ${offset}`);
    return QueueCommands.peekDLQ<T>(this.conn, this.queueName, limit, offset);
  }

  /**
   * Move a message from DLQ back to the main queue (replay/retry).
   * The message will be reset with attempts = 0 and become available for consumption.
   * @param messageId ID of the message to move
   * @returns true if the message was moved, false if not found
   */
  async replay(messageId: string): Promise<boolean> {
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
  private terminalError: unknown = null;

  constructor(
    private readonly conn: NexoConnection,
    private readonly queueName: string,
    private readonly logger: Logger,
    private readonly callback: QueueHandler<T>,
    private readonly batchSize: number,
    private readonly waitMs: number,
    private readonly concurrency: number,
    private readonly stopTimeoutMs: number,
  ) { }

  get closed(): Promise<void> {
    return this.loopPromise;
  }

  get error(): unknown {
    return this.terminalError;
  }

  start(): void {
    this.active = true;
    this.loopPromise = this.loop().catch(error => {
      this.terminalError = error;
      this.logger.error(`[CRITICAL] Queue loop crashed for ${this.queueName}`, error);
    }).finally(() => {
      this.active = false;
    });
  }

  async stop(): Promise<void> {
    this.active = false;
    this.abortController?.abort();
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        this.loopPromise,
        new Promise<void>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`stop() drain timeout after ${this.stopTimeoutMs}ms`)), this.stopTimeoutMs);
          timer.unref();
        }),
      ]);
    } catch (error: any) {
      this.logger.warn(`[Queue:${this.queueName}] ${error.message}`);
      throw error;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
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
            await this.callback(msg.data, { id: msg.id });
            QueueCommands.ack(this.conn, this.queueName, msg.id, msg.deliveryToken);
          } catch (e: any) {
            if (!this.conn.isConnected) return;
            const reason = e instanceof Error ? e.message : String(e);
            this.logger.error(`[Queue:${this.queueName}] Consumer error, sending NACK. Reason: ${reason}`);
            QueueCommands.nack(this.conn, this.queueName, msg.id, msg.deliveryToken, reason);
          }
        });

      } catch (e: any) {
        this.abortController = null;
        if (!this.active) break;
        if (e instanceof RequestCancelledError) break;
        if (!this.conn.isConnected || e instanceof ConnectionClosedError || e instanceof RequestTimeoutError || e.code === 'ECONNRESET') {
          await new Promise(r => setTimeout(r, DEFAULT_CONFIG.connection.backoff.short));
          continue;
        }
        this.terminalError = e;
        this.logger.error(`[Queue:${this.queueName}] Consumer stopping:`, e.message);
        break;
      }
    }
  }
}

export class NexoQueue<T = any> {
  private readonly _dlq: NexoDLQ<T>;

  constructor(
    private readonly conn: NexoConnection,
    public readonly name: string,
    private readonly logger: Logger
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
  async subscribe(callback: QueueHandler<T>, options: QueueSubscribeOptions = {}): Promise<Subscription> {
    const batchSize = options.batchSize ?? DEFAULT_CONFIG.queue.batchSize;
    const waitMs = options.waitMs ?? DEFAULT_CONFIG.queue.waitMs;
    const concurrency = options.concurrency ?? DEFAULT_CONFIG.queue.concurrency;

    if (batchSize < 1) throw new Error(`batchSize must be >= 1, got ${batchSize}`);
    if (concurrency < 1) throw new Error(`concurrency must be >= 1, got ${concurrency}`);

    await QueueCommands.describe(this.conn, this.name);
    const sub = new QueueSubscription<T>(this.conn, this.name, this.logger, callback, batchSize, waitMs, concurrency, DEFAULT_CONFIG.queue.stopTimeoutMs);
    sub.start();

    return new Subscription(
      () => sub.stop(),
      () => sub.active,
      sub.closed,
      () => sub.error,
    );
  }
}

export class NexoQueueFacade {
  constructor(
    private readonly conn: NexoConnection,
    private readonly logger: Logger,
  ) { }

  create(name: string, options: QueueCreateOptions = {}): Promise<ProvisionResult<QueueDefinition>> {
    return QueueCommands.create(this.conn, name, options);
  }

  describe(name: string): Promise<QueueDefinition> {
    return QueueCommands.describe(this.conn, name);
  }

  async get<T = any>(name: string): Promise<NexoQueue<T>> {
    await QueueCommands.describe(this.conn, name);
    return new NexoQueue<T>(this.conn, name, this.logger);
  }

  exists(name: string): Promise<boolean> {
    return QueueCommands.exists(this.conn, name);
  }

  async delete(name: string): Promise<void> {
    await QueueCommands.delete(this.conn, name);
  }
}

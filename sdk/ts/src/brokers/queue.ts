import { NexoConnection } from '../connection';
import { FrameCodec, Cursor } from '../codec';
import { Logger } from '../utils/logger';
import { ConnectionClosedError } from '../errors';

enum QueueOpcode {
  Q_CREATE = 0x10,
  Q_PUSH = 0x11,
  Q_CONSUME = 0x12,
  Q_ACK = 0x13,
  Q_EXISTS = 0x14,
  Q_DELETE = 0x15,
  Q_PEEK_DLQ = 0x16,
  Q_MOVE_TO_QUEUE = 0x17,
  Q_DELETE_DLQ = 0x18,
  Q_PURGE_DLQ = 0x19,
  Q_NACK = 0x1A,
}

const QueueCommands = {
  create: (conn: NexoConnection, name: string, config: QueueConfig) =>
    conn.send(
      QueueOpcode.Q_CREATE,
      FrameCodec.string(name),
      FrameCodec.string(JSON.stringify(config || {}))
    ),

  exists: async (conn: NexoConnection, name: string) => {
    try {
      const res = await conn.send(QueueOpcode.Q_EXISTS, FrameCodec.string(name));
      return res.status === 0x00;
    } catch {
      return false;
    }
  },

  delete: (conn: NexoConnection, name: string) =>
    conn.send(QueueOpcode.Q_DELETE, FrameCodec.string(name)),

  push: (conn: NexoConnection, name: string, data: any, options: QueuePushOptions) =>
    conn.send(
      QueueOpcode.Q_PUSH,
      FrameCodec.string(name),
      FrameCodec.string(JSON.stringify(options || {})),
      FrameCodec.any(data)
    ),

  consume: async <T>(conn: NexoConnection, name: string, batchSize: number, waitMs: number): Promise<{ id: string, data: T }[]> => {
    const res = await conn.send(
      QueueOpcode.Q_CONSUME,
      FrameCodec.string(name),
      FrameCodec.string(JSON.stringify({ batchSize, waitMs }))
    );

    const count = res.cursor.readU32();
    if (count === 0) return [];

    const messages: { id: string; data: T }[] = [];
    for (let i = 0; i < count; i++) {
      const idHex = res.cursor.readUUID();
      const payloadLen = res.cursor.readU32();
      const payloadBuf = res.cursor.readBuffer(payloadLen);
      const data = FrameCodec.decodeAny(new Cursor(payloadBuf));
      messages.push({ id: idHex, data });
    }
    return messages;
  },

  ack: (conn: NexoConnection, name: string, id: string) =>
    conn.send(QueueOpcode.Q_ACK, FrameCodec.uuid(id), FrameCodec.string(name)),

  nack: (conn: NexoConnection, name: string, id: string, reason: string) =>
    conn.send(
      QueueOpcode.Q_NACK,
      FrameCodec.uuid(id),
      FrameCodec.string(name),
      FrameCodec.string(reason)
    ),

  // DLQ Commands
  peekDLQ: async <T>(conn: NexoConnection, name: string, limit: number, offset: number): Promise<{ total: number, items: { id: string, data: T, attempts: number, failureReason: string }[] }> => {
    const res = await conn.send(
      QueueOpcode.Q_PEEK_DLQ,
      FrameCodec.string(name),
      FrameCodec.u32(limit),
      FrameCodec.u32(offset)
    );

    const total = res.cursor.readU32();
    const count = res.cursor.readU32();
    
    const items: { id: string; data: T; attempts: number; failureReason: string }[] = [];
    for (let i = 0; i < count; i++) {
      const idHex = res.cursor.readUUID();
      const payloadLen = res.cursor.readU32();
      const payloadBuf = res.cursor.readBuffer(payloadLen);
      const data = FrameCodec.decodeAny(new Cursor(payloadBuf));
      const attempts = res.cursor.readU32();
      const failureReason = res.cursor.readString();
      items.push({ id: idHex, data, attempts, failureReason });
    }
    return { total, items };
  },

  moveToQueue: async (conn: NexoConnection, name: string, messageId: string): Promise<boolean> => {
    const res = await conn.send(
      QueueOpcode.Q_MOVE_TO_QUEUE,
      FrameCodec.string(name),
      FrameCodec.uuid(messageId)
    );
    return res.cursor.readU8() === 1;
  },

  deleteDLQ: async (conn: NexoConnection, name: string, messageId: string): Promise<boolean> => {
    const res = await conn.send(
      QueueOpcode.Q_DELETE_DLQ,
      FrameCodec.string(name),
      FrameCodec.uuid(messageId)
    );
    return res.cursor.readU8() === 1;
  },

  purgeDLQ: async (conn: NexoConnection, name: string): Promise<number> => {
    const res = await conn.send(
      QueueOpcode.Q_PURGE_DLQ,
      FrameCodec.string(name)
    );
    return res.cursor.readU32();
  },
};

export type PersistenceStrategy = 'file_sync' | 'file_async';

export interface QueueConfig {
  visibilityTimeoutMs?: number;
  maxRetries?: number;
  ttlMs?: number;
  persistence?: PersistenceStrategy;
}

export interface QueueSubscribeOptions {
  batchSize?: number;
  waitMs?: number;
  concurrency?: number;
}

export interface QueuePushOptions {
  priority?: number;
  delayMs?: number;
}

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

/**
 * Dead Letter Queue (DLQ) management for a queue.
 * Provides methods to inspect, replay, delete, and purge failed messages.
 */
export class NexoDLQ<T = any> {
  constructor(
    private conn: NexoConnection,
    private queueName: string,
    private logger: Logger
  ) {}

  /**
   * Peek messages in the DLQ without consuming them.
   * @param limit Maximum number of messages to return (default: 10)
   * @param offset Pagination offset (default: 0)
   * @returns Object containing total count and array of messages
   */
  async peek(limit: number = 10, offset: number = 0): Promise<{ total: number, items: { id: string; data: T; attempts: number; failureReason: string }[] }> {
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

export class NexoQueue<T = any> {
  private isSubscribed = false;
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

  async subscribe(callback: (data: T) => Promise<any> | any, options: QueueSubscribeOptions = {}): Promise<{ stop: () => void }> {
    if (this.isSubscribed) throw new Error(`Queue '${this.name}' already subscribed.`);

    // Fail Fast: Check existence first
    if (!(await this.exists())) {
      throw new Error(`Queue '${this.name}' not found`);
    }

    this.isSubscribed = true;

    const batchSize = options.batchSize ?? 50;
    const waitMs = options.waitMs ?? 20000;
    const concurrency = options.concurrency ?? 5;

    let active = true;

    const loop = async () => {
      while (active) {
        if (!this.conn.isConnected) {
          await new Promise(r => setTimeout(r, 500));
          continue;
        }

        // Add small jitter to avoid thundering herd on reconnect
        await new Promise(r => setTimeout(r, Math.random() * 500));

        try {
          // Double check before sending
          if (!this.conn.isConnected) continue;

          const messages = await QueueCommands.consume<T>(this.conn, this.name, batchSize, waitMs);

          if (messages.length === 0) continue;

          await runConcurrent(messages, concurrency, async (msg) => {
            if (!active) {
              if (this.conn.isConnected) {
                // If stopped while consuming, release message immediately so it's available for others
                await this.nack(msg.id, "Consumer stopped").catch(() => {});
              }
              return;
            }
            try {
              await callback(msg.data);
              await this.ack(msg.id);
            } catch (e: any) {
              if (!this.conn.isConnected) return;
              const reason = e instanceof Error ? e.message : String(e);
              this.logger.error(`[Queue:${this.name}] Consumer error, sending NACK. Reason: ${reason}`);
              await this.nack(msg.id, reason);
            }
          });

        } catch (e: any) {
          if (!active) break;
          // Catch-all for connection issues to prevent Unhandled Rejection
          if (!this.conn.isConnected || e instanceof ConnectionClosedError || e.code === 'ECONNRESET') {
            await new Promise(r => setTimeout(r, 500));
            continue;
          }
          this.logger.error(`[QUEUE-LOOP:${this.name}] CRITICAL ERROR:`, e);
          await new Promise(r => setTimeout(r, 1000));
        }
      }
      this.isSubscribed = false;
    };

    loop().catch(err => {
      this.logger.error(`[CRITICAL] Queue loop crashed for ${this.name}`, err);
    });

    return { 
      stop: () => { 
        active = false;
        this.isSubscribed = false;
      } 
    };
  }

  private async ack(id: string): Promise<void> {
    await QueueCommands.ack(this.conn, this.name, id);
  }

  private async nack(id: string, reason: string): Promise<void> {
    await QueueCommands.nack(this.conn, this.name, id, reason);
  }
}

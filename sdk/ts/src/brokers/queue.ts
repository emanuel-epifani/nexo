import { NexoConnection } from '../connection';
import { FrameCodec, Cursor } from '../codec';
import { logger } from '../utils/logger';

export enum QueueOpcode {
  Q_CREATE = 0x10,
  Q_PUSH = 0x11,
  Q_CONSUME = 0x12,
  Q_ACK = 0x13,
  Q_EXISTS = 0x14,
}

export const QueueCommands = {
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
};

export type PersistenceStrategy = 'memory' | 'file_sync' | 'file_async';

export type PersistenceOptions =
  | { strategy: 'memory' }
  | { strategy: 'file_sync' }
  | { strategy: 'file_async'; flushIntervalMs?: number };

export interface QueueConfig {
  visibilityTimeoutMs?: number;
  maxRetries?: number;
  ttlMs?: number;
  persistence?: PersistenceOptions;
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

export class NexoQueue<T = any> {
  private isSubscribed = false;

  constructor(
    private conn: NexoConnection,
    public readonly name: string,
    private _config?: QueueConfig
  ) { }

  async create(config: QueueConfig = {}): Promise<this> {
    await QueueCommands.create(this.conn, this.name, config);
    return this;
  }

  async exists(): Promise<boolean> {
    return QueueCommands.exists(this.conn, this.name);
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
      while (active && this.conn.isConnected) {
        try {
          const messages = await QueueCommands.consume<T>(this.conn, this.name, batchSize, waitMs);
          if (messages.length === 0) continue;

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
    await QueueCommands.ack(this.conn, this.name, id);
  }
}

import { NexoConnection } from '../connection';
import { Opcode } from '../protocol';
import { FrameCodec, Cursor } from '../codec';
import { logger } from '../utils/logger';

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

export interface PushOptions {
  priority?: number;
  delayMs?: number;
}

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

export class NexoQueue<T = any> {
  private isSubscribed = false;

  constructor(
    private conn: NexoConnection,
    public readonly name: string,
    private _config?: QueueConfig
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

  async subscribe(callback: (data: T) => Promise<any> | any, options: QueueSubscribeOptions = {}): Promise<{ stop: () => void }> {
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

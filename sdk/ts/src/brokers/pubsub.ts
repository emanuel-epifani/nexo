import { NexoConnection } from '../connection';
import { Logger } from '../utils/logger';

enum PubSubOpcode {
  PUB = 0x21,
  SUB = 0x22,
  UNSUB = 0x23,
}

const PubSubCommands = {
  publish: (conn: NexoConnection, topic: string, data: any, options: PublishOptions) => {
    const retain = options?.retain === true;
    if (options?.ttl !== undefined && (options.ttl < 0 || !Number.isInteger(options.ttl))) {
      throw new Error(`[PubSub] Invalid ttl: ${options.ttl}`);
    }
    const hasTtl = options?.ttl !== undefined;
    const flags = (retain ? 0x01 : 0x00) | (hasTtl ? 0x02 : 0x00);
    return conn.send(PubSubOpcode.PUB, w => {
      w.string(topic).u8(flags);
      if (hasTtl) w.u32(options!.ttl!);
      w.any(data);
    });
  },

  clear: (conn: NexoConnection, topic: string) =>
    conn.send(PubSubOpcode.PUB, w => w.string(topic).u8(0x04).any(Buffer.alloc(0))),

  subscribe: (conn: NexoConnection, topic: string) =>
    conn.send(PubSubOpcode.SUB, w => w.string(topic)),

  unsubscribe: (conn: NexoConnection, topic: string) =>
    conn.send(PubSubOpcode.UNSUB, w => w.string(topic)),
};

export interface PublishOptions {
  retain?: boolean;
  ttl?: number;
}

export class NexoTopic<T = any> {
  constructor(private broker: NexoPubSub, public readonly name: string) { }
  async publish(data: T, options?: PublishOptions) { return this.broker.publish(this.name, data, options); }
  async clear() { return this.broker.clear(this.name); }
  async subscribe(cb: (data: T) => void) { return this.broker.subscribe(this.name, cb); }
  async unsubscribe() { return this.broker.unsubscribe(this.name); }
}

type Handler = (data: any) => void | Promise<void>;

class Subscription {
  handler: Handler;
  queue: any[] = [];
  pending: Promise<void> | null = null;
  running = false;

  constructor(handler: Handler) {
    this.handler = handler;
  }
}

export class NexoPubSub {
  private exact = new Map<string, Subscription>();
  private wild = new Map<string, { parts: string[], sub: Subscription }>();

  constructor(private conn: NexoConnection, private logger: Logger) {
    conn.onPush = (topic, data) => this.dispatch(topic, data);

    conn.on('reconnect', async () => {
      const topics = [...this.exact.keys(), ...this.wild.keys()];
      if (topics.length === 0) return;
      this.logger.info(`[PubSub] Restoring ${topics.length} subscription(s)...`);
      const results = await Promise.allSettled(
        topics.map(t => PubSubCommands.subscribe(this.conn, t))
      );
      results.forEach((r, i) => {
        if (r.status === 'rejected') {
          this.logger.error(`[PubSub] Failed to resubscribe to ${topics[i]}`, r.reason);
        }
      });
    });
  }

  async publish(topic: string, data: any, options?: PublishOptions): Promise<void> {
    await PubSubCommands.publish(this.conn, topic, data, options || {});
  }

  async clear(topic: string): Promise<void> {
    await PubSubCommands.clear(this.conn, topic);
  }

  async subscribe(topic: string, callback: Handler): Promise<void> {
    if (this.exact.has(topic) || this.wild.has(topic)) {
      throw new Error(`[PubSub] Already subscribed to "${topic}". Call unsubscribe() first.`);
    }

    const sub = new Subscription(callback);
    const isWild = NexoPubSub.isWildcard(topic);
    if (isWild) {
      this.wild.set(topic, { parts: topic.split('/'), sub });
    } else {
      this.exact.set(topic, sub);
    }

    try {
      await PubSubCommands.subscribe(this.conn, topic);
    } catch (e) {
      if (isWild) this.wild.delete(topic);
      else this.exact.delete(topic);
      throw e;
    }
  }

  async unsubscribe(topic: string): Promise<void> {
    const exactSub = this.exact.get(topic);
    const wildEntry = this.wild.get(topic);
    if (!exactSub && !wildEntry) return;

    await PubSubCommands.unsubscribe(this.conn, topic);
    this.exact.delete(topic);
    this.wild.delete(topic);
  }

  private dispatch(topic: string, data: any) {
    const exactSub = this.exact.get(topic);
    if (exactSub) {
      this.enqueue(exactSub, data);
    }

    if (this.wild.size === 0) return;

    const tParts = topic.split('/');
    for (const { parts, sub } of this.wild.values()) {
      if (NexoPubSub.matchesParts(parts, tParts)) {
        this.enqueue(sub, data);
      }
    }
  }

  private enqueue(sub: Subscription, data: any): void {
    sub.queue.push(data);
    if (!sub.running) {
      sub.running = true;
      sub.pending = this.consume(sub);
    }
  }

  private async consume(sub: Subscription): Promise<void> {
    while (sub.queue.length > 0) {
      const data = sub.queue.shift()!;
      try {
        await sub.handler(data);
      } catch (e) {
        this.logger.error('[PubSub] handler error', e);
      }
    }
    sub.running = false;
  }

  private static isWildcard(topic: string): boolean {
    return topic.includes('+') || topic.includes('#');
  }

  private static matchesParts(pParts: string[], tParts: string[]): boolean {
    for (let i = 0; i < pParts.length; i++) {
      if (pParts[i] === '#') return true;
      if (i >= tParts.length || (pParts[i] !== '+' && pParts[i] !== tParts[i])) return false;
    }
    return pParts.length === tParts.length;
  }
}

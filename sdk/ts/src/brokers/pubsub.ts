import { NexoConnection } from '../connection';
import { Logger } from '../utils/logger';

enum PubSubOpcode {
  PUB = 0x21,
  SUB = 0x22,
  UNSUB = 0x23,
}

const PubSubCommands = {
  publish: (conn: NexoConnection, topic: string, data: any, options: PublishOptions) =>
    conn.send(PubSubOpcode.PUB, w => w
      .string(topic)
      .string(JSON.stringify(options || {}))
      .any(data)
    ),

  subscribe: (conn: NexoConnection, topic: string) =>
    conn.send(PubSubOpcode.SUB, w => w.string(topic)),

  unsubscribe: (conn: NexoConnection, topic: string) =>
    conn.send(PubSubOpcode.UNSUB, w => w.string(topic)),
};

export interface PublishOptions {
  retain?: boolean;
}

export class NexoTopic<T = any> {
  constructor(private broker: NexoPubSub, public readonly name: string) { }
  async publish(data: T, options?: PublishOptions) { return this.broker.publish(this.name, data, options); }
  async subscribe(cb: (data: T) => void) { return this.broker.subscribe(this.name, cb); }
  async unsubscribe() { return this.broker.unsubscribe(this.name); }
}

type Handler = (data: any) => void;

export class NexoPubSub {
  private exact = new Map<string, Handler>();
  private wild = new Map<string, { parts: string[], cb: Handler }>();

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

  async subscribe(topic: string, callback: Handler): Promise<void> {
    if (this.exact.has(topic) || this.wild.has(topic)) {
      throw new Error(`[PubSub] Already subscribed to "${topic}". Call unsubscribe() first.`);
    }

    const isWild = NexoPubSub.isWildcard(topic);
    if (isWild) {
      this.wild.set(topic, { parts: topic.split('/'), cb: callback });
    } else {
      this.exact.set(topic, callback);
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
    if (!this.exact.has(topic) && !this.wild.has(topic)) return;

    await PubSubCommands.unsubscribe(this.conn, topic);
    this.exact.delete(topic);
    this.wild.delete(topic);
  }

  private dispatch(topic: string, data: any) {
    const exactCb = this.exact.get(topic);
    if (exactCb) {
      try { exactCb(data); } catch (e) { this.logger.error('[PubSub] handler error', e); }
    }

    if (this.wild.size === 0) return;

    const tParts = topic.split('/');
    for (const { parts, cb } of this.wild.values()) {
      if (NexoPubSub.matchesParts(parts, tParts)) {
        try { cb(data); } catch (e) { this.logger.error('[PubSub] handler error', e); }
      }
    }
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

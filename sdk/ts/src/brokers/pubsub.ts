import { NexoConnection } from '../connection';
import { FrameCodec } from '../codec';
import { Logger } from '../utils/logger';

enum PubSubOpcode {
  PUB = 0x21,
  SUB = 0x22,
  UNSUB = 0x23,
}

const PubSubCommands = {
  publish: (conn: NexoConnection, topic: string, data: any, options: PublishOptions) =>
    conn.send(
      PubSubOpcode.PUB,
      FrameCodec.string(topic),
      FrameCodec.string(JSON.stringify(options || {})),
      FrameCodec.any(data)
    ),

  subscribe: (conn: NexoConnection, topic: string) =>
    conn.send(PubSubOpcode.SUB, FrameCodec.string(topic)),

  unsubscribe: (conn: NexoConnection, topic: string) =>
    conn.send(PubSubOpcode.UNSUB, FrameCodec.string(topic)),
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
  private exact = new Map<string, Handler[]>();
  private wild = new Map<string, { parts: string[], cbs: Handler[] }>();

  constructor(private conn: NexoConnection, private logger: Logger) {
    conn.onPush = (topic, data) => this.dispatch(topic, data);

    conn.on('reconnect', async () => {
      this.logger.info("[PubSub] Restoring subscriptions...");
      const topics = [...this.exact.keys(), ...this.wild.keys()];
      for (const topic of topics) {
        try {
          await PubSubCommands.subscribe(this.conn, topic);
        } catch (e) {
          this.logger.error(`[PubSub] Failed to resubscribe to ${topic}`, e);
        }
      }
    });
  }

  async publish(topic: string, data: any, options?: PublishOptions): Promise<void> {
    await PubSubCommands.publish(this.conn, topic, data, options || {});
  }

  async subscribe(topic: string, callback: Handler): Promise<void> {
    if (NexoPubSub.isWildcard(topic)) {
      let entry = this.wild.get(topic);
      if (!entry) {
        entry = { parts: topic.split('/'), cbs: [] };
        this.wild.set(topic, entry);
        await PubSubCommands.subscribe(this.conn, topic);
      }
      entry.cbs.push(callback);
    } else {
      let cbs = this.exact.get(topic);
      if (!cbs) {
        cbs = [];
        this.exact.set(topic, cbs);
        await PubSubCommands.subscribe(this.conn, topic);
      }
      cbs.push(callback);
    }
  }

  async unsubscribe(topic: string): Promise<void> {
    if (this.exact.delete(topic) || this.wild.delete(topic)) {
      await PubSubCommands.unsubscribe(this.conn, topic);
    }
  }

  private dispatch(topic: string, data: any) {
    const exactCbs = this.exact.get(topic);
    if (exactCbs) {
      for (const cb of exactCbs) {
        try { cb(data); } catch (e) { this.logger.error('[PubSub] handler error', e); }
      }
    }

    if (this.wild.size === 0) return;

    const tParts = topic.split('/');
    for (const { parts, cbs } of this.wild.values()) {
      if (NexoPubSub.matchesParts(parts, tParts)) {
        for (const cb of cbs) {
          try { cb(data); } catch (e) { this.logger.error('[PubSub] handler error', e); }
        }
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

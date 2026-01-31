import { NexoConnection } from '../connection';
import { FrameCodec } from '../codec';

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

export class NexoPubSub {
  private handlers = new Map<string, Array<(data: any) => void>>();

  constructor(private conn: NexoConnection) {
    conn.onPush = (topic, data) => this.dispatch(topic, data);
  }

  async publish(topic: string, data: any, options?: PublishOptions): Promise<void> {
    await PubSubCommands.publish(this.conn, topic, data, options || {});
  }

  async subscribe(topic: string, callback: (data: any) => void): Promise<void> {
    if (!this.handlers.has(topic)) this.handlers.set(topic, []);
    this.handlers.get(topic)!.push(callback);

    if (this.handlers.get(topic)!.length === 1) {
      await PubSubCommands.subscribe(this.conn, topic);
    }
  }

  async unsubscribe(topic: string): Promise<void> {
    if (this.handlers.has(topic)) {
      this.handlers.delete(topic);
      await PubSubCommands.unsubscribe(this.conn, topic);
    }
  }

  private dispatch(topic: string, data: any) {
    this.handlers.get(topic)?.forEach(cb => { try { cb(data); } catch (e) { console.error(e); } });
    for (const [pattern, cbs] of this.handlers) {
      if (pattern === topic) continue;
      if (this.matches(pattern, topic)) {
        cbs.forEach(cb => { try { cb(data); } catch (e) { console.error(e); } });
      }
    }
  }

  private matches(pattern: string, topic: string): boolean {
    const pParts = pattern.split('/');
    const tParts = topic.split('/');
    for (let i = 0; i < pParts.length; i++) {
      if (pParts[i] === '#') return true;
      if (i >= tParts.length || (pParts[i] !== '+' && pParts[i] !== tParts[i])) return false;
    }
    return pParts.length === tParts.length;
  }
}

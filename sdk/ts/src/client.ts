import { logger } from './utils/logger';
import {NexoOptions, Opcode, QueueConfig} from './protocol';
import { NexoConnection } from './connection';
import { NexoStore } from './brokers/store';
import { NexoQueue } from './brokers/queue';
import { NexoPubSub, NexoTopic } from './brokers/pubsub';
import { NexoStream } from './brokers/stream';
import { FrameCodec } from './codec';

export * from './protocol';
export * from './connection';
export * from './brokers/store';
export * from './brokers/queue';
export * from './brokers/pubsub';
export * from './brokers/stream';

export class NexoClient {
  private conn: NexoConnection;
  private queues = new Map<string, NexoQueue<any>>();
  private streams = new Map<string, NexoStream<any>>();
  private topics = new Map<string, NexoTopic<any>>();

  public readonly store: NexoStore;
  private readonly pubsubBroker: NexoPubSub;

  constructor(options: NexoOptions = {}) {
    this.conn = new NexoConnection(options.host || '127.0.0.1', options.port || 7654, options);
    this.store = new NexoStore(this.conn);
    this.pubsubBroker = new NexoPubSub(this.conn);
    this.setupGracefulShutdown();
  }

  static async connect(options?: NexoOptions): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.conn.connect();
    return client;
  }

  get connected() { return this.conn.isConnected; }
  disconnect() { this.conn.disconnect(); }

  queue<T = any>(name: string, config?: QueueConfig): NexoQueue<T> {
    if (!this.queues.has(name)) this.queues.set(name, new NexoQueue<T>(this.conn, name, config));
    return this.queues.get(name) as NexoQueue<T>;
  }

  stream<T = any>(name: string, group?: string): NexoStream<T> {
    const key = group ? `${name}:${group}` : name;
    if (!this.streams.has(key)) this.streams.set(key, new NexoStream<T>(this.conn, name, group));
    return this.streams.get(key) as NexoStream<T>;
  }

  pubsub<T = any>(name: string): NexoTopic<T> {
    if (!this.topics.has(name)) this.topics.set(name, new NexoTopic<T>(this.pubsubBroker, name));
    return this.topics.get(name) as NexoTopic<T>;
  }

  get debug() {
    return {
      echo: async (data: any) => {
        const res = await this.conn.send(Opcode.DEBUG_ECHO, FrameCodec.any(data));
        return FrameCodec.decodeAny(res.cursor);
      }
    };
  }

  private setupGracefulShutdown() {
    const shutdown = async () => {
      logger.info("Graceful shutdown triggered. Disconnecting...");
      this.disconnect();
      process.exit(0);
    };

    if (typeof process !== 'undefined' && process.listenerCount && process.listenerCount('SIGINT') === 0) {
      process.on('SIGINT', shutdown);
      process.on('SIGTERM', shutdown);
    }
  }
}

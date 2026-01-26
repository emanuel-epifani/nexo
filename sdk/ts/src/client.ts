import { logger } from './utils/logger';
import {NexoOptions, Opcode, QueueConfig} from './protocol';
import { NexoConnection } from './connection';
import { NexoStore } from './brokers/store';
import { NexoQueue } from './brokers/queue';
import { NexoPubSub, NexoTopic } from './brokers/pubsub';
import { NexoStream } from './brokers/stream';
import { FrameCodec } from './codec';

export class NexoClient {
  private conn: NexoConnection;
  private queues = new Map<string, NexoQueue<any>>();
  private streams = new Map<string, NexoStream<any>>();
  private topics = new Map<string, NexoTopic<any>>();

  public readonly store: NexoStore;
  private readonly pubsubBroker: NexoPubSub;

  constructor(options: NexoOptions) {
    this.conn = new NexoConnection(options.host, options.port, options);
    this.store = new NexoStore(this.conn);
    this.pubsubBroker = new NexoPubSub(this.conn);
    this.setupGracefulShutdown();
  }

  static async connect(options: NexoOptions): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.conn.connect();
    return client;
  }

  get connected() { return this.conn.isConnected; }
  disconnect() { this.conn.disconnect(); }

  queue<T = any>(name: string, config?: QueueConfig): NexoQueue<T> {
    let q = this.queues.get(name);
    if (!q) {
      q = new NexoQueue<T>(this.conn, name, config);
      this.queues.set(name, q);
    }
    return q;
  }

  stream<T = any>(name: string, group?: string): NexoStream<T> {
    const key = group ? `${name}:${group}` : name;
    let s = this.streams.get(key);
    if (!s) {
      s = new NexoStream<T>(this.conn, name, group);
      this.streams.set(key, s);
    }
    return s;
  }

  pubsub<T = any>(name: string): NexoTopic<T> {
    let t = this.topics.get(name);
    if (!t) {
      t = new NexoTopic<T>(this.pubsubBroker, name);
      this.topics.set(name, t);
    }
    return t;
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

import { Logger, LogHandler } from './utils/logger';
import { NexoConnection } from './connection';
import { NexoStore } from './brokers/store';
import { NexoQueue, QueueConfig } from './brokers/queue';
import { NexoPubSub, NexoTopic } from './brokers/pubsub';
import { NexoStream } from './brokers/stream';
import { FrameCodec } from './codec';

export interface NexoOptions {
  host: string;
  port: number;
  requestTimeoutMs?: number;
  logger?: LogHandler;
  logLevel?: string;
}

export class NexoClient {
  private conn: NexoConnection;
  private queues = new Map<string, NexoQueue<any>>();
  private streams = new Map<string, NexoStream<any>>();
  private topics = new Map<string, NexoTopic<any>>();
  private logger: Logger;

  public readonly store: NexoStore;
  private readonly pubsubBroker: NexoPubSub;

  constructor(options: NexoOptions) {
    this.logger = new Logger({ handler: options.logger, level: options.logLevel });
    this.conn = new NexoConnection(options, this.logger);
    this.store = new NexoStore(this.conn);
    this.pubsubBroker = new NexoPubSub(this.conn, this.logger);
    this.setupGracefulShutdown();
  }

  static async connect(options: NexoOptions): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.conn.connect();
    return client;
  }

  disconnect() { this.conn.disconnect(); }

  queue<T = any>(name: string): NexoQueue<T> {
    let q = this.queues.get(name);
    if (!q) {
      q = new NexoQueue<T>(this.conn, name, this.logger);
      this.queues.set(name, q);
    }
    return q;
  }

  stream<T = any>(name: string): NexoStream<T> {
    let s = this.streams.get(name);
    if (!s) {
      s = new NexoStream<T>(this.conn, name, this.logger);
      this.streams.set(name, s);
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

  private setupGracefulShutdown() {
    const shutdown = async () => {
      this.logger.info("Graceful shutdown triggered. Disconnecting...");
      this.disconnect();
      process.exit(0);
    };

    if (typeof process !== 'undefined' && process.listenerCount && process.listenerCount('SIGINT') === 0) {
      process.on('SIGINT', shutdown);
      process.on('SIGTERM', shutdown);
    }
  }
}

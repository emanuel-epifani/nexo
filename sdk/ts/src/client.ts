import { Logger, LogHandler } from './utils/logger';
import { DEFAULT_CONFIG } from './config';
import { NexoConnection } from './connection';
import { NexoStore } from './brokers/store';
import { NexoQueue } from './brokers/queue';
import { NexoPubSub, NexoTopic } from './brokers/pubsub';
import { NexoStream } from './brokers/stream';

export interface NexoOptions {
  host: string;
  port: number;
  logger?: LogHandler;
  logLevel?: string;
}

export class NexoClient {
  private conn: NexoConnection;
  private logger: Logger;

  public readonly store: NexoStore;
  private readonly pubsubBroker: NexoPubSub;

  constructor(options: NexoOptions) {
    this.logger = new Logger({ 
      handler: options.logger, 
      level: options.logLevel ?? DEFAULT_CONFIG.logger.level 
    });

    this.conn = new NexoConnection({
      host: options.host,
      port: options.port,
      ...DEFAULT_CONFIG.connection,
    }, this.logger);

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
    return new NexoQueue<T>(this.conn, name, this.logger);
  }

  stream<T = any>(name: string): NexoStream<T> {
    return new NexoStream<T>(this.conn, name, this.logger);
  }

  pubsub<T = any>(name: string): NexoTopic<T> {
    return new NexoTopic<T>(this.pubsubBroker, name);
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

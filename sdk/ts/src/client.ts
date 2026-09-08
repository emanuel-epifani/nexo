import { Logger, LogHandler } from './utils/logger';
import { DEFAULT_CONFIG, DEFAULT_HOST, DEFAULT_PORT } from './config';
import { NexoConnection } from './transport/tcp/connection';
import { NexoStore } from './brokers/store';
import { NexoQueueFacade } from './brokers/queue';
import { NexoPubSub } from './brokers/pubsub';
import { NexoStreamFacade } from './brokers/stream';

export interface NexoOptions {
  host?: string;
  port?: number;
  logger?: LogHandler;
  logLevel?: string;
}

export class NexoClient {
  private conn: NexoConnection;
  private logger: Logger;

  public readonly store: NexoStore;
  public readonly queue: NexoQueueFacade;
  public readonly stream: NexoStreamFacade;
  public readonly pubsub: NexoPubSub;
  private shutdownHandler: (() => void) | null = null;

  constructor(options: NexoOptions = {}) {
    this.logger = new Logger({
      handler: options.logger,
      level: options.logLevel ?? DEFAULT_CONFIG.logger.level
    });

    this.conn = new NexoConnection({
      host: options.host ?? DEFAULT_HOST,
      port: options.port ?? DEFAULT_PORT,
      ...DEFAULT_CONFIG.connection,
    }, this.logger);

    this.store = new NexoStore(this.conn);
    this.queue = new NexoQueueFacade(this.conn, this.logger);
    this.stream = new NexoStreamFacade(this.conn, this.logger);
    this.pubsub = new NexoPubSub(this.conn, this.logger);
    this.setupGracefulShutdown();
  }

  static async connect(options: NexoOptions = {}): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.conn.connect();
    return client;
  }

  disconnect() {
    if (this.shutdownHandler && typeof process !== 'undefined') {
      process.removeListener('SIGINT', this.shutdownHandler);
      process.removeListener('SIGTERM', this.shutdownHandler);
      this.shutdownHandler = null;
    }
    this.conn.disconnect();
  }

  private setupGracefulShutdown() {
    this.shutdownHandler = () => {
      this.disconnect();
    };
    if (typeof process !== 'undefined') {
      process.on('SIGINT', this.shutdownHandler);
      process.on('SIGTERM', this.shutdownHandler);
    }
  }
}

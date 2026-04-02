export interface NexoConnectionConfig {
  host: string;
  port: number;
  requestTimeoutMs: number;
  reconnectDelayMs: number;
  sweepIntervalMs: number;
  backoff: {
    short: number;
    long: number;
  };
}

export interface NexoQueueConfig {
  batchSize: number;
  waitMs: number;
  concurrency: number;
  peek: {
    limit: number;
    offset: number;
  };
}

export interface NexoStreamConfig {
  batchSize: number;
  waitMs: number;
}

export const DEFAULT_CONFIG = {
  connection: {
    requestTimeoutMs: 15000,
    reconnectDelayMs: 1500,
    sweepIntervalMs: 1000,
    backoff: {
      short: 1000,
      long: 2000,
    },
  },
  queue: {
    batchSize: 50,
    waitMs: 20000,
    concurrency: 5,
    peek: {
      limit: 10,
      offset: 0,
    },
  },
  stream: {
    batchSize: 100,
    waitMs: 20000,
  },
  logger: {
    level: 'ERROR',
  },
} as const;

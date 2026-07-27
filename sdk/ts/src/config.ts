// Protocol defaults — must match server config and other SDKs.
// See README for canonical values.
export const DEFAULT_HOST = '127.0.0.1';
export const DEFAULT_PORT = 7654;

// Connection contract used by NexoConnection.
export interface NexoConnectionConfig {
  host: string;
  port: number;
  requestTimeoutMs: number;
  reconnectDelayMs: number;
  backoff: {
    short: number;
    long: number;
  };
}

// SDK runtime tuning — client-side behavior only.
export const DEFAULT_CONFIG = {
  connection: {
    requestTimeoutMs: 15000,
    reconnectDelayMs: 1500,
    backoff: {
      short: 1000,
      long: 2000,
    },
  },
  queue: {
    batchSize: 50,
    waitMs: 20000,
    concurrency: 5,
    stopTimeoutMs: 5000,
    peek: {
      limit: 10,
      offset: 0,
    },
  },
  stream: {
    batchSize: 100,
    waitMs: 20000,
    concurrency: 1,
    stopTimeoutMs: 30000,
  },
  logger: {
    level: 'ERROR',
  },
} as const;

export class NexoError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NexoError';
  }
}

export class ConnectionClosedError extends NexoError {
  constructor() {
    super("Connection closed");
    this.name = 'ConnectionClosedError';
  }
}

export class RequestTimeoutError extends NexoError {
  constructor(timeoutMs: number) {
    super(`Request timeout after ${timeoutMs}ms`);
    this.name = 'RequestTimeoutError';
  }
}

export class NotConnectedError extends NexoError {
  constructor() {
    super("Client not connected");
    this.name = 'NotConnectedError';
  }
}

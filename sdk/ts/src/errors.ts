import { ErrorCode } from './protocol/generated';

export interface ConfigurationDifference {
  path: string;
  requested: unknown;
  actual: unknown;
}

export interface ResourceConfigurationConflictDetails {
  resourceKind: 'queue' | 'stream';
  resourceName: string;
  requested: Record<string, unknown>;
  actual: Record<string, unknown>;
  differences: ConfigurationDifference[];
}

export class NexoError<TDetails = unknown> extends Error {
  constructor(
    message: string,
    public readonly code?: ErrorCode | number,
    public readonly details?: TDetails,
  ) {
    super(message);
    this.name = 'NexoError';
  }
}

export class InternalError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.INTERNAL, details);
    this.name = 'InternalError';
  }
}

export class InvalidArgumentError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.INVALID_ARGUMENT, details);
    this.name = 'InvalidArgumentError';
  }
}

export class ResourceNotFoundError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.RESOURCE_NOT_FOUND, details);
    this.name = 'ResourceNotFoundError';
  }
}

export class ResourceConfigurationConflictError extends NexoError<ResourceConfigurationConflictDetails> {
  constructor(message: string, details?: ResourceConfigurationConflictDetails) {
    super(message, ErrorCode.RESOURCE_CONFIG_CONFLICT, details);
    this.name = 'ResourceConfigurationConflictError';
  }
}

export class NotAuthorizedError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.NOT_AUTHORIZED, details);
    this.name = 'NotAuthorizedError';
  }
}

export class FencedError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.FENCED, details);
    this.name = 'FencedError';
  }
}

export class NotMemberError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.NOT_MEMBER, details);
    this.name = 'NotMemberError';
  }
}

export class SlowConsumerError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.SLOW_CONSUMER, details);
    this.name = 'SlowConsumerError';
  }
}

export class StorageError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.STORAGE_ERROR, details);
    this.name = 'StorageError';
  }
}

export class ProtocolError extends NexoError {
  constructor(message: string, details?: unknown) {
    super(message, ErrorCode.PROTOCOL_ERROR, details);
    this.name = 'ProtocolError';
  }
}

export function errorFromCode(code: number, message: string, details?: unknown): NexoError {
  switch (code) {
    case ErrorCode.INTERNAL:
      return new InternalError(message, details);
    case ErrorCode.INVALID_ARGUMENT:
      return new InvalidArgumentError(message, details);
    case ErrorCode.RESOURCE_NOT_FOUND:
      return new ResourceNotFoundError(message, details);
    case ErrorCode.RESOURCE_CONFIG_CONFLICT:
      return new ResourceConfigurationConflictError(message, details as ResourceConfigurationConflictDetails | undefined);
    case ErrorCode.NOT_AUTHORIZED:
      return new NotAuthorizedError(message, details);
    case ErrorCode.FENCED:
      return new FencedError(message, details);
    case ErrorCode.NOT_MEMBER:
      return new NotMemberError(message, details);
    case ErrorCode.SLOW_CONSUMER:
      return new SlowConsumerError(message, details);
    case ErrorCode.STORAGE_ERROR:
      return new StorageError(message, details);
    case ErrorCode.PROTOCOL_ERROR:
      return new ProtocolError(message, details);
    default:
      return new NexoError(message, code, details);
  }
}

export function decodeErrorPayload(payload: Buffer): NexoError {
  if (payload.length < 5) {
    return new ProtocolError('Malformed error response payload');
  }

  const code = payload.readUInt8(0);
  const messageLength = payload.readUInt32BE(1);
  const messageEnd = 5 + messageLength;
  if (messageEnd > payload.length) {
    return new ProtocolError('Malformed error response message length');
  }

  const message = payload.toString('utf8', 5, messageEnd);
  if (messageEnd === payload.length) {
    return errorFromCode(code, message);
  }

  try {
    const details = JSON.parse(payload.toString('utf8', messageEnd));
    return errorFromCode(code, message, details);
  } catch {
    return new ProtocolError('Malformed error response details', { code, message });
  }
}

export class ConnectionClosedError extends NexoError {
  constructor() {
    super('Connection closed');
    this.name = 'ConnectionClosedError';
  }
}

export class RequestTimeoutError extends NexoError {
  constructor(timeoutMs: number) {
    super(`Request timeout after ${timeoutMs}ms`);
    this.name = 'RequestTimeoutError';
  }
}

export class RequestCancelledError extends NexoError {
  constructor() {
    super('Request cancelled');
    this.name = 'RequestCancelledError';
  }
}

export class NotConnectedError extends NexoError {
  constructor() {
    super('Client not connected');
    this.name = 'NotConnectedError';
  }
}

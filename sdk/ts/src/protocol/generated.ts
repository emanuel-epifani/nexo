export const PROTOCOL_VERSION = 0x07;

export enum FrameType {
  REQUEST = 0x01,
  RESPONSE = 0x02,
  PUSH_PUBSUB = 0x03,
  REQUEST_NO_RESPONSE = 0x04,
}

export enum ResponseStatus {
  OK = 0x00,
  ERR = 0x01,
  NULL = 0x02,
  DATA = 0x03,
}

export enum ErrorCode {
  INTERNAL = 0x00,
  INVALID_ARGUMENT = 0x01,
  RESOURCE_NOT_FOUND = 0x02,
  RESOURCE_CONFIG_CONFLICT = 0x03,
  NOT_AUTHORIZED = 0x04,
  FENCED = 0x05,
  NOT_MEMBER = 0x06,
  SLOW_CONSUMER = 0x07,
  STORAGE_ERROR = 0x08,
  PROTOCOL_ERROR = 0x09,
}

export enum ProvisionStatus {
  CREATED = 0x01,
  UNCHANGED = 0x02,
}

export enum DataType {
  RAW = 0x00,
  STRING = 0x01,
  JSON = 0x02,
  INT = 0x03,
}

export const HEADER_SIZE = 11;
export const HEADER_OFFSET = {
  VERSION: 0,
  TYPE: 1,
  META: 2,
  ID: 3,
  PAYLOAD_LEN: 7,
} as const;

export enum StoreOpcode {
  MAP_SET = 0x02,
  MAP_GET = 0x03,
  MAP_DEL = 0x04,
  MAP_INCR = 0x05,
  MAP_CLEAR_ALL = 0x06,
  MAP_CLEAR_PREFIX = 0x07,
}

export enum QueueOpcode {
  Q_CREATE = 0x10,
  Q_PUSH = 0x11,
  Q_CONSUME = 0x12,
  Q_ACK = 0x13,
  Q_EXISTS = 0x14,
  Q_DELETE = 0x15,
  Q_PEEK_DLQ = 0x16,
  Q_MOVE_TO_QUEUE = 0x17,
  Q_DELETE_DLQ = 0x18,
  Q_PURGE_DLQ = 0x19,
  Q_NACK = 0x1A,
  Q_DESCRIBE = 0x1B,
}

export enum PubSubOpcode {
  PUB = 0x21,
  SUB = 0x22,
  UNSUB = 0x23,
}

export enum StreamOpcode {
  S_CREATE = 0x30,
  S_PUB = 0x31,
  S_FETCH = 0x32,
  S_JOIN = 0x33,
  S_ACK = 0x34,
  S_EXISTS = 0x35,
  S_DELETE = 0x36,
  S_DESCRIBE = 0x37,
  S_SEEK = 0x38,
  S_LEAVE = 0x39,
  S_PEEK_DLS = 0x3A,
  S_MOVE_TO_STREAM = 0x3B,
  S_DELETE_DLS = 0x3C,
  S_PURGE_DLS = 0x3D,
}

export const QUEUE_MAX_PUSH_ITEMS = 10_000;
export const STREAM_MAX_PUBLISH_BATCH = 65_536;
export const STREAM_MAX_FETCH_BATCH_SIZE = 65_536;
export const STREAM_MAX_KEY_BYTES = 65_535;

export const FLAG_STORE_MAP_SET_HAS_TTL = 0x01;
export const FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT = 0x01;
export const FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES = 0x02;
export const FLAG_QUEUE_Q_PUSH_HAS_PRIORITY = 0x01;
export const FLAG_PUBSUB_PUB_RETAIN = 0x01;
export const FLAG_PUBSUB_PUB_HAS_TTL = 0x02;
export const FLAG_PUBSUB_PUB_CLEAR = 0x04;
export const FLAG_STREAM_S_CREATE_HAS_MAX_AGE = 0x01;
export const FLAG_STREAM_S_CREATE_HAS_MAX_BYTES = 0x02;

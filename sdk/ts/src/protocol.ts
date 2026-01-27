/** @internal */
export enum FrameType {
  REQUEST = 0x01,
  RESPONSE = 0x02,
  PUSH = 0x03,
  ERROR = 0x04,
  PING = 0x05,
  PONG = 0x06,
}

/** @internal */
export enum ResponseStatus {
  OK = 0x00,
  ERR = 0x01,
  NULL = 0x02,
  DATA = 0x03,
}

/** @internal */
export enum Opcode {
  DEBUG_ECHO = 0x00,
  // Store KV: 0x02 - 0x05
  KV_SET = 0x02,
  KV_GET = 0x03,
  KV_DEL = 0x04,
  // Future: KV_INCR = 0x05
  // Future Store Hash: 0x06 - 0x09
  // Queue: 0x10 - 0x1F
  Q_CREATE = 0x10,
  Q_PUSH = 0x11,
  Q_CONSUME = 0x12,
  Q_ACK = 0x13,
  Q_EXISTS = 0x14,
  // Topic: 0x20 - 0x2F
  PUB = 0x21,
  SUB = 0x22,
  UNSUB = 0x23,
  // Stream: 0x30 - 0x3F
  S_CREATE = 0x30,
  S_PUB = 0x31,
  S_FETCH = 0x32,
  S_JOIN = 0x33,
  S_COMMIT = 0x34,
  S_EXISTS = 0x35,
}

/** @internal */
export enum DataType {
  RAW = 0x00,
  STRING = 0x01,
  JSON = 0x02,
}

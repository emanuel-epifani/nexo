/** @internal */
export enum FrameType {
  REQUEST = 0x01,
  RESPONSE = 0x02,
  PUSH = 0x03,
}

/** @internal */
export enum PushType {
  PUBSUB = 0x01,
}

/** @internal */
export enum ResponseStatus {
  OK = 0x00,
  ERR = 0x01,
  NULL = 0x02,
  DATA = 0x03,
}

/** @internal */
export enum DataType {
  RAW = 0x00,
  STRING = 0x01,
  JSON = 0x02,
}

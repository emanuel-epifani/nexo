/**
 * Protocol version: first byte of every frame header. Must match the server's
 * PROTOCOL_VERSION; mismatched frames are rejected on both ends.
 * @internal
 */
export const PROTOCOL_VERSION = 0x05;

/** @internal */
export enum FrameType {
  REQUEST = 0x01,
  RESPONSE = 0x02,
  PUSH_PUBSUB = 0x03,
  REQUEST_NO_RESPONSE = 0x04,
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
  INT = 0x03,
}

/**
 * Frame header layout (11 bytes, big-endian):
 * [Version:1][FrameType:1][Meta:1][CorrelationID:4][PayloadLen:4]
 * @internal
 */
export const HEADER_SIZE = 11;
export const HEADER_OFFSET = {
  VERSION: 0,
  TYPE: 1,
  META: 2,
  ID: 3,
  PAYLOAD_LEN: 7,
} as const;

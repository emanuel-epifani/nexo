from enum import IntEnum

PROTOCOL_VERSION = 0x05

HEADER_SIZE = 11
HEADER_OFFSET_VERSION = 0
HEADER_OFFSET_TYPE = 1
HEADER_OFFSET_META = 2
HEADER_OFFSET_ID = 3
HEADER_OFFSET_PAYLOAD_LEN = 7


class FrameType(IntEnum):
    REQUEST = 0x01
    RESPONSE = 0x02
    PUSH_PUBSUB = 0x03
    REQUEST_NO_RESPONSE = 0x04


class ResponseStatus(IntEnum):
    OK = 0x00
    ERR = 0x01
    NULL = 0x02
    DATA = 0x03


class DataType(IntEnum):
    RAW = 0x00
    STRING = 0x01
    JSON = 0x02
    INT = 0x03

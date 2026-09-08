pub const PROTOCOL_VERSION: u8 = 0x07;
pub const HEADER_SIZE: usize = 11;
pub const HEADER_OFFSET_VERSION: usize = 0;
pub const HEADER_OFFSET_TYPE: usize = 1;
pub const HEADER_OFFSET_META: usize = 2;
pub const HEADER_OFFSET_ID: usize = 3;
pub const HEADER_OFFSET_PAYLOAD_LEN: usize = 7;

pub const TYPE_REQUEST: u8 = 0x01;
pub const TYPE_RESPONSE: u8 = 0x02;
pub const TYPE_PUSH_PUBSUB: u8 = 0x03;
pub const TYPE_REQUEST_NO_RESPONSE: u8 = 0x04;

pub const STATUS_OK: u8 = 0x00;
pub const STATUS_ERR: u8 = 0x01;
pub const STATUS_NULL: u8 = 0x02;
pub const STATUS_DATA: u8 = 0x03;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Internal = 0x00,
    InvalidArgument = 0x01,
    ResourceNotFound = 0x02,
    ResourceConfigConflict = 0x03,
    NotAuthorized = 0x04,
    Fenced = 0x05,
    NotMember = 0x06,
    SlowConsumer = 0x07,
    StorageError = 0x08,
    ProtocolError = 0x09,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionStatus {
    Created = 0x01,
    Unchanged = 0x02,
}

pub const DATA_TYPE_RAW: u8 = 0x00;
pub const DATA_TYPE_STRING: u8 = 0x01;
pub const DATA_TYPE_JSON: u8 = 0x02;
pub const DATA_TYPE_INT: u8 = 0x03;

pub const OP_DEBUG_ECHO: u8 = 0x00;

pub const STORE_OPCODE_MIN: u8 = 0x02;
pub const STORE_OPCODE_MAX: u8 = 0x0F;
pub const OP_MAP_SET: u8 = 0x02;
pub const OP_MAP_GET: u8 = 0x03;
pub const OP_MAP_DEL: u8 = 0x04;
pub const OP_MAP_INCR: u8 = 0x05;
pub const OP_MAP_CLEAR_ALL: u8 = 0x06;
pub const OP_MAP_CLEAR_PREFIX: u8 = 0x07;

pub const QUEUE_OPCODE_MIN: u8 = 0x10;
pub const QUEUE_OPCODE_MAX: u8 = 0x1F;
pub const OP_Q_CREATE: u8 = 0x10;
pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_CONSUME: u8 = 0x12;
pub const OP_Q_ACK: u8 = 0x13;
pub const OP_Q_EXISTS: u8 = 0x14;
pub const OP_Q_DELETE: u8 = 0x15;
pub const OP_Q_PEEK_DLQ: u8 = 0x16;
pub const OP_Q_MOVE_TO_QUEUE: u8 = 0x17;
pub const OP_Q_DELETE_DLQ: u8 = 0x18;
pub const OP_Q_PURGE_DLQ: u8 = 0x19;
pub const OP_Q_NACK: u8 = 0x1A;
pub const OP_Q_DESCRIBE: u8 = 0x1B;

pub const PUBSUB_OPCODE_MIN: u8 = 0x21;
pub const PUBSUB_OPCODE_MAX: u8 = 0x2F;
pub const OP_PUB: u8 = 0x21;
pub const OP_SUB: u8 = 0x22;
pub const OP_UNSUB: u8 = 0x23;

pub const STREAM_OPCODE_MIN: u8 = 0x30;
pub const STREAM_OPCODE_MAX: u8 = 0x3F;
pub const OP_S_CREATE: u8 = 0x30;
pub const OP_S_PUB: u8 = 0x31;
pub const OP_S_FETCH: u8 = 0x32;
pub const OP_S_JOIN: u8 = 0x33;
pub const OP_S_ACK: u8 = 0x34;
pub const OP_S_EXISTS: u8 = 0x35;
pub const OP_S_DELETE: u8 = 0x36;
pub const OP_S_DESCRIBE: u8 = 0x37;
pub const OP_S_SEEK: u8 = 0x38;
pub const OP_S_LEAVE: u8 = 0x39;
pub const OP_S_PEEK_DLT: u8 = 0x3A;
pub const OP_S_MOVE_TO_STREAM: u8 = 0x3B;
pub const OP_S_DELETE_DLT: u8 = 0x3C;
pub const OP_S_PURGE_DLT: u8 = 0x3D;

pub const QUEUE_MAX_PUSH_ITEMS: usize = 10_000;
pub const STREAM_MAX_PUBLISH_BATCH: usize = 65_536;
pub const STREAM_MAX_FETCH_BATCH_SIZE: usize = 65_536;
pub const STREAM_MAX_KEY_BYTES: usize = 65_535;

pub const FLAG_STORE_MAP_SET_HAS_TTL: u8 = 0x01;
pub const FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT: u8 = 0x01;
pub const FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES: u8 = 0x02;
pub const FLAG_QUEUE_Q_PUSH_HAS_PRIORITY: u8 = 0x01;
pub const FLAG_PUBSUB_PUB_RETAIN: u8 = 0x01;
pub const FLAG_PUBSUB_PUB_HAS_TTL: u8 = 0x02;
pub const FLAG_PUBSUB_PUB_CLEAR: u8 = 0x04;
pub const FLAG_STREAM_S_CREATE_HAS_MAX_AGE: u8 = 0x01;
pub const FLAG_STREAM_S_CREATE_HAS_MAX_BYTES: u8 = 0x02;

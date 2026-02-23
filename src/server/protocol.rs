//! Nexo Binary Protocol Specification
//!
//! Request Frame (Total Header: 10 bytes):
//! [FrameType: 1 byte] [Opcode: 1 byte] [CorrelationID: 4 bytes (BigEndian)] [PayloadLen: 4 bytes (BigEndian)]
//! Payload: [...args (JSON strings)] [Data (if applicable)]
//!
//! Response Frame (Total Header: 10 bytes):
//! [FrameType: 1 byte] [Status: 1 byte] [CorrelationID: 4 bytes (BigEndian)] [PayloadLen: 4 bytes (BigEndian)]
//! Payload: [Data...] (no status byte, status is in header)
//!
//! Push Frame (Total Header: 10 bytes):
//! [FrameType: 1 byte] [PushType: 1 byte] [CorrelationID: 4 bytes (BigEndian)] [PayloadLen: 4 bytes (BigEndian)]
//! Payload: [Data...] 
//!
//! Data Structure (auto-contained):
//! [DataType: 1 byte] [Data...]
//!
//! DataTypes (1 byte):
//! 0x0: RAW (Binary)
//! 0x1: STRING (UTF-8)
//! 0x2: JSON (UTF-8)
//! 0x3-0xFF: Reserved

// ========================================
// FRAME TYPES (The "Envelope" Type)
// ========================================
pub const TYPE_REQUEST: u8  = 0x01;
pub const TYPE_RESPONSE: u8 = 0x02;
pub const TYPE_PUSH: u8     = 0x03;

// ========================================
// PUSH TYPES (In Push Header, byte 1)
// ========================================
pub const PUSH_TYPE_PUBSUB: u8 = 0x01;

// ========================================
// RESPONSE STATUS (In Response Header, byte 1)
// ========================================
pub const STATUS_OK: u8   = 0x00;
pub const STATUS_ERR: u8  = 0x01;
pub const STATUS_NULL: u8 = 0x02;
pub const STATUS_DATA: u8 = 0x03;

// ========================================
// DATA TYPE FLAGS (First byte of data payload)
// ========================================
/// Data types (1 byte)
pub const DATA_TYPE_RAW: u8    = 0x00;
pub const DATA_TYPE_STRING: u8 = 0x01;
pub const DATA_TYPE_JSON: u8   = 0x02;

// ========================================
// OPCODES RANGES
// ========================================
// STORE
pub const OPCODE_MIN_STORE: u8   = 0x02;
pub const OPCODE_MAX_STORE: u8   = 0x0F;
// QUEUE
pub const OPCODE_MIN_QUEUE: u8   = 0x10;
pub const OPCODE_MAX_QUEUE: u8   = 0x1F;
// PUBSUB
pub const OPCODE_MIN_PUBSUB: u8  = 0x21;
pub const OPCODE_MAX_PUBSUB: u8  = 0x2F;
// STREAM
pub const OPCODE_MIN_STREAM: u8  = 0x30;
pub const OPCODE_MAX_STREAM: u8  = 0x3F;



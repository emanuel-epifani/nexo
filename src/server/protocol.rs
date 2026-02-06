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
//! Data Structure (auto-contained):
//! [TypeFlags: 1 byte] [Bytes...]
//!
//! TypeFlags (1 byte):
//! [DataType: 4 bits] [Compressed: 1 bit] [Encrypted: 1 bit] [Reserved: 2 bits]
//!
//! DataTypes (4 bits = 16 values):
//! 0x0: RAW (Binary)
//! 0x1: STRING (UTF-8)
//! 0x2: JSON (UTF-8)
//! 0x3-0xF: Reserved

// ========================================
// PRIMITIVE SIZES (Fundamental building blocks)
// ========================================
pub const SIZE_U8: usize   = 1;
pub const SIZE_U16: usize  = 2;
pub const SIZE_U32: usize  = 4;
pub const SIZE_U64: usize  = 8;
pub const SIZE_UUID: usize = 16;

// ========================================
// FIELD ENCODING SIZES (Protocol-level constructs)
// ========================================
/// String length prefix (u32 BigEndian)
pub const SIZE_STRING_PREFIX: usize = SIZE_U32;

/// DataType indicator (u8)
pub const SIZE_DATA_TYPE: usize = SIZE_U8;

/// Status code in response payloads (u8)
pub const SIZE_STATUS: usize = SIZE_U8;

/// Opcode in request payloads (u8)
pub const SIZE_OPCODE: usize = SIZE_U8;

// ========================================
// HEADER GEOMETRY (Sizes in bytes)
// ========================================
pub const HEADER_SIZE_FRAME_TYPE: usize     = SIZE_U8;
pub const HEADER_SIZE_OPCODE_OR_STATUS: usize = SIZE_U8;  // Opcode for requests, Status for responses
pub const HEADER_SIZE_CORRELATION_ID: usize = SIZE_U32;
pub const HEADER_SIZE_PAYLOAD_LEN: usize    = SIZE_U32;

/// Total size of the binary header (Sum of all header fields)
pub const HEADER_SIZE: usize = HEADER_SIZE_FRAME_TYPE + HEADER_SIZE_OPCODE_OR_STATUS + HEADER_SIZE_CORRELATION_ID + HEADER_SIZE_PAYLOAD_LEN;

// Offsets within the header
pub const HEADER_OFFSET_FRAME_TYPE: usize     = 0;
pub const HEADER_OFFSET_OPCODE: usize         = HEADER_OFFSET_FRAME_TYPE + HEADER_SIZE_FRAME_TYPE;
pub const HEADER_OFFSET_STATUS: usize         = HEADER_OFFSET_FRAME_TYPE + HEADER_SIZE_FRAME_TYPE;  // Same position as opcode
pub const HEADER_OFFSET_CORRELATION_ID: usize = HEADER_OFFSET_OPCODE + HEADER_SIZE_OPCODE_OR_STATUS;
pub const HEADER_OFFSET_PAYLOAD_LEN: usize    = HEADER_OFFSET_CORRELATION_ID + HEADER_SIZE_CORRELATION_ID;

// ========================================
// PAYLOAD GEOMETRY
// ========================================
/// For data carrying TypeFlags, the first byte indicates type and flags
pub const DATA_OFFSET_TYPE_FLAGS: usize = 0;

// ========================================
// FRAME TYPES (The "Envelope" Type)
// ========================================
pub const TYPE_REQUEST: u8  = 0x01;
pub const TYPE_RESPONSE: u8 = 0x02;
pub const TYPE_PUSH: u8     = 0x03;
pub const TYPE_ERROR: u8    = 0x04;
pub const TYPE_PING: u8     = 0x05;
pub const TYPE_PONG: u8     = 0x06;

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
/// Base data types (4 bits)
pub const DATA_TYPE_RAW: u8    = 0x00;
pub const DATA_TYPE_STRING: u8 = 0x01;
pub const DATA_TYPE_JSON: u8   = 0x02;

/// Data flags (bits 4-5)
pub const DATA_FLAG_COMPRESSED: u8 = 0x08;  // bit 4
pub const DATA_FLAG_ENCRYPTED: u8  = 0x04;  // bit 5

/// Masks for extracting type and flags
pub const DATA_TYPE_MASK: u8  = 0xF0;  // Upper 4 bits
pub const DATA_FLAGS_MASK: u8 = 0x0C;  // Bits 4-5

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

// ========================================
// NETWORK TUNING
// ========================================
pub const NETWORK_BUFFER_READ_SIZE: usize  = 64 * 1024; // 64KB
pub const NETWORK_BUFFER_WRITE_SIZE: usize = 16 * 1024; // 16KB
pub const CHANNEL_CAPACITY_SOCKET_WRITE: usize = 1024;

// ========================================
// DASHBOARD UTILS
// ========================================

/// Converts a protocol-compliant data payload into a serde_json::Value for dashboard visualization.
/// Assumes the payload follows: [TypeFlags: 1 byte] [Data...]
pub fn payload_to_dashboard_value(payload: &[u8]) -> serde_json::Value {
    if payload.is_empty() {
        return serde_json::Value::Null;
    }

    let type_flags = payload[DATA_OFFSET_TYPE_FLAGS];
    let data_type = (type_flags & DATA_TYPE_MASK) >> 4;
    let content = &payload[SIZE_DATA_TYPE..]; // Skip TypeFlags byte

    match data_type {
        DATA_TYPE_JSON => {
            serde_json::from_slice(content).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(content).to_string())
            })
        }
        DATA_TYPE_RAW => {
            serde_json::Value::String(format!("0x{}", hex::encode(content)))
        }
        _ => {
            // String or fallback
            serde_json::Value::String(String::from_utf8_lossy(content).to_string())
        }
    }
}

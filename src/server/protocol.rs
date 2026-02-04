//! Nexo Binary Protocol Specification
//!
//! Binary Frame Structure (Total Header: 9 bytes):
//! [FrameType: 1 byte] [CorrelationID: 4 bytes (BigEndian)] [PayloadLen: 4 bytes (BigEndian)]
//!
//! Payload Structure:
//! [Opcode/Status: 1 byte] [DataType: 1 byte (Mandatory for DATA frames)] [Actual Data...]
//!
//! DataTypes (Mandatory first byte of user data):
//! 0x00: RAW (Binary)
//! 0x01: STRING (UTF-8)
//! 0x02: JSON (UTF-8)

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
pub const HEADER_SIZE_CORRELATION_ID: usize = SIZE_U32;
pub const HEADER_SIZE_PAYLOAD_LEN: usize    = SIZE_U32;

/// Total size of the binary header (Sum of all header fields)
pub const HEADER_SIZE: usize = HEADER_SIZE_FRAME_TYPE + HEADER_SIZE_CORRELATION_ID + HEADER_SIZE_PAYLOAD_LEN;

// Offsets within the header
pub const HEADER_OFFSET_FRAME_TYPE: usize     = 0;
pub const HEADER_OFFSET_CORRELATION_ID: usize = HEADER_SIZE_FRAME_TYPE;
pub const HEADER_OFFSET_PAYLOAD_LEN: usize    = HEADER_SIZE_FRAME_TYPE + HEADER_SIZE_CORRELATION_ID;

// ========================================
// PAYLOAD GEOMETRY
// ========================================
/// Every payload starts with an Opcode (Requests) or a Status (Responses)
pub const PAYLOAD_OFFSET_OPCODE: usize = 0;
pub const PAYLOAD_OFFSET_STATUS: usize = 0;

/// For frames carrying data, the first byte of actual content is the DataType
pub const PAYLOAD_OFFSET_DATA_TYPE: usize = 0;

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
// RESPONSE STATUS (Inside Response Payload)
// ========================================
pub const STATUS_OK: u8   = 0x00;
pub const STATUS_ERR: u8  = 0x01;
pub const STATUS_NULL: u8 = 0x02;
pub const STATUS_DATA: u8 = 0x03;

// ========================================
// DATA TYPES (First byte of actual data)
// ========================================
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

// ========================================
// NETWORK TUNING
// ========================================
pub const NETWORK_BUFFER_READ_SIZE: usize  = 64 * 1024; // 64KB
pub const NETWORK_BUFFER_WRITE_SIZE: usize = 16 * 1024; // 16KB
pub const CHANNEL_CAPACITY_SOCKET_WRITE: usize = 1024;

// ========================================
// DASHBOARD UTILS
// ========================================

/// Converts a protocol-compliant payload into a serde_json::Value for dashboard visualization.
/// Assumes the payload follows: [DataType: 1 byte] [Data...]
pub fn payload_to_dashboard_value(payload: &[u8]) -> serde_json::Value {
    if payload.is_empty() {
        return serde_json::Value::Null;
    }

    let data_type = payload[PAYLOAD_OFFSET_DATA_TYPE];
    let content = &payload[SIZE_DATA_TYPE..]; // Skip DataType byte

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

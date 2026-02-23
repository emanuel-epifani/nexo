//! Nexo Binary Protocol: Frame Types and Constants
//!
//! Request Frame (Total Header: 10 bytes):
//! [FrameType: 1 byte] [Meta/Opcode: 1 byte] [CorrelationID: 4 bytes (BE)] [PayloadLen: 4 bytes (BE)]
//! Payload: [...args (JSON strings)] [Data (if applicable)]
//!
//! Response Frame (Total Header: 10 bytes):
//! [FrameType: 1 byte] [Meta/Status: 1 byte] [CorrelationID: 4 bytes (BE)] [PayloadLen: 4 bytes (BE)]
//! Payload: [Data...]
//!
//! Push Frame (Total Header: 10 bytes):
//! [FrameType: 1 byte] [Meta/PushType: 1 byte] [CorrelationID: 4 bytes (BE)] [PayloadLen: 4 bytes (BE)]
//! Payload: [Data...]
//!
//! Data Structure (auto-contained):
//! [DataType: 1 byte] [Data...]

use bytes::Bytes;
use bytemuck::{Pod, Zeroable};

// ========================================
// FRAME TYPES
// ========================================
pub const TYPE_REQUEST: u8 = 0x01;
pub const TYPE_RESPONSE: u8 = 0x02;
pub const TYPE_PUSH: u8 = 0x03;

// ========================================
// PUSH TYPES (Meta byte for Push frames)
// ========================================
pub const PUSH_TYPE_PUBSUB: u8 = 0x01;

// ========================================
// RESPONSE STATUS (Meta byte for Response frames)
// ========================================
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_ERR: u8 = 0x01;
pub const STATUS_NULL: u8 = 0x02;
pub const STATUS_DATA: u8 = 0x03;

// ========================================
// DATA TYPE FLAGS (First byte of data payload)
// ========================================
pub const DATA_TYPE_RAW: u8 = 0x00;
pub const DATA_TYPE_STRING: u8 = 0x01;
pub const DATA_TYPE_JSON: u8 = 0x02;

// ========================================
// OPCODES RANGES (Meta byte for Request frames)
// ========================================
pub const OPCODE_MIN_STORE: u8 = 0x02;
pub const OPCODE_MAX_STORE: u8 = 0x0F;
pub const OPCODE_MIN_QUEUE: u8 = 0x10;
pub const OPCODE_MAX_QUEUE: u8 = 0x1F;
pub const OPCODE_MIN_PUBSUB: u8 = 0x21;
pub const OPCODE_MAX_PUBSUB: u8 = 0x2F;
pub const OPCODE_MIN_STREAM: u8 = 0x30;
pub const OPCODE_MAX_STREAM: u8 = 0x3F;

// ========================================
// FRAME HEADER
// ========================================

/// Fixed-size Header: [FrameType: 1] [Meta: 1] [CorrelationID: 4] [PayloadLen: 4]
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Pod, Zeroable)]
pub struct FrameHeader {
    pub frame_type: u8,
    pub meta: u8,
    pub id: [u8; 4],
    pub payload_len: [u8; 4],
}

impl FrameHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn id(&self) -> u32 {
        u32::from_be_bytes(self.id)
    }

    pub fn payload_len(&self) -> u32 {
        u32::from_be_bytes(self.payload_len)
    }
}

// ========================================
// FRAME TYPES
// ========================================

/// Inbound frame: decoded by NexoCodec from the socket
#[derive(Debug)]
pub struct InboundFrame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

/// Outbound frame: encoded by NexoCodec to the socket
#[derive(Debug)]
pub enum OutboundFrame {
    Response { id: u32, response: Response },
    Push { id: u32, push_type: u8, payload: Bytes },
}

/// Represents a response to be sent back
#[derive(Debug)]
pub enum Response {
    Ok,
    Data(Bytes),
    Error(String),
    Null,
}

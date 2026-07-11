//! Nexo Binary Protocol: Frame Types and Constants
//!
//! Every frame starts with a fixed 11-byte header. The first byte is the
//! protocol version, so the framing is fully self-describing and stateless:
//! a peer can reject an incompatible version per-frame, with no handshake.
//!
//! Header (11 bytes):
//! [Version: 1] [FrameType: 1] [Meta: 1] [CorrelationID: 4 (BE)] [PayloadLen: 4 (BE)]
//!   - Version : PROTOCOL_VERSION; mismatched frames are rejected.
//!   - Meta    : opcode (Request) / status (Response) / push-type (Push).
//!
//! Request payload : [binary typed fields per broker] [Data (if applicable)]
//! Response payload: STATUS_DATA -> [Data...]; STATUS_ERR -> [utf8 message...]
//!                   (both read to end of payload; OK/NULL carry no payload).
//! Push payload    : [Data...]  (CorrelationID is unused for pushes -> 0).
//!
//! Data Structure (auto-contained):
//! [DataType: 1 byte] [Data...]

use bytes::Bytes;
use bytemuck::{Pod, Zeroable};

// ========================================
// PROTOCOL VERSION
// ========================================
/// Bumped on any breaking change to the framing or payload layout. Peers must
/// reject frames whose first byte does not match.
pub const PROTOCOL_VERSION: u8 = 0x02;

// ========================================
// FRAME TYPES
// ========================================
pub const TYPE_REQUEST: u8 = 0x01;
pub const TYPE_RESPONSE: u8 = 0x02;
pub const TYPE_PUSH_PUBSUB: u8 = 0x03;

// ========================================
// RESPONSE STATUS (Meta byte for Response frames)
// ========================================
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_ERR: u8 = 0x01;
pub const STATUS_NULL: u8 = 0x02;
pub const STATUS_DATA: u8 = 0x03;

// ========================================
// DATA TYPE FLAGS (First byte of a user data payload)
//
// This prefix is a CLIENT-SIDE payload-encoding contract: SDKs write it when
// serializing a value and read it when deserializing, so all SDKs must agree
// on these values. The server is agnostic — it stores/forwards the payload
// (prefix included) as opaque bytes and never relies on it on the data-plane.
// ========================================
pub const DATA_TYPE_RAW: u8 = 0x00;
pub const DATA_TYPE_STRING: u8 = 0x01;
pub const DATA_TYPE_JSON: u8 = 0x02;

// ========================================
// FRAME HEADER
// ========================================

/// Fixed-size Header: [Version: 1] [FrameType: 1] [Meta: 1] [CorrelationID: 4] [PayloadLen: 4]
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Pod, Zeroable)]
pub struct FrameHeader {
    pub version: u8,
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
    PushPubSub { id: u32, payload: Bytes },
}

/// Represents a response to be sent back
#[derive(Debug)]
pub enum Response {
    Ok,
    Data(Bytes),
    Error(String),
    Null,
}

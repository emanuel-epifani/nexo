//! Binary Protocol Implementation: Header Parsing and Encoding
//! This module handles the physical layer of the binary frames.

pub use crate::server::protocol::*;
use bytes::{BufMut, BytesMut, Bytes};
use bytemuck::{Pod, Zeroable};

// ========================================
// TYPES
// ========================================

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    Incomplete,
    Invalid(String),
}

/// Fixed-size Header: [FrameType: 1] [Meta: 1] [CorrelationID: 4] [PayloadLen: 4]
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Pod, Zeroable)]
pub struct FrameHeader {
    pub frame_type: u8,
    pub meta: u8,
    pub id: [u8; 4],          // [u8; 4] prevents padding and allows bytemuck safety
    pub payload_len: [u8; 4],
}

impl FrameHeader {
    /// Total size of the header from protocol specification
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Parse header from bytes without consuming the buffer
    /// Returns a reference to the struct inside the buffer (Zero Copy)
    pub fn parse(buf: &[u8]) -> Result<Option<&Self>, ParseError> {
        if buf.len() < Self::SIZE {
            return Ok(None);
        }

        // Zero-copy safe cast verified by bytemuck
        match bytemuck::try_from_bytes(&buf[..Self::SIZE]) {
            Ok(header) => Ok(Some(header)),
            Err(_) => Err(ParseError::Invalid("Header alignment or size mismatch".to_string())),
        }
    }

    pub fn id(&self) -> u32 {
        u32::from_be_bytes(self.id)
    }

    pub fn payload_len(&self) -> u32 {
        u32::from_be_bytes(self.payload_len)
    }
}

/// Represents a parsed frame (Header + Payload slice)
#[derive(Debug)]
pub struct Frame<'a> {
    pub header: FrameHeader,
    pub payload: &'a [u8],
}

/// Represents a response to be sent back
#[derive(Debug)]
pub enum Response {
    Ok,
    Data(Bytes),
    Error(String),
    Null,
}

// ========================================
// PARSER (Socket -> Server)
// ========================================

/// Attempts to parse a full frame from the buffer
pub fn parse_frame(buf: &[u8]) -> Result<Option<(Frame<'_>, usize)>, ParseError> {
    // 1. Zero-copy parse header
    let header_ref = match FrameHeader::parse(buf)? {
        Some(h) => h,
        None => return Ok(None),
    };
    
    // 2. Check total length
    let payload_len = header_ref.payload_len() as usize;
    let total_len = FrameHeader::SIZE + payload_len;
    
    if buf.len() < total_len { 
        return Ok(None); 
    }
    
    // 3. Extract payload
    let payload = &buf[FrameHeader::SIZE..total_len];
    
    // Copy header (10 bytes) to Frame struct, keep payload as ref
    Ok(Some((Frame { header: *header_ref, payload }, total_len)))
}

// ========================================
// ENCODER (Server -> Socket)
// ========================================

/// Encodes a Response enum into binary bytes for the wire
pub fn encode_response(id: u32, response: &Response) -> Bytes {
    // Extract status and payload from response
    let (status, payload) = match response {
        Response::Ok => (STATUS_OK, Bytes::new()),
        Response::Null => (STATUS_NULL, Bytes::new()),
        Response::Error(msg) => {
            let mut buf = BytesMut::with_capacity(4 + msg.len());
            buf.put_u32(msg.len() as u32);
            buf.put_slice(msg.as_bytes());
            (STATUS_ERR, buf.freeze())
        }
        Response::Data(data) => (STATUS_DATA, data.clone()),
    };
    
    // Allocate buffer: Header + Payload
    let mut buf = BytesMut::with_capacity(FrameHeader::SIZE + payload.len());
    
    // Header: [Type:1][Status:1][ID:4][Len:4]
    buf.put_u8(TYPE_RESPONSE);
    buf.put_u8(status);
    buf.put_u32(id);
    buf.put_u32(payload.len() as u32);
    
    // Payload (if any)
    buf.put_slice(&payload);
    
    buf.freeze()
}

/// Encodes a push notification frame
pub fn encode_push(id: u32, push_type: u8, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(FrameHeader::SIZE + payload.len());
    buf.put_u8(TYPE_PUSH);
    buf.put_u8(push_type);
    buf.put_u32(id);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

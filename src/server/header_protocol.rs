//! Binary Protocol Implementation: Header Parsing and Encoding
//! This module handles the physical layer of the binary frames.

pub use crate::server::protocol::*;
use bytes::{BufMut, BytesMut, Bytes};
use std::convert::TryInto;

// ========================================
// TYPES
// ========================================

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    Incomplete,
    Invalid(String),
}

/// Fixed-size Header: [FrameType: 1] [Opcode: 1] [CorrelationID: 4] [PayloadLen: 4]
#[derive(Debug, Clone, Copy)]
pub struct RequestHeader {
    pub frame_type: u8,
    pub opcode: u8,
    pub id: u32,
    pub payload_len: u32,
}

impl RequestHeader {
    /// Total size of the header from protocol specification
    pub const SIZE: usize = HEADER_SIZE;

    /// Parse header from bytes without consuming the buffer
    pub fn parse(buf: &[u8]) -> Result<Option<Self>, ParseError> {
        if buf.len() < Self::SIZE {
            return Ok(None);
        }

        // Field extraction based on protocol geometry
        let frame_type = buf[HEADER_OFFSET_FRAME_TYPE];
        let opcode = buf[HEADER_OFFSET_OPCODE];
        
        let id_start = HEADER_OFFSET_CORRELATION_ID;
        let id_end = id_start + HEADER_SIZE_CORRELATION_ID;
        let id = u32::from_be_bytes(buf[id_start..id_end].try_into().map_err(|_| ParseError::Incomplete)?);
        
        let len_start = HEADER_OFFSET_PAYLOAD_LEN;
        let len_end = len_start + HEADER_SIZE_PAYLOAD_LEN;
        let payload_len = u32::from_be_bytes(buf[len_start..len_end].try_into().map_err(|_| ParseError::Incomplete)?);

        Ok(Some(Self {
            frame_type,
            opcode,
            id,
            payload_len,
        }))
    }
}

/// Represents a parsed frame (Header + Payload slice)
#[derive(Debug)]
pub struct Frame<'a> {
    pub header: RequestHeader,
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
    let header = match RequestHeader::parse(buf)? {
        Some(h) => h,
        None => return Ok(None),
    };
    
    let total_len = RequestHeader::SIZE + header.payload_len as usize;
    if buf.len() < total_len { 
        return Ok(None); 
    }
    
    let payload = &buf[RequestHeader::SIZE..total_len];
    Ok(Some((Frame { header, payload }, total_len)))
}

// ========================================
// ENCODER (Server -> Socket)
// ========================================

/// Encodes a Response enum into binary bytes for the wire
pub fn encode_response(id: u32, response: &Response) -> Bytes {
    // Pre-calculate extra payload size based on response type
    let extra_len = match response {
        Response::Ok | Response::Null => 0,
        Response::Error(msg) => SIZE_STRING_PREFIX + msg.len(),
        Response::Data(data) => data.len(),
    };
    
    // Total: Header + Status + extra
    let mut buf = BytesMut::with_capacity(RequestHeader::SIZE + SIZE_STATUS + extra_len);
    
    // Header: [Type:1][Opcode:1][ID:4][Len:4]
    buf.put_u8(TYPE_RESPONSE);
    buf.put_u8(0x00); // Opcode = 0 for responses (unused)
    buf.put_u32(id);
    
    match response {
        Response::Ok => { 
            buf.put_u32(SIZE_STATUS as u32);
            buf.put_u8(STATUS_OK); 
        }
        Response::Error(msg) => {
            buf.put_u32((SIZE_STATUS + SIZE_STRING_PREFIX + msg.len()) as u32);
            buf.put_u8(STATUS_ERR);
            buf.put_u32(msg.len() as u32);
            buf.put_slice(msg.as_bytes());
        }
        Response::Null => { 
            buf.put_u32(SIZE_STATUS as u32);
            buf.put_u8(STATUS_NULL); 
        }
        Response::Data(data) => {
            buf.put_u32((SIZE_STATUS + data.len()) as u32);
            buf.put_u8(STATUS_DATA);
            buf.put_slice(data);
        }
    }
    buf.freeze()
}

/// Encodes a push notification frame
pub fn encode_push(id: u32, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(RequestHeader::SIZE + payload.len());
    buf.put_u8(TYPE_PUSH);
    buf.put_u8(0x00); // Opcode = 0 for push (unused)
    buf.put_u32(id);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

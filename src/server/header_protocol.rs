//! Binary Protocol Implementation
//! Header (9 bytes): [FrameType: 1] [CorrelationID: 4] [PayloadLen: 4]
//! Payload: [Opcode/Status: 1] [Data...]

use bytes::{BufMut, BytesMut, Bytes};
use std::convert::TryInto;

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
// TYPES
// ========================================

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    Incomplete,
    Invalid(String),
}

/// Fixed-size Header (9 bytes): [FrameType: 1] [CorrelationID: 4] [PayloadLen: 4]
#[derive(Debug, Clone, Copy)]
pub struct RequestHeader {
    pub frame_type: u8,
    pub id: u32,
    pub payload_len: u32,
}

impl RequestHeader {
    pub const SIZE: usize = 9;

    /// Parse header from bytes without consuming
    pub fn parse(buf: &[u8]) -> Result<Option<Self>, ParseError> {
        if buf.len() < Self::SIZE {
            return Ok(None);
        }
        Ok(Some(Self {
            frame_type: buf[0],
            id: u32::from_be_bytes(buf[1..5].try_into().map_err(|_| ParseError::Incomplete)?),
            payload_len: u32::from_be_bytes(buf[5..9].try_into().map_err(|_| ParseError::Incomplete)?),
        }))
    }
}

/// Represents a parsed frame (Header + Body slice)
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
// PARSER (from socket -> to server)
// ========================================

pub fn parse_frame(buf: &[u8]) -> Result<Option<(Frame<'_>, usize)>, ParseError> {
    let header = match RequestHeader::parse(buf)? {
        Some(h) => h,
        None => return Ok(None),
    };
    
    let total_len = RequestHeader::SIZE + header.payload_len as usize;
    if buf.len() < total_len { return Ok(None); }
    let payload = &buf[RequestHeader::SIZE..total_len];
    Ok(Some((Frame { header, payload }, total_len)))
}

// ========================================
// ENCODER (from server -> to socket)
// ========================================

pub fn encode_response(id: u32, response: &Response) -> Bytes {
    // Pre-calculate exact size
    let extra_len = match response {
        Response::Ok | Response::Null => 0,
        Response::Error(msg) => 4 + msg.len(), // MsgLen(4) + Msg
        Response::Data(data) => data.len(),
    };
    
    // Header (9) + Status (1) + optional extra
    let mut buf = BytesMut::with_capacity(RequestHeader::SIZE + 1 + extra_len);
    
    buf.put_u8(TYPE_RESPONSE);
    buf.put_u32(id);
    match response {
        Response::Ok => { 
            buf.put_u32(1); // Payload len: 1 byte status
            buf.put_u8(STATUS_OK); 
        }
        Response::Error(msg) => {
            buf.put_u32((1 + 4 + msg.len()) as u32); // Status(1) + MsgLen(4) + Msg
            buf.put_u8(STATUS_ERR);
            buf.put_u32(msg.len() as u32);
            buf.put_slice(msg.as_bytes());
        }
        Response::Null => { 
            buf.put_u32(1); 
            buf.put_u8(STATUS_NULL); 
        }
        Response::Data(data) => {
            buf.put_u32((1 + data.len()) as u32);
            buf.put_u8(STATUS_DATA);
            buf.put_slice(data);
        }
    }
    buf.freeze()
}

pub fn encode_push(id: u32, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(RequestHeader::SIZE + payload.len());
    buf.put_u8(TYPE_PUSH);
    buf.put_u32(id);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

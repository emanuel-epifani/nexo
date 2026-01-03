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
pub const STATUS_Q_DATA: u8 = 0x04; // New: Queue Data [UUID:16][Payload]

// ========================================
// TYPES
// ========================================

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    Incomplete,
    Invalid(String),
}

/// Represents a parsed frame (Header + Body slice)
#[derive(Debug)]
pub struct Frame<'a> {
    pub frame_type: u8,
    pub id: u32,
    pub payload: &'a [u8],
}

/// Represents a response to be sent back
#[derive(Debug)]
pub enum Response {
    Ok,
    Data(Bytes),
    Error(String),
    Null,
    QueueData(uuid::Uuid, Bytes),
    AsyncConsume(tokio::sync::oneshot::Receiver<crate::brokers::queues::Message>),
}

// ========================================
// PARSER (from socket -> to server)
// ========================================

// 512MB Hard Limit to prevent OOM attacks
const MAX_PAYLOAD_SIZE: usize = 512 * 1024 * 1024;

pub fn parse_frame(buf: &[u8]) -> Result<Option<(Frame<'_>, usize)>, ParseError> {
    if buf.len() < 9 { return Ok(None); }
    let frame_type = buf[0];
    let id = u32::from_be_bytes(buf[1..5].try_into().map_err(|_| ParseError::Incomplete)?);
    let payload_len = u32::from_be_bytes(buf[5..9].try_into().map_err(|_| ParseError::Incomplete)?) as usize;
    
    // Security Check: Hard Limit
    if payload_len > MAX_PAYLOAD_SIZE {
        return Err(ParseError::Invalid(format!(
            "Payload too large: {} bytes (Limit: {} bytes)", 
            payload_len, MAX_PAYLOAD_SIZE
        )));
    }

    let total_len = 9 + payload_len;
    if buf.len() < total_len { return Ok(None); }
    let payload = &buf[9..total_len];
    Ok(Some((Frame { frame_type, id, payload }, total_len)))
}

// ========================================
// ENCODER (from server -> to socket)
// ========================================

pub fn encode_response(id: u32, response: &Response) -> Bytes {
    // Pre-calculate exact size
    let data_len = match response {
        Response::Ok | Response::Null => 0,
        Response::Error(msg) => msg.len(),
        Response::Data(data) => data.len(),
        Response::QueueData(_, data) => 16 + data.len(),
        Response::AsyncConsume(_) => 0, // Should not be called directly
    };
    
    let mut buf = BytesMut::with_capacity(10 + data_len);
    
    buf.put_u8(TYPE_RESPONSE);
    buf.put_u32(id);
    match response {
        Response::Ok => { buf.put_u32(1); buf.put_u8(STATUS_OK); }
        Response::Error(msg) => {
            buf.put_u32((1 + msg.len()) as u32);
            buf.put_u8(STATUS_ERR);
            buf.put_slice(msg.as_bytes());
        }
        Response::Null => { buf.put_u32(1); buf.put_u8(STATUS_NULL); }
        Response::Data(data) => {
            buf.put_u32((1 + data.len()) as u32);
            buf.put_u8(STATUS_DATA);
            buf.put_slice(data);
        }
        Response::QueueData(uuid, data) => {
            buf.put_u32((1 + 16 + data.len()) as u32);
            buf.put_u8(STATUS_Q_DATA);
            buf.put_slice(uuid.as_bytes());
            buf.put_slice(data);
        }
        Response::AsyncConsume(_) => { buf.put_u32(1); buf.put_u8(STATUS_OK); }
    }
    buf.freeze()
}

pub fn encode_push(id: u32, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(9 + payload.len());
    buf.put_u8(TYPE_PUSH);
    buf.put_u32(id);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

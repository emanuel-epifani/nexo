//! Binary Protocol Implementation
//! Header: [Cmd: 1 byte] [BodyLen: 4 bytes] [Body...]

use bytes::{BufMut, BytesMut};
use std::convert::TryInto;

// ========================================
// TYPES
// ========================================

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    Incomplete,
    Invalid(String),
}

/// Represents a parsed request frame (Header + Body slice)
#[derive(Debug)]
pub struct Request<'a> {
    pub opcode: u8,
    pub payload: &'a [u8],
}

/// Represents a response to be sent back
#[derive(Debug)]
pub enum Response {
    Ok,
    Data(Vec<u8>),
    Error(String),
    Null, // e.g. Key not found
}

// ========================================
// CONSTANTS
// ========================================

// Response Status Codes
const STATUS_OK: u8   = 0x00;
const STATUS_ERR: u8  = 0x01;
const STATUS_NULL: u8 = 0x02;
const STATUS_DATA: u8 = 0x03;

// ========================================
// PARSER
// ========================================

pub fn parse_request(buf: &[u8]) -> Result<Option<(Request<'_>, usize)>, ParseError> {
    // 1. Check Header Size (1 byte Opcode + 4 bytes Len = 5 bytes)
    if buf.len() < 5 {
        return Ok(None);
    }

    // 2. Read Header
    let opcode = buf[0];
    // Read 4 bytes for length (Big Endian)
    let len_bytes: [u8; 4] = buf[1..5].try_into().map_err(|_| ParseError::Incomplete)?;
    let payload_len = u32::from_be_bytes(len_bytes) as usize;

    let total_len = 5 + payload_len;

    // 3. Check Payload Size
    if buf.len() < total_len {
        return Ok(None);
    }

    // 4. Extract Payload (Zero-Copy)
    let payload = &buf[5..total_len];

    Ok(Some((
        Request {
            opcode,
            payload,
        },
        total_len,
    )))
}

// ========================================
// ENCODER
// ========================================

pub fn encode_response(response: &Response) -> Vec<u8> {
    let mut buf = BytesMut::new();

    match response {
        Response::Ok => {
            buf.put_u8(STATUS_OK);
            buf.put_u32(0); // Len 0
        }
        Response::Error(msg) => {
            buf.put_u8(STATUS_ERR);
            let bytes = msg.as_bytes();
            buf.put_u32(bytes.len() as u32);
            buf.put_slice(bytes);
        }
        Response::Null => {
            buf.put_u8(STATUS_NULL);
            buf.put_u32(0);
        }
        Response::Data(data) => {
            buf.put_u8(STATUS_DATA);
            buf.put_u32(data.len() as u32);
            buf.put_slice(data);
        }
    }

    buf.to_vec()
}

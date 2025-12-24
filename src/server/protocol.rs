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
// COMMANDS (High-Level AST)
// ========================================

#[derive(Debug)]
pub enum Command {
    Ping,
    Kv(KvCommand),
    Queue(QueueCommand),
    Topic(TopicCommand),
    Stream(StreamCommand),
}

#[derive(Debug)]
pub enum KvCommand {
    Set { key: String, value: Vec<u8>, ttl: Option<u64> },
    Get { key: String },
    Del { key: String },
}

#[derive(Debug)]
pub enum QueueCommand {
    Push { queue_name: String, value: Vec<u8> },
    Pop { queue_name: String },
}

#[derive(Debug)]
pub enum TopicCommand {
    Publish { topic: String, value: Vec<u8> },
    Subscribe { topic: String },
}

#[derive(Debug)]
pub enum StreamCommand {
    Add { topic: String, value: Vec<u8> },
    Read { topic: String, offset: u64 },
}

impl Command {
    pub fn from_request(req: Request) -> Result<Self, String> {
        match req.opcode {
            OP_PING => Ok(Command::Ping),
            
            OP_KV_SET | OP_KV_GET | OP_KV_DEL => {
                Ok(Command::Kv(KvCommand::parse(req.opcode, req.payload)?))
            }
            
            OP_Q_PUSH | OP_Q_POP => {
                Ok(Command::Queue(QueueCommand::parse(req.opcode, req.payload)?))
            }
            
            OP_PUB | OP_SUB => {
                Ok(Command::Topic(TopicCommand::parse(req.opcode, req.payload)?))
            }
            
            OP_S_ADD | OP_S_READ => {
                Ok(Command::Stream(StreamCommand::parse(req.opcode, req.payload)?))
            }
            
            _ => Err(format!("Unknown Opcode: 0x{:02X}", req.opcode)),
        }
    }
}

// Implement parse methods for sub-commands
impl KvCommand {
    fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_KV_SET => {
                if payload.len() < 4 { return Err("Payload too short".to_string()); }
                let key_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + key_len { return Err("Invalid frame format".to_string()); }

                let key_bytes = &payload[4..4 + key_len];
                let value = payload[4 + key_len..].to_vec();
                let key = std::str::from_utf8(key_bytes).map_err(|_| "Key must be UTF-8".to_string())?.to_string();

                Ok(KvCommand::Set { key, value, ttl: None })
            },
            OP_KV_GET => {
                let key = std::str::from_utf8(payload).map_err(|_| "Key must be UTF-8".to_string())?.to_string();
                Ok(KvCommand::Get { key })
            },
            OP_KV_DEL => {
                let key = std::str::from_utf8(payload).map_err(|_| "Key must be UTF-8".to_string())?.to_string();
                Ok(KvCommand::Del { key })
            },
            _ => Err("Invalid KV opcode".to_string()),
        }
    }
}

impl QueueCommand {
    fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_Q_PUSH => {
                if payload.len() < 4 { return Err("Payload too short".to_string()); }
                let q_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + q_len { return Err("Invalid frame format".to_string()); }

                let q_name_bytes = &payload[4..4 + q_len];
                let value = payload[4 + q_len..].to_vec();
                let queue_name = std::str::from_utf8(q_name_bytes).map_err(|_| "Queue name must be UTF-8".to_string())?.to_string();

                Ok(QueueCommand::Push { queue_name, value })
            },
            OP_Q_POP => {
                let queue_name = std::str::from_utf8(payload).map_err(|_| "Queue name must be UTF-8".to_string())?.to_string();
                Ok(QueueCommand::Pop { queue_name })
            },
            _ => Err("Invalid Queue opcode".to_string()),
        }
    }
}

impl TopicCommand {
    fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_PUB => {
                if payload.len() < 4 { return Err("Payload too short".to_string()); }
                let t_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + t_len { return Err("Invalid frame format".to_string()); }
                
                let topic_bytes = &payload[4..4 + t_len];
                let value = payload[4 + t_len..].to_vec();
                let topic = std::str::from_utf8(topic_bytes).map_err(|_| "Topic must be UTF-8".to_string())?.to_string();

                Ok(TopicCommand::Publish { topic, value })
            },
            OP_SUB => {
                let topic = std::str::from_utf8(payload).map_err(|_| "Topic must be UTF-8".to_string())?.to_string();
                Ok(TopicCommand::Subscribe { topic })
            },
            _ => Err("Invalid Topic opcode".to_string()),
        }
    }
}

impl StreamCommand {
    fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_S_ADD => {
                if payload.len() < 4 { return Err("Payload too short".to_string()); }
                let t_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + t_len { return Err("Invalid frame format".to_string()); }
                
                let topic_bytes = &payload[4..4 + t_len];
                let value = payload[4 + t_len..].to_vec();
                let topic = std::str::from_utf8(topic_bytes).map_err(|_| "Topic must be UTF-8".to_string())?.to_string();

                Ok(StreamCommand::Add { topic, value })
            },
            OP_S_READ => {
                if payload.len() < 4 { return Err("Payload too short".to_string()); }
                let t_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                let offset_start = 4 + t_len;
                if payload.len() < offset_start + 8 { return Err("Missing offset or invalid format".to_string()); }
                
                let topic_bytes = &payload[4..4 + t_len];
                let offset_bytes = &payload[offset_start..offset_start+8];
                let offset = u64::from_be_bytes(offset_bytes.try_into().unwrap());
                let topic = std::str::from_utf8(topic_bytes).map_err(|_| "Topic must be UTF-8".to_string())?.to_string();

                Ok(StreamCommand::Read { topic, offset })
            },
            _ => Err("Invalid Stream opcode".to_string()),
        }
    }
}

// ========================================
// CONSTANTS (Opcodes & Status)
// ========================================

// Request Opcodes
pub const OP_PING: u8    = 0x01;

// KV (0x02 - 0x0F)
pub const OP_KV_SET: u8  = 0x02;
pub const OP_KV_GET: u8  = 0x03;
pub const OP_KV_DEL: u8  = 0x04;

// QUEUE (0x10 - 0x1F)
pub const OP_Q_PUSH: u8  = 0x11; 
pub const OP_Q_POP: u8   = 0x12; 

// TOPIC / MQTT (0x20 - 0x2F)
pub const OP_PUB: u8     = 0x21; 
pub const OP_SUB: u8     = 0x22; 

// STREAM (0x30 - 0x3F)
pub const OP_S_ADD: u8   = 0x31; 
pub const OP_S_READ: u8  = 0x32; 

// Response Status Codes
const STATUS_OK: u8   = 0x00;
const STATUS_ERR: u8  = 0x01;
const STATUS_NULL: u8 = 0x02;
const STATUS_DATA: u8 = 0x03;

// ========================================
// PARSER
// ========================================

/// Tries to parse a complete frame from the buffer.
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

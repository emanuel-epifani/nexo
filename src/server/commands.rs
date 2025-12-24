//! All protocol commands - centralized parsing and execution

use crate::server::protocol::Response;
use crate::features::kv::KvManager;
use crate::features::queue::QueueManager;
use crate::features::topic::{TopicManager, ClientId};
use crate::features::stream::StreamManager;
use std::convert::TryInto;

// ========================================
// OPCODES
// ========================================
pub const OP_KV_SET: u8 = 0x02;
pub const OP_KV_GET: u8 = 0x03;
pub const OP_KV_DEL: u8 = 0x04;

pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_POP: u8 = 0x12;

pub const OP_PUB: u8 = 0x21;
pub const OP_SUB: u8 = 0x22;

pub const OP_S_ADD: u8 = 0x31;
pub const OP_S_READ: u8 = 0x32;

// ========================================
// COMMAND ENUMS
// ========================================

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

// ========================================
// PARSING HELPERS (shared across all commands)
// ========================================

/// Parse [len:u32][string][rest] → (string, rest)
fn parse_length_prefixed_string(payload: &[u8]) -> Result<(&str, &[u8]), String> {
    if payload.len() < 4 { 
        return Err("Payload too short".to_string()); 
    }
    
    let len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
    let end = 4 + len;
    
    if payload.len() < end { 
        return Err("Invalid frame format".to_string()); 
    }
    
    let string = std::str::from_utf8(&payload[4..end])
        .map_err(|_| "Invalid UTF-8".to_string())?;
    
    Ok((string, &payload[end..]))
}

/// Parse entire payload as UTF-8 string
fn parse_string(payload: &[u8]) -> Result<String, String> {
    std::str::from_utf8(payload)
        .map(|s| s.to_string())
        .map_err(|_| "Invalid UTF-8".to_string())
}

/// Parse [len:u32][string][u64] → (string, u64)
fn parse_string_and_u64(payload: &[u8]) -> Result<(&str, u64), String> {
    let (s, rest) = parse_length_prefixed_string(payload)?;
    
    if rest.len() < 8 {
        return Err("Missing u64 field".to_string());
    }
    
    let num = u64::from_be_bytes(rest[0..8].try_into().unwrap());
    Ok((s, num))
}

// ========================================
// KV COMMAND
// ========================================

impl KvCommand {
    /// Parse opcode + payload → KvCommand
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_KV_SET => {
                let (key, value) = parse_length_prefixed_string(payload)?;
                Ok(KvCommand::Set { 
                    key: key.to_string(), 
                    value: value.to_vec(), 
                    ttl: None 
                })
            },
            OP_KV_GET => {
                let key = parse_string(payload)?;
                Ok(KvCommand::Get { key })
            },
            OP_KV_DEL => {
                let key = parse_string(payload)?;
                Ok(KvCommand::Del { key })
            },
            _ => Err(format!("Unknown KV opcode: 0x{:02X}", opcode)),
        }
    }

    /// Execute command on manager
    pub fn execute(self, manager: &KvManager) -> Result<Response, String> {
        match self {
            KvCommand::Set { key, value, ttl } => {
                manager.set(key, value, ttl)?;
                Ok(Response::Ok)
            },
            KvCommand::Get { key } => {
                match manager.get(&key)? {
                    Some(val) => Ok(Response::Data(val)),
                    None => Ok(Response::Null),
                }
            },
            KvCommand::Del { key } => {
                manager.del(&key)?;
                Ok(Response::Ok)
            },
        }
    }
}

// ========================================
// QUEUE COMMAND
// ========================================

impl QueueCommand {
    /// Parse opcode + payload → QueueCommand
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_Q_PUSH => {
                let (queue_name, value) = parse_length_prefixed_string(payload)?;
                Ok(QueueCommand::Push { 
                    queue_name: queue_name.to_string(), 
                    value: value.to_vec() 
                })
            },
            OP_Q_POP => {
                let queue_name = parse_string(payload)?;
                Ok(QueueCommand::Pop { queue_name })
            },
            _ => Err(format!("Unknown Queue opcode: 0x{:02X}", opcode)),
        }
    }

    /// Execute command on manager
    pub fn execute(self, manager: &QueueManager) -> Result<Response, String> {
        match self {
            QueueCommand::Push { queue_name, value } => {
                manager.push(queue_name, value);
                Ok(Response::Ok)
            },
            QueueCommand::Pop { queue_name } => {
                match manager.pop(&queue_name) {
                    Some(val) => Ok(Response::Data(val)),
                    None => Ok(Response::Null),
                }
            },
        }
    }
}

// ========================================
// TOPIC COMMAND
// ========================================

impl TopicCommand {
    /// Parse opcode + payload → TopicCommand
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_PUB => {
                let (topic, value) = parse_length_prefixed_string(payload)?;
                Ok(TopicCommand::Publish { 
                    topic: topic.to_string(), 
                    value: value.to_vec() 
                })
            },
            OP_SUB => {
                let topic = parse_string(payload)?;
                Ok(TopicCommand::Subscribe { topic })
            },
            _ => Err(format!("Unknown Topic opcode: 0x{:02X}", opcode)),
        }
    }

    /// Execute command on manager
    pub fn execute(self, manager: &TopicManager) -> Result<Response, String> {
        match self {
            TopicCommand::Publish { topic, value: _ } => {
                // TODO: ClientID should come from connection context
                // TODO: use value to send to clients
                let _clients = manager.publish(&topic);
                Ok(Response::Ok)
            },
            TopicCommand::Subscribe { topic } => {
                // TODO: ClientID should come from connection context
                let dummy_client_id = ClientId("test-client".to_string());
                manager.subscribe(&topic, dummy_client_id);
                Ok(Response::Ok)
            },
        }
    }
}

// ========================================
// STREAM COMMAND
// ========================================

impl StreamCommand {
    /// Parse opcode + payload → StreamCommand
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_S_ADD => {
                let (topic, value) = parse_length_prefixed_string(payload)?;
                Ok(StreamCommand::Add { 
                    topic: topic.to_string(), 
                    value: value.to_vec() 
                })
            },
            OP_S_READ => {
                let (topic, offset) = parse_string_and_u64(payload)?;
                Ok(StreamCommand::Read { 
                    topic: topic.to_string(), 
                    offset 
                })
            },
            _ => Err(format!("Unknown Stream opcode: 0x{:02X}", opcode)),
        }
    }

    /// Execute command on manager
    pub fn execute(self, manager: &StreamManager) -> Result<Response, String> {
        match self {
            StreamCommand::Add { topic, value } => {
                let offset = manager.append(topic, value);
                Ok(Response::Data(offset.to_be_bytes().to_vec()))
            },
            StreamCommand::Read { topic, offset } => {
                let messages = manager.read(&topic, offset as usize);
                if let Some(msg) = messages.first() {
                    Ok(Response::Data(msg.payload.clone()))
                } else {
                    Ok(Response::Null)
                }
            },
        }
    }
}


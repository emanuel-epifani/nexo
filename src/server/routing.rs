//! Routing Layer: Command parsing + dispatching to managers

use crate::features::kv::KvManager;
use crate::server::protocol::{
    Request, Response, OP_KV_DEL, OP_KV_GET, OP_KV_SET, OP_PING,
};
use std::convert::TryInto;

// ========================================
// COMMAND ENUM
// ========================================

/// Represents all commands supported by Nexo
#[derive(Debug, Clone)]
pub enum Command<'a> {
    // Meta commands
    Ping,

    // KV commands
    KvSet {
        key: &'a str,
        value: &'a [u8],
        // ttl: Option<u64>, // TODO: Add TTL support in binary protocol later
    },
    KvGet {
        key: &'a str,
    },
    KvDel {
        key: &'a str,
    },
}

// ========================================
// COMMAND PARSING
// ========================================

impl<'a> Command<'a> {
    /// Parse command from Binary Request
    fn from_request(req: Request<'a>) -> Result<Self, String> {
        match req.opcode {
            OP_PING => Ok(Command::Ping),

            OP_KV_SET => {
                // Payload: [KeyLen: 4 bytes] [Key bytes] [Value bytes]
                let payload = req.payload;
                if payload.len() < 4 {
                    return Err("Payload too short for KeyLen".to_string());
                }

                let key_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + key_len {
                    return Err("Payload too short for Key".to_string());
                }

                let key_bytes = &payload[4..4 + key_len];
                let value = &payload[4 + key_len..];

                let key = std::str::from_utf8(key_bytes)
                    .map_err(|e| format!("Invalid UTF-8 key: {}", e))?;

                Ok(Command::KvSet {
                    key,
                    value,
                    // ttl: None, // Simplified for now
                })
            }

            OP_KV_GET => {
                // Payload: [Key bytes]
                let key = std::str::from_utf8(req.payload)
                    .map_err(|e| format!("Invalid UTF-8 key: {}", e))?;
                Ok(Command::KvGet { key })
            }

            OP_KV_DEL => {
                // Payload: [Key bytes]
                let key = std::str::from_utf8(req.payload)
                    .map_err(|e| format!("Invalid UTF-8 key: {}", e))?;
                Ok(Command::KvDel { key })
            }

            _ => Err(format!("Unknown Opcode: 0x{:02X}", req.opcode)),
        }
    }
}

// ========================================
// DISPATCHER
// ========================================

/// Route parsed command to appropriate manager
pub fn route(req: Request, kv_manager: &KvManager) -> Result<Response, String> {
    // Parse and validate command
    let command = Command::from_request(req)?;

    // Route to appropriate handler
    match command {
        // ===== Meta Commands =====
        Command::Ping => Ok(Response::Ok),

        // ===== KV Commands =====
        Command::KvSet { key, value } => {
            // ONLY HERE we perform allocation, because we need to store data in the map
            kv_manager.set(key.to_string(), value.to_vec(), None)?;
            Ok(Response::Ok)
        }

        Command::KvGet { key } => match kv_manager.get(key)? {
            // No allocation for key lookup!
            Some(value) => Ok(Response::Data(value)),
            None => Ok(Response::Null),
        },

        Command::KvDel { key } => {
            // No allocation for key lookup!
            let _deleted = kv_manager.del(key)?;
            Ok(Response::Ok)
        }
    }
}


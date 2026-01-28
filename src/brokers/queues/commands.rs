use bytes::Bytes;
use uuid::Uuid;
use crate::server::payload_cursor::PayloadCursor;
use serde::Deserialize;

pub const OP_Q_CREATE: u8 = 0x10;
pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_CONSUME: u8 = 0x12;
pub const OP_Q_ACK: u8 = 0x13;
pub const OP_Q_EXISTS: u8 = 0x14;

#[derive(Debug, Deserialize)]
pub struct QueueCreateOptions {
    pub visibility_timeout_ms: Option<u64>,
    pub max_retries: Option<u32>,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct QueuePushOptions {
    #[serde(default)]
    pub priority: u8,
    pub delay_ms: Option<u64>,
}

#[derive(Debug)]
pub enum QueueCommand {
    /// CREATE: [QNameLen:4][QName][JSONLen:4][JSON]
    Create {
        q_name: String,
        options: QueueCreateOptions,
    },
    /// PUSH: [QNameLen:4][QName][JSONLen:4][JSON][Data...]
    Push {
        q_name: String,
        options: QueuePushOptions,
        payload: Bytes,
    },
    /// CONSUME: [MaxBatch:4][WaitMs:8][QNameLen:4][QName]
    Consume {
        max_batch: usize,
        wait_ms: u64,
        q_name: String,
    },
    /// ACK: [ID:16][QNameLen:4][QName]
    Ack {
        id: Uuid,
        q_name: String,
    },
    /// EXISTS: [QNameLen:4][QName]
    Exists {
        q_name: String,
    },
}

impl QueueCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, String> {
        match opcode {
            OP_Q_CREATE => {
                let _flags = cursor.read_u8()?; // Reserved/Legacy
                let q_name = cursor.read_string()?;
                let json_str = cursor.read_string()?;

                let options: QueueCreateOptions = serde_json::from_str(&json_str)
                    .map_err(|e| format!("Invalid JSON config: {}", e))?;
                
                Ok(Self::Create { q_name, options })
            }
            OP_Q_PUSH => {
                let q_name = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                
                let options: QueuePushOptions = serde_json::from_str(&json_str)
                    .map_err(|e| format!("Invalid JSON options: {}", e))?;

                let payload = cursor.read_remaining();
                Ok(Self::Push { q_name, options, payload })
            }
            OP_Q_CONSUME => {
                let max_batch = cursor.read_u32()? as usize;
                let wait_ms = cursor.read_u64()?;
                let q_name = cursor.read_string()?;
                Ok(Self::Consume { max_batch, wait_ms, q_name })
            }
            OP_Q_ACK => {
                let id_bytes = cursor.read_uuid_bytes()?;
                let id = Uuid::from_bytes(id_bytes);
                let q_name = cursor.read_string()?;
                Ok(Self::Ack { id, q_name })
            }
            OP_Q_EXISTS => {
                let q_name = cursor.read_string()?;
                Ok(Self::Exists { q_name })
            }
            _ => Err(format!("Unknown Queue opcode: 0x{:02X}", opcode)),
        }
    }
}

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
#[serde(tag = "strategy")]
#[serde(rename_all = "camelCase")]
pub enum PersistenceOptions {
    #[serde(rename = "memory")]
    Memory,
    #[serde(rename = "file_sync")]
    FileSync,
    #[serde(rename = "file_async")]
    FileAsync {
        flush_interval_ms: Option<u64>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueCreateOptions {
    pub visibility_timeout_ms: Option<u64>,
    pub max_retries: Option<u32>,
    pub ttl_ms: Option<u64>,
    pub persistence: Option<PersistenceOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuePushOptions {
    pub priority: Option<u8>,
    pub delay_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueConsumeOptions {
    pub batch_size: Option<usize>,
    pub wait_ms: Option<u64>,
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
    /// CONSUME: [QNameLen:4][QName][JSONLen:4][JSON]
    Consume {
        q_name: String,
        options: QueueConsumeOptions,
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
                let q_name = cursor.read_string()?;
                let json_str = cursor.read_string()?;

                let options: QueueConsumeOptions = serde_json::from_str(&json_str)
                    .map_err(|e| format!("Invalid JSON options: {}", e))?;

                Ok(Self::Consume { q_name, options })
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

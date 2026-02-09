use bytes::Bytes;
use uuid::Uuid;
use crate::server::payload_cursor::PayloadCursor;
use serde::Deserialize;

pub const OP_Q_CREATE: u8 = 0x10;
pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_CONSUME: u8 = 0x12;
pub const OP_Q_ACK: u8 = 0x13;
pub const OP_Q_EXISTS: u8 = 0x14;
pub const OP_Q_DELETE: u8 = 0x15;
pub const OP_Q_NACK: u8 = 0x1A;

// DLQ Operations
pub const OP_Q_PEEK_DLQ: u8 = 0x16;
pub const OP_Q_MOVE_TO_QUEUE: u8 = 0x17;
pub const OP_Q_DELETE_DLQ: u8 = 0x18;
pub const OP_Q_PURGE_DLQ: u8 = 0x19;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceOptions {
    FileSync,
    FileAsync,
}

#[derive(Debug, Deserialize, Default, Clone)]
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
    /// DELETE: [NameLen:4][Name]
    Delete {
        q_name: String,
    },
    /// ACK: [ID:16][QNameLen:4][QName]
    Ack {
        id: Uuid,
        q_name: String,
    },
    /// NACK: [ID:16][QNameLen:4][QName][ReasonLen:4][Reason]
    Nack {
        id: Uuid,
        q_name: String,
        reason: String,
    },
    /// EXISTS: [QNameLen:4][QName]
    Exists {
        q_name: String,
    },
    
    // DLQ Commands
    /// PEEK_DLQ: [QNameLen:4][QName][Limit:4][Offset:4]
    PeekDLQ {
        q_name: String,
        limit: usize,
        offset: usize,
    },
    /// MOVE_TO_QUEUE: [QNameLen:4][QName][MessageID:16]
    MoveToQueue {
        q_name: String,
        message_id: Uuid,
    },
    /// DELETE_DLQ: [QNameLen:4][QName][MessageID:16]
    DeleteDLQ {
        q_name: String,
        message_id: Uuid,
    },
    /// PURGE_DLQ: [QNameLen:4][QName]
    PurgeDLQ {
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
            OP_Q_NACK => {
                let id_bytes = cursor.read_uuid_bytes()?;
                let id = Uuid::from_bytes(id_bytes);
                let q_name = cursor.read_string()?;
                let reason = cursor.read_string()?;
                Ok(Self::Nack { id, q_name, reason })
            }
            OP_Q_EXISTS => {
                let q_name = cursor.read_string()?;
                Ok(Self::Exists { q_name })
            }
            OP_Q_DELETE => {
                let q_name = cursor.read_string()?;
                Ok(Self::Delete { q_name })
            }
            OP_Q_PEEK_DLQ => {
                let q_name = cursor.read_string()?;
                let limit = cursor.read_u32()? as usize;
                let offset = cursor.read_u32()? as usize;
                Ok(Self::PeekDLQ { q_name, limit, offset })
            }
            OP_Q_MOVE_TO_QUEUE => {
                let q_name = cursor.read_string()?;
                let id_bytes = cursor.read_uuid_bytes()?;
                let message_id = Uuid::from_bytes(id_bytes);
                Ok(Self::MoveToQueue { q_name, message_id })
            }
            OP_Q_DELETE_DLQ => {
                let q_name = cursor.read_string()?;
                let id_bytes = cursor.read_uuid_bytes()?;
                let message_id = Uuid::from_bytes(id_bytes);
                Ok(Self::DeleteDLQ { q_name, message_id })
            }
            OP_Q_PURGE_DLQ => {
                let q_name = cursor.read_string()?;
                Ok(Self::PurgeDLQ { q_name })
            }
            _ => Err(format!("Unknown Queue opcode: 0x{:02X}", opcode)),
        }
    }
}

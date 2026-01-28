use bytes::Bytes;
use uuid::Uuid;
use crate::server::payload_cursor::PayloadCursor;
use crate::brokers::queues::QueueConfig;

pub const OP_Q_CREATE: u8 = 0x10;
pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_CONSUME: u8 = 0x12;
pub const OP_Q_ACK: u8 = 0x13;
pub const OP_Q_EXISTS: u8 = 0x14;

#[derive(Debug)]
pub enum QueueCommand {
    /// CREATE: [Flags:1][Visibility:8][MaxRetries:4][TTL:8][Delay:8][QNameLen:4][QName]
    Create {
        config: QueueConfig,
        q_name: String,
    },
    /// PUSH: [Priority:1][Delay:8][QNameLen:4][QName][Data...]
    Push {
        priority: u8,
        delay: Option<u64>,
        q_name: String,
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
                let _flags = cursor.read_u8()?;
                // Flags reserved (was passive 0x01)
                let visibility_timeout_ms = cursor.read_u64()?;
                let max_retries = cursor.read_u32()?;
                let ttl_ms = cursor.read_u64()?;
                let q_name = cursor.read_string()?;
                
                let config = QueueConfig {
                    visibility_timeout_ms,
                    max_retries,
                    ttl_ms,
                };
                
                Ok(Self::Create { config, q_name })
            }
            OP_Q_PUSH => {
                let priority = cursor.read_u8()?;
                let delay_ms = cursor.read_u64()?;
                let delay = if delay_ms == 0 { None } else { Some(delay_ms) };
                let q_name = cursor.read_string()?;
                let payload = cursor.read_remaining();
                Ok(Self::Push { priority, delay, q_name, payload })
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

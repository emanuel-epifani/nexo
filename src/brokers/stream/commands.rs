use bytes::Bytes;
use crate::server::payload_cursor::PayloadCursor;
use crate::server::protocol::ParseError;
use serde::Deserialize;

pub const OP_S_CREATE: u8 = 0x30;
pub const OP_S_PUB: u8 = 0x31;
pub const OP_S_FETCH: u8 = 0x32;
pub const OP_S_JOIN: u8 = 0x33;
pub const OP_S_ACK: u8 = 0x34;
pub const OP_S_EXISTS: u8 = 0x35;
pub const OP_S_DELETE: u8 = 0x36;
pub const OP_S_NACK: u8 = 0x37;
pub const OP_S_SEEK: u8 = 0x38;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceOptions {
    FileSync,
    FileAsync,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RetentionOptions {
    pub max_age_ms: Option<u64>,
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamCreateOptions {
    pub persistence: Option<PersistenceOptions>,
    pub retention: Option<RetentionOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeekTarget {
    Beginning,
    End,
}

#[derive(Debug)]
pub enum StreamCommand {
    /// CREATE: [TopicLen:4][Topic][JSONLen:4][JSON]
    Create {
        topic: String,
        options: StreamCreateOptions,
    },
    /// PUB: [TopicLen:4][Topic][Data...]
    Publish {
        topic: String,
        payload: Bytes,
    },
    /// FETCH: [TopicLen:4][Topic][GroupLen:4][Group][Limit:4]
    Fetch {
        topic: String,
        group: String,
        limit: u32,
    },
    /// JOIN: [GroupLen:4][Group][TopicLen:4][Topic]
    Join {
        group: String,
        topic: String,
    },
    /// ACK: [TopicLen:4][Topic][GroupLen:4][Group][Seq:8]
    Ack {
        topic: String,
        group: String,
        seq: u64,
    },
    /// NACK: [TopicLen:4][Topic][GroupLen:4][Group][Seq:8]
    Nack {
        topic: String,
        group: String,
        seq: u64,
    },
    /// SEEK: [TopicLen:4][Topic][GroupLen:4][Group][Target:1] (0=beginning, 1=end)
    Seek {
        topic: String,
        group: String,
        target: SeekTarget,
    },
    /// EXISTS: [TopicLen:4][Topic]
    Exists {
        topic: String,
    },
    /// DELETE: [TopicLen:4][Topic]
    Delete {
        topic: String,
    },
}

impl StreamCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_S_CREATE => {
                let topic = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                let options: StreamCreateOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON config: {}", e)))?;
                Ok(Self::Create { topic, options })
            }
            OP_S_PUB => {
                let topic = cursor.read_string()?;
                let payload = cursor.read_remaining();
                Ok(Self::Publish { topic, payload })
            }
            OP_S_FETCH => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let limit = cursor.read_u32()?;
                Ok(Self::Fetch { topic, group, limit })
            }
            OP_S_JOIN => {
                let group = cursor.read_string()?;
                let topic = cursor.read_string()?;
                Ok(Self::Join { group, topic })
            }
            OP_S_ACK => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let seq = cursor.read_u64()?;
                Ok(Self::Ack { topic, group, seq })
            }
            OP_S_NACK => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let seq = cursor.read_u64()?;
                Ok(Self::Nack { topic, group, seq })
            }
            OP_S_SEEK => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let target_byte = cursor.read_u8()?;
                let target = match target_byte {
                    0 => SeekTarget::Beginning,
                    1 => SeekTarget::End,
                    _ => return Err(ParseError::Invalid(format!("Invalid seek target: {}", target_byte))),
                };
                Ok(Self::Seek { topic, group, target })
            }
            OP_S_EXISTS => {
                let topic = cursor.read_string()?;
                Ok(Self::Exists { topic })
            }
            OP_S_DELETE => {
                let topic = cursor.read_string()?;
                Ok(Self::Delete { topic })
            }
            _ => Err(ParseError::Invalid(format!("Unknown Stream opcode: 0x{:02X}", opcode))),
        }
    }
}

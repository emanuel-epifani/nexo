use bytes::Bytes;
use crate::server::payload_cursor::PayloadCursor;
use crate::server::protocol::ParseError;
use serde::Deserialize;

pub const OP_S_CREATE: u8 = 0x30;
pub const OP_S_PUB: u8 = 0x31;
pub const OP_S_FETCH: u8 = 0x32;
pub const OP_S_JOIN: u8 = 0x33;
pub const OP_S_COMMIT: u8 = 0x34;
pub const OP_S_EXISTS: u8 = 0x35;
pub const OP_S_DELETE: u8 = 0x36;

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
    pub partitions: Option<u32>,
    pub persistence: Option<PersistenceOptions>,
    pub retention: Option<RetentionOptions>,
}

#[derive(Debug, Deserialize)]
pub struct StreamPublishOptions {
    pub key: Option<String>,
}

#[derive(Debug)]
pub enum StreamCommand {
    /// CREATE: [TopicLen:4][Topic][JSONLen:4][JSON]
    Create {
        topic: String,
        options: StreamCreateOptions,
    },
    /// PUB: [TopicLen:4][Topic][JSONLen:4][JSON][Data...]
    Publish {
        topic: String,
        options: StreamPublishOptions,
        payload: Bytes,
    },
    /// FETCH: [GenID:8][TopicLen:4][Topic][GroupLen:4][Group][Partition:4][Offset:8][Limit:4]
    Fetch {
        gen_id: u64,
        topic: String,
        group: String,
        partition: u32,
        offset: u64,
        limit: u32,
    },
    /// JOIN: [GroupLen:4][Group][TopicLen:4][Topic]
    Join {
        group: String,
        topic: String,
    },
    /// COMMIT: [GenID:8][GroupLen:4][Group][TopicLen:4][Topic][Partition:4][Offset:8]
    Commit {
        gen_id: u64,
        group: String,
        topic: String,
        partition: u32,
        offset: u64,
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
                tracing::debug!("Parsed S_CREATE: topic={}", topic);
                Ok(Self::Create { topic, options })
            }
            OP_S_PUB => {
                let topic = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                
                let options: StreamPublishOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON options: {}", e)))?;

                let payload = cursor.read_remaining();
                tracing::debug!("Parsed S_PUB: topic={}, len={}", topic, payload.len());
                Ok(Self::Publish { topic, options, payload })
            }
            OP_S_FETCH => {
                let gen_id = cursor.read_u64()?;
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let partition = cursor.read_u32()?;
                let offset = cursor.read_u64()?;
                let limit = cursor.read_u32()?;
                tracing::debug!("Parsed S_FETCH: topic={} group={} p={} off={} lim={} gen={}", topic, group, partition, offset, limit, gen_id);
                Ok(Self::Fetch { gen_id, topic, group, partition, offset, limit })
            }
            OP_S_JOIN => {
                let group = cursor.read_string()?;
                let topic = cursor.read_string()?;
                tracing::debug!("Parsed S_JOIN: topic={} group={}", topic, group);
                Ok(Self::Join { group, topic })
            }
            OP_S_COMMIT => {
                let gen_id = cursor.read_u64()?;
                let group = cursor.read_string()?;
                let topic = cursor.read_string()?;
                let partition = cursor.read_u32()?;
                let offset = cursor.read_u64()?;
                tracing::debug!("Parsed S_COMMIT: topic={} group={} p={} off={} gen={}", topic, group, partition, offset, gen_id);
                Ok(Self::Commit { gen_id, group, topic, partition, offset })
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

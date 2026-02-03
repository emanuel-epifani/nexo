use bytes::Bytes;
use crate::server::payload_cursor::PayloadCursor;
use serde::Deserialize;

pub const OP_PUB: u8 = 0x21;
pub const OP_SUB: u8 = 0x22;
pub const OP_UNSUB: u8 = 0x23;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubPublishOptions {
    pub retain: Option<bool>,
    pub ttl: Option<u64>,  // TTL in seconds
}

#[derive(Debug)]
pub enum PubSubCommand {
    /// PUB: [TopicLen:4][Topic][JSONLen:4][JSON][Data...]
    Publish {
        topic: String,
        options: PubSubPublishOptions,
        payload: Bytes,
    },
    /// SUB: [TopicLen:4][Topic]
    Subscribe {
        topic: String,
    },
    /// UNSUB: [TopicLen:4][Topic]
    Unsubscribe {
        topic: String,
    },
}

impl PubSubCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, String> {
        match opcode {
            OP_PUB => {
                let topic = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                
                let options: PubSubPublishOptions = serde_json::from_str(&json_str)
                    .map_err(|e| format!("Invalid JSON options: {}", e))?;

                let payload = cursor.read_remaining();
                Ok(Self::Publish { topic, options, payload })
            }
            OP_SUB => {
                let topic = cursor.read_string()?;
                Ok(Self::Subscribe { topic })
            }
            OP_UNSUB => {
                let topic = cursor.read_string()?;
                Ok(Self::Unsubscribe { topic })
            }
            _ => Err(format!("Unknown PubSub opcode: 0x{:02X}", opcode)),
        }
    }
}

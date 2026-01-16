use bytes::Bytes;
use crate::server::payload_cursor::PayloadCursor;

pub const OP_PUB: u8 = 0x21;
pub const OP_SUB: u8 = 0x22;
pub const OP_UNSUB: u8 = 0x23;

#[derive(Debug)]
pub enum PubSubCommand {
    /// PUB: [Flags:1][TopicLen:4][Topic][Data...]
    Publish {
        flags: u8,
        topic: String,
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
                let flags = cursor.read_u8()?;
                let topic = cursor.read_string()?;
                let payload = cursor.read_remaining();
                Ok(Self::Publish { flags, topic, payload })
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

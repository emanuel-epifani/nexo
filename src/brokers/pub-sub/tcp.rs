//! PubSub broker TCP surface: opcodes, command parsing, dispatch entry point.

use bytes::Bytes;

use crate::brokers::pub_sub::options::{PubSubPublishConfig, PubSubPublishOptions};
use crate::brokers::pub_sub::types::ClientId;
use crate::config::Config;
use crate::server::protocol::cursor::PayloadCursor;
use crate::server::protocol::{ParseError, Response};
use crate::NexoEngine;

// ==========================================
// OPCODES
// ==========================================

pub const OPCODE_MIN: u8 = 0x21;
pub const OPCODE_MAX: u8 = 0x2F;

pub const OP_PUB: u8 = 0x21;
pub const OP_SUB: u8 = 0x22;
pub const OP_UNSUB: u8 = 0x23;

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
enum PubSubCommand {
    Publish { topic: String, options: PubSubPublishOptions, payload: Bytes },
    Subscribe { topic: String },
    Unsubscribe { topic: String },
}

impl PubSubCommand {
    fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_PUB => {
                let topic = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                let options: PubSubPublishOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON options: {}", e)))?;
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
            _ => Err(ParseError::Invalid(format!("Unknown PubSub opcode: 0x{:02X}", opcode))),
        }
    }
}

// ==========================================
// DISPATCH ENTRY POINT
// ==========================================

pub async fn handle(
    opcode: u8,
    cursor: &mut PayloadCursor,
    engine: &NexoEngine,
    client_id: &ClientId,
) -> Response {
    let cmd = match PubSubCommand::parse(opcode, cursor) {
        Ok(c) => c,
        Err(e) => return Response::Error(e.to_string()),
    };

    let pubsub = &engine.pubsub;

    match cmd {
        PubSubCommand::Publish { options, topic, payload } => {
            let config = PubSubPublishConfig::from_options(options, &Config::global().pubsub);
            let _count = pubsub.publish(&topic, payload, config.retain, Some(config.ttl_seconds));
            Response::Ok
        }
        PubSubCommand::Subscribe { topic } => {
            pubsub.subscribe(client_id, &topic);
            Response::Ok
        }
        PubSubCommand::Unsubscribe { topic } => {
            pubsub.unsubscribe(client_id, &topic);
            Response::Ok
        }
    }
}

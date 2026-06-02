//! PubSub broker TCP surface: opcodes, command parsing, dispatch entry point.

use bytes::Bytes;

use crate::brokers::pub_sub::ClientId;
use crate::transport::tcp::protocol::cursor::PayloadCursor;
use crate::transport::tcp::protocol::{ParseError, Response};
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
    Publish { topic: String, retain: Option<bool>, ttl: Option<u64>, payload: Bytes },
    Subscribe { topic: String },
    Unsubscribe { topic: String },
}

impl PubSubCommand {
    fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_PUB => {
                let topic = cursor.read_string()?;
                let flags = cursor.read_u8()?;
                let retain: Option<bool> = if flags & 0x01 != 0 { Some(true) } else { None };
                let ttl = if flags & 0x02 != 0 { Some(cursor.read_u64()?) } else { None };
                let payload = cursor.read_remaining();
                Ok(Self::Publish { topic, retain, ttl, payload })
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
        PubSubCommand::Publish { topic, retain, ttl, payload } => {
            let _count = pubsub.publish(&topic, payload, retain.unwrap_or(false), ttl);
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

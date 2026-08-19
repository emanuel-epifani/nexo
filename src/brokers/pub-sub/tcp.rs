//! PubSub broker TCP surface: opcodes, command parsing, dispatch entry point.

use bytes::Bytes;

use crate::protocol::wire::PayloadCursor;
use crate::protocol::{FLAG_PUBSUB_PUB_CLEAR, FLAG_PUBSUB_PUB_HAS_TTL, FLAG_PUBSUB_PUB_RETAIN, ParseError, Response};
use crate::NexoEngine;

// ==========================================
// OPCODES
// ==========================================

pub use crate::protocol::{
    OP_PUB, OP_SUB, OP_UNSUB, PUBSUB_OPCODE_MAX as OPCODE_MAX, PUBSUB_OPCODE_MIN as OPCODE_MIN,
};

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
pub enum PubSubCommand {
    Publish { topic: String, retain: bool, clear: bool, ttl: Option<u32>, payload: Bytes },
    Subscribe { topic: String },
    Unsubscribe { topic: String },
}

impl PubSubCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_PUB => {
                let topic = cursor.read_string()?;
                let flags = cursor.read_u8()?;
                let retain = flags & FLAG_PUBSUB_PUB_RETAIN != 0;
                let clear = flags & FLAG_PUBSUB_PUB_CLEAR != 0;
                let ttl = if flags & FLAG_PUBSUB_PUB_HAS_TTL != 0 { Some(cursor.read_u32()?) } else { None };
                let payload = cursor.read_remaining();
                Ok(Self::Publish { topic, retain, clear, ttl, payload })
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
    session_id: &str,
) -> Response {
    let cmd = match PubSubCommand::parse(opcode, cursor) {
        Ok(c) => c,
        Err(e) => return Response::Error(e.to_string()),
    };

    let pubsub = &engine.pubsub;

    match cmd {
        PubSubCommand::Publish { topic, retain, clear, ttl, payload } => {
            pubsub.publish(&topic, payload, retain, clear, ttl)
                .map(|_| Response::Ok)
                .map_err(Response::Error)
                .unwrap_or_else(|e| e)
        }
        PubSubCommand::Subscribe { topic } => {
            pubsub.subscribe(session_id, &topic)
                .map(|_| Response::Ok)
                .map_err(Response::Error)
                .unwrap_or_else(|e| e)
        }
        PubSubCommand::Unsubscribe { topic } => {
            pubsub.unsubscribe(session_id, &topic);
            Response::Ok
        }
    }
}

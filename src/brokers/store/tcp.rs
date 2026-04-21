//! Store broker TCP surface: opcodes, command parsing, dispatch entry point.

use bytes::Bytes;
use serde::Deserialize;

use crate::transport::tcp::protocol::cursor::PayloadCursor;
use crate::transport::tcp::protocol::{ParseError, Response};
use crate::NexoEngine;

// ==========================================
// OPCODES
// ==========================================

pub const OPCODE_MIN: u8 = 0x02;
pub const OPCODE_MAX: u8 = 0x0F;

pub const OP_MAP_SET: u8 = 0x02;
pub const OP_MAP_GET: u8 = 0x03;
pub const OP_MAP_DEL: u8 = 0x04;

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MapSetOptions {
    pub ttl: Option<u64>,
}

#[derive(Debug)]
enum StoreCommand {
    MapSet { key: String, options: MapSetOptions, value: Bytes },
    MapGet { key: String },
    MapDel { key: String },
}

impl StoreCommand {
    fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_MAP_SET => {
                let key = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                let options: MapSetOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON options: {}", e)))?;
                let value = cursor.read_remaining();
                Ok(Self::MapSet { key, options, value })
            }
            OP_MAP_GET => {
                let key = cursor.read_string()?;
                Ok(Self::MapGet { key })
            }
            OP_MAP_DEL => {
                let key = cursor.read_string()?;
                Ok(Self::MapDel { key })
            }
            _ => Err(ParseError::Invalid(format!("Unknown Store opcode: 0x{:02X}", opcode))),
        }
    }
}

// ==========================================
// DISPATCH ENTRY POINT
// ==========================================

pub fn handle(opcode: u8, cursor: &mut PayloadCursor, engine: &NexoEngine) -> Response {
    let cmd = match StoreCommand::parse(opcode, cursor) {
        Ok(c) => c,
        Err(e) => return Response::Error(e.to_string()),
    };

    match cmd {
        StoreCommand::MapSet { key, options, value } => {
            engine.store.map.set(key, value, options.ttl);
            Response::Ok
        }
        StoreCommand::MapGet { key } => engine
            .store
            .map
            .get(&key)
            .map(Response::Data)
            .unwrap_or(Response::Null),
        StoreCommand::MapDel { key } => {
            engine.store.map.del(&key);
            Response::Ok
        }
    }
}

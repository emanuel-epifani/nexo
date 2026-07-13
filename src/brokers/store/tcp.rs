//! Store broker TCP surface: opcodes, command parsing, dispatch entry point.

use bytes::Bytes;

use crate::transport::tcp::protocol::wire::PayloadCursor;
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

#[derive(Debug)]
enum StoreCommand {
    Map(MapCmd),
}

#[derive(Debug)]
enum MapCmd {
    Set { key: String, ttl: Option<u64>, value: Bytes },
    Get { key: String },
    Del { key: String },
}

impl MapCmd {
    fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_MAP_SET => {
                let key = cursor.read_string()?;
                let flags = cursor.read_u8()?;
                let ttl = if flags & 0x01 != 0 { Some(cursor.read_u64()?) } else { None };
                let value = cursor.read_remaining();
                Ok(Self::Set { key, ttl, value })
            }
            OP_MAP_GET => {
                let key = cursor.read_string()?;
                Ok(Self::Get { key })
            }
            OP_MAP_DEL => {
                let key = cursor.read_string()?;
                Ok(Self::Del { key })
            }
            _ => Err(ParseError::Invalid(format!("Unknown Map opcode: 0x{:02X}", opcode))),
        }
    }
}

impl StoreCommand {
    fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_MAP_SET..=OP_MAP_DEL => Ok(Self::Map(MapCmd::parse(opcode, cursor)?)),
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
        StoreCommand::Map(c) => match c {
            MapCmd::Set { key, ttl, value } => {
                engine.store.map.set(key, value, ttl);
                Response::Ok
            }
            MapCmd::Get { key } => engine
                .store
                .map
                .get(&key)
                .map(Response::Data)
                .unwrap_or(Response::Null),
            MapCmd::Del { key } => {
                engine.store.map.del(&key);
                Response::Ok
            }
        },
    }
}

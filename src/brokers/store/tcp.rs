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
pub const OP_MAP_INCR: u8 = 0x05;
pub const OP_MAP_CLEAR_ALL: u8 = 0x06;
pub const OP_MAP_CLEAR_PREFIX: u8 = 0x07;

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
pub enum StoreCommand {
    Map(MapCmd),
}

#[derive(Debug)]
pub enum MapCmd {
    Set { key: String, ttl: Option<u64>, value: Bytes },
    Get { key: String },
    Del { key: String },
    Incr { key: String, delta: i64 },
    ClearAll,
    ClearPrefix { prefix: String },
}

impl MapCmd {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
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
            OP_MAP_INCR => {
                let key = cursor.read_string()?;
                let delta = cursor.read_i64()?;
                Ok(Self::Incr { key, delta })
            }
            OP_MAP_CLEAR_ALL => Ok(Self::ClearAll),
            OP_MAP_CLEAR_PREFIX => {
                let prefix = cursor.read_string()?;
                Ok(Self::ClearPrefix { prefix })
            }
            _ => Err(ParseError::Invalid(format!("Unknown Map opcode: 0x{:02X}", opcode))),
        }
    }
}

impl StoreCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_MAP_SET..=OP_MAP_CLEAR_PREFIX => Ok(Self::Map(MapCmd::parse(opcode, cursor)?)),
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
                match engine.store.map.set(key, value, ttl) {
                    Ok(()) => Response::Ok,
                    Err(msg) => Response::Error(msg),
                }
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
            MapCmd::Incr { key, delta } => {
                match engine.store.map.incr(&key, delta) {
                    Ok(new_val) => Response::Data(new_val),
                    Err(msg) => Response::Error(msg),
                }
            }
            MapCmd::ClearAll => {
                let count = engine.store.map.clear_all();
                let mut buf = vec![0x03u8];
                buf.extend_from_slice(&(count as i64).to_be_bytes());
                Response::Data(Bytes::from(buf))
            }
            MapCmd::ClearPrefix { prefix } => {
                let count = engine.store.map.clear_with_prefix(&prefix);
                let mut buf = vec![0x03u8];
                buf.extend_from_slice(&(count as i64).to_be_bytes());
                Response::Data(Bytes::from(buf))
            }
        },
    }
}

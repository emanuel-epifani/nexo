//! Store broker TCP surface: opcodes, command parsing, dispatch entry point.

use bytes::Bytes;

use crate::brokers::BrokerError;
use crate::protocol::wire::PayloadCursor;
use crate::protocol::{ErrorCode, ParseError, Response, DATA_TYPE_INT, FLAG_STORE_MAP_SET_HAS_TTL};
use crate::transport::tcp::error_response;
use crate::NexoEngine;

// ==========================================
// OPCODES
// ==========================================

pub use crate::protocol::{
    OP_MAP_CLEAR_ALL, OP_MAP_CLEAR_PREFIX, OP_MAP_DEL, OP_MAP_GET, OP_MAP_INCR, OP_MAP_SET,
    STORE_OPCODE_MAX as OPCODE_MAX, STORE_OPCODE_MIN as OPCODE_MIN,
};

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
pub enum StoreCommand {
    Map(MapCmd),
}

#[derive(Debug)]
pub enum MapCmd {
    Set {
        key: String,
        ttl: Option<u64>,
        value: Bytes,
    },
    Get {
        key: String,
    },
    Del {
        key: String,
    },
    Incr {
        key: String,
        delta: i64,
    },
    ClearAll,
    ClearPrefix {
        prefix: String,
    },
}

impl MapCmd {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_MAP_SET => {
                let key = cursor.read_string()?;
                let flags = cursor.read_u8()?;
                let ttl = if flags & FLAG_STORE_MAP_SET_HAS_TTL != 0 {
                    Some(cursor.read_u64()?)
                } else {
                    None
                };
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
            _ => Err(ParseError::Invalid(format!(
                "Unknown Map opcode: 0x{:02X}",
                opcode
            ))),
        }
    }
}

impl StoreCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_MAP_SET..=OP_MAP_CLEAR_PREFIX => Ok(Self::Map(MapCmd::parse(opcode, cursor)?)),
            _ => Err(ParseError::Invalid(format!(
                "Unknown Store opcode: 0x{:02X}",
                opcode
            ))),
        }
    }
}

// ==========================================
// DISPATCH ENTRY POINT
// ==========================================

pub fn handle(opcode: u8, cursor: &mut PayloadCursor, engine: &NexoEngine) -> Response {
    let cmd = match StoreCommand::parse(opcode, cursor) {
        Ok(c) => c,
        Err(error) => return Response::error(ErrorCode::ProtocolError, error.to_string()),
    };

    match cmd {
        StoreCommand::Map(c) => match c {
            MapCmd::Set { key, ttl, value } => match engine.store.map.set(key, value, ttl) {
                Ok(()) => Response::Ok,
                Err(message) => error_response(BrokerError::invalid_argument(message)),
            },
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
            MapCmd::Incr { key, delta } => match engine.store.map.incr(&key, delta) {
                Ok(new_val) => Response::Data(new_val),
                Err(message) => error_response(BrokerError::invalid_argument(message)),
            },
            MapCmd::ClearAll => {
                let count = engine.store.map.clear_all();
                let mut buf = vec![DATA_TYPE_INT];
                buf.extend_from_slice(&(count as i64).to_be_bytes());
                Response::Data(Bytes::from(buf))
            }
            MapCmd::ClearPrefix { prefix } => {
                let count = engine.store.map.clear_with_prefix(&prefix);
                let mut buf = vec![DATA_TYPE_INT];
                buf.extend_from_slice(&(count as i64).to_be_bytes());
                Response::Data(Bytes::from(buf))
            }
        },
    }
}

use crate::server::payload_cursor::PayloadCursor;
use crate::server::protocol::ParseError;
use crate::brokers::store::map::commands::{MapCommand, OP_MAP_SET, OP_MAP_GET, OP_MAP_DEL};

#[derive(Debug)]
pub enum StoreCommand {
    Map(MapCommand),
}

impl StoreCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            // Dispatch to sub-modules based on opcode ranges or specific opcodes
            self::OP_MAP_SET | self::OP_MAP_GET | self::OP_MAP_DEL => {
                let cmd = MapCommand::parse(opcode, cursor)?;
                Ok(Self::Map(cmd))
            }
            _ => Err(ParseError::Invalid(format!("Unknown Store opcode: 0x{:02X}", opcode))),
        }
    }
}

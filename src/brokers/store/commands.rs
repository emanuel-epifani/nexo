use bytes::Bytes;
use crate::server::payload_cursor::PayloadCursor;

pub const OP_KV_SET: u8 = 0x02;
pub const OP_KV_GET: u8 = 0x03;
pub const OP_KV_DEL: u8 = 0x04;

#[derive(Debug)]
pub enum StoreCommand {
    /// SET: [TTL:8][KeyLen:4][Key][Val...]
    Set {
        ttl: Option<u64>,
        key: String,
        value: Bytes,
    },
    /// GET: [KeyLen:4][Key]
    Get {
        key: String,
    },
    /// DEL: [KeyLen:4][Key]
    Del {
        key: String,
    },
}

impl StoreCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, String> {
        match opcode {
            OP_KV_SET => {
                let ttl_secs = cursor.read_u64()?;
                let ttl = if ttl_secs == 0 { None } else { Some(ttl_secs) };
                let key = cursor.read_string()?;
                let value = cursor.read_remaining();
                Ok(Self::Set { ttl, key, value })
            }
            OP_KV_GET => {
                let key = cursor.read_string()?;
                Ok(Self::Get { key })
            }
            OP_KV_DEL => {
                let key = cursor.read_string()?;
                Ok(Self::Del { key })
            }
            _ => Err(format!("Unknown KV opcode: 0x{:02X}", opcode)),
        }
    }
}

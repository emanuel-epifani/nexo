use bytes::Bytes;
use crate::server::payload_cursor::PayloadCursor;
use serde::Deserialize;

pub const OP_KV_SET: u8 = 0x02;
pub const OP_KV_GET: u8 = 0x03;
pub const OP_KV_DEL: u8 = 0x04;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreSetOptions {
    pub ttl: Option<u64>,
}

#[derive(Debug)]
pub enum StoreCommand {
    /// SET: [KeyLen:4][Key][JSONLen:4][JSON][Val...]
    Set {
        key: String,
        options: StoreSetOptions,
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
                let key = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                
                let options: StoreSetOptions = serde_json::from_str(&json_str)
                    .map_err(|e| format!("Invalid JSON options: {}", e))?;

                let value = cursor.read_remaining();
                Ok(Self::Set { key, options, value })
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

//! Stream broker TCP surface: opcodes, command parsing, response wire
//! encoding and the dispatch entry point `handle(...)`.

use bytes::Bytes;

use crate::brokers::stream::domain::message::Message;
use crate::brokers::stream::options::{RetentionOptions, SeekTarget, StreamCreateOptions};
use crate::transport::tcp::protocol::wire::{PayloadCursor, PayloadWriter};
use crate::transport::tcp::protocol::{ParseError, Response};
use crate::NexoEngine;

// ==========================================
// OPCODES
// ==========================================

pub const OPCODE_MIN: u8 = 0x30;
pub const OPCODE_MAX: u8 = 0x3F;

pub const OP_S_CREATE: u8 = 0x30;
pub const OP_S_PUB: u8 = 0x31;
pub const OP_S_FETCH: u8 = 0x32;
pub const OP_S_JOIN: u8 = 0x33;
pub const OP_S_ACK: u8 = 0x34;
pub const OP_S_EXISTS: u8 = 0x35;
pub const OP_S_DELETE: u8 = 0x36;
pub const OP_S_SEEK: u8 = 0x38;
pub const OP_S_LEAVE: u8 = 0x39;
pub const OP_S_PEEK_DLT: u8 = 0x3A;
pub const OP_S_MOVE_TO_STREAM: u8 = 0x3B;
pub const OP_S_DELETE_DLT: u8 = 0x3C;
pub const OP_S_PURGE_DLT: u8 = 0x3D;

const MAX_PUBLISH_BATCH: usize = 65_536;
const MIN_PUBLISH_ITEM_BYTES: usize = 6;

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
pub struct PubItem {
    pub key: Option<Bytes>,
    pub payload: Bytes,
}

#[derive(Debug)]
pub enum StreamCommand {
    Create { topic: String, options: StreamCreateOptions },
    Publish { topic: String, items: Vec<PubItem> },
    Fetch { topic: String, group: String, consumer_id: String, generation: u64, limit: u32, wait_ms: u32 },
    Join { topic: String, group: String },
    Ack { topic: String, group: String, consumer_id: String, generation: u64, seq: u64 },
    Seek { topic: String, group: String, target: SeekTarget },
    Exists { topic: String },
    Delete { topic: String },
    Leave { topic: String, group: String, consumer_id: String, generation: u64 },
    PeekDlt { topic: String, group: String, limit: u32, offset: u32 },
    MoveToStream { topic: String, group: String, seq: u64 },
    DeleteDlt { topic: String, group: String, seq: u64 },
    PurgeDlt { topic: String, group: String },
}

impl StreamCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_S_CREATE => {
                let topic = cursor.read_string()?;
                let flags = cursor.read_u8()?;
                let max_age_ms = if flags & 0x01 != 0 { Some(cursor.read_u64()?) } else { None };
                let max_bytes = if flags & 0x02 != 0 { Some(cursor.read_u64()?) } else { None };
                let retention = if max_age_ms.is_some() || max_bytes.is_some() {
                    Some(RetentionOptions { max_age_ms, max_bytes })
                } else {
                    None
                };
                Ok(Self::Create { topic, options: StreamCreateOptions { retention } })
            }
            OP_S_PUB => {
                let topic = cursor.read_string()?;
                let count = cursor.read_u32()? as usize;
                if count > MAX_PUBLISH_BATCH {
                    return Err(ParseError::Invalid(format!(
                        "Publish batch too large: {} items (max: {})",
                        count, MAX_PUBLISH_BATCH
                    )));
                }
                if count > cursor.len() / MIN_PUBLISH_ITEM_BYTES {
                    return Err(ParseError::Invalid(format!(
                        "Publish batch count {} exceeds remaining payload",
                        count
                    )));
                }
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let key_len = cursor.read_u16()? as usize;
                    let key = if key_len > 0 { Some(cursor.read_bytes(key_len)?) } else { None };
                    let payload_len = cursor.read_u32()? as usize;
                    let payload = cursor.read_bytes(payload_len)?;
                    items.push(PubItem { key, payload });
                }
                Ok(Self::Publish { topic, items })
            }
            OP_S_FETCH => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let consumer_id = cursor.read_string()?;
                let generation = cursor.read_u64()?;
                let limit = cursor.read_u32()?;
                let wait_ms = cursor.read_u32()?;
                Ok(Self::Fetch { topic, group, consumer_id, generation, limit, wait_ms })
            }
            OP_S_JOIN => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                Ok(Self::Join { topic, group })
            }
            OP_S_ACK => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let consumer_id = cursor.read_string()?;
                let generation = cursor.read_u64()?;
                let seq = cursor.read_u64()?;
                Ok(Self::Ack { topic, group, consumer_id, generation, seq })
            }
            OP_S_SEEK => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let target_byte = cursor.read_u8()?;
                let target = match target_byte {
                    0 => SeekTarget::Beginning,
                    1 => SeekTarget::End,
                    _ => return Err(ParseError::Invalid(format!("Invalid seek target: {}", target_byte))),
                };
                Ok(Self::Seek { topic, group, target })
            }
            OP_S_EXISTS => {
                let topic = cursor.read_string()?;
                Ok(Self::Exists { topic })
            }
            OP_S_DELETE => {
                let topic = cursor.read_string()?;
                Ok(Self::Delete { topic })
            }
            OP_S_LEAVE => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let consumer_id = cursor.read_string()?;
                let generation = cursor.read_u64()?;
                Ok(Self::Leave { topic, group, consumer_id, generation })
            }
            OP_S_PEEK_DLT => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let limit = cursor.read_u32()?;
                let offset = cursor.read_u32()?;
                Ok(Self::PeekDlt { topic, group, limit, offset })
            }
            OP_S_MOVE_TO_STREAM => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let seq = cursor.read_u64()?;
                Ok(Self::MoveToStream { topic, group, seq })
            }
            OP_S_DELETE_DLT => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                let seq = cursor.read_u64()?;
                Ok(Self::DeleteDlt { topic, group, seq })
            }
            OP_S_PURGE_DLT => {
                let topic = cursor.read_string()?;
                let group = cursor.read_string()?;
                Ok(Self::PurgeDlt { topic, group })
            }
            _ => Err(ParseError::Invalid(format!("Unknown Stream opcode: 0x{:02X}", opcode))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_parser_rejects_huge_count_before_allocation() {
        let mut writer = PayloadWriter::new();
        writer.put_str("topic").put_u32(u32::MAX);
        let mut cursor = PayloadCursor::new(writer.into_bytes());

        let error = StreamCommand::parse(OP_S_PUB, &mut cursor).unwrap_err();

        assert!(error.to_string().contains("Publish batch too large"));
    }

    #[test]
    fn publish_parser_rejects_count_larger_than_payload() {
        let mut writer = PayloadWriter::new();
        writer.put_str("topic").put_u32(2);
        let mut cursor = PayloadCursor::new(writer.into_bytes());

        let error = StreamCommand::parse(OP_S_PUB, &mut cursor).unwrap_err();

        assert!(error.to_string().contains("exceeds remaining payload"));
    }
}

// ==========================================
// WIRE RESPONSES
// ==========================================

fn encode_publish_batch(seqs: &[u64]) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u32(seqs.len() as u32);
    for &seq in seqs {
        w.put_u64(seq);
    }
    w.into_bytes()
}

fn encode_fetch(messages: &[Message]) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u32(messages.len() as u32);
    for msg in messages {
        w.put_u64(msg.seq);
        w.put_u64(msg.timestamp);
        let key_len = msg.key.as_ref().map_or(0, |k| k.len());
        w.put_u16(key_len as u16);
        if let Some(k) = &msg.key { w.put_raw(k); }
        w.put_bytes(&msg.payload);
    }
    w.into_bytes()
}

fn encode_join_group(ack_floor: u64, generation: u64, consumer_id: &str) -> Bytes {
    let mut w = PayloadWriter::with_capacity(16 + 4 + consumer_id.len());
    w.put_u64(ack_floor);
    w.put_u64(generation);
    w.put_str(consumer_id);
    w.into_bytes()
}

fn encode_bool(value: bool) -> Bytes {
    let mut w = PayloadWriter::with_capacity(1);
    w.put_bool(value);
    w.into_bytes()
}

fn encode_peek_dlt(entries: &[(u64, String, u32, Option<Bytes>)]) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u32(entries.len() as u32);
    for (seq, reason, attempts, key) in entries {
        w.put_u64(*seq);
        w.put_str(reason);
        w.put_u32(*attempts);
        let key_len = key.as_ref().map_or(0, |k| k.len());
        w.put_u16(key_len as u16);
        if let Some(k) = key { w.put_raw(k); }
    }
    w.into_bytes()
}

fn encode_purge_dlt(count: usize) -> Bytes {
    let mut w = PayloadWriter::with_capacity(4);
    w.put_u32(count as u32);
    w.into_bytes()
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
    let cmd = match StreamCommand::parse(opcode, cursor) {
        Ok(c) => c,
        Err(e) => return Response::Error(e.to_string()),
    };

    let stream = &engine.stream;
    let client = session_id.to_owned();

    match cmd {
        StreamCommand::Create { topic, options } => match stream.create_topic(topic, options).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        StreamCommand::Publish { topic, items } => {
            let batch: Vec<(Option<Bytes>, Bytes)> = items
                .into_iter()
                .map(|item| (item.key, item.payload))
                .collect();
            match stream.publish_batch(&topic, batch).await {
                Ok(seqs) => Response::Data(encode_publish_batch(&seqs)),
                Err(e) => Response::Error(e),
            }
        },
        StreamCommand::Fetch { topic, group, consumer_id, generation, limit, wait_ms } => {
            match stream.fetch(&group, &consumer_id, generation, limit as usize, &topic, wait_ms as u64).await {
                Ok(messages) => Response::Data(encode_fetch(&messages)),
                Err(e) => Response::Error(e),
            }
        }
        StreamCommand::Join { topic, group } => match stream.join_group(&group, &topic, &client).await {
            Ok(result) => Response::Data(encode_join_group(
                result.ack_floor,
                result.generation,
                &result.consumer_id,
            )),
            Err(e) => Response::Error(e),
        },
        StreamCommand::Ack { topic, group, consumer_id, generation, seq } => match stream.ack(&group, &topic, &consumer_id, generation, seq).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        StreamCommand::Seek { topic, group, target } => match stream.seek(&group, &topic, target).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        StreamCommand::Leave { topic, group, consumer_id, generation } => match stream.leave_group(&group, &topic, &consumer_id, generation).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        StreamCommand::Exists { topic } => {
            let found = stream.exists(&topic).await;
            Response::Data(encode_bool(found))
        }
        StreamCommand::Delete { topic } => match stream.delete_topic(topic).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        StreamCommand::PeekDlt { topic, group, limit, offset } => match stream.peek_dlt(&topic, &group, limit as usize, offset as usize).await {
            Ok(entries) => Response::Data(encode_peek_dlt(&entries)),
            Err(e) => Response::Error(e),
        },
        StreamCommand::MoveToStream { topic, group, seq } => match stream.move_to_stream(&topic, &group, seq).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        StreamCommand::DeleteDlt { topic, group, seq } => match stream.delete_dlt(&topic, &group, seq).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        StreamCommand::PurgeDlt { topic, group } => match stream.purge_dlt(&topic, &group).await {
            Ok(count) => Response::Data(encode_purge_dlt(count)),
            Err(e) => Response::Error(e),
        },
    }
}

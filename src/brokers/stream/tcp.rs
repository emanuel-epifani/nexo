//! Stream broker TCP surface: opcodes, command parsing, response wire
//! encoding and the dispatch entry point `handle(...)`.

use bytes::{Bytes, BufMut, BytesMut};

use crate::brokers::pub_sub::ClientId;
use crate::brokers::stream::domain::message::Message;
use crate::brokers::stream::options::{SeekTarget, StreamCreateOptions};
use crate::transport::tcp::protocol::cursor::PayloadCursor;
use crate::transport::tcp::protocol::{ParseError, Response, ToWire};
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

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
enum StreamCommand {
    Create { topic: String, options: StreamCreateOptions },
    Publish { topic: String, payload: Bytes },
    Fetch { topic: String, group: String, consumer_id: String, generation: u64, limit: u32, wait_ms: u32 },
    Join { group: String, topic: String },
    Ack { topic: String, group: String, consumer_id: String, generation: u64, seq: u64 },
    Seek { topic: String, group: String, target: SeekTarget },
    Exists { topic: String },
    Delete { topic: String },
    Leave { topic: String, group: String, consumer_id: String, generation: u64 },
}

impl StreamCommand {
    fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_S_CREATE => {
                let topic = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                let options: StreamCreateOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON config: {}", e)))?;
                Ok(Self::Create { topic, options })
            }
            OP_S_PUB => {
                let topic = cursor.read_string()?;
                let payload = cursor.read_remaining();
                Ok(Self::Publish { topic, payload })
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
                let group = cursor.read_string()?;
                let topic = cursor.read_string()?;
                Ok(Self::Join { group, topic })
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
            _ => Err(ParseError::Invalid(format!("Unknown Stream opcode: 0x{:02X}", opcode))),
        }
    }
}

// ==========================================
// WIRE RESPONSES
// ==========================================

struct PublishResponse { seq: u64 }

impl ToWire for PublishResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(self.seq);
        buf.freeze()
    }
}

struct FetchResponse { messages: Vec<Message> }

impl ToWire for FetchResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(self.messages.len() as u32);
        for msg in &self.messages {
            buf.put_u64(msg.seq);
            buf.put_u64(msg.timestamp);
            buf.put_u32(msg.payload.len() as u32);
            buf.put_slice(&msg.payload);
        }
        buf.freeze()
    }
}

struct JoinGroupResponse {
    ack_floor: u64,
    generation: u64,
    consumer_id: String,
}

impl ToWire for JoinGroupResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16 + 4 + self.consumer_id.len());
        buf.put_u64(self.ack_floor);
        buf.put_u64(self.generation);
        buf.put_u32(self.consumer_id.len() as u32);
        buf.put_slice(self.consumer_id.as_bytes());
        buf.freeze()
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
    let cmd = match StreamCommand::parse(opcode, cursor) {
        Ok(c) => c,
        Err(e) => return Response::Error(e.to_string()),
    };

    let stream = &engine.stream;
    let client = client_id.0.clone();

    match cmd {
        StreamCommand::Create { topic, options } => match stream.create_topic(topic, options).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        StreamCommand::Publish { topic, payload } => match stream.publish(&topic, payload).await {
            Ok(seq) => Response::Data(PublishResponse { seq }.to_wire()),
            Err(e) => Response::Error(e),
        },
        StreamCommand::Fetch { topic, group, consumer_id, generation, limit, wait_ms } => {
            match stream.fetch(&group, &consumer_id, generation, limit as usize, &topic, wait_ms as u64).await {
                Ok(messages) => Response::Data(FetchResponse { messages }.to_wire()),
                Err(e) => Response::Error(e),
            }
        }
        StreamCommand::Join { group, topic } => match stream.join_group(&group, &topic, &client).await {
            Ok(result) => Response::Data(JoinGroupResponse {
                ack_floor: result.ack_floor,
                generation: result.generation,
                consumer_id: result.consumer_id,
            }.to_wire()),
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
        StreamCommand::Exists { topic } => match stream.exists(&topic).await {
            true => Response::Ok,
            false => Response::Error("Stream not found".to_string()),
        },
        StreamCommand::Delete { topic } => match stream.delete_topic(topic).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
    }
}

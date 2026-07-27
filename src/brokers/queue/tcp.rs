//! Queue broker TCP surface: opcodes, command parsing, response wire
//! encoding and the single dispatch entry point `handle(...)`.

use bytes::Bytes;
use uuid::Uuid;

use crate::transport::tcp::protocol::wire::{PayloadCursor, PayloadWriter};
use crate::transport::tcp::protocol::{ParseError, Response};
use crate::NexoEngine;

use crate::brokers::queue::domain::dlq::DlqMessage;
use crate::brokers::queue::options::QueueCreateOptions;
use crate::brokers::queue::domain::queue::Message;

// ==========================================
// OPCODES
// ==========================================

pub const OPCODE_MIN: u8 = 0x10;
pub const OPCODE_MAX: u8 = 0x1F;

pub const OP_Q_CREATE: u8 = 0x10;
pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_CONSUME: u8 = 0x12;
pub const OP_Q_ACK: u8 = 0x13;
pub const OP_Q_EXISTS: u8 = 0x14;
pub const OP_Q_DELETE: u8 = 0x15;
pub const OP_Q_NACK: u8 = 0x1A;

// DLQ Operations
pub const OP_Q_PEEK_DLQ: u8 = 0x16;
pub const OP_Q_MOVE_TO_QUEUE: u8 = 0x17;
pub const OP_Q_DELETE_DLQ: u8 = 0x18;
pub const OP_Q_PURGE_DLQ: u8 = 0x19;

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
pub struct PushItem {
    pub priority: Option<u8>,
    pub payload: Bytes,
}

#[derive(Debug)]
pub enum QueueCommand {
    Create { q_name: String, options: QueueCreateOptions },
    Push { q_name: String, items: Vec<PushItem> },
    Consume { q_name: String, batch_size: usize, wait_ms: u64 },
    Delete { q_name: String },
    Ack { id: Uuid, delivery_token: u64, q_name: String },
    Nack { id: Uuid, delivery_token: u64, q_name: String, reason: String },
    Exists { q_name: String },
    PeekDLQ { q_name: String, limit: usize, offset: usize },
    MoveToQueue { q_name: String, message_id: Uuid },
    DeleteDLQ { q_name: String, message_id: Uuid },
    PurgeDLQ { q_name: String },
}

impl QueueCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_Q_CREATE => {
                let q_name = cursor.read_string()?;
                let flags = cursor.read_u8()?;
                let visibility_timeout_ms = if flags & 0x01 != 0 { Some(cursor.read_u64()?) } else { None };
                let max_deliveries = if flags & 0x02 != 0 { Some(cursor.read_u32()?) } else { None };
                Ok(Self::Create { q_name, options: QueueCreateOptions { visibility_timeout_ms, max_deliveries } })
            }
            OP_Q_PUSH => {
                let q_name = cursor.read_string()?;
                let count = cursor.read_u32()? as usize;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let flags = cursor.read_u8()?;
                    let priority = if flags & 0x01 != 0 { Some(cursor.read_u8()?) } else { None };
                    let payload_len = cursor.read_u32()? as usize;
                    let payload = cursor.read_bytes(payload_len)?;
                    items.push(PushItem { priority, payload });
                }
                Ok(Self::Push { q_name, items })
            }
            OP_Q_CONSUME => {
                let q_name = cursor.read_string()?;
                let batch_size = cursor.read_u32()? as usize;
                let wait_ms = cursor.read_u32()? as u64;
                Ok(Self::Consume { q_name, batch_size, wait_ms })
            }
            OP_Q_ACK => {
                let id = Uuid::from_bytes(cursor.read_uuid_bytes()?);
                let delivery_token = cursor.read_u64()?;
                let q_name = cursor.read_string()?;
                Ok(Self::Ack { id, delivery_token, q_name })
            }
            OP_Q_NACK => {
                let id = Uuid::from_bytes(cursor.read_uuid_bytes()?);
                let delivery_token = cursor.read_u64()?;
                let q_name = cursor.read_string()?;
                let reason = cursor.read_string()?;
                Ok(Self::Nack { id, delivery_token, q_name, reason })
            }
            OP_Q_EXISTS => {
                let q_name = cursor.read_string()?;
                Ok(Self::Exists { q_name })
            }
            OP_Q_DELETE => {
                let q_name = cursor.read_string()?;
                Ok(Self::Delete { q_name })
            }
            OP_Q_PEEK_DLQ => {
                let q_name = cursor.read_string()?;
                let limit = cursor.read_u32()? as usize;
                let offset = cursor.read_u32()? as usize;
                Ok(Self::PeekDLQ { q_name, limit, offset })
            }
            OP_Q_MOVE_TO_QUEUE => {
                let q_name = cursor.read_string()?;
                let message_id = Uuid::from_bytes(cursor.read_uuid_bytes()?);
                Ok(Self::MoveToQueue { q_name, message_id })
            }
            OP_Q_DELETE_DLQ => {
                let q_name = cursor.read_string()?;
                let message_id = Uuid::from_bytes(cursor.read_uuid_bytes()?);
                Ok(Self::DeleteDLQ { q_name, message_id })
            }
            OP_Q_PURGE_DLQ => {
                let q_name = cursor.read_string()?;
                Ok(Self::PurgeDLQ { q_name })
            }
            _ => Err(ParseError::Invalid(format!("Unknown Queue opcode: 0x{:02X}", opcode))),
        }
    }
}

// ==========================================
// WIRE RESPONSES
// ==========================================

fn encode_consume_batch(messages: &[Message]) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u32(messages.len() as u32);
    for msg in messages {
        w.put_uuid(msg.id.as_bytes());
        w.put_u64(msg.delivery_token);
        w.put_bytes(&msg.payload);
    }
    w.into_bytes()
}

fn encode_peek_dlq(total: usize, messages: &[DlqMessage]) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u32(total as u32);
    w.put_u32(messages.len() as u32);
    for msg in messages {
        w.put_uuid(msg.id.as_bytes());
        w.put_bytes(&msg.payload);
        w.put_u32(msg.attempts);
        w.put_str(&msg.failure_reason);
    }
    w.into_bytes()
}

fn encode_bool(value: bool) -> Bytes {
    let mut w = PayloadWriter::with_capacity(1);
    w.put_bool(value);
    w.into_bytes()
}

fn encode_count(count: usize) -> Bytes {
    let mut w = PayloadWriter::with_capacity(4);
    w.put_u32(count as u32);
    w.into_bytes()
}

// ==========================================
// DISPATCH ENTRY POINT
// ==========================================

pub async fn handle(opcode: u8, cursor: &mut PayloadCursor, engine: &NexoEngine) -> Response {
    let cmd = match QueueCommand::parse(opcode, cursor) {
        Ok(c) => c,
        Err(e) => return Response::Error(e.to_string()),
    };

    let queue = &engine.queue;

    match cmd {
        QueueCommand::Create { q_name, options } => match queue.create_queue(q_name, options).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        QueueCommand::Push { q_name, items } => {
            let batch: Vec<(Bytes, u8)> = items
                .into_iter()
                .map(|item| (item.payload, item.priority.unwrap_or(0)))
                .collect();
            match queue.push_batch(q_name, batch).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Consume { q_name, batch_size, wait_ms } => {
            match queue.consume_batch(q_name, Some(batch_size), Some(wait_ms)).await {
                Ok(messages) => Response::Data(encode_consume_batch(&messages)),
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Ack { id, delivery_token, q_name } => {
            let found = queue.ack(&q_name, id, delivery_token).await;
            Response::Data(encode_bool(found))
        }
        QueueCommand::Nack { id, delivery_token, q_name, reason } => {
            let found = queue.nack(&q_name, id, delivery_token, reason).await;
            Response::Data(encode_bool(found))
        }
        QueueCommand::Exists { q_name } => {
            let found = queue.exists(&q_name).await;
            Response::Data(encode_bool(found))
        }
        QueueCommand::Delete { q_name } => match queue.delete_queue(q_name).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        QueueCommand::PeekDLQ { q_name, limit, offset } => {
            match queue.peek_dlq(&q_name, limit, offset).await {
                Ok((total, messages)) => Response::Data(encode_peek_dlq(total, &messages)),
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::MoveToQueue { q_name, message_id } => {
            match queue.move_to_queue(&q_name, message_id).await {
                Ok(found) => Response::Data(encode_bool(found)),
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::DeleteDLQ { q_name, message_id } => {
            match queue.delete_dlq(&q_name, message_id).await {
                Ok(found) => Response::Data(encode_bool(found)),
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::PurgeDLQ { q_name } => match queue.purge_dlq(&q_name).await {
            Ok(count) => Response::Data(encode_count(count)),
            Err(e) => Response::Error(e),
        },
    }
}

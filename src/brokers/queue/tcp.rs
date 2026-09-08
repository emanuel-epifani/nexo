//! Queue broker TCP surface: opcodes, command parsing, response wire
//! encoding and the single dispatch entry point `handle(...)`.

use bytes::Bytes;
use uuid::Uuid;

use crate::protocol::wire::{PayloadCursor, PayloadWriter};
use crate::protocol::{
    ErrorCode, ParseError, ProvisionStatus, Response, FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES,
    FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT, FLAG_QUEUE_Q_PUSH_HAS_PRIORITY,
};
use crate::NexoEngine;

use crate::brokers::queue::domain::dlq::DlqMessage;
use crate::brokers::queue::domain::queue::{Message, QueueDefinition};
use crate::brokers::queue::options::QueueCreateOptions;
use crate::brokers::{ProvisionOutcome, ProvisionResult};
use crate::transport::tcp::error_response;

// ==========================================
// OPCODES
// ==========================================

use crate::protocol::QUEUE_MAX_PUSH_ITEMS as MAX_PUSH_ITEMS;
pub use crate::protocol::{
    OP_Q_ACK, OP_Q_CONSUME, OP_Q_CREATE, OP_Q_DELETE, OP_Q_DELETE_DLQ, OP_Q_DESCRIBE, OP_Q_EXISTS,
    OP_Q_MOVE_TO_QUEUE, OP_Q_NACK, OP_Q_PEEK_DLQ, OP_Q_PURGE_DLQ, OP_Q_PUSH,
    QUEUE_OPCODE_MAX as OPCODE_MAX, QUEUE_OPCODE_MIN as OPCODE_MIN,
};

// ==========================================
// WIRE LIMITS (server-side caps before allocation)
// ==========================================

/// Minimum bytes per push item (flags:1 + payload_len:4).
const MIN_PUSH_ITEM_BYTES: usize = 5;

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
    Create {
        q_name: String,
        options: QueueCreateOptions,
    },
    Push {
        q_name: String,
        items: Vec<PushItem>,
    },
    Consume {
        q_name: String,
        batch_size: usize,
        wait_ms: u64,
    },
    Delete {
        q_name: String,
    },
    Ack {
        id: Uuid,
        delivery_token: u64,
        q_name: String,
    },
    Nack {
        id: Uuid,
        delivery_token: u64,
        q_name: String,
        reason: String,
    },
    Exists {
        q_name: String,
    },
    Describe {
        q_name: String,
    },
    PeekDLQ {
        q_name: String,
        limit: usize,
        offset: usize,
    },
    MoveToQueue {
        q_name: String,
        message_id: Uuid,
    },
    DeleteDLQ {
        q_name: String,
        message_id: Uuid,
    },
    PurgeDLQ {
        q_name: String,
    },
}

impl QueueCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_Q_CREATE => {
                let q_name = cursor.read_string()?;
                let flags = cursor.read_u8()?;
                let visibility_timeout_ms =
                    if flags & FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT != 0 {
                        Some(cursor.read_u64()?)
                    } else {
                        None
                    };
                let max_deliveries = if flags & FLAG_QUEUE_Q_CREATE_HAS_MAX_DELIVERIES != 0 {
                    Some(cursor.read_u32()?)
                } else {
                    None
                };
                Ok(Self::Create {
                    q_name,
                    options: QueueCreateOptions {
                        visibility_timeout_ms,
                        max_deliveries,
                    },
                })
            }
            OP_Q_PUSH => {
                let q_name = cursor.read_string()?;
                let count = cursor.read_u32()? as usize;
                if count > MAX_PUSH_ITEMS {
                    return Err(ParseError::Invalid(format!(
                        "Push count too large: {} (max {})",
                        count, MAX_PUSH_ITEMS
                    )));
                }
                if count > cursor.len() / MIN_PUSH_ITEM_BYTES {
                    return Err(ParseError::Invalid(format!(
                        "Push count {} exceeds remaining payload",
                        count
                    )));
                }
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let flags = cursor.read_u8()?;
                    let priority = if flags & FLAG_QUEUE_Q_PUSH_HAS_PRIORITY != 0 {
                        Some(cursor.read_u8()?)
                    } else {
                        None
                    };
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
                Ok(Self::Consume {
                    q_name,
                    batch_size,
                    wait_ms,
                })
            }
            OP_Q_ACK => {
                let id = Uuid::from_bytes(cursor.read_uuid_bytes()?);
                let delivery_token = cursor.read_u64()?;
                let q_name = cursor.read_string()?;
                Ok(Self::Ack {
                    id,
                    delivery_token,
                    q_name,
                })
            }
            OP_Q_NACK => {
                let id = Uuid::from_bytes(cursor.read_uuid_bytes()?);
                let delivery_token = cursor.read_u64()?;
                let q_name = cursor.read_string()?;
                let reason = cursor.read_string()?;
                Ok(Self::Nack {
                    id,
                    delivery_token,
                    q_name,
                    reason,
                })
            }
            OP_Q_EXISTS => {
                let q_name = cursor.read_string()?;
                Ok(Self::Exists { q_name })
            }
            OP_Q_DESCRIBE => {
                let q_name = cursor.read_string()?;
                Ok(Self::Describe { q_name })
            }
            OP_Q_DELETE => {
                let q_name = cursor.read_string()?;
                Ok(Self::Delete { q_name })
            }
            OP_Q_PEEK_DLQ => {
                let q_name = cursor.read_string()?;
                let limit = cursor.read_u32()? as usize;
                let offset = cursor.read_u32()? as usize;
                Ok(Self::PeekDLQ {
                    q_name,
                    limit,
                    offset,
                })
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
            _ => Err(ParseError::Invalid(format!(
                "Unknown Queue opcode: 0x{:02X}",
                opcode
            ))),
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

fn put_definition(writer: &mut PayloadWriter, definition: &QueueDefinition) {
    writer
        .put_str(&definition.name)
        .put_u64(definition.config.visibility_timeout_ms)
        .put_u32(definition.config.max_deliveries);
}

fn encode_definition(definition: &QueueDefinition) -> Bytes {
    let mut writer = PayloadWriter::new();
    put_definition(&mut writer, definition);
    writer.into_bytes()
}

fn encode_provision_result(result: &ProvisionResult<QueueDefinition>) -> Bytes {
    let mut writer = PayloadWriter::new();
    writer.put_u8(match result.outcome {
        ProvisionOutcome::Created => ProvisionStatus::Created as u8,
        ProvisionOutcome::Unchanged => ProvisionStatus::Unchanged as u8,
    });
    put_definition(&mut writer, &result.definition);
    writer.into_bytes()
}

// ==========================================
// DISPATCH ENTRY POINT
// ==========================================

pub async fn handle(opcode: u8, cursor: &mut PayloadCursor, engine: &NexoEngine) -> Response {
    let cmd = match QueueCommand::parse(opcode, cursor) {
        Ok(c) => c,
        Err(error) => return Response::error(ErrorCode::ProtocolError, error.to_string()),
    };

    let queue = &engine.queue;

    match cmd {
        QueueCommand::Create { q_name, options } => match queue.create_queue(q_name, options).await
        {
            Ok(result) => Response::Data(encode_provision_result(&result)),
            Err(error) => error_response(error),
        },
        QueueCommand::Push { q_name, items } => {
            let batch: Vec<(Bytes, u8)> = items
                .into_iter()
                .map(|item| (item.payload, item.priority.unwrap_or(0)))
                .collect();
            match queue.push_batch(q_name, batch).await {
                Ok(_) => Response::Ok,
                Err(error) => error_response(error),
            }
        }
        QueueCommand::Consume {
            q_name,
            batch_size,
            wait_ms,
        } => {
            match queue
                .consume_batch(q_name, Some(batch_size), Some(wait_ms))
                .await
            {
                Ok(messages) => Response::Data(encode_consume_batch(&messages)),
                Err(error) => error_response(error),
            }
        }
        QueueCommand::Ack {
            id,
            delivery_token,
            q_name,
        } => {
            let found = queue.ack(&q_name, id, delivery_token).await;
            Response::Data(encode_bool(found))
        }
        QueueCommand::Nack {
            id,
            delivery_token,
            q_name,
            reason,
        } => {
            let found = queue.nack(&q_name, id, delivery_token, reason).await;
            Response::Data(encode_bool(found))
        }
        QueueCommand::Exists { q_name } => {
            let found = queue.exists(&q_name).await;
            Response::Data(encode_bool(found))
        }
        QueueCommand::Describe { q_name } => match queue.describe(&q_name).await {
            Ok(definition) => Response::Data(encode_definition(&definition)),
            Err(error) => error_response(error),
        },
        QueueCommand::Delete { q_name } => match queue.delete_queue(q_name).await {
            Ok(_) => Response::Ok,
            Err(error) => error_response(error),
        },
        QueueCommand::PeekDLQ {
            q_name,
            limit,
            offset,
        } => match queue.peek_dlq(&q_name, limit, offset).await {
            Ok((total, messages)) => Response::Data(encode_peek_dlq(total, &messages)),
            Err(error) => error_response(error),
        },
        QueueCommand::MoveToQueue { q_name, message_id } => {
            match queue.move_to_queue(&q_name, message_id).await {
                Ok(found) => Response::Data(encode_bool(found)),
                Err(error) => error_response(error),
            }
        }
        QueueCommand::DeleteDLQ { q_name, message_id } => {
            match queue.delete_dlq(&q_name, message_id).await {
                Ok(found) => Response::Data(encode_bool(found)),
                Err(error) => error_response(error),
            }
        }
        QueueCommand::PurgeDLQ { q_name } => match queue.purge_dlq(&q_name).await {
            Ok(count) => Response::Data(encode_count(count)),
            Err(error) => error_response(error),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::wire::{PayloadCursor, PayloadWriter};

    fn make_cursor(op: u8, payload: bytes::Bytes) -> (u8, PayloadCursor) {
        (op, PayloadCursor::new(payload))
    }

    #[test]
    fn push_rejects_count_exceeding_max() {
        let mut w = PayloadWriter::new();
        w.put_str("test_queue");
        w.put_u32((MAX_PUSH_ITEMS + 1) as u32);
        let (op, mut cursor) = make_cursor(OP_Q_PUSH, w.into_bytes());
        let result = QueueCommand::parse(op, &mut cursor);
        assert!(
            result.is_err(),
            "Push with count > MAX_PUSH_ITEMS must fail"
        );
    }

    #[test]
    fn push_rejects_huge_count_before_allocation() {
        let mut w = PayloadWriter::new();
        w.put_str("test_queue");
        w.put_u32(u32::MAX);
        let (op, mut cursor) = make_cursor(OP_Q_PUSH, w.into_bytes());
        let result = QueueCommand::parse(op, &mut cursor);
        assert!(
            result.is_err(),
            "Push with u32::MAX count must fail before allocation"
        );
    }

    #[test]
    fn push_rejects_count_exceeding_remaining_payload() {
        let mut w = PayloadWriter::new();
        w.put_str("test_queue");
        w.put_u32(100);
        let (op, mut cursor) = make_cursor(OP_Q_PUSH, w.into_bytes());
        let result = QueueCommand::parse(op, &mut cursor);
        assert!(
            result.is_err(),
            "Push with count > remaining payload must fail"
        );
    }
}

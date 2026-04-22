//! Queue broker TCP surface: opcodes, command parsing, response wire
//! encoding and the single dispatch entry point `handle(...)`.

use bytes::Bytes;
use uuid::Uuid;

use crate::transport::tcp::protocol::cursor::PayloadCursor;
use crate::transport::tcp::protocol::{ParseError, Response, ToWire};
use crate::NexoEngine;

use crate::brokers::queue::domain::dlq::DlqMessage;
use crate::brokers::queue::options::{QueueConsumeOptions, QueueCreateOptions, QueuePushOptions};
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
enum QueueCommand {
    Create { q_name: String, options: QueueCreateOptions },
    Push { q_name: String, options: QueuePushOptions, payload: Bytes },
    Consume { q_name: String, options: QueueConsumeOptions },
    Delete { q_name: String },
    Ack { id: Uuid, q_name: String },
    Nack { id: Uuid, q_name: String, reason: String },
    Exists { q_name: String },
    PeekDLQ { q_name: String, limit: usize, offset: usize },
    MoveToQueue { q_name: String, message_id: Uuid },
    DeleteDLQ { q_name: String, message_id: Uuid },
    PurgeDLQ { q_name: String },
}

impl QueueCommand {
    fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_Q_CREATE => {
                let q_name = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                let options: QueueCreateOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON config: {}", e)))?;
                Ok(Self::Create { q_name, options })
            }
            OP_Q_PUSH => {
                let q_name = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                let options: QueuePushOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON options: {}", e)))?;
                let payload = cursor.read_remaining();
                Ok(Self::Push { q_name, options, payload })
            }
            OP_Q_CONSUME => {
                let q_name = cursor.read_string()?;
                let json_str = cursor.read_string()?;
                let options: QueueConsumeOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON options: {}", e)))?;
                Ok(Self::Consume { q_name, options })
            }
            OP_Q_ACK => {
                let id = Uuid::from_bytes(cursor.read_uuid_bytes()?);
                let q_name = cursor.read_string()?;
                Ok(Self::Ack { id, q_name })
            }
            OP_Q_NACK => {
                let id = Uuid::from_bytes(cursor.read_uuid_bytes()?);
                let q_name = cursor.read_string()?;
                let reason = cursor.read_string()?;
                Ok(Self::Nack { id, q_name, reason })
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

struct ConsumeBatchResponse {
    messages: Vec<Message>,
}

impl ToWire for ConsumeBatchResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.messages.len() as u32).to_be_bytes());
        for msg in &self.messages {
            buf.extend_from_slice(msg.id.as_bytes());
            buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&msg.payload);
        }
        Bytes::from(buf)
    }
}

struct PeekDlqResponse {
    total: usize,
    messages: Vec<DlqMessage>,
}

impl ToWire for PeekDlqResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.total as u32).to_be_bytes());
        buf.extend_from_slice(&(self.messages.len() as u32).to_be_bytes());
        for msg in &self.messages {
            buf.extend_from_slice(msg.id.as_bytes());
            buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&msg.payload);
            buf.extend_from_slice(&msg.attempts.to_be_bytes());

            let reason_bytes = msg.failure_reason.as_bytes();
            buf.extend_from_slice(&(reason_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(reason_bytes);
        }
        Bytes::from(buf)
    }
}

struct BoolResponse {
    value: bool,
}

impl ToWire for BoolResponse {
    fn to_wire(&self) -> Bytes {
        Bytes::from(vec![if self.value { 1u8 } else { 0u8 }])
    }
}

struct CountResponse {
    count: usize,
}

impl ToWire for CountResponse {
    fn to_wire(&self) -> Bytes {
        Bytes::from(Vec::from((self.count as u32).to_be_bytes()))
    }
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
        QueueCommand::Push { q_name, options, payload } => {
            let priority = options.priority.unwrap_or(0);
            match queue.push(q_name, payload, priority, options.delay_ms).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Consume { q_name, options } => {
            match queue.consume_batch(q_name, options.batch_size, options.wait_ms).await {
                Ok(messages) => Response::Data(ConsumeBatchResponse { messages }.to_wire()),
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Ack { id, q_name } => match queue.ack(&q_name, id).await {
            true => Response::Ok,
            false => Response::Error("ACK failed".to_string()),
        },
        QueueCommand::Nack { id, q_name, reason } => match queue.nack(&q_name, id, reason).await {
            true => Response::Ok,
            false => Response::Error("NACK failed".to_string()),
        },
        QueueCommand::Exists { q_name } => match queue.exists(&q_name).await {
            true => Response::Ok,
            false => Response::Error("Queue not found".to_string()),
        },
        QueueCommand::Delete { q_name } => match queue.delete_queue(q_name).await {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        QueueCommand::PeekDLQ { q_name, limit, offset } => {
            match queue.peek_dlq(&q_name, limit, offset).await {
                Ok((total, messages)) => Response::Data(PeekDlqResponse { total, messages }.to_wire()),
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::MoveToQueue { q_name, message_id } => {
            match queue.move_to_queue(&q_name, message_id).await {
                Ok(found) => Response::Data(BoolResponse { value: found }.to_wire()),
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::DeleteDLQ { q_name, message_id } => {
            match queue.delete_dlq(&q_name, message_id).await {
                Ok(found) => Response::Data(BoolResponse { value: found }.to_wire()),
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::PurgeDLQ { q_name } => match queue.purge_dlq(&q_name).await {
            Ok(count) => Response::Data(CountResponse { count }.to_wire()),
            Err(e) => Response::Error(e),
        },
    }
}

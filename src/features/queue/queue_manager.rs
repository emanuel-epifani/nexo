//! Queue Manager: Message queue functionality
use dashmap::DashMap;
use std::sync::Mutex;
use std::collections::VecDeque;
use std::convert::TryInto;
use crate::server::protocol::{OP_Q_PUSH, OP_Q_POP, Response};

// ========================================
// COMMAND PATTERN
// ========================================

#[derive(Debug)]
pub enum QueueCommand {
    Push { queue_name: String, value: Vec<u8> },
    Pop { queue_name: String },
}

impl QueueCommand {
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_Q_PUSH => {
                // Payload: [QueueNameLen: 4 bytes] [QueueName] [Value]
                if payload.len() < 4 {
                    return Err("Payload too short".to_string());
                }
                let q_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + q_len {
                    return Err("Invalid frame format".to_string());
                }

                let q_name_bytes = &payload[4..4 + q_len];
                let value = payload[4 + q_len..].to_vec();

                let queue_name = std::str::from_utf8(q_name_bytes)
                    .map_err(|_| "Queue name must be UTF-8".to_string())?
                    .to_string();

                Ok(QueueCommand::Push { queue_name, value })
            },
            OP_Q_POP => {
                // Payload: [QueueName]
                let queue_name = std::str::from_utf8(payload)
                    .map_err(|_| "Queue name must be UTF-8".to_string())?
                    .to_string();
                Ok(QueueCommand::Pop { queue_name })
            },
            _ => Err(format!("Unknown Queue Opcode: 0x{:02X}", opcode)),
        }
    }
}

pub struct QueueManager {
    // DashMap separa le code (Coda A non blocca Coda B).
    // Mutex protegge l'ordine FIFO dentro la singola coda.
    queues: DashMap<String, Mutex<VecDeque<Vec<u8>>>>,
}

impl QueueManager {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
        }
    }

    /// Execute a parsed command
    pub fn execute(&self, cmd: QueueCommand) -> Result<Response, String> {
        match cmd {
            QueueCommand::Push { queue_name, value } => {
                self.push(queue_name, value);
                Ok(Response::Ok)
            },
            QueueCommand::Pop { queue_name } => {
                match self.pop(&queue_name) {
                    Some(val) => Ok(Response::Data(val)),
                    None => Ok(Response::Null),
                }
            }
        }
    }

    /// Push a message to the back of the queue
    pub fn push(&self, queue_name: String, value: Vec<u8>) {
        let entry = self.queues.entry(queue_name).or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut queue = entry.lock().unwrap();
        queue.push_back(value);
    }

    /// Pop a message from the front of the queue
    pub fn pop(&self, queue_name: &str) -> Option<Vec<u8>> {
        if let Some(entry) = self.queues.get(queue_name) {
            let mut queue = entry.lock().unwrap();
            return queue.pop_front();
        }
        None
    }
}

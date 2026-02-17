use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;
use crate::brokers::queue::queue::Message;
use crate::brokers::queue::dlq::DlqMessage;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PersistenceMode {
    Sync,
    Async { flush_ms: u64 },
}

use crate::config::Config;

impl Default for PersistenceMode {
    fn default() -> Self {
        Self::Async { flush_ms: Config::global().queue.default_flush_ms }
    }
}

/// Atomic operations that storage can execute
#[derive(Debug)]
pub enum StorageOp {
    /// Insert a new message (Push)
    Insert(Message),
    /// Remove a message (Ack)
    Delete(Uuid),
    /// Update visibility and attempts (Nack / Timeout / In-flight)
    UpdateState {
        id: Uuid,
        visible_at: u64,
        attempts: u32,
    },
    
    // DLQ Operations
    /// Insert a message into DLQ
    InsertDLQ(DlqMessage),
    /// Delete a message from DLQ
    DeleteDLQ(Uuid),
    /// Move message from main queue to DLQ (atomic)
    MoveToDLQ {
        id: Uuid,
        msg: DlqMessage,
    },
    /// Move message from DLQ to main queue (atomic)
    MoveToMain {
        id: Uuid,
        msg: Message,
    },
    /// Purge all messages from DLQ
    PurgeDLQ,
}

/// The command sent to the Writer Thread
pub struct StoreCommand {
    pub op: StorageOp,
    /// If present, the Actor waits for this response (Sync Mode)
    pub sync_channel: Option<oneshot::Sender<Result<(), String>>>,
}

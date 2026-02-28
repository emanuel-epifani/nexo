use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::brokers::queue::queue::Message;
use crate::brokers::queue::dlq::DlqMessage;

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


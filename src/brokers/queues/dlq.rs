//! DLQ State: Specialized state management for Dead Letter Queue
//! 
//! Optimized for:
//! - Chronological ordering (Insert order)
//! - Fast lookup by ID (O(1))
//! - Simple pagination (Offset/Limit)

use hashlink::LinkedHashMap;
use uuid::Uuid;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use crate::brokers::queues::queue::{Message, MessageState, current_time_ms};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessage {
    pub id: Uuid,
    pub payload: Bytes,
    pub priority: u8,
    pub attempts: u32,
    pub created_at: u64,
    pub failed_at: u64,
    pub failure_reason: String,
}

impl DlqMessage {
    pub fn from_message(msg: Message, reason: String) -> Self {
        Self {
            id: msg.id,
            payload: msg.payload,
            priority: msg.priority,
            attempts: msg.attempts,
            created_at: msg.created_at,
            failed_at: current_time_ms(),
            failure_reason: reason,
        }
    }

    pub fn to_message(self) -> Message {
        Message {
            id: self.id,
            payload: self.payload,
            priority: self.priority,
            attempts: 0, // Reset attempts on replay
            created_at: self.created_at,
            visible_at: 0, // Ready immediately
            delayed_until: None,
            failure_reason: None, // Clear reason
            state: MessageState::Ready,
        }
    }
}

pub struct DlqState {
    /// Ordered map of failed messages.
    /// Order is FIFO (insertion order).
    messages: LinkedHashMap<Uuid, DlqMessage>,
}

impl DlqState {
    pub fn new() -> Self {
        Self {
            messages: LinkedHashMap::new(),
        }
    }

    pub fn push(&mut self, msg: DlqMessage) {
        // Updates position to end if already exists (which shouldn't happen usually)
        self.messages.insert(msg.id, msg);
    }

    pub fn remove(&mut self, id: &Uuid) -> Option<DlqMessage> {
        self.messages.remove(id)
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Peek messages with pagination.
    /// Returns (total_count, paginated_items)
    /// Items are ordered from MOST RECENT failure to OLDEST (Reverse insertion order).
    pub fn peek(&self, offset: usize, limit: usize) -> (usize, Vec<DlqMessage>) {
        let total = self.messages.len();
        
        let items = self.messages.values()
            .rev()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
            
        (total, items)
    }
}

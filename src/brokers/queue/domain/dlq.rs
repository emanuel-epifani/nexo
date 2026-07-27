#![allow(clippy::too_many_arguments)]
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
use crate::brokers::queue::domain::queue::{Message, current_time_ms};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessage {
    pub id: Uuid,
    pub payload: Bytes,
    pub priority: u8,
    pub attempts: u32,
    pub created_at: u64,
    pub failed_at: u64,
    pub dlq_seq: u64,
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
            dlq_seq: 0,
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
            ready_seq: 0, // Will be assigned by QueueState::push
            delivery_token: 0, // Will be assigned on next pop
            failure_reason: None, // Clear reason
        }
    }
}

pub struct DlqState {
    /// Ordered map of failed messages.
    /// Order is FIFO (insertion order).
    messages: LinkedHashMap<Uuid, DlqMessage>,
    /// Monotonic counter for dlq_seq (ordering that survives restart)
    dlq_seq_counter: u64,
}

impl DlqState {
    pub fn new() -> Self {
        Self {
            messages: LinkedHashMap::new(),
            dlq_seq_counter: 0,
        }
    }

    pub fn push(&mut self, msg: &mut DlqMessage) {
        self.dlq_seq_counter += 1;
        msg.dlq_seq = self.dlq_seq_counter;
        // Updates position to end if already exists (which shouldn't happen usually)
        self.messages.insert(msg.id, msg.clone());
    }

    /// Restore a message from persistence without reassigning dlq_seq.
    /// Syncs the dlq_seq_counter to max(current, msg.dlq_seq).
    pub fn restore(&mut self, msg: DlqMessage) {
        if msg.dlq_seq > self.dlq_seq_counter {
            self.dlq_seq_counter = msg.dlq_seq;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dlq_msg(id: u8) -> DlqMessage {
        DlqMessage {
            id: Uuid::from_u128(id as u128),
            payload: Bytes::from(format!("msg{}", id)),
            priority: 0,
            attempts: 1,
            created_at: 0,
            failed_at: 0,
            dlq_seq: 0,
            failure_reason: "fail".to_string(),
        }
    }

    #[test]
    fn push_assigns_monotonic_dlq_seq() {
        let mut state = DlqState::new();

        let mut m1 = dlq_msg(1);
        state.push(&mut m1);
        assert_eq!(m1.dlq_seq, 1);

        let mut m2 = dlq_msg(2);
        state.push(&mut m2);
        assert_eq!(m2.dlq_seq, 2);

        let (_, peeked) = state.peek(0, 2);
        assert_eq!(peeked.len(), 2);
        // peek is most-recent first, so m2 (seq 2) then m1 (seq 1)
        assert_eq!(peeked[0].dlq_seq, 2);
        assert_eq!(peeked[1].dlq_seq, 1);
    }
}

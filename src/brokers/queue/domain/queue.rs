#![allow(clippy::too_many_arguments)]
//! Queue State: Internal state management for queue broker
//! 
//! This module contains the pure state logic without any concurrency primitives.
//! The QueueManager wraps this state in a Mutex<QueueInner> per queue.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::brokers::queue::options::QueueCreateOptions;
use crate::brokers::queue::config::SystemQueueConfig;
use crate::brokers::queue::domain::dlq::DlqMessage;

// ==========================================
// MESSAGE & CONFIG
// ==========================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub payload: Bytes,
    pub priority: u8,
    pub attempts: u32,
    pub created_at: u64,
    pub visible_at: u64,
    pub ready_seq: u64,
    pub delivery_token: u64,
    pub failure_reason: Option<String>,
}

impl Message {
    pub fn new(payload: Bytes, priority: u8, now: u64) -> Self {
        Self {
            id: Uuid::new_v4(),
            payload,
            priority,
            attempts: 0,
            created_at: now,
            visible_at: 0,
            ready_seq: 0,
            delivery_token: 0,
            failure_reason: None,
        }
    }

    /// A message is in-flight if its visibility lease has not expired.
    /// `visible_at == 0` means it has never been popped or has been requeued.
    #[inline]
    pub fn is_in_flight(&self) -> bool {
        self.visible_at > 0 && self.visible_at > current_time_ms()
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub visibility_timeout_ms: u64,
    pub max_deliveries: u32,
}

impl QueueConfig {
    pub fn from_options(opts: QueueCreateOptions, sys: &SystemQueueConfig) -> Self {
        Self {
            visibility_timeout_ms: opts.visibility_timeout_ms.unwrap_or(sys.visibility_timeout_ms),
            max_deliveries: opts.max_deliveries.unwrap_or(sys.max_deliveries),
        }
    }
}

// ==========================================
// QUEUE STATE
// ==========================================

pub struct QueueState {
    /// Source of truth for all messages
    registry: HashMap<Uuid, Message>,
    /// Ready messages ordered by (priority desc, ready_seq asc).
    ready: PriorityQueue<Uuid, (u8, Reverse<u64>)>,
    /// In-flight messages ordered by (visible_at asc, delivery_token asc).
    in_flight: PriorityQueue<Uuid, Reverse<(u64, u64)>>,
    /// Monotonic counter for ready_seq (FIFO ordering that survives restart)
    ready_seq_counter: u64,
    /// Monotonic counter for delivery_token (identifies a specific delivery)
    delivery_counter: u64,
}

impl QueueState {
    /// Returns the earliest visibility timeout (ms) for in-flight messages.
    pub fn next_inflight_timeout(&self) -> Option<u64> {
        self.in_flight.peek().map(|(_, Reverse((ts, _)))| *ts)
    }

    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            ready: PriorityQueue::new(),
            in_flight: PriorityQueue::new(),
            ready_seq_counter: 0,
            delivery_counter: 0,
        }
    }

    /// Push a message to the queue.
    /// Assigns a new ready_seq from the monotonic counter in-place on `msg`.
    /// The caller can then persist `msg` with its assigned `ready_seq`.
    pub fn push(&mut self, msg: &mut Message) {
        let id = msg.id;
        let priority = msg.priority;

        msg.ready_seq = self.next_ready_seq();

        self.registry.insert(id, msg.clone());

        if msg.visible_at > 0 {
            self.in_flight.push(id, Reverse((msg.visible_at, msg.delivery_token)));
        } else {
            self.ready.push(id, (priority, Reverse(msg.ready_seq)));
        }
    }

    /// Restore a message from persistence without reassigning ready_seq.
    /// Syncs the ready_seq_counter and delivery_counter to max(current, msg.*).
    pub fn restore(&mut self, msg: Message) {
        let id = msg.id;
        let priority = msg.priority;

        if msg.ready_seq > self.ready_seq_counter {
            self.ready_seq_counter = msg.ready_seq;
        }
        if msg.delivery_token > self.delivery_counter {
            self.delivery_counter = msg.delivery_token;
        }

        self.registry.insert(id, msg.clone());

        if msg.visible_at > 0 {
            self.in_flight.push(id, Reverse((msg.visible_at, msg.delivery_token)));
        } else {
            self.ready.push(id, (priority, Reverse(msg.ready_seq)));
        }
    }

    #[inline]
    fn next_ready_seq(&mut self) -> u64 {
        self.ready_seq_counter += 1;
        self.ready_seq_counter
    }

    /// Pop the highest priority message.
    pub fn pop(&mut self, visibility_timeout_ms: u64, now: u64) -> Option<Message> {
        self.pop_single(visibility_timeout_ms, now)
    }

    /// Acknowledge a message (remove from system).
    /// Returns false if the delivery is not active or the delivery_token doesn't match (stale ACK).
    pub fn ack(&mut self, id: Uuid, delivery_token: u64) -> bool {
        let now = current_time_ms();
        if let Some(msg) = self.registry.get(&id) {
            // Must be an active in-flight delivery with a matching token.
            if msg.delivery_token != delivery_token || msg.visible_at == 0 || msg.visible_at <= now {
                return false;
            }
        } else {
            return false;
        }
        self.delete_message(id)
    }

    /// Take up to `max` messages for batch consumption.
    pub fn take_batch(&mut self, max: usize, visibility_timeout_ms: u64) -> Vec<Message> {
        let now = current_time_ms();
        let mut result = Vec::with_capacity(max);

        while result.len() < max {
            match self.pop_single(visibility_timeout_ms, now) {
                Some(msg) => result.push(msg),
                None => break,
            }
        }

        result
    }

    /// Negative Acknowledge. Returns (requeued_msg, dlq_msg).
    /// If dlq_msg is Some, the message was removed from this state and should be added to DLQ state.
    /// Returns (None, None) if the delivery is not active or the delivery_token doesn't match (stale NACK).
    pub fn nack(&mut self, id: Uuid, delivery_token: u64, reason: String, max_deliveries: u32) -> (Option<Message>, Option<DlqMessage>) {
        let now = current_time_ms();

        // 1. Check existence, token, and active lease
        let (should_dlq, priority) = if let Some(msg) = self.registry.get_mut(&id) {
            // Must be an active in-flight delivery with a matching token.
            if msg.delivery_token != delivery_token || msg.visible_at == 0 || msg.visible_at <= now {
                return (None, None);
            }
            msg.failure_reason = Some(reason.clone());
            (msg.attempts >= max_deliveries, msg.priority)
        } else {
            return (None, None);
        };

        // 2. Action
        if should_dlq {
            if let Some(msg) = self.delete_message_and_return(id) {
                let dlq_msg = DlqMessage::from_message(msg, reason);
                return (None, Some(dlq_msg));
            }
            (None, None)
        } else {
            self.in_flight.remove(&id);
            let new_seq = self.next_ready_seq();
            if let Some(msg) = self.registry.get_mut(&id) {
                msg.visible_at = 0;
                msg.ready_seq = new_seq;
                msg.delivery_token = 0;
                self.ready.push(id, (priority, Reverse(new_seq)));
                return (Some(msg.clone()), None);
            }
            (None, None)
        }
    }

    /// Process expired in-flight timeouts.
    /// Returns (requeued_messages, dlq_messages).
    /// requeued_messages: messages that transitioned to Ready (need UpdateState in DB)
    /// dlq_messages: messages moved to DLQ (need MoveToDlq in DB)
    pub fn process_expired(&mut self, max_deliveries: u32) -> (Vec<Message>, Vec<DlqMessage>) {
        let now = current_time_ms();
        let mut requeued_msgs = Vec::new();
        let mut dlq_msgs = Vec::new();

        while let Some((_, Reverse((visible_at, _)))) = self.in_flight.peek() {
            if *visible_at > now {
                break;
            }

            let (id, _) = self.in_flight.pop().expect("non-empty in-flight heap");

            let should_dlq = self.registry.get(&id)
                .map(|m| m.attempts >= max_deliveries)
                .unwrap_or(false);

            if should_dlq {
                if let Some(msg) = self.registry.remove(&id) {
                    dlq_msgs.push(DlqMessage::from_message(msg, "Timeout".to_string()));
                }
            } else {
                let new_seq = self.next_ready_seq();
                if let Some(msg) = self.registry.get_mut(&id) {
                    msg.visible_at = 0;
                    msg.ready_seq = new_seq;
                    msg.delivery_token = 0;
                    let priority = msg.priority;
                    requeued_msgs.push(msg.clone());
                    self.ready.push(id, (priority, Reverse(new_seq)));
                }
            }
        }

        (requeued_msgs, dlq_msgs)
    }

    // --- Internal helpers ---

    /// Pop a single message from the queue.
    fn pop_single(&mut self, visibility_timeout_ms: u64, now: u64) -> Option<Message> {
        let (next_id, _) = self.ready.pop()?;

        let timeout = now + visibility_timeout_ms;

        let msg = self.registry.get_mut(&next_id)?;
        msg.visible_at = timeout;
        msg.attempts += 1;
        self.delivery_counter += 1;
        msg.delivery_token = self.delivery_counter;

        self.in_flight.push(next_id, Reverse((timeout, msg.delivery_token)));

        Some(msg.clone())
    }

    fn delete_message(&mut self, id: Uuid) -> bool {
        self.delete_message_and_return(id).is_some()
    }

    fn delete_message_and_return(&mut self, id: Uuid) -> Option<Message> {
        let msg = self.registry.remove(&id)?;
        self.ready.remove(&id);
        self.in_flight.remove(&id);
        Some(msg)
    }
}

// ==========================================
// HELPERS
// ==========================================

pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn b(data: &str) -> Bytes {
        Bytes::copy_from_slice(data.as_bytes())
    }

    #[test]
    fn ack_accepts_active_delivery_and_rejects_wrong_token() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        state.push(&mut msg);

        let msg = state.pop(1000, now).unwrap();
        assert_eq!(msg.delivery_token, 1);

        assert!(!state.ack(msg.id, msg.delivery_token + 999), "wrong token must be rejected");
        assert!(state.ack(msg.id, msg.delivery_token), "active delivery with correct token must be accepted");
    }

    #[test]
    fn ack_rejects_ready_message_and_token_zero() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        let id = msg.id;
        state.push(&mut msg);

        // Token 0 on a never-popped (ready) message must be stale.
        assert!(!state.ack(id, 0), "ack with token 0 on a ready message must be rejected");
        // Any token on a ready message must be rejected.
        assert!(!state.ack(id, 123), "ack on a ready message must be rejected");
    }

    #[test]
    fn ack_rejects_expired_lease() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        state.push(&mut msg);

        let msg = state.pop(1, now).unwrap();
        std::thread::sleep(Duration::from_millis(5));

        assert!(!state.ack(msg.id, msg.delivery_token), "ack after lease expiration must be rejected");
    }

    #[test]
    fn nack_rejects_wrong_token_and_ready_message() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        state.push(&mut msg);

        let msg = state.pop(1000, now).unwrap();
        let (requeued, dlq) = state.nack(msg.id, msg.delivery_token + 999, "fail".to_string(), 5);
        assert!(requeued.is_none() && dlq.is_none(), "wrong token nack must be a no-op");

        let (requeued, dlq) = state.nack(msg.id, 0, "fail".to_string(), 5);
        assert!(requeued.is_none() && dlq.is_none(), "nack with token 0 on in-flight must be rejected");
    }

    #[test]
    fn nack_rejects_expired_lease() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        state.push(&mut msg);

        let msg = state.pop(1, now).unwrap();
        std::thread::sleep(Duration::from_millis(5));

        let (requeued, dlq) = state.nack(msg.id, msg.delivery_token, "fail".to_string(), 5);
        assert!(requeued.is_none() && dlq.is_none(), "nack after lease expiration must be rejected");
    }

    #[test]
    fn nack_resets_delivery_token_on_requeue() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        state.push(&mut msg);

        let msg = state.pop(1000, now).unwrap();
        let token1 = msg.delivery_token;

        let (requeued, dlq) = state.nack(msg.id, token1, "fail".to_string(), 5);
        assert!(requeued.is_some() && dlq.is_none(), "nack should requeue");

        // Old token on the requeued message must be stale.
        assert!(!state.ack(msg.id, token1), "ack with old token after nack requeue must fail");

        let msg2 = state.pop(1000, current_time_ms()).unwrap();
        assert_eq!(msg2.delivery_token, 2, "second delivery should have a new token");
        assert!(state.ack(msg2.id, msg2.delivery_token), "ack with current token should succeed");
    }

    #[test]
    fn process_expired_resets_delivery_token_on_requeue() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        state.push(&mut msg);

        let msg = state.pop(1, now).unwrap();
        let token1 = msg.delivery_token;

        std::thread::sleep(Duration::from_millis(5));
        let (requeued, dlq) = state.process_expired(5);
        assert_eq!(requeued.len(), 1);
        assert!(dlq.is_empty());

        // Old token on the requeued message must be stale.
        assert!(!state.ack(msg.id, token1), "ack with old token after timeout requeue must fail");

        let msg2 = state.pop(1000, current_time_ms()).unwrap();
        assert_eq!(msg2.delivery_token, 2, "second delivery should have a new token");
        assert!(state.ack(msg2.id, msg2.delivery_token), "ack with current token should succeed");
    }

    #[test]
    fn nack_moves_to_dlq_when_max_deliveries_reached() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        state.push(&mut msg);

        let msg = state.pop(1000, now).unwrap();
        let (requeued, dlq) = state.nack(msg.id, msg.delivery_token, "fail".to_string(), 1);
        assert!(requeued.is_none() && dlq.is_some(), "first delivery at max_deliveries=1 should go to DLQ");
        assert_eq!(dlq.unwrap().id, msg.id);
    }

    #[test]
    fn process_expired_moves_to_dlq_when_max_deliveries_reached() {
        let mut state = QueueState::new();
        let now = current_time_ms();
        let mut msg = Message::new(b("x"), 0, now);
        state.push(&mut msg);

        let msg = state.pop(1, now).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let (requeued, dlq) = state.process_expired(1);
        assert!(requeued.is_empty());
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].id, msg.id);
    }

    #[test]
    fn push_assigns_monotonic_ready_seq() {
        let mut state = QueueState::new();
        let now = current_time_ms();

        let mut msg1 = Message::new(b("a"), 0, now);
        state.push(&mut msg1);
        assert_eq!(msg1.ready_seq, 1);

        let mut msg2 = Message::new(b("b"), 0, now);
        state.push(&mut msg2);
        assert_eq!(msg2.ready_seq, 2);
    }

    #[test]
    fn ready_seq_order_preserved_after_requeue() {
        let mut state = QueueState::new();
        let now = current_time_ms();

        let mut first = Message::new(b("first"), 0, now);
        state.push(&mut first);
        let mut second = Message::new(b("second"), 0, now);
        state.push(&mut second);

        // Pop first, let it timeout and get requeued.
        let m1 = state.pop(1, now).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let (requeued, _) = state.process_expired(5);
        assert_eq!(requeued.len(), 1);

        // New push should have a higher ready_seq than the requeued message.
        let mut third = Message::new(b("third"), 0, current_time_ms());
        state.push(&mut third);
        assert!(third.ready_seq > m1.ready_seq, "new push must have higher ready_seq than requeued message");
    }

    #[test]
    fn higher_priority_pops_first() {
        let mut state = QueueState::new();
        let now = current_time_ms();

        let mut low = Message::new(b("low"), 1, now);
        let mut high = Message::new(b("high"), 10, now);
        state.push(&mut low);
        state.push(&mut high);

        let first = state.pop(1000, now).unwrap();
        assert_eq!(first.priority, 10);
        assert_eq!(first.payload, b("high"));

        let second = state.pop(1000, now).unwrap();
        assert_eq!(second.priority, 1);
        assert_eq!(second.payload, b("low"));
    }

    #[test]
    fn fifo_at_same_priority() {
        let mut state = QueueState::new();
        let now = current_time_ms();

        let mut first = Message::new(b("first"), 5, now);
        let mut second = Message::new(b("second"), 5, now);
        state.push(&mut first);
        state.push(&mut second);

        let m1 = state.pop(1000, now).unwrap();
        let m2 = state.pop(1000, now).unwrap();

        assert_eq!(m1.payload, b("first"));
        assert_eq!(m2.payload, b("second"));
        assert!(m1.ready_seq < m2.ready_seq);
    }
}

#![allow(clippy::too_many_arguments)]
//! Queue State: Internal state management for queue broker
//! 
//! This module contains the pure state logic without any concurrency primitives.
//! The QueueManager wraps this state in a Mutex<QueueInner> per queue.

use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use hashlink::LinkedHashSet;

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
            failure_reason: None,
        }
    }

    /// A message is in-flight if it has a visible_at > 0 (i.e. it has been popped and is waiting for ack).
    #[inline]
    pub fn is_in_flight(&self) -> bool {
        self.visible_at > 0
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
// QUEUE STATE (Pure State, No Concurrency)
// ==========================================

pub struct QueueState {
    /// Source of truth for all messages
    registry: HashMap<Uuid, Message>,
    /// Ready messages by priority (high priority first)
    waiting_for_dispatch: BTreeMap<u8, LinkedHashSet<Uuid>>,
    /// In-flight messages by timeout time
    waiting_for_ack: BTreeMap<u64, LinkedHashSet<Uuid>>,
    /// Monotonic counter for ready_seq (FIFO ordering that survives restart)
    ready_seq_counter: u64,
}

impl QueueState {
    /// Returns the earliest visibility timeout (ms) for in-flight messages.
    pub fn next_inflight_timeout(&self) -> Option<u64> {
        self.waiting_for_ack.keys().next().cloned()
    }

    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            waiting_for_dispatch: BTreeMap::new(),
            waiting_for_ack: BTreeMap::new(),
            ready_seq_counter: 0,
        }
    }

    /// Push a message to the queue.
    /// Assigns a new ready_seq from the monotonic counter.
    pub fn push(&mut self, mut msg: Message) {
        let id = msg.id;
        let priority = msg.priority;
        let visible_at = msg.visible_at;

        msg.ready_seq = self.next_ready_seq();

        self.registry.insert(id, msg);

        if visible_at > 0 {
            self.waiting_for_ack.entry(visible_at).or_default().insert(id);
        } else {
            self.waiting_for_dispatch.entry(priority).or_default().insert(id);
        }
    }

    /// Restore a message from persistence without reassigning ready_seq.
    /// Syncs the ready_seq_counter to max(current, msg.ready_seq).
    pub fn restore(&mut self, msg: Message) {
        let id = msg.id;
        let priority = msg.priority;
        let visible_at = msg.visible_at;

        if msg.ready_seq > self.ready_seq_counter {
            self.ready_seq_counter = msg.ready_seq;
        }

        self.registry.insert(id, msg);

        if visible_at > 0 {
            self.waiting_for_ack.entry(visible_at).or_default().insert(id);
        } else {
            self.waiting_for_dispatch.entry(priority).or_default().insert(id);
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
    pub fn ack(&mut self, id: Uuid) -> bool {
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
    pub fn nack(&mut self, id: Uuid, reason: String, max_deliveries: u32) -> (Option<Message>, Option<DlqMessage>) {
        // 1. Check existence and update fields
        let (should_dlq, priority, visible_at) = if let Some(msg) = self.registry.get_mut(&id) {
            msg.failure_reason = Some(reason.clone());
            (msg.attempts >= max_deliveries, msg.priority, msg.visible_at)
        } else {
            return (None, None);
        };

        // 2. Action
        if should_dlq {
            // Remove from here, return for DLQ
            if let Some(msg) = self.delete_message_and_return(id) {
                let dlq_msg = DlqMessage::from_message(msg, reason);
                return (None, Some(dlq_msg));
            }
            (None, None)
        } else {
            // Requeue: remove from in-flight index, reset visible_at, assign new ready_seq, add to ready index
            self.remove_from_index(id, priority, visible_at);
            let new_seq = self.next_ready_seq();
            if let Some(msg) = self.registry.get_mut(&id) {
                msg.visible_at = 0;
                msg.ready_seq = new_seq;
                self.waiting_for_dispatch.entry(priority).or_default().insert(id);
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

        // Collect expired timestamps (BTreeMap keys are sorted ascending)
        let expired_ts: Vec<u64> = self.waiting_for_ack
            .keys()
            .take_while(|&&ts| ts <= now)
            .copied()
            .collect();

        for ts in expired_ts {
            let ids = self.waiting_for_ack.remove(&ts).unwrap_or_default();
            for id in ids {
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
                        let priority = msg.priority;
                        requeued_msgs.push(msg.clone());
                        self.waiting_for_dispatch.entry(priority).or_default().insert(id);
                    }
                }
            }
        }

        (requeued_msgs, dlq_msgs)
    }

    // --- Internal helpers ---

    /// Pop a single message from the queue.
    fn pop_single(&mut self, visibility_timeout_ms: u64, now: u64) -> Option<Message> {
        // Find highest priority ready message
        let next_id = self.waiting_for_dispatch
            .iter()
            .rev()
            .find_map(|(_, queue)| queue.front().cloned());

        let next_id = match next_id {
            Some(id) => id,
            None => return None,
        };

        let timeout = now + visibility_timeout_ms;

        // Get priority before mutable borrow
        let priority = self.registry.get(&next_id)?.priority;

        // Remove from ready index
        self.remove_from_index(next_id, priority, 0);

        // Update to in-flight
        let msg = self.registry.get_mut(&next_id)?;
        msg.visible_at = timeout;
        msg.attempts += 1;

        // Add to in-flight index
        self.waiting_for_ack.entry(timeout).or_default().insert(next_id);

        Some(msg.clone())
    }

    /// Remove a message ID from the appropriate index based on visible_at.
    /// visible_at > 0 → in-flight index; visible_at == 0 → ready index.
    fn remove_from_index(&mut self, id: Uuid, priority: u8, visible_at: u64) {
        if visible_at > 0 {
            if let Some(queue) = self.waiting_for_ack.get_mut(&visible_at) {
                queue.remove(&id);
                if queue.is_empty() { self.waiting_for_ack.remove(&visible_at); }
            }
        } else {
            if let Some(queue) = self.waiting_for_dispatch.get_mut(&priority) {
                queue.remove(&id);
                if queue.is_empty() { self.waiting_for_dispatch.remove(&priority); }
            }
        }
    }

    fn delete_message(&mut self, id: Uuid) -> bool {
        self.delete_message_and_return(id).is_some()
    }

    fn delete_message_and_return(&mut self, id: Uuid) -> Option<Message> {
        let msg = self.registry.remove(&id)?;
        self.remove_from_index(id, msg.priority, msg.visible_at);
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


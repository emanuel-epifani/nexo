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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageState {
    Ready,                  // In waiting_for_dispatch
    InFlight,               // In waiting_for_ack (timestamp in visible_at)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub payload: Bytes,
    pub priority: u8,
    pub attempts: u32,
    pub created_at: u64,
    pub visible_at: u64,
    pub failure_reason: Option<String>,
    pub state: MessageState,
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
            failure_reason: None,
            state: MessageState::Ready,
        }
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub visibility_timeout_ms: u64,
    pub max_retries: u32,
}

impl QueueConfig {
    pub fn from_options(opts: QueueCreateOptions, sys: &SystemQueueConfig) -> Self {
        Self {
            visibility_timeout_ms: opts.visibility_timeout_ms.unwrap_or(sys.visibility_timeout_ms),
            max_retries: opts.max_retries.unwrap_or(sys.max_retries),
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
        }
    }

    /// Push a message to the queue.
    pub fn push(&mut self, msg: Message) {
        let id = msg.id;
        let initial_state = msg.state.clone();
        let priority = msg.priority;
        let visible_at = msg.visible_at;

        self.registry.insert(id, msg);

        match initial_state {
            MessageState::Ready => {
                self.waiting_for_dispatch.entry(priority).or_default().insert(id);
            }
            MessageState::InFlight => {
                self.waiting_for_ack.entry(visible_at).or_default().insert(id);
            }
        }
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
    pub fn nack(&mut self, id: Uuid, reason: String, max_retries: u32) -> (Option<Message>, Option<DlqMessage>) {
        // 1. Check existence and update fields
        let (should_dlq, _priority) = if let Some(msg) = self.registry.get_mut(&id) {
            msg.failure_reason = Some(reason.clone());
            (msg.attempts >= max_retries, msg.priority)
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
            // Requeue (Ready)
            if self.transition_to(id, MessageState::Ready) {
                if let Some(msg) = self.registry.get_mut(&id) {
                    msg.visible_at = 0;
                    return (Some(msg.clone()), None);
                }
            }
            (None, None)
        }
    }

    /// Process expired in-flight timeouts.
    /// Returns (requeued_messages, dlq_messages).
    /// requeued_messages: messages that transitioned to Ready (need UpdateState in DB)
    /// dlq_messages: messages moved to DLQ (need MoveToDlq in DB)
    pub fn process_expired(&mut self, max_retries: u32) -> (Vec<Message>, Vec<DlqMessage>) {
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
                    .map(|m| m.attempts >= max_retries)
                    .unwrap_or(false);

                if should_dlq {
                    if let Some(msg) = self.registry.remove(&id) {
                        dlq_msgs.push(DlqMessage::from_message(msg, "Timeout".to_string()));
                    }
                } else {
                    if let Some(msg) = self.registry.get_mut(&id) {
                        msg.state = MessageState::Ready;
                        msg.visible_at = 0;
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

        // Update message fields before transition so visible_at is the correct key
        if let Some(msg) = self.registry.get_mut(&next_id) {
            msg.visible_at = timeout;
            msg.attempts += 1;
        }

        self.transition_to(next_id, MessageState::InFlight);

        if let Some(msg) = self.registry.get(&next_id) {
            return Some(msg.clone());
        }

        None
    }

    /// Remove a message ID from the appropriate index based on its state
    fn remove_from_index(&mut self, state: &MessageState, id: Uuid, priority: u8, visible_at: u64) {
        match state {
            MessageState::Ready => {
                if let Some(queue) = self.waiting_for_dispatch.get_mut(&priority) {
                    queue.remove(&id);
                    if queue.is_empty() { self.waiting_for_dispatch.remove(&priority); }
                }
            }
            MessageState::InFlight => {
                if let Some(queue) = self.waiting_for_ack.get_mut(&visible_at) {
                    queue.remove(&id);
                    if queue.is_empty() { self.waiting_for_ack.remove(&visible_at); }
                }
            }
        }
    }

    fn transition_to(&mut self, id: Uuid, new_state: MessageState) -> bool {
        let (old_state, priority, visible_at) = match self.registry.get(&id) {
            Some(m) => (m.state.clone(), m.priority, m.visible_at),
            None => return false,
        };

        if old_state == new_state {
            return true;
        }

        // Remove from old index
        self.remove_from_index(&old_state, id, priority, visible_at);

        // Update state and add to new index
        if let Some(msg) = self.registry.get_mut(&id) {
            msg.state = new_state.clone();

            match new_state {
                MessageState::Ready => {
                    self.waiting_for_dispatch
                        .entry(msg.priority)
                        .or_default()
                        .insert(id);
                }
                MessageState::InFlight => {
                    self.waiting_for_ack.entry(msg.visible_at).or_default().insert(id);
                }
            }
        }

        true
    }

    fn delete_message(&mut self, id: Uuid) -> bool {
        self.delete_message_and_return(id).is_some()
    }

    fn delete_message_and_return(&mut self, id: Uuid) -> Option<Message> {
        let msg = self.registry.remove(&id)?;

        // Remove from index
        self.remove_from_index(&msg.state, id, msg.priority, msg.visible_at);

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


//! Queue State: Internal state management for queue broker
//! 
//! This module contains the pure state logic without any concurrency primitives.
//! The QueueManager wraps this state in a Mutex<QueueInner> per queue.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use hashlink::LinkedHashSet;
use chrono::{DateTime, Utc};

use crate::dashboard::queue::QueueSummary;
use crate::brokers::queue::commands::QueueCreateOptions;
use crate::brokers::queue::config::SystemQueueConfig;
use crate::brokers::queue::dlq::DlqMessage;

// ==========================================
// MESSAGE & CONFIG
// ==========================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageState {
    Ready,                  // In waiting_for_dispatch
    Scheduled(u64),         // In waiting_for_time (timestamp)
    InFlight(u64),          // In waiting_for_ack (timestamp scadenza)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub payload: Bytes,
    pub priority: u8,
    pub attempts: u32,
    pub created_at: u64,
    pub visible_at: u64,
    pub delayed_until: Option<u64>,
    pub failure_reason: Option<String>,
    pub state: MessageState,
}

#[derive(Debug, Clone)]
pub struct QueueMessageView {
    pub id: Uuid,
    pub payload: Bytes,
    pub state: String,
    pub priority: u8,
    pub attempts: u32,
    pub next_delivery_at: Option<String>,
}

impl Message {
    pub fn new(payload: Bytes, priority: u8, delay_ms: Option<u64>) -> Self {
        let now = current_time_ms();

        let (state, visible_at) = if let Some(delay) = delay_ms {
            let ts = now + delay;
            (MessageState::Scheduled(ts), ts)
        } else {
            (MessageState::Ready, 0)
        };

        Self {
            id: Uuid::new_v4(),
            payload,
            priority,
            attempts: 0,
            created_at: now,
            visible_at,
            delayed_until: delay_ms.map(|d| now + d),
            failure_reason: None,
            state,
        }
    }

    pub fn from_dlq(dlq_msg: DlqMessage) -> Self {
        Self {
            id: dlq_msg.id,
            payload: dlq_msg.payload,
            priority: dlq_msg.priority,
            attempts: 0,
            created_at: dlq_msg.created_at,
            visible_at: 0,
            delayed_until: None,
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
        /// Scheduled messages by activation time
        waiting_for_time: BTreeMap<u64, LinkedHashSet<Uuid>>,
        /// In-flight messages by timeout time
        waiting_for_ack: BTreeMap<u64, LinkedHashSet<Uuid>>,

    }
    impl QueueState {
        /// Returns the earliest timestamp (ms) of the next scheduled event (Scheduled or Retry).
        pub fn next_timeout(&self) -> Option<u64> {
            let next_scheduled = self.waiting_for_time.keys().next().cloned();
            let next_retry = self.waiting_for_ack.keys().next().cloned();

            match (next_scheduled, next_retry) {
                (Some(ts1), Some(ts2)) => Some(ts1.min(ts2)),
                (Some(ts), None) => Some(ts),
                (None, Some(ts)) => Some(ts),
                (None, None) => None,
            }
        }
        pub fn new() -> Self {
            Self {
                registry: HashMap::new(),
                waiting_for_dispatch: BTreeMap::new(),
                waiting_for_time: BTreeMap::new(),
                waiting_for_ack: BTreeMap::new(),
            }
        }

        /// Push a message to the queue.
        pub fn push(&mut self, msg: Message) {
            let id = msg.id;
            let initial_state = msg.state.clone();
            let priority = msg.priority;

            self.registry.insert(id, msg);

            match initial_state {
                MessageState::Ready => {
                    self.waiting_for_dispatch.entry(priority).or_default().insert(id);
                }
                MessageState::Scheduled(ts) => {
                    self.waiting_for_time.entry(ts).or_default().insert(id);
                }
                MessageState::InFlight(ts) => {
                    self.waiting_for_ack.entry(ts).or_default().insert(id);
                }
            }
        }

        /// Pop the highest priority message. Returns (message, needs_pulse).
        pub fn pop(&mut self, visibility_timeout_ms: u64) -> (Option<Message>, bool) {
            self.pop_single(visibility_timeout_ms)
        }

        /// Acknowledge a message (remove from system).
        pub fn ack(&mut self, id: Uuid) -> bool {
            self.delete_message(id)
        }

        /// Take up to `max` messages for batch consumption.
        pub fn take_batch(&mut self, max: usize, visibility_timeout_ms: u64) -> (Vec<Message>, bool) {
            let mut result = Vec::with_capacity(max);
            let mut any_earliest = false;

            while result.len() < max {
                match self.pop_single(visibility_timeout_ms) {
                    (Some(msg), is_earliest) => {
                        if is_earliest {
                            any_earliest = true;
                        }
                        result.push(msg);
                    }
                    (None, _) => break,
                }
            }

            (result, any_earliest)
        }

        /// Negative Acknowledge. Returns (requeued_msg, dlq_msg).
        /// If dlq_msg is Some, the message was removed from this state and should be added to DLQ state.
        pub fn nack(&mut self, id: Uuid, reason: String, max_retries: u32) -> (Option<Message>, Option<DlqMessage>) {
            // 1. Check existence and update fields
            let (should_dlq, priority) = if let Some(msg) = self.registry.get_mut(&id) {
                msg.failure_reason = Some(reason.clone());
                msg.attempts += 1;
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

        /// Process expired events (scheduled -> ready, timeout -> retry/DLQ).
        /// Returns (requeued_messages, dlq_messages).
        /// requeued_messages: messages that transitioned to Ready (need UpdateState in DB)
        /// dlq_messages: messages moved to DLQ (need MoveToDlq in DB)
        pub fn process_expired(&mut self, max_retries: u32) -> (Vec<Message>, Vec<DlqMessage>) {
            let now = current_time_ms();
            let mut ids_to_ready = Vec::new();
            let mut ids_to_dlq = Vec::new();

            // Scheduled messages that are now ready
            for (&ts, ids) in &self.waiting_for_time {
                if ts <= now {
                    ids_to_ready.extend(ids.iter().cloned());
                } else {
                    break;
                }
            }

            // Timed out in-flight messages
            for (&ts, ids) in &self.waiting_for_ack {
                if ts <= now {
                    for &id in ids {
                        if let Some(msg) = self.registry.get(&id) {
                            if msg.attempts >= max_retries {
                                ids_to_dlq.push(id);
                            } else {
                                ids_to_ready.push(id);
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            let mut requeued_msgs = Vec::new();
            // Transition to ready
            for id in ids_to_ready {
                if self.transition_to(id, MessageState::Ready) {
                    if let Some(msg) = self.registry.get_mut(&id) {
                        msg.visible_at = 0; // Ready immediately
                        requeued_msgs.push(msg.clone());
                    }
                }
            }

            // Collect DLQ messages and delete from active
            let mut dlq_msgs = Vec::new();
            for id in ids_to_dlq {
                if let Some(msg) = self.registry.remove(&id) { // Remove returns value
                    // Clean up indexes
                    match msg.state {
                        MessageState::InFlight(ts) => {
                            if let Some(queue) = self.waiting_for_ack.get_mut(&ts) {
                                queue.remove(&id);
                                if queue.is_empty() { self.waiting_for_ack.remove(&ts); }
                            }
                        },
                        // Should be InFlight mostly, but handle others if logic changes
                        _ => {}
                    }
                    dlq_msgs.push(DlqMessage::from_message(msg, "Timeout".to_string()));
                }
            }

            (requeued_msgs, dlq_msgs)
        }

        pub fn get_counters(&self) -> (usize, usize, usize) {
            let mut pending = 0;
            let mut inflight = 0;
            let mut scheduled = 0;

            for (_, queue) in &self.waiting_for_dispatch {
                pending += queue.len();
            }

            for (_, list) in &self.waiting_for_ack {
                inflight += list.len();
            }

            for (_, list) in &self.waiting_for_time {
                scheduled += list.len();
            }

            (pending, inflight, scheduled)
        }

        pub fn get_messages(&self, state_filter: String, offset: usize, limit: usize, search: Option<String>) -> (usize, Vec<QueueMessageView>) {
            let mut all_filtered: Vec<&Message> = Vec::new();
            // Collect IDs based on state filter (no dynamic dispatch)
            let ids: Vec<&Uuid> = match state_filter.to_lowercase().as_str() {
                "pending" => self.waiting_for_dispatch.values().flat_map(|q| q.iter()).collect(),
                "inflight" => self.waiting_for_ack.values().flat_map(|q| q.iter()).collect(),
                "scheduled" => self.waiting_for_time.values().flat_map(|q| q.iter()).collect(),
                _ => return (0, vec![]),
            };

            for id in ids {
                if let Some(msg) = self.registry.get(id) {
                    let matches_search = match &search {
                        Some(s) => String::from_utf8_lossy(&msg.payload).contains(s),
                        None => true,
                    };

                    if matches_search {
                        all_filtered.push(msg);
                    }
                }
            }

            let total = all_filtered.len();
            let paged: Vec<QueueMessageView> = all_filtered
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|msg| {
                    let next_delivery_at = match msg.state {
                        MessageState::Scheduled(ts) => Some(format_time(ts)),
                        _ => None,
                    };
                    QueueMessageView {
                        id: msg.id,
                        payload: msg.payload.clone(),
                        state: state_filter.clone(),
                        priority: msg.priority,
                        attempts: msg.attempts,
                        next_delivery_at,
                    }
                })
                .collect();

            (total, paged)
        }

        pub fn has_ready_messages(&self) -> bool {
            !self.waiting_for_dispatch.is_empty()
        }

        /// Re-queue an InFlight message back to Ready state immediately.
        /// Used when a waiter (consumer connection) dies before receiving the response.
        /// Undoes the attempt increment from take_batch since the message was never delivered.
        pub fn requeue_inflight(&mut self, id: Uuid) -> bool {
            let is_inflight = self.registry.get(&id)
                .map(|m| matches!(m.state, MessageState::InFlight(_)))
                .unwrap_or(false);

            if !is_inflight { return false; }

            if self.transition_to(id, MessageState::Ready) {
                if let Some(msg) = self.registry.get_mut(&id) {
                    msg.visible_at = 0;
                    msg.attempts = msg.attempts.saturating_sub(1);
                }
                true
            } else {
                false
            }
        }

        /// Peek messages without consuming them (for DLQ inspection)
        pub fn peek_messages(&self, limit: usize) -> Vec<Message> {
            let mut messages = Vec::new();
            let mut count = 0;

            for (_, queue) in self.waiting_for_dispatch.iter().rev() {
                for id in queue.iter() {
                    if count >= limit {
                        break;
                    }
                    if let Some(msg) = self.registry.get(id) {
                        messages.push(msg.clone());
                        count += 1;
                    }
                }
                if count >= limit {
                    break;
                }
            }

            messages
        }

        /// Get total message count
        pub fn len(&self) -> usize {
            self.registry.len()
        }

        /// Remove a message by ID (for DLQ operations)
        pub fn remove_by_id(&mut self, id: Uuid) -> Option<Message> {
            self.delete_message_and_return(id)
        }

        /// Clear all messages (for purge)
        pub fn clear(&mut self) {
            self.registry.clear();
            self.waiting_for_dispatch.clear();
            self.waiting_for_time.clear();
            self.waiting_for_ack.clear();
        }

        // --- Internal helpers ---

        /// Pop a single message from the queue. Returns (message, is_earliest_timeout).
        fn pop_single(&mut self, visibility_timeout_ms: u64) -> (Option<Message>, bool) {
            let now = current_time_ms();

            // Find highest priority ready message
            let next_id = self.waiting_for_dispatch
                .iter()
                .rev()
                .find_map(|(_, queue)| queue.front().cloned());

            let next_id = match next_id {
                Some(id) => id,
                None => return (None, false),
            };

            let timeout = now + visibility_timeout_ms;
            self.transition_to(next_id, MessageState::InFlight(timeout));

            // Update message fields
            if let Some(msg) = self.registry.get_mut(&next_id) {
                msg.visible_at = timeout;
                msg.attempts += 1;

                // Check if this is the earliest timeout
                let is_earliest = self.waiting_for_ack
                    .keys()
                    .next()
                    .map(|&t| t == timeout)
                    .unwrap_or(false);

                return (Some(msg.clone()), is_earliest);
            }

            (None, false)
        }

        /// Remove a message ID from the appropriate index based on its state
        fn remove_from_index(&mut self, state: &MessageState, id: Uuid, priority: u8) {
            match state {
                MessageState::Ready => {
                    if let Some(queue) = self.waiting_for_dispatch.get_mut(&priority) {
                        queue.remove(&id);
                        if queue.is_empty() {
                            self.waiting_for_dispatch.remove(&priority);
                        }
                    }
                }
                MessageState::Scheduled(ts) => {
                    if let Some(queue) = self.waiting_for_time.get_mut(ts) {
                        queue.remove(&id);
                        if queue.is_empty() {
                            self.waiting_for_time.remove(ts);
                        }
                    }
                }
                MessageState::InFlight(ts) => {
                    if let Some(queue) = self.waiting_for_ack.get_mut(ts) {
                        queue.remove(&id);
                        if queue.is_empty() {
                            self.waiting_for_ack.remove(ts);
                        }
                    }
                }
            }
        }

        fn transition_to(&mut self, id: Uuid, new_state: MessageState) -> bool {
            let (old_state, priority) = match self.registry.get(&id) {
                Some(m) => (m.state.clone(), m.priority),
                None => return false,
            };

            if old_state == new_state {
                return true;
            }

            // Remove from old index
            self.remove_from_index(&old_state, id, priority);

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
                    MessageState::Scheduled(ts) => {
                        self.waiting_for_time.entry(ts).or_default().insert(id);
                    }
                    MessageState::InFlight(ts) => {
                        self.waiting_for_ack.entry(ts).or_default().insert(id);
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
            self.remove_from_index(&msg.state, id, msg.priority);

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

    fn format_time(ts: u64) -> String {
        let d = UNIX_EPOCH + Duration::from_millis(ts);
        let datetime = DateTime::<Utc>::from(d);
        datetime.to_rfc3339()
    }


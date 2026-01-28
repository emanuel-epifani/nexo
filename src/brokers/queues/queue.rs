//! Queue State: Internal state management for queue actor
//! 
//! This module contains the pure state logic without any concurrency primitives.
//! The QueueActor owns this state and operates on it sequentially.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use hashlink::LinkedHashSet;
use chrono::{DateTime, Utc};

use crate::dashboard::models::queues::{QueueSummary, MessageSummary};

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
    pub state: MessageState,
}

impl Message {
    pub fn new(payload: Bytes, priority: u8, delay_ms: Option<u64>) -> Self {
        let now = current_time_ms();
        
        let state = if let Some(delay) = delay_ms {
            MessageState::Scheduled(now + delay)
        } else {
            MessageState::Ready
        };

        Self {
            id: Uuid::new_v4(),
            payload,
            priority,
            attempts: 0,
            created_at: now,
            visible_at: 0,
            delayed_until: delay_ms.map(|d| now + d),
            state,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub visibility_timeout_ms: u64,
    pub max_retries: u32,
    pub ttl_ms: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            visibility_timeout_ms: 30000,
            max_retries: 5,
            ttl_ms: 604800000, // 7 days in ms
        }
    }
}

impl QueueConfig {
    pub fn merge_defaults(&mut self) {
        let def = Self::default();
        if self.visibility_timeout_ms == 0 {
            self.visibility_timeout_ms = def.visibility_timeout_ms;
        }
        if self.max_retries == 0 {
            self.max_retries = def.max_retries;
        }
        if self.ttl_ms == 0 {
            self.ttl_ms = def.ttl_ms;
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
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            waiting_for_dispatch: BTreeMap::new(),
            waiting_for_time: BTreeMap::new(),
            waiting_for_ack: BTreeMap::new(),
        }
    }

    /// Push a message. Returns true if pulse loop should wake (new earliest event).
    pub fn push(&mut self, payload: Bytes, priority: u8, delay_ms: Option<u64>) -> bool {
        let msg = Message::new(payload, priority, delay_ms);
        let id = msg.id;
        let initial_state = msg.state.clone();
        
        self.registry.insert(id, msg);

        match initial_state {
            MessageState::Ready => {
                self.waiting_for_dispatch.entry(priority).or_default().insert(id);
                false // No time-based wake needed
            }
            MessageState::Scheduled(ts) => {
                self.waiting_for_time.entry(ts).or_default().insert(id);
                // Check if this is the earliest scheduled
                self.waiting_for_time.keys().next().map(|&t| t == ts).unwrap_or(false)
            }
            MessageState::InFlight(_) => false, // Impossible for new message
        }
    }

    /// Pop the highest priority message. Returns (message, needs_pulse).
    pub fn pop(&mut self, visibility_timeout_ms: u64) -> (Option<Message>, bool) {
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

    /// Acknowledge a message (remove from system).
    pub fn ack(&mut self, id: Uuid) -> bool {
        self.delete_message(id)
    }

    /// Take up to `max` messages for batch consumption.
    pub fn take_batch(&mut self, max: usize, visibility_timeout_ms: u64) -> (Vec<Message>, bool) {
        let mut result = Vec::with_capacity(max);
        let now = current_time_ms();
        let mut any_earliest = false;

        while result.len() < max {
            let next_id = match self.waiting_for_dispatch
                .iter()
                .rev()
                .find_map(|(_, queue)| queue.front().cloned())
            {
                Some(id) => id,
                None => break,
            };

            let timeout = now + visibility_timeout_ms;
            self.transition_to(next_id, MessageState::InFlight(timeout));

            if let Some(msg) = self.registry.get_mut(&next_id) {
                msg.visible_at = timeout;
                msg.attempts += 1;
                result.push(msg.clone());

                let is_earliest = self.waiting_for_ack
                    .keys()
                    .next()
                    .map(|&t| t == timeout)
                    .unwrap_or(false);
                if is_earliest {
                    any_earliest = true;
                }
            }
        }

        (result, any_earliest)
    }

    /// Process expired events (scheduled -> ready, timeout -> retry/DLQ).
    /// Returns list of (payload, priority) for DLQ.
    pub fn process_expired(&mut self, max_retries: u32) -> Vec<(Bytes, u8)> {
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

        // Transition to ready
        for id in ids_to_ready {
            self.transition_to(id, MessageState::Ready);
        }

        // Collect DLQ payloads and delete
        let mut dlq_items = Vec::new();
        for id in ids_to_dlq {
            if let Some(msg) = self.registry.get(&id) {
                dlq_items.push((msg.payload.clone(), msg.priority));
            }
            self.delete_message(id);
        }

        dlq_items
    }

    pub fn get_snapshot(&self, name: &str) -> QueueSummary {
        let mut pending = Vec::new();
        let mut inflight = Vec::new();
        let mut scheduled = Vec::new();

        // Pending
        for (priority, queue) in &self.waiting_for_dispatch {
            for id in queue {
                if let Some(msg) = self.registry.get(id) {
                    pending.push(MessageSummary {
                        id: msg.id,
                        payload: String::from_utf8_lossy(&msg.payload[1..]).to_string(),
                        state: "Pending".to_string(),
                        priority: *priority,
                        attempts: msg.attempts,
                        next_delivery_at: None,
                    });
                }
            }
        }

        // InFlight
        for (expiry, list) in &self.waiting_for_ack {
            for id in list {
                if let Some(msg) = self.registry.get(id) {
                    inflight.push(MessageSummary {
                        id: msg.id,
                        payload: String::from_utf8_lossy(&msg.payload[1..]).to_string(),
                        state: "InFlight".to_string(),
                        priority: msg.priority,
                        attempts: msg.attempts,
                        next_delivery_at: None,
                    });
                }
            }
        }

        // Scheduled
        for (time, list) in &self.waiting_for_time {
            for id in list {
                if let Some(msg) = self.registry.get(id) {
                    scheduled.push(MessageSummary {
                        id: msg.id,
                        payload: String::from_utf8_lossy(&msg.payload[1..]).to_string(),
                        state: "Scheduled".to_string(),
                        priority: msg.priority,
                        attempts: msg.attempts,
                        next_delivery_at: Some(format_time(*time)),
                    });
                }
            }
        }

        QueueSummary {
            name: name.to_string(),
            pending,
            inflight,
            scheduled,
        }
    }

    pub fn has_ready_messages(&self) -> bool {
        !self.waiting_for_dispatch.is_empty()
    }

    // --- Internal helpers ---

    fn transition_to(&mut self, id: Uuid, new_state: MessageState) -> bool {
        let msg = match self.registry.get_mut(&id) {
            Some(m) => m,
            None => return false,
        };

        let old_state = msg.state.clone();
        if old_state == new_state {
            return true;
        }

        // Remove from old index
        match old_state {
            MessageState::Ready => {
                if let Some(queue) = self.waiting_for_dispatch.get_mut(&msg.priority) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_dispatch.remove(&msg.priority);
                    }
                }
            }
            MessageState::Scheduled(ts) => {
                if let Some(queue) = self.waiting_for_time.get_mut(&ts) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_time.remove(&ts);
                    }
                }
            }
            MessageState::InFlight(ts) => {
                if let Some(queue) = self.waiting_for_ack.get_mut(&ts) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_ack.remove(&ts);
                    }
                }
            }
        }

        // Update state
        msg.state = new_state.clone();

        // Add to new index
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

        true
    }

    fn delete_message(&mut self, id: Uuid) -> bool {
        let msg = match self.registry.remove(&id) {
            Some(m) => m,
            None => return false,
        };

        match msg.state {
            MessageState::Ready => {
                if let Some(queue) = self.waiting_for_dispatch.get_mut(&msg.priority) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_dispatch.remove(&msg.priority);
                    }
                }
            }
            MessageState::Scheduled(ts) => {
                if let Some(queue) = self.waiting_for_time.get_mut(&ts) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_time.remove(&ts);
                    }
                }
            }
            MessageState::InFlight(ts) => {
                if let Some(queue) = self.waiting_for_ack.get_mut(&ts) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_ack.remove(&ts);
                    }
                }
            }
        }

        true
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

// ==========================================
// TESTS
// ==========================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_push_pop_ack() {
        let mut state = QueueState::new();
        let payload = Bytes::from("test_payload");

        // Push
        state.push(payload.clone(), 0, None);
        assert_eq!(state.waiting_for_dispatch.values().map(|q| q.len()).sum::<usize>(), 1);

        // Pop
        let (msg, _) = state.pop(30000);
        let msg = msg.expect("Should return message");
        assert_eq!(msg.payload, payload);
        assert_eq!(state.waiting_for_dispatch.values().map(|q| q.len()).sum::<usize>(), 0);
        assert_eq!(state.waiting_for_ack.values().map(|q| q.len()).sum::<usize>(), 1);

        // Ack
        assert!(state.ack(msg.id));
        assert_eq!(state.waiting_for_ack.values().map(|q| q.len()).sum::<usize>(), 0);
    }

    #[test]
    fn test_priority_fifo() {
        let mut state = QueueState::new();

        state.push(Bytes::from("low"), 0, None);
        state.push(Bytes::from("high_1"), 10, None);
        state.push(Bytes::from("high_2"), 10, None);
        state.push(Bytes::from("mid"), 5, None);

        let (msg1, _) = state.pop(30000);
        assert_eq!(msg1.unwrap().payload, Bytes::from("high_1"));

        let (msg2, _) = state.pop(30000);
        assert_eq!(msg2.unwrap().payload, Bytes::from("high_2"));

        let (msg3, _) = state.pop(30000);
        assert_eq!(msg3.unwrap().payload, Bytes::from("mid"));

        let (msg4, _) = state.pop(30000);
        assert_eq!(msg4.unwrap().payload, Bytes::from("low"));
    }

    #[test]
    fn test_scheduled_message() {
        let mut state = QueueState::new();

        // Push with delay
        state.push(Bytes::from("delayed"), 0, Some(100));
        
        assert_eq!(state.waiting_for_time.values().map(|q| q.len()).sum::<usize>(), 1);
        assert_eq!(state.waiting_for_dispatch.values().map(|q| q.len()).sum::<usize>(), 0);

        // Pop should return nothing (not ready yet)
        let (msg, _) = state.pop(30000);
        assert!(msg.is_none());
    }

    #[test]
    fn test_batch_take() {
        let mut state = QueueState::new();

        for i in 0..10 {
            state.push(Bytes::from(format!("msg_{}", i)), 0, None);
        }

        let (batch, _) = state.take_batch(5, 30000);
        assert_eq!(batch.len(), 5);
        assert_eq!(state.waiting_for_dispatch.values().map(|q| q.len()).sum::<usize>(), 5);
        assert_eq!(state.waiting_for_ack.values().map(|q| q.len()).sum::<usize>(), 5);
    }
}

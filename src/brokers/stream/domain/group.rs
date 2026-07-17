//! Consumer Group: JetStream-like per-message ack tracking
//!
//! Each group tracks:
//! - ack_floor: highest seq such that ALL seqs 1..=ack_floor are acked
//! - pending: messages delivered but not yet acked
//! - redeliver: messages that need to be redelivered (nack or timeout)

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::brokers::stream::domain::message::Message;

#[derive(Clone)]
pub struct PendingMsg {
    pub consumer_id: String,
    pub delivered_at: Instant,
    pub delivery_count: u32,
    pub key: Option<Bytes>,
}

#[derive(Clone, Debug)]
pub struct DltEntry {
    pub reason: String,
    pub attempts: u32,
    pub key: Option<Bytes>,
}

pub struct ConsumerGroup {
    pub id: String,
    // === Persistent state (saved to disk) ===
    pub ack_floor: u64,
    // === Volatile state (rebuilt on restart) ===
    pub next_deliver_seq: u64,
    pub pending: HashMap<u64, PendingMsg>,
    pub redeliver: VecDeque<u64>,
    pub dlt: HashMap<u64, DltEntry>,
    pub parked_keys: HashSet<Bytes>,
    pub delivery_attempts: HashMap<u64, u32>,
    pub keys_in_flight: HashMap<Bytes, u64>,
    pub blocked_by_key: HashMap<Bytes, VecDeque<u64>>,
    // === Config ===
    pub max_ack_pending: usize,
    pub ack_wait: Duration,
    pub max_deliveries: u32,
    // === Member tracking (for disconnect cleanup) ===
    members: HashMap<String, String>, // consumer_id → connection_client_id
    // === Runtime State ===
    pub is_fetching_cold: bool,
    pub generation: u64,
    pub cancel: CancellationToken,
    last_clamped_head: u64,
    earliest_deadline: Option<Instant>,
}

impl ConsumerGroup {
    pub fn new(id: String, head_seq: u64, max_ack_pending: usize, ack_wait: Duration, max_deliveries: u32) -> Self {
        let head_seq = head_seq.max(1);
        Self {
            id,
            ack_floor: head_seq.saturating_sub(1),
            next_deliver_seq: head_seq,
            pending: HashMap::new(),
            redeliver: VecDeque::new(),
            dlt: HashMap::new(),
            parked_keys: HashSet::new(),
            delivery_attempts: HashMap::new(),
            keys_in_flight: HashMap::new(),
            blocked_by_key: HashMap::new(),
            max_ack_pending,
            ack_wait,
            max_deliveries,
            members: HashMap::new(),
            is_fetching_cold: false,
            generation: 1,
            cancel: CancellationToken::new(),
            last_clamped_head: 0,
            earliest_deadline: None,
        }
    }

    pub fn is_backpressured(&self) -> bool {
        self.pending.len() >= self.max_ack_pending
    }

    pub fn restore(id: String, ack_floor: u64, head_seq: u64, max_ack_pending: usize, ack_wait: Duration, max_deliveries: u32, dlt_entries: HashMap<u64, DltEntry>, parked_keys: HashSet<Bytes>) -> Self {
        let normalized_floor = ack_floor.max(head_seq.saturating_sub(1));
        Self {
            id,
            ack_floor: normalized_floor,
            next_deliver_seq: normalized_floor.saturating_add(1).max(head_seq.max(1)),
            pending: HashMap::new(),
            redeliver: VecDeque::new(),
            dlt: dlt_entries,
            parked_keys,
            delivery_attempts: HashMap::new(),
            keys_in_flight: HashMap::new(),
            blocked_by_key: HashMap::new(),
            max_ack_pending,
            ack_wait,
            max_deliveries,
            members: HashMap::new(),
            is_fetching_cold: false,
            generation: 1,
            cancel: CancellationToken::new(),
            last_clamped_head: 0,
            earliest_deadline: None,
        }
    }

    /// Fetch messages for a client. Serves redeliver queue first, then fresh messages.
    pub fn fetch(&mut self, consumer_id: &str, generation: u64, limit: usize, log: &VecDeque<Message>, ram_start_seq: u64, head_seq: u64) -> Result<Vec<Message>, String> {
        self.ensure_active_consumer(consumer_id, generation)?;
        self.clamp_head(head_seq);

        if self.pending.len() >= self.max_ack_pending {
            return Ok(vec![]); // backpressure
        }

        let budget = limit.min(self.max_ack_pending - self.pending.len());
        let mut result = Vec::with_capacity(budget);

        // 1. Redeliver first
        while result.len() < budget {
            if let Some(seq) = self.redeliver.pop_front() {
                if seq < head_seq {
                    self.delivery_attempts.remove(&seq);
                    self.dlt.remove(&seq);
                    continue;
                }

                if let Some(msg) = Self::read_from_log(log, ram_start_seq, seq) {
                    if let Some(msg) = self.issue_delivery(consumer_id, msg) {
                        result.push(msg);
                    }
                } else {
                    // Cold message: put back and stop (needs disk read or wait for RAM)
                    self.redeliver.push_front(seq);
                    break;
                }
            } else {
                break;
            }
        }

        // 2. Fresh messages
        while result.len() < budget {
            let seq = self.next_deliver_seq.max(head_seq);
            if self.next_deliver_seq < head_seq {
                self.next_deliver_seq = head_seq;
                self.ack_floor = self.ack_floor.max(head_seq.saturating_sub(1));
            }

            if let Some(msg) = Self::read_from_log(log, ram_start_seq, seq) {
                self.next_deliver_seq = seq + 1;
                if let Some(msg) = self.issue_delivery(consumer_id, msg) {
                    result.push(msg);
                }
            } else {
                break; // no more messages in log
            }
        }

        Ok(result)
    }

    /// Acknowledge a message. Removes from pending and tries to advance ack_floor.
    pub fn ack(&mut self, consumer_id: &str, generation: u64, seq: u64) -> Result<bool, String> {
        self.ensure_active_consumer(consumer_id, generation)?;

        let pending_msg = match self.pending.get(&seq) {
            Some(msg) if msg.consumer_id == consumer_id => msg.clone(),
            Some(_) => return Err("NOT_OWNER".to_string()),
            None => return Err(format!("seq {} not pending", seq)),
        };

        self.pending.remove(&seq);
        if self.pending.is_empty() {
            self.earliest_deadline = None;
        }
        self.delivery_attempts.remove(&seq);

        let mut key_unblocked = false;
        if let Some(key) = &pending_msg.key {
            self.keys_in_flight.remove(key);
            if let Some(blocked) = self.blocked_by_key.get_mut(key) {
                if let Some(next_seq) = blocked.pop_front() {
                    self.redeliver.push_back(next_seq);
                    key_unblocked = true;
                }
                if blocked.is_empty() {
                    self.blocked_by_key.remove(key);
                }
            }
        }

        self.try_advance_floor();
        Ok(key_unblocked)
    }

    /// Negative acknowledge: move message back to redeliver queue.
    pub fn check_redelivery(&mut self) -> bool {
        if self.pending.is_empty() {
            self.earliest_deadline = None;
            return false;
        }
        let now = Instant::now();
        if let Some(deadline) = self.earliest_deadline {
            if now < deadline {
                return false;
            }
        }

        let expired: Vec<u64> = self.pending.iter()
            .filter(|(_, msg)| now.duration_since(msg.delivered_at) > self.ack_wait)
            .map(|(seq, _)| *seq)
            .collect();

        if expired.is_empty() {
            self.earliest_deadline = self.pending.values().map(|m| m.delivered_at + self.ack_wait).min();
            return false;
        }

        for seq in expired {
            if let Some(msg) = self.pending.remove(&seq) {
                tracing::debug!("[Group:{}] Redelivery timeout seq={} (attempts={})", self.id, seq, msg.delivery_count);
                self.release_seq(seq, msg.delivery_count, msg.key);
            }
        }

        self.earliest_deadline = self.pending.values().map(|m| m.delivered_at + self.ack_wait).min();
        self.try_advance_floor();
        true
    }

    pub fn seek_beginning(&mut self, head_seq: u64) {
        self.ack_floor = head_seq.max(1).saturating_sub(1);
        self.next_deliver_seq = head_seq.max(1);
        self.reset_runtime();
    }

    pub fn seek_end(&mut self, last_seq: u64) {
        self.ack_floor = last_seq;
        self.next_deliver_seq = last_seq + 1;
        self.reset_runtime();
    }

    pub fn clamp_head(&mut self, head_seq: u64) -> bool {
        let head_seq = head_seq.max(1);
        if head_seq <= self.last_clamped_head {
            return false;
        }
        let mut changed = false;

        if self.ack_floor < head_seq.saturating_sub(1) {
            self.ack_floor = head_seq.saturating_sub(1);
            changed = true;
        }

        if self.next_deliver_seq < head_seq {
            self.next_deliver_seq = head_seq;
            changed = true;
        }

        let stale_pending: Vec<u64> = self.pending.keys().copied().filter(|seq| *seq < head_seq).collect();
        if !stale_pending.is_empty() {
            for seq in stale_pending {
                self.pending.remove(&seq);
                self.delivery_attempts.remove(&seq);
            }
            changed = true;
        }

        let before_redeliver = self.redeliver.len();
        self.redeliver.retain(|seq| *seq >= head_seq);
        if self.redeliver.len() != before_redeliver {
            changed = true;
        }

        let before_dlt = self.dlt.len();
        let stale_dlt_keys: Vec<u64> = self.dlt.keys().copied().filter(|seq| *seq < head_seq).collect();
        for seq in &stale_dlt_keys {
            self.dlt.remove(seq);
        }
        if self.dlt.len() != before_dlt {
            changed = true;
        }

        // Clean up parked_keys that no longer have any DLT entries
        self.parked_keys.retain(|key| {
            self.dlt.values().any(|e| e.key.as_ref() == Some(key))
        });

        let before_attempts = self.delivery_attempts.len();
        self.delivery_attempts.retain(|seq, _| *seq >= head_seq);
        if self.delivery_attempts.len() != before_attempts {
            changed = true;
        }

        let before_keys = self.keys_in_flight.len();
        self.keys_in_flight.retain(|_, seq| *seq >= head_seq);
        if self.keys_in_flight.len() != before_keys {
            changed = true;
        }

        let before_blocked = self.blocked_by_key.values().map(|v| v.len()).sum::<usize>();
        self.blocked_by_key.retain(|_, blocked| {
            blocked.retain(|seq| *seq >= head_seq);
            !blocked.is_empty()
        });
        let after_blocked = self.blocked_by_key.values().map(|v| v.len()).sum::<usize>();
        if after_blocked != before_blocked {
            changed = true;
        }

        if changed {
            self.is_fetching_cold = false;
            self.try_advance_floor();
            self.invalidate_inflight();
        }

        self.last_clamped_head = head_seq;
        changed
    }

    pub fn next_fetch_seq(&self, head_seq: u64) -> u64 {
        self.redeliver.front().copied().unwrap_or(self.next_deliver_seq).max(head_seq.max(1))
    }

    fn reset_runtime(&mut self) {
        self.pending.clear();
        self.redeliver.clear();
        self.dlt.clear();
        self.parked_keys.clear();
        self.delivery_attempts.clear();
        self.keys_in_flight.clear();
        self.blocked_by_key.clear();
        self.members.clear();
        self.is_fetching_cold = false;
        self.generation = self.generation.saturating_add(1);
        self.last_clamped_head = 0;
        self.earliest_deadline = None;
        self.invalidate_inflight();
    }

    /// Cancel all in-flight fetches and issue a fresh token.
    fn invalidate_inflight(&mut self) {
        self.cancel.cancel();
        self.cancel = CancellationToken::new();
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    // --- Members ---

    pub fn add_member(&mut self, connection_client_id: String) -> String {
        let consumer_id = Uuid::new_v4().to_string();
        self.members.insert(consumer_id.clone(), connection_client_id);
        consumer_id
    }

    pub fn remove_member(&mut self, consumer_id: &str) -> Option<String> {
        if let Some(connection_client_id) = self.members.remove(consumer_id) {
            self.release_consumer(consumer_id);
            self.invalidate_inflight();
            return Some(connection_client_id);
        }
        None
    }

    pub fn is_member(&self, consumer_id: &str) -> bool {
        self.members.contains_key(consumer_id)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    // --- Internal ---

    /// Specifically registers messages retrieved from disk into the group's pending state.
    pub fn register_cold_messages(&mut self, consumer_id: &str, generation: u64, messages: Vec<Message>, head_seq: u64) -> Result<Vec<Message>, String> {
        self.ensure_active_consumer(consumer_id, generation)?;
        self.clamp_head(head_seq);

        let mut result = Vec::new();

        for msg in messages {
            if self.pending.len() >= self.max_ack_pending { break; }

            let seq = msg.seq;
            if seq < head_seq {
                continue;
            }

            // 1. Is it a redelivery?
            let mut is_redelivery = false;
            // Check if it's in our redeliver queue
            if let Some(pos) = self.redeliver.iter().position(|&s| s == seq) {
                self.redeliver.remove(pos);
                is_redelivery = true;
            }

            // 2. Is it a fresh message?
            let next_fresh_seq = self.next_deliver_seq.max(head_seq.max(1));
            if self.next_deliver_seq < head_seq {
                self.next_deliver_seq = head_seq;
                self.ack_floor = self.ack_floor.max(head_seq.saturating_sub(1));
            }
            let is_fresh = seq == next_fresh_seq;

            if is_redelivery || is_fresh {
                if is_fresh {
                    self.next_deliver_seq = seq + 1;
                }

                if let Some(msg) = self.issue_delivery(consumer_id, msg) {
                    result.push(msg);
                }
            }
        }

        Ok(result)
    }

    fn try_advance_floor(&mut self) {
        while self.ack_floor + 1 < self.next_deliver_seq
            && !self.pending.contains_key(&(self.ack_floor + 1))
            && !self.redeliver.iter().any(|seq| *seq == self.ack_floor + 1)
        {
            self.ack_floor += 1;
        }
    }

    fn read_from_log(log: &VecDeque<Message>, ram_start_seq: u64, seq: u64) -> Option<Message> {
        if seq < ram_start_seq || log.is_empty() {
            return None; // cold read needed — handled by caller
        }
        let idx = (seq - ram_start_seq) as usize;
        log.get(idx).cloned()
    }

    fn ensure_active_consumer(&self, consumer_id: &str, generation: u64) -> Result<(), String> {
        if generation != self.generation {
            return Err("FENCED".to_string());
        }
        if !self.members.contains_key(consumer_id) {
            return Err("NOT_MEMBER".to_string());
        }
        Ok(())
    }

    fn issue_delivery(&mut self, consumer_id: &str, msg: Message) -> Option<Message> {
        // If this key is poisoned (a previous message with same key was moved to DLT), move to DLT immediately
        if let Some(key) = &msg.key {
            if self.parked_keys.contains(key) {
                self.move_to_dlt(msg.seq, "auto-parked (poisoned key)".to_string(), 0, msg.key.clone());
                self.try_advance_floor();
                return None;
            }
        }

        let next_attempt = self.delivery_attempts.get(&msg.seq).copied().unwrap_or(0).saturating_add(1);
        if next_attempt > self.max_deliveries {
            self.park_msg(&msg);
            self.try_advance_floor();
            return None;
        }

        if let Some(key) = &msg.key {
            if let Some(&in_flight_seq) = self.keys_in_flight.get(key) {
                if in_flight_seq != msg.seq {
                    self.blocked_by_key.entry(key.clone()).or_default().push_back(msg.seq);
                    return None;
                }
                // Same seq: redelivery of the in-flight message — allow it through.
            } else {
                self.keys_in_flight.insert(key.clone(), msg.seq);
            }
        }

        let delivered_at = Instant::now();
        let deadline = delivered_at + self.ack_wait;
        self.earliest_deadline = Some(match self.earliest_deadline {
            Some(d) if d < deadline => d,
            _ => deadline,
        });
        self.delivery_attempts.insert(msg.seq, next_attempt);
        self.pending.insert(msg.seq, PendingMsg {
            consumer_id: consumer_id.to_string(),
            delivered_at,
            delivery_count: next_attempt,
            key: msg.key.clone(),
        });
        Some(msg)
    }

    fn move_to_dlt(&mut self, seq: u64, reason: String, attempts: u32, key: Option<Bytes>) {
        self.dlt.insert(seq, DltEntry { reason, attempts, key: key.clone() });
        self.delivery_attempts.remove(&seq);
        if let Some(k) = &key {
            self.keys_in_flight.remove(k);
            self.parked_keys.insert(k.clone());
            if let Some(blocked) = self.blocked_by_key.remove(k) {
                for blocked_seq in blocked {
                    self.dlt.insert(blocked_seq, DltEntry {
                        reason: "auto-parked (poisoned key)".to_string(),
                        attempts: 0,
                        key: key.clone(),
                    });
                    self.delivery_attempts.remove(&blocked_seq);
                }
            }
        }
    }

    fn park_msg(&mut self, msg: &Message) {
        let attempts = self.delivery_attempts.get(&msg.seq).copied().unwrap_or(0);
        self.move_to_dlt(msg.seq, format!("max_deliveries exceeded ({})", self.max_deliveries), attempts, msg.key.clone());
    }

    fn release_seq(&mut self, seq: u64, delivery_count: u32, key: Option<Bytes>) {
        if delivery_count >= self.max_deliveries {
            self.move_to_dlt(seq, format!("max_deliveries exceeded ({})", self.max_deliveries), delivery_count, key);
        } else {
            // Keep key in keys_in_flight to preserve per-key ordering during redelivery.
            // issue_delivery allows re-delivery of the same seq via keys_in_flight[key] == seq check.
            if !self.redeliver.iter().any(|queued| *queued == seq) {
                self.redeliver.push_back(seq);
            }
        }
    }

    fn release_consumer(&mut self, consumer_id: &str) {
        let seqs: Vec<(u64, u32, Option<Bytes>)> = self.pending.iter()
            .filter(|(_, msg)| msg.consumer_id == consumer_id)
            .map(|(seq, msg)| (*seq, msg.delivery_count, msg.key.clone()))
            .collect();

        for (seq, delivery_count, key) in seqs {
            self.pending.remove(&seq);
            self.release_seq(seq, delivery_count, key);
        }

        if self.pending.is_empty() {
            self.earliest_deadline = None;
        }
        self.try_advance_floor();
    }

    // --- DLT Operations ---

    pub fn peek_dlt(&self, limit: usize, offset: usize) -> Vec<(u64, DltEntry)> {
        let mut entries: Vec<(u64, DltEntry)> = self.dlt.iter().map(|(seq, e)| (*seq, e.clone())).collect();
        entries.sort_by_key(|(seq, _)| *seq);
        entries.into_iter().skip(offset).take(limit).collect()
    }

    /// Move a message from DLT back to redeliver queue. Returns true if a key was unblocked.
    pub fn move_to_stream(&mut self, seq: u64) -> Result<bool, String> {
        let entry = self.remove_dlt_entry(seq).ok_or("seq not in DLT")?;
        self.delivery_attempts.remove(&seq);
        if !self.redeliver.iter().any(|&s| s == seq) {
            let pos = self.redeliver.iter().position(|&s| s > seq).unwrap_or(self.redeliver.len());
            self.redeliver.insert(pos, seq);
        }
        let key_unblocked = entry.key.as_ref().map_or(false, |k| !self.parked_keys.contains(k));
        Ok(key_unblocked)
    }

    /// Remove a message from DLT permanently. Returns true if a key was unblocked.
    pub fn delete_dlt(&mut self, seq: u64) -> Result<bool, String> {
        let entry = self.remove_dlt_entry(seq).ok_or("seq not in DLT")?;
        let key_unblocked = entry.key.as_ref().map_or(false, |k| !self.parked_keys.contains(k));
        Ok(key_unblocked)
    }

    /// Clear all DLT entries and parked keys. Returns count of removed entries.
    pub fn purge_dlt(&mut self) -> usize {
        let count = self.dlt.len();
        self.dlt.clear();
        self.parked_keys.clear();
        count
    }

    fn remove_dlt_entry(&mut self, seq: u64) -> Option<DltEntry> {
        let entry = self.dlt.remove(&seq)?;
        let key_still_in_dlt = self.dlt.values().any(|e| e.key == entry.key);
        if !key_still_in_dlt {
            if let Some(key) = &entry.key {
                self.parked_keys.remove(key);
            }
        }
        Some(entry)
    }
}

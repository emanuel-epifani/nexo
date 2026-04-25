//! Consumer Group: JetStream-like per-message ack tracking
//!
//! Each group tracks:
//! - ack_floor: highest seq such that ALL seqs 1..=ack_floor are acked
//! - pending: messages delivered but not yet acked
//! - redeliver: messages that need to be redelivered (nack or timeout)

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::brokers::stream::domain::message::Message;

pub struct PendingMsg {
    pub consumer_id: String,
    pub delivered_at: Instant,
    pub delivery_count: u32,
}

struct GroupMember {
    connection_client_id: String,
}

pub struct ConsumerGroup {
    pub id: String,
    // === Persistent state (saved to disk) ===
    pub ack_floor: u64,
    // === Volatile state (rebuilt on restart) ===
    pub next_deliver_seq: u64,
    pub pending: HashMap<u64, PendingMsg>,
    pub redeliver: VecDeque<u64>,
    pub parked: HashSet<u64>,
    pub delivery_attempts: HashMap<u64, u32>,
    // === Config ===
    pub max_ack_pending: usize,
    pub ack_wait: Duration,
    pub max_deliveries: u32,
    // === Member tracking (for disconnect cleanup) ===
    members: HashMap<String, GroupMember>,
    // === Runtime State ===
    pub is_fetching_cold: bool,
    pub generation: u64,
    pub cancel: CancellationToken,
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
            parked: HashSet::new(),
            delivery_attempts: HashMap::new(),
            max_ack_pending,
            ack_wait,
            max_deliveries,
            members: HashMap::new(),
            is_fetching_cold: false,
            generation: 1,
            cancel: CancellationToken::new(),
        }
    }

    pub fn is_backpressured(&self) -> bool {
        self.pending.len() >= self.max_ack_pending
    }

    pub fn restore(id: String, ack_floor: u64, head_seq: u64, max_ack_pending: usize, ack_wait: Duration, max_deliveries: u32) -> Self {
        let normalized_floor = ack_floor.max(head_seq.saturating_sub(1));
        Self {
            id,
            ack_floor: normalized_floor,
            next_deliver_seq: normalized_floor.saturating_add(1).max(head_seq.max(1)),
            pending: HashMap::new(),
            redeliver: VecDeque::new(),
            parked: HashSet::new(),
            delivery_attempts: HashMap::new(),
            max_ack_pending,
            ack_wait,
            max_deliveries,
            members: HashMap::new(),
            is_fetching_cold: false,
            generation: 1,
            cancel: CancellationToken::new(),
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
                    self.parked.remove(&seq);
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
    pub fn ack(&mut self, consumer_id: &str, generation: u64, seq: u64) -> Result<(), String> {
        self.ensure_active_consumer(consumer_id, generation)?;

        match self.pending.get(&seq) {
            Some(msg) if msg.consumer_id == consumer_id => {}
            Some(_) => return Err("NOT_OWNER".to_string()),
            None => return Err(format!("seq {} not pending", seq)),
        }

        self.pending.remove(&seq);
        self.delivery_attempts.remove(&seq);
        self.parked.remove(&seq);
        self.try_advance_floor();
        Ok(())
    }

    /// Negative acknowledge: move message back to redeliver queue.
    pub fn check_redelivery(&mut self) -> bool {
        let now = Instant::now();
        let expired: Vec<u64> = self.pending.iter()
            .filter(|(_, msg)| now.duration_since(msg.delivered_at) > self.ack_wait)
            .map(|(seq, _)| *seq)
            .collect();

        if expired.is_empty() {
            return false;
        }

        for seq in expired {
            if let Some(msg) = self.pending.remove(&seq) {
                tracing::debug!("[Group:{}] Redelivery timeout seq={} (attempts={})", self.id, seq, msg.delivery_count);
                self.release_seq(seq, msg.delivery_count);
            }
        }

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

        let before_parked = self.parked.len();
        self.parked.retain(|seq| *seq >= head_seq);
        if self.parked.len() != before_parked {
            changed = true;
        }

        let before_attempts = self.delivery_attempts.len();
        self.delivery_attempts.retain(|seq, _| *seq >= head_seq);
        if self.delivery_attempts.len() != before_attempts {
            changed = true;
        }

        if changed {
            self.is_fetching_cold = false;
            self.try_advance_floor();
            self.invalidate_inflight();
        }

        changed
    }

    pub fn next_fetch_seq(&self, head_seq: u64) -> u64 {
        self.redeliver.front().copied().unwrap_or(self.next_deliver_seq).max(head_seq.max(1))
    }

    fn reset_runtime(&mut self) {
        self.pending.clear();
        self.redeliver.clear();
        self.parked.clear();
        self.delivery_attempts.clear();
        self.members.clear();
        self.is_fetching_cold = false;
        self.generation = self.generation.saturating_add(1);
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
        self.members.insert(consumer_id.clone(), GroupMember {
            connection_client_id,
        });
        consumer_id
    }

    pub fn remove_member(&mut self, consumer_id: &str) -> Option<String> {
        if let Some(member) = self.members.remove(consumer_id) {
            self.release_consumer(consumer_id);
            self.invalidate_inflight();
            return Some(member.connection_client_id);
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
        let next_attempt = self.delivery_attempts.get(&msg.seq).copied().unwrap_or(0).saturating_add(1);
        if next_attempt > self.max_deliveries {
            self.parked.insert(msg.seq);
            self.delivery_attempts.remove(&msg.seq);
            self.try_advance_floor();
            return None;
        }

        self.delivery_attempts.insert(msg.seq, next_attempt);
        self.pending.insert(msg.seq, PendingMsg {
            consumer_id: consumer_id.to_string(),
            delivered_at: Instant::now(),
            delivery_count: next_attempt,
        });
        Some(msg)
    }

    fn release_seq(&mut self, seq: u64, delivery_count: u32) {
        if delivery_count >= self.max_deliveries {
            self.parked.insert(seq);
            self.delivery_attempts.remove(&seq);
        } else if !self.redeliver.iter().any(|queued| *queued == seq) {
            self.redeliver.push_back(seq);
        }
    }

    fn release_consumer(&mut self, consumer_id: &str) {
        let seqs: Vec<(u64, u32)> = self.pending.iter()
            .filter(|(_, msg)| msg.consumer_id == consumer_id)
            .map(|(seq, msg)| (*seq, msg.delivery_count))
            .collect();

        for (seq, delivery_count) in seqs {
            self.pending.remove(&seq);
            self.release_seq(seq, delivery_count);
        }

        self.try_advance_floor();
    }
}

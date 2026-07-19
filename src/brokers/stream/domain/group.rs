//! Consumer Group: JetStream-like per-message ack tracking
//!
//! Each group tracks:
//! - ack_floor: highest seq such that ALL seqs 1..=ack_floor are acked
//! - pending: messages delivered but not yet acked
//! - redeliver: messages that need to be redelivered (nack or timeout)

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
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
    pub pending: BTreeMap<u64, PendingMsg>,
    pub redeliver: BTreeSet<u64>,
    pub dlt: BTreeMap<u64, DltEntry>,
    pub parked_keys: HashSet<Bytes>,
    pub delivery_attempts: BTreeMap<u64, u32>,
    pub keys_in_flight: HashMap<Bytes, u64>,
    pub blocked_by_key: HashMap<Bytes, BTreeSet<u64>>,
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
    deadlines: BTreeMap<Instant, BTreeSet<u64>>,
}

impl ConsumerGroup {
    pub fn new(id: String, head_seq: u64, max_ack_pending: usize, ack_wait: Duration, max_deliveries: u32) -> Self {
        let head_seq = head_seq.max(1);
        Self {
            id,
            ack_floor: head_seq.saturating_sub(1),
            next_deliver_seq: head_seq,
            pending: BTreeMap::new(),
            redeliver: BTreeSet::new(),
            dlt: BTreeMap::new(),
            parked_keys: HashSet::new(),
            delivery_attempts: BTreeMap::new(),
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
            deadlines: BTreeMap::new(),
        }
    }

    pub fn is_backpressured(&self) -> bool {
        self.pending.len() >= self.max_ack_pending
    }

    pub fn restore(id: String, ack_floor: u64, head_seq: u64, max_ack_pending: usize, ack_wait: Duration, max_deliveries: u32, dlt_entries: BTreeMap<u64, DltEntry>, parked_keys: HashSet<Bytes>) -> Self {
        let normalized_floor = ack_floor.max(head_seq.saturating_sub(1));
        Self {
            id,
            ack_floor: normalized_floor,
            next_deliver_seq: normalized_floor.saturating_add(1).max(head_seq.max(1)),
            pending: BTreeMap::new(),
            redeliver: BTreeSet::new(),
            dlt: dlt_entries,
            parked_keys,
            delivery_attempts: BTreeMap::new(),
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
            deadlines: BTreeMap::new(),
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
            if let Some(seq) = self.redeliver.pop_first() {
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
                    self.redeliver.insert(seq);
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

        self.remove_pending(seq);
        self.delivery_attempts.remove(&seq);

        let mut key_unblocked = false;
        if let Some(key) = &pending_msg.key {
            self.keys_in_flight.remove(key);
            if let Some(blocked) = self.blocked_by_key.get_mut(key) {
                if let Some(next_seq) = blocked.pop_first() {
                    self.redeliver.insert(next_seq);
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
        if self.deadlines.is_empty() {
            return false;
        }
        let now = Instant::now();
        if let Some((&deadline, _)) = self.deadlines.first_key_value() {
            if now < deadline {
                return false;
            }
        }

        let expired: Vec<u64> = self.deadlines
            .range(..=now)
            .flat_map(|(_, seqs)| seqs.iter().copied())
            .collect();

        if expired.is_empty() {
            return false;
        }

        for seq in expired {
            if let Some(msg) = self.remove_pending(seq) {
                tracing::debug!("[Group:{}] Redelivery timeout seq={} (attempts={})", self.id, seq, msg.delivery_count);
                self.release_seq(seq, msg.delivery_count, msg.key);
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

        let stale_pending: Vec<u64> = self.pending.range(..head_seq).map(|(seq, _)| *seq).collect();
        if !stale_pending.is_empty() {
            for seq in &stale_pending {
                self.remove_pending(*seq);
                self.delivery_attempts.remove(seq);
            }
            changed = true;
        }

        let kept_redeliver = self.redeliver.split_off(&head_seq);
        let removed_redeliver = self.redeliver.len();
        self.redeliver = kept_redeliver;
        if removed_redeliver > 0 {
            changed = true;
        }

        let before_dlt = self.dlt.len();
        let stale_dlt_keys: Vec<u64> = self.dlt.range(..head_seq).map(|(seq, _)| *seq).collect();
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

        let kept_attempts = self.delivery_attempts.split_off(&head_seq);
        let removed_attempts = self.delivery_attempts.len();
        self.delivery_attempts = kept_attempts;
        if removed_attempts > 0 {
            changed = true;
        }

        let before_keys = self.keys_in_flight.len();
        self.keys_in_flight.retain(|_, seq| *seq >= head_seq);
        if self.keys_in_flight.len() != before_keys {
            changed = true;
        }

        let before_blocked = self.blocked_by_key.values().map(|v| v.len()).sum::<usize>();
        self.blocked_by_key.retain(|_, blocked| {
            let kept = blocked.split_off(&head_seq);
            *blocked = kept;
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
        self.redeliver.first().copied().unwrap_or(self.next_deliver_seq).max(head_seq.max(1))
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
        self.deadlines.clear();
        self.invalidate_inflight();
    }

    /// Remove a seq from pending and deadlines atomically.
    fn remove_pending(&mut self, seq: u64) -> Option<PendingMsg> {
        let msg = self.pending.remove(&seq)?;
        let deadline = msg.delivered_at + self.ack_wait;
        if let Some(set) = self.deadlines.get_mut(&deadline) {
            set.remove(&seq);
            if set.is_empty() {
                self.deadlines.remove(&deadline);
            }
        }
        Some(msg)
    }

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
            if self.redeliver.remove(&seq) {
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
            && !self.redeliver.contains(&(self.ack_floor + 1))
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
                    self.blocked_by_key.entry(key.clone()).or_default().insert(msg.seq);
                    return None;
                }
                // Same seq: redelivery of the in-flight message — allow it through.
            } else {
                self.keys_in_flight.insert(key.clone(), msg.seq);
            }
        }

        let delivered_at = Instant::now();
        let deadline = delivered_at + self.ack_wait;
        self.deadlines.entry(deadline).or_default().insert(msg.seq);
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
            if !self.redeliver.contains(&seq) {
                self.redeliver.insert(seq);
            }
        }
    }

    fn release_consumer(&mut self, consumer_id: &str) {
        let seqs: Vec<(u64, u32, Option<Bytes>)> = self.pending.iter()
            .filter(|(_, msg)| msg.consumer_id == consumer_id)
            .map(|(seq, msg)| (*seq, msg.delivery_count, msg.key.clone()))
            .collect();

        for (seq, delivery_count, key) in seqs {
            self.remove_pending(seq);
            self.release_seq(seq, delivery_count, key);
        }

        self.try_advance_floor();
    }

    // --- DLT Operations ---

    pub fn peek_dlt(&self, limit: usize, offset: usize) -> Vec<(u64, DltEntry)> {
        self.dlt.iter().skip(offset).take(limit).map(|(seq, e)| (*seq, e.clone())).collect()
    }

    /// Move a message from DLT back to redeliver queue. Returns true if a key was unblocked.
    pub fn move_to_stream(&mut self, seq: u64) -> Result<bool, String> {
        let entry = self.remove_dlt_entry(seq).ok_or("seq not in DLT")?;
        self.delivery_attempts.remove(&seq);
        self.redeliver.insert(seq);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::brokers::stream::domain::message::Message;
    use bytes::Bytes;
    use std::collections::VecDeque;

    fn make_group(max_ack_pending: usize, ack_wait_ms: u64, max_deliveries: u32) -> ConsumerGroup {
        ConsumerGroup::new(
            "test-group".to_string(),
            1,
            max_ack_pending,
            Duration::from_millis(ack_wait_ms),
            max_deliveries,
        )
    }

    fn make_msg(seq: u64, key: Option<Bytes>) -> Message {
        Message {
            seq,
            timestamp: 0,
            key,
            payload: Bytes::from(format!("msg-{}", seq)),
        }
    }

    fn fill_log(count: usize) -> VecDeque<Message> {
        let mut log = VecDeque::new();
        for i in 1..=count {
            log.push_back(make_msg(i as u64, None));
        }
        log
    }

    // === Basic ack/floor tests ===

    #[test]
    fn test_ack_advances_floor_when_contiguous() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        let msgs = g.fetch(&consumer, gen, 3, &log, 1, 1).unwrap();
        assert_eq!(msgs.len(), 3);

        // ack 1, 2, 3 in order → floor should advance to 3
        g.ack(&consumer, gen, 1).unwrap();
        assert_eq!(g.ack_floor, 1);
        g.ack(&consumer, gen, 2).unwrap();
        assert_eq!(g.ack_floor, 2);
        g.ack(&consumer, gen, 3).unwrap();
        assert_eq!(g.ack_floor, 3);
    }

    #[test]
    fn test_ack_out_of_order_floor_stays_at_gap() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        let msgs = g.fetch(&consumer, gen, 3, &log, 1, 1).unwrap();
        assert_eq!(msgs.len(), 3);

        // ack 3 first → floor should NOT advance past 0 (1 and 2 still pending)
        g.ack(&consumer, gen, 3).unwrap();
        assert_eq!(g.ack_floor, 0);

        // ack 1 → floor advances to 1 (2 is still pending)
        g.ack(&consumer, gen, 1).unwrap();
        assert_eq!(g.ack_floor, 1);

        // ack 2 → floor advances to 3 (all acked)
        g.ack(&consumer, gen, 2).unwrap();
        assert_eq!(g.ack_floor, 3);
    }

    #[test]
    fn test_ack_wrong_consumer_returns_not_owner() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let c1 = g.add_member("conn1".to_string());
        let c2 = g.add_member("conn2".to_string());
        let gen = g.generation();

        g.fetch(&c1, gen, 1, &log, 1, 1).unwrap();

        let err = g.ack(&c2, gen, 1).unwrap_err();
        assert_eq!(err, "NOT_OWNER");
    }

    #[test]
    fn test_ack_not_pending_returns_error() {
        let mut g = make_group(100, 30000, 5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        let err = g.ack(&consumer, gen, 99).unwrap_err();
        assert!(err.contains("not pending"));
    }

    // === Backpressure tests ===

    #[test]
    fn test_backpressure_blocks_fetch() {
        let mut g = make_group(3, 30000, 5);
        let log = fill_log(10);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Fetch 3 (fills max_ack_pending)
        let msgs = g.fetch(&consumer, gen, 10, &log, 1, 1).unwrap();
        assert_eq!(msgs.len(), 3);
        assert!(g.is_backpressured());

        // Further fetch returns empty
        let msgs2 = g.fetch(&consumer, gen, 10, &log, 1, 1).unwrap();
        assert!(msgs2.is_empty());

        // Ack one → can fetch one more
        g.ack(&consumer, gen, 1).unwrap();
        let msgs3 = g.fetch(&consumer, gen, 10, &log, 1, 1).unwrap();
        assert_eq!(msgs3.len(), 1);
    }

    // === Redeliver tests ===

    #[test]
    fn test_redeliver_served_before_fresh() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(10);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Fetch and deliver seq 1, 2
        let msgs = g.fetch(&consumer, gen, 2, &log, 1, 1).unwrap();
        assert_eq!(msgs.len(), 2);

        // Simulate redelivery of seq 1 (e.g. from release_consumer)
        g.release_consumer(&consumer);
        assert!(g.redeliver.contains(&1));
        assert!(g.redeliver.contains(&2));

        // Next fetch should serve redelivered 1, 2 before fresh 3
        let msgs2 = g.fetch(&consumer, gen, 3, &log, 1, 1).unwrap();
        assert_eq!(msgs2[0].seq, 1);
        assert_eq!(msgs2[1].seq, 2);
        assert_eq!(msgs2[2].seq, 3);
    }

    #[test]
    fn test_redeliver_no_duplicates() {
        let mut g = make_group(100, 30000, 5);
        let consumer = g.add_member("conn1".to_string());

        // Manually insert into pending then release
        let now = Instant::now();
        g.pending.insert(1, PendingMsg {
            consumer_id: consumer.clone(),
            delivered_at: now,
            delivery_count: 1,
            key: None,
        });
        g.deadlines.entry(now + g.ack_wait).or_default().insert(1);

        g.release_consumer(&consumer);
        assert_eq!(g.redeliver.len(), 1);

        // release_seq again should not duplicate
        g.release_seq(1, 1, None);
        assert_eq!(g.redeliver.len(), 1);
    }

    #[test]
    fn test_move_to_stream_inserts_in_order() {
        let mut g = make_group(100, 30000, 5);

        // Populate DLT
        g.dlt.insert(5, DltEntry { reason: "test".to_string(), attempts: 1, key: None });
        g.dlt.insert(3, DltEntry { reason: "test".to_string(), attempts: 1, key: None });
        g.dlt.insert(7, DltEntry { reason: "test".to_string(), attempts: 1, key: None });

        // Move 5 back to stream
        g.move_to_stream(5).unwrap();
        // Move 3 back
        g.move_to_stream(3).unwrap();
        // Move 7 back
        g.move_to_stream(7).unwrap();

        // redeliver should be ordered: 3, 5, 7
        let seqs: Vec<u64> = g.redeliver.iter().copied().collect();
        assert_eq!(seqs, vec![3, 5, 7]);
    }

    // === check_redelivery / deadlines tests ===

    #[test]
    fn test_check_redelivery_no_expired() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 3, &log, 1, 1).unwrap();

        // Immediately check → nothing expired
        assert!(!g.check_redelivery());
        assert_eq!(g.pending.len(), 3);
    }

    #[test]
    fn test_check_redelivery_with_expired() {
        let mut g = make_group(100, 50, 5); // 50ms ack_wait
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 3, &log, 1, 1).unwrap();
        assert_eq!(g.pending.len(), 3);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(60));

        assert!(g.check_redelivery());
        // Messages should be moved to redeliver
        assert_eq!(g.pending.len(), 0);
        assert_eq!(g.redeliver.len(), 3);
    }

    #[test]
    fn test_check_redelivery_empty_pending_returns_false() {
        let mut g = make_group(100, 30000, 5);
        assert!(!g.check_redelivery());
    }

    #[test]
    fn test_deadlines_index_stays_in_sync() {
        let mut g = make_group(100, 50, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Deliver 3 messages
        g.fetch(&consumer, gen, 3, &log, 1, 1).unwrap();
        let total_in_deadlines: usize = g.deadlines.values().map(|s| s.len()).sum();
        assert_eq!(total_in_deadlines, 3);

        // Ack seq 2
        g.ack(&consumer, gen, 2).unwrap();
        let total_in_deadlines: usize = g.deadlines.values().map(|s| s.len()).sum();
        assert_eq!(total_in_deadlines, 2);

        // Ack remaining
        g.ack(&consumer, gen, 1).unwrap();
        g.ack(&consumer, gen, 3).unwrap();
        assert!(g.deadlines.is_empty());
    }

    // === clamp_head tests ===

    #[test]
    fn test_clamp_head_removes_stale_pending() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(10);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 5, &log, 1, 1).unwrap();
        assert_eq!(g.pending.len(), 5);

        // Clamp head to 4 → pending 1,2,3 should be removed
        g.clamp_head(4);
        assert!(!g.pending.contains_key(&1));
        assert!(!g.pending.contains_key(&2));
        assert!(!g.pending.contains_key(&3));
        assert!(g.pending.contains_key(&4));
        assert!(g.pending.contains_key(&5));
        assert_eq!(g.ack_floor, 3); // head-1
    }

    #[test]
    fn test_clamp_head_removes_stale_redeliver() {
        let mut g = make_group(100, 30000, 5);
        g.redeliver.insert(1);
        g.redeliver.insert(2);
        g.redeliver.insert(5);
        g.redeliver.insert(8);

        g.clamp_head(5);
        assert!(!g.redeliver.contains(&1));
        assert!(!g.redeliver.contains(&2));
        assert!(g.redeliver.contains(&5));
        assert!(g.redeliver.contains(&8));
    }

    #[test]
    fn test_clamp_head_removes_stale_dlt() {
        let mut g = make_group(100, 30000, 5);
        g.dlt.insert(1, DltEntry { reason: "test".to_string(), attempts: 1, key: None });
        g.dlt.insert(3, DltEntry { reason: "test".to_string(), attempts: 1, key: None });
        g.dlt.insert(7, DltEntry { reason: "test".to_string(), attempts: 1, key: None });

        g.clamp_head(5);
        assert!(!g.dlt.contains_key(&1));
        assert!(!g.dlt.contains_key(&3));
        assert!(g.dlt.contains_key(&7));
    }

    #[test]
    fn test_clamp_head_idempotent() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(10);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 3, &log, 1, 1).unwrap();
        let changed1 = g.clamp_head(3);
        let changed2 = g.clamp_head(3);
        assert!(changed1);
        assert!(!changed2);
    }

    #[test]
    fn test_clamp_head_advances_next_deliver_seq() {
        let mut g = make_group(100, 30000, 5);
        g.next_deliver_seq = 1;

        g.clamp_head(10);
        assert_eq!(g.next_deliver_seq, 10);
    }

    // === DLT tests ===

    #[test]
    fn test_peek_dlt_returns_sorted() {
        let mut g = make_group(100, 30000, 5);
        g.dlt.insert(5, DltEntry { reason: "r5".to_string(), attempts: 1, key: None });
        g.dlt.insert(1, DltEntry { reason: "r1".to_string(), attempts: 1, key: None });
        g.dlt.insert(3, DltEntry { reason: "r3".to_string(), attempts: 1, key: None });

        let entries = g.peek_dlt(10, 0);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[1].0, 3);
        assert_eq!(entries[2].0, 5);
    }

    #[test]
    fn test_peek_dlt_with_offset_and_limit() {
        let mut g = make_group(100, 30000, 5);
        for i in 1..=10 {
            g.dlt.insert(i, DltEntry { reason: format!("r{}", i), attempts: 1, key: None });
        }

        let page = g.peek_dlt(3, 2);
        assert_eq!(page.len(), 3);
        assert_eq!(page[0].0, 3);
        assert_eq!(page[1].0, 4);
        assert_eq!(page[2].0, 5);
    }

    #[test]
    fn test_purge_dlt_clears_all() {
        let mut g = make_group(100, 30000, 5);
        g.dlt.insert(1, DltEntry { reason: "r".to_string(), attempts: 1, key: None });
        g.dlt.insert(2, DltEntry { reason: "r".to_string(), attempts: 1, key: None });
        g.parked_keys.insert(Bytes::from("key1"));

        let count = g.purge_dlt();
        assert_eq!(count, 2);
        assert!(g.dlt.is_empty());
        assert!(g.parked_keys.is_empty());
    }

    #[test]
    fn test_delete_dlt_removes_entry() {
        let mut g = make_group(100, 30000, 5);
        g.dlt.insert(1, DltEntry { reason: "r".to_string(), attempts: 1, key: None });
        g.dlt.insert(2, DltEntry { reason: "r".to_string(), attempts: 1, key: None });

        assert!(g.delete_dlt(1).is_ok());
        assert!(!g.dlt.contains_key(&1));
        assert!(g.dlt.contains_key(&2));

        let err = g.delete_dlt(99).unwrap_err();
        assert_eq!(err, "seq not in DLT");
    }

    #[test]
    fn test_delete_dlt_unparks_key_when_no_more_entries() {
        let mut g = make_group(100, 30000, 5);
        let key = Bytes::from("key1");
        g.dlt.insert(1, DltEntry { reason: "r".to_string(), attempts: 1, key: Some(key.clone()) });
        g.parked_keys.insert(key.clone());

        // Delete the only entry with this key → key should be unparked
        let unblocked = g.delete_dlt(1).unwrap();
        assert!(unblocked);
        assert!(!g.parked_keys.contains(&key));
    }

    #[test]
    fn test_delete_dlt_keeps_key_when_other_entries_exist() {
        let mut g = make_group(100, 30000, 5);
        let key = Bytes::from("key1");
        g.dlt.insert(1, DltEntry { reason: "r".to_string(), attempts: 1, key: Some(key.clone()) });
        g.dlt.insert(2, DltEntry { reason: "r".to_string(), attempts: 1, key: Some(key.clone()) });
        g.parked_keys.insert(key.clone());

        let unblocked = g.delete_dlt(1).unwrap();
        assert!(!unblocked);
        assert!(g.parked_keys.contains(&key));
    }

    // === Per-key ordering tests ===

    #[test]
    fn test_per_key_ordering_blocks_second_message() {
        let mut g = make_group(100, 30000, 5);
        let key = Bytes::from("key-A");
        let log = VecDeque::from(vec![
            make_msg(1, Some(key.clone())),
            make_msg(2, Some(key.clone())),
        ]);

        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Fetch both — only first should be delivered, second blocked
        let msgs = g.fetch(&consumer, gen, 10, &log, 1, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq, 1);

        // seq 2 should be in blocked_by_key
        assert!(g.blocked_by_key.get(&key).is_some());
        assert!(g.blocked_by_key.get(&key).unwrap().contains(&2));
    }

    #[test]
    fn test_per_key_ordering_ack_unblocks() {
        let mut g = make_group(100, 30000, 5);
        let key = Bytes::from("key-A");
        let log = VecDeque::from(vec![
            make_msg(1, Some(key.clone())),
            make_msg(2, Some(key.clone())),
            make_msg(3, Some(key.clone())),
        ]);

        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 10, &log, 1, 1).unwrap();
        assert!(g.blocked_by_key.contains_key(&key));

        // Ack seq 1 → seq 2 should be unblocked into redeliver, seq 3 still blocked
        let key_unblocked = g.ack(&consumer, gen, 1).unwrap();
        assert!(key_unblocked);
        assert!(g.redeliver.contains(&2));
        // seq 3 is still blocked
        assert!(g.blocked_by_key.contains_key(&key));
        assert!(g.blocked_by_key.get(&key).unwrap().contains(&3));

        // Ack seq 2 → seq 3 should be unblocked
        // First re-fetch to deliver seq 2 from redeliver
        g.fetch(&consumer, gen, 10, &log, 1, 1).unwrap();
        let key_unblocked2 = g.ack(&consumer, gen, 2).unwrap();
        assert!(key_unblocked2);
        assert!(g.redeliver.contains(&3));
        assert!(!g.blocked_by_key.contains_key(&key));
    }

    // === Fencing / generation tests ===

    #[test]
    fn test_fenced_generation_rejected() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 1, &log, 1, 1).unwrap();

        // Reset runtime bumps generation
        g.seek_beginning(1);
        let new_gen = g.generation();
        assert_ne!(gen, new_gen);

        // Old generation should be rejected
        let err = g.ack(&consumer, gen, 1).unwrap_err();
        assert_eq!(err, "FENCED");
    }

    #[test]
    fn test_non_member_rejected() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let gen = g.generation();

        // Never added as member
        let err = g.fetch("fake-consumer", gen, 1, &log, 1, 1).unwrap_err();
        assert_eq!(err, "NOT_MEMBER");
    }

    // === release_consumer tests ===

    #[test]
    fn test_release_consumer_redelivers_pending() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let c1 = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&c1, gen, 3, &log, 1, 1).unwrap();
        assert_eq!(g.pending.len(), 3);

        // Remove member → pending should go to redeliver
        g.remove_member(&c1);
        assert_eq!(g.pending.len(), 0);
        assert_eq!(g.redeliver.len(), 3);
    }

    #[test]
    fn test_release_consumer_only_affects_that_consumer() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(10);
        let c1 = g.add_member("conn1".to_string());
        let c2 = g.add_member("conn2".to_string());
        let gen = g.generation();

        g.fetch(&c1, gen, 2, &log, 1, 1).unwrap();
        g.fetch(&c2, gen, 2, &log, 1, 1).unwrap();
        assert_eq!(g.pending.len(), 4);

        // Remove c1 → only c1's messages should be redelivered
        g.remove_member(&c1);
        assert_eq!(g.pending.len(), 2); // c2's messages still pending
        assert_eq!(g.redeliver.len(), 2); // c1's messages in redeliver
    }

    // === try_advance_floor edge cases ===

    #[test]
    fn test_floor_advances_past_redeliver_gap() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Deliver 1,2,3
        g.fetch(&consumer, gen, 3, &log, 1, 1).unwrap();

        // Ack 1 and 3 (gap at 2)
        g.ack(&consumer, gen, 1).unwrap();
        g.ack(&consumer, gen, 3).unwrap();
        assert_eq!(g.ack_floor, 1); // stuck at 1 because 2 is pending

        // Ack 2 → floor should jump to 3
        g.ack(&consumer, gen, 2).unwrap();
        assert_eq!(g.ack_floor, 3);
    }

    #[test]
    fn test_floor_does_not_advance_past_next_deliver_seq() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Deliver only 1
        g.fetch(&consumer, gen, 1, &log, 1, 1).unwrap();
        assert_eq!(g.next_deliver_seq, 2);

        // Ack 1 → floor should be 1, not higher (next_deliver_seq is 2)
        g.ack(&consumer, gen, 1).unwrap();
        assert_eq!(g.ack_floor, 1);
    }

    // === restore tests ===

    #[test]
    fn test_restore_preserves_dlt_and_parked_keys() {
        let mut dlt = BTreeMap::new();
        let key = Bytes::from("key1");
        dlt.insert(5, DltEntry { reason: "test".to_string(), attempts: 2, key: Some(key.clone()) });
        let mut parked = HashSet::new();
        parked.insert(key.clone());

        let g = ConsumerGroup::restore(
            "grp".to_string(),
            3,
            5,
            1000,
            Duration::from_millis(30000),
            5,
            dlt,
            parked,
        );

        assert_eq!(g.ack_floor, 4); // max(3, 5-1)
        assert_eq!(g.next_deliver_seq, 5);
        assert!(g.dlt.contains_key(&5));
        assert!(g.parked_keys.contains(&key));
        assert!(g.pending.is_empty());
        assert!(g.redeliver.is_empty());
        assert!(g.deadlines.is_empty());
    }

    // === Invariant: pending ↔ deadlines sync ===

    #[test]
    fn test_invariant_pending_deadlines_count_match() {
        let mut g = make_group(100, 50, 5);
        let log = fill_log(10);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Deliver 5 messages
        g.fetch(&consumer, gen, 5, &log, 1, 1).unwrap();
        let total_deadlines: usize = g.deadlines.values().map(|s| s.len()).sum();
        assert_eq!(g.pending.len(), total_deadlines);

        // Ack some
        g.ack(&consumer, gen, 2).unwrap();
        g.ack(&consumer, gen, 4).unwrap();
        let total_deadlines: usize = g.deadlines.values().map(|s| s.len()).sum();
        assert_eq!(g.pending.len(), total_deadlines);

        // Timeout redelivery
        std::thread::sleep(Duration::from_millis(60));
        g.check_redelivery();
        let total_deadlines: usize = g.deadlines.values().map(|s| s.len()).sum();
        assert_eq!(g.pending.len(), total_deadlines);
        assert_eq!(g.pending.len(), 0);
        assert_eq!(total_deadlines, 0);
    }

    // === Invariant: redeliver no duplicates ===

    #[test]
    fn test_invariant_redeliver_no_duplicates_after_clamp() {
        let mut g = make_group(100, 30000, 5);
        g.redeliver.insert(1);
        g.redeliver.insert(3);
        g.redeliver.insert(5);
        g.redeliver.insert(7);

        g.clamp_head(4);
        // All elements should be unique
        let seqs: Vec<u64> = g.redeliver.iter().copied().collect();
        let unique: std::collections::HashSet<u64> = seqs.iter().copied().collect();
        assert_eq!(seqs.len(), unique.len());
        assert!(seqs.iter().all(|s| *s >= 4));
    }

    // === next_fetch_seq tests ===

    #[test]
    fn test_next_fetch_seq_prefers_redeliver() {
        let mut g = make_group(100, 30000, 5);
        g.next_deliver_seq = 10;
        g.redeliver.insert(5);

        assert_eq!(g.next_fetch_seq(1), 5);
    }

    #[test]
    fn test_next_fetch_seq_uses_next_deliver_when_redeliver_empty() {
        let mut g = make_group(100, 30000, 5);
        g.next_deliver_seq = 10;

        assert_eq!(g.next_fetch_seq(1), 10);
    }

    #[test]
    fn test_next_fetch_seq_clamps_to_head() {
        let mut g = make_group(100, 30000, 5);
        g.next_deliver_seq = 1;

        assert_eq!(g.next_fetch_seq(5), 5);
    }
}

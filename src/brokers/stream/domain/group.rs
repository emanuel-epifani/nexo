//! Consumer Group: JetStream-like per-message ack tracking
//!
//! Each group tracks:
//! - ack_floor: highest seq such that ALL seqs 1..=ack_floor are acked
//! - msgs: unified state map (Pending / Redeliver / Dlt)
//! - keys: unified key state (in_flight / blocked / poisoned)

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};
use priority_queue::PriorityQueue;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::brokers::stream::domain::message::Message;

#[derive(Clone, Debug)]
pub struct DltEntry {
    pub reason: String,
    pub attempts: u32,
    pub key: Option<Bytes>,
}

/// Unified message state — Pending, Redeliver, or Dlt
#[derive(Clone, Debug)]
enum MsgState {
    Pending {
        consumer_id: String,
        delivery_count: u32,
        key: Option<Bytes>,
    },
    Redeliver {
        attempts: u32,
    },
    Dlt(DltEntry),
}

/// Unified key state — in_flight, blocked, or poisoned
#[derive(Default)]
struct KeyState {
    in_flight: Option<u64>,
    blocked: BTreeSet<u64>,
    poisoned: bool,
}

pub struct ConsumerGroup {
    pub id: String,
    // === Persistent state (saved to disk) ===
    pub ack_floor: u64,
    // === Volatile state (rebuilt on restart) ===
    pub next_deliver_seq: u64,
    msgs: BTreeMap<u64, MsgState>,
    redeliver_idx: BTreeSet<u64>, // O(1) index of Redeliver seqs — avoids O(N) scan of msgs
    keys: HashMap<Bytes, KeyState>,
    dlt_key_counts: HashMap<Bytes, usize>,
    pending_count: usize,
    // === Config ===
    pub max_ack_pending: usize,
    pub ack_wait: Duration,
    pub max_deliveries: u32,
    // === Member tracking (for disconnect cleanup) ===
    members: HashMap<String, String>, // consumer_id → connection_client_id
    // === Runtime State ===
    pub generation: u64,
    pub cancel: CancellationToken,
    last_clamped_head: u64,
    deadlines: PriorityQueue<u64, Reverse<Instant>>,
}

impl ConsumerGroup {
    pub fn new(id: String, head_seq: u64, max_ack_pending: usize, ack_wait: Duration, max_deliveries: u32) -> Self {
        let head_seq = head_seq.max(1);
        Self {
            id,
            ack_floor: head_seq.saturating_sub(1),
            next_deliver_seq: head_seq,
            msgs: BTreeMap::new(),
            redeliver_idx: BTreeSet::new(),
            keys: HashMap::new(),
            dlt_key_counts: HashMap::new(),
            pending_count: 0,
            max_ack_pending,
            ack_wait,
            max_deliveries,
            members: HashMap::new(),
            generation: 1,
            cancel: CancellationToken::new(),
            last_clamped_head: 0,
            deadlines: PriorityQueue::new(),
        }
    }

    pub fn is_backpressured(&self) -> bool {
        self.pending_count >= self.max_ack_pending
    }

    pub fn restore(id: String, ack_floor: u64, head_seq: u64, max_ack_pending: usize, ack_wait: Duration, max_deliveries: u32, dlt_entries: BTreeMap<u64, DltEntry>, parked_keys: HashSet<Bytes>, redeliver_entries: BTreeMap<u64, u32>) -> Self {
        let normalized_floor = ack_floor.max(head_seq.saturating_sub(1));
        let mut msgs = BTreeMap::new();
        let mut keys: HashMap<Bytes, KeyState> = HashMap::new();
        let mut dlt_key_counts = HashMap::new();
        let mut redeliver_idx = BTreeSet::new();
        for (seq, attempts) in redeliver_entries {
            if seq >= head_seq {
                msgs.insert(seq, MsgState::Redeliver { attempts });
                redeliver_idx.insert(seq);
            }
        }
        for (seq, entry) in &dlt_entries {
            msgs.insert(*seq, MsgState::Dlt(entry.clone()));
            redeliver_idx.remove(seq);
            if let Some(k) = &entry.key {
                keys.entry(k.clone()).or_default().poisoned = true;
                *dlt_key_counts.entry(k.clone()).or_insert(0) += 1;
            }
        }
        for k in &parked_keys {
            keys.entry(k.clone()).or_default().poisoned = true;
        }
        Self {
            id,
            ack_floor: normalized_floor,
            next_deliver_seq: normalized_floor.saturating_add(1).max(head_seq.max(1)),
            msgs,
            redeliver_idx,
            keys,
            dlt_key_counts,
            pending_count: 0,
            max_ack_pending,
            ack_wait,
            max_deliveries,
            members: HashMap::new(),
            generation: 1,
            cancel: CancellationToken::new(),
            last_clamped_head: 0,
            deadlines: PriorityQueue::new(),
        }
    }

    /// Fetch messages for a client. Serves redeliver queue first, then fresh messages.
    /// `messages` is a pre-read batch of messages from the log file (by the manager).
    pub fn fetch(&mut self, consumer_id: &str, generation: u64, limit: usize, messages: &[Message], head_seq: u64) -> Result<Vec<Message>, String> {
        self.ensure_active_consumer(consumer_id, generation)?;
        self.clamp_head(head_seq);

        if self.pending_count >= self.max_ack_pending {
            return Ok(vec![]);
        }

        let budget = limit.min(self.max_ack_pending - self.pending_count);
        let mut result = Vec::with_capacity(budget);

        // Messages from the log are sorted by seq. Use binary search for O(log n) lookup.
        let find_msg = |seq: u64| -> Option<&Message> {
            messages.binary_search_by_key(&seq, |m| m.seq).ok().map(|i| &messages[i])
        };

        // 1. Redeliver first — use O(k) index
        let redeliver_seqs: Vec<u64> = self.redeliver_idx.iter()
            .copied()
            .take(budget - result.len())
            .collect();

        for seq in redeliver_seqs {
            if seq < head_seq {
                self.msgs.remove(&seq);
                self.redeliver_idx.remove(&seq);
                continue;
            }
            if let Some(msg) = find_msg(seq) {
                if let Some(msg) = self.issue_delivery(consumer_id, msg.clone()) {
                    result.push(msg);
                }
            } else {
                break; // message not in this batch — leave as Redeliver, stop
            }
        }

        // 2. Fresh messages
        while result.len() < budget {
            let seq = self.next_deliver_seq.max(head_seq);
            if self.next_deliver_seq < head_seq {
                self.next_deliver_seq = head_seq;
                self.ack_floor = self.ack_floor.max(head_seq.saturating_sub(1));
            }

            if let Some(msg) = find_msg(seq) {
                self.next_deliver_seq = seq + 1;
                if let Some(msg) = self.issue_delivery(consumer_id, msg.clone()) {
                    result.push(msg);
                }
            } else {
                break; // no more messages in this batch
            }
        }

        Ok(result)
    }

    /// Acknowledge a message. Removes from pending and tries to advance ack_floor.
    pub fn ack(&mut self, consumer_id: &str, generation: u64, seq: u64) -> Result<bool, String> {
        self.ensure_active_consumer(consumer_id, generation)?;
        self.ack_pending(consumer_id, seq)
    }

    fn ack_pending(&mut self, consumer_id: &str, seq: u64) -> Result<bool, String> {
        let key = match self.msgs.get(&seq) {
            Some(MsgState::Pending { consumer_id: cid, key, .. }) if cid == consumer_id => key.clone(),
            Some(MsgState::Pending { .. }) => return Err("NOT_OWNER".to_string()),
            _ => return Err(format!("seq {} not pending", seq)),
        };

        self.remove_pending(seq);

        let mut key_unblocked = false;
        if let Some(k) = &key {
            let mut remove_key_state = false;
            if let Some(ks) = self.keys.get_mut(k) {
                ks.in_flight = None;
                if let Some(next_seq) = ks.blocked.pop_first() {
                    self.msgs.insert(next_seq, MsgState::Redeliver {
                        attempts: 0,
                    });
                    self.redeliver_idx.insert(next_seq);
                    key_unblocked = true;
                }
                remove_key_state = ks.in_flight.is_none() && ks.blocked.is_empty() && !ks.poisoned;
            }
            if remove_key_state {
                self.keys.remove(k);
            }
        }

        self.try_advance_floor();
        Ok(key_unblocked)
    }

    /// Move timed-out pending messages back to redeliver queue.
    pub fn check_redelivery(&mut self) -> bool {
        let now = Instant::now();
        let mut changed = false;
        while let Some((_, Reverse(d))) = self.deadlines.peek() {
            if *d > now { break; }
            let (seq, _) = self.deadlines.pop().unwrap();
            if let Some((delivery_count, key)) = self.remove_pending(seq) {
                tracing::debug!("[Group:{}] Redelivery timeout seq={} (attempts={})", self.id, seq, delivery_count);
                self.release_seq(seq, delivery_count, key);
                changed = true;
            }
        }
        if changed { self.try_advance_floor(); }
        changed
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

        // Remove all msgs below head_seq
        let stale: Vec<u64> = self.msgs.range(..head_seq).map(|(s, _)| *s).collect();
        for seq in &stale {
            if let Some(state) = self.msgs.remove(seq) {
                if let MsgState::Pending { .. } = &state {
                    self.deadlines.remove(seq);
                    self.pending_count -= 1;
                }
                if let MsgState::Dlt(entry) = &state {
                    if let Some(key) = &entry.key {
                        match self.dlt_key_counts.get_mut(key) {
                            Some(count) if *count > 1 => *count -= 1,
                            Some(_) => {
                                self.dlt_key_counts.remove(key);
                            }
                            None => {}
                        }
                    }
                }
                self.redeliver_idx.remove(seq);
                changed = true;
            }
        }

        // Clean up keys: remove in_flight and blocked below head_seq, un-poison stale keys
        let dlt_key_counts = &self.dlt_key_counts;

        self.keys.retain(|key, ks| {
            if let Some(seq) = ks.in_flight {
                if seq < head_seq { ks.in_flight = None; }
            }
            let valid = ks.blocked.split_off(&head_seq);
            if ks.blocked.len() > 0 { changed = true; }
            ks.blocked = valid;
            if ks.poisoned && !dlt_key_counts.contains_key(key) {
                ks.poisoned = false;
                changed = true;
            }
            ks.in_flight.is_some() || !ks.blocked.is_empty() || ks.poisoned
        });

        if changed {
            self.try_advance_floor();
            self.invalidate_inflight();
        }

        self.last_clamped_head = head_seq;
        changed
    }

    /// Compute which seqs need to be read from storage for a fetch operation.
    /// Returns redeliver seqs first, then fresh seqs, up to the budget.
    pub fn fetch_plan(&self, head_seq: u64, limit: usize) -> Vec<u64> {
        let budget = limit.min(self.max_ack_pending.saturating_sub(self.pending_count));
        if budget == 0 { return Vec::new(); }

        let mut seqs = Vec::with_capacity(budget);

        // Redeliver seqs (up to budget) — O(k) via index
        for &seq in self.redeliver_idx.iter() {
            if seqs.len() >= budget { break; }
            if seq >= head_seq {
                seqs.push(seq);
            }
        }

        // Fresh seqs (fill remaining budget)
        let fresh_start = self.next_deliver_seq.max(head_seq.max(1));
        let fresh_count = budget - seqs.len();
        for i in 0..fresh_count {
            seqs.push(fresh_start + i as u64);
        }

        seqs
    }

    fn reset_runtime(&mut self) {
        self.msgs.clear();
        self.redeliver_idx.clear();
        self.keys.clear();
        self.dlt_key_counts.clear();
        self.pending_count = 0;
        self.members.clear();
        self.generation = self.generation.saturating_add(1);
        self.last_clamped_head = 0;
        self.deadlines.clear();
        self.invalidate_inflight();
    }

    /// Remove a seq from pending and deadlines atomically. Returns (delivery_count, key).
    fn remove_pending(&mut self, seq: u64) -> Option<(u32, Option<Bytes>)> {
        let state = self.msgs.remove(&seq)?;
        match state {
            MsgState::Pending { delivery_count, key, .. } => {
                self.deadlines.remove(&seq);
                self.pending_count -= 1;
                Some((delivery_count, key))
            }
            other => {
                self.msgs.insert(seq, other);
                None
            }
        }
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

    // --- Accessors ---

    pub fn dlt_snapshot(&self) -> BTreeMap<u64, DltEntry> {
        self.msgs.iter()
            .filter_map(|(seq, state)| match state {
                MsgState::Dlt(e) => Some((*seq, e.clone())),
                _ => None,
            })
            .collect()
    }

    pub fn redeliver_snapshot(&self) -> BTreeMap<u64, u32> {
        self.msgs.iter()
            .filter_map(|(seq, state)| match state {
                MsgState::Redeliver { attempts } => Some((*seq, *attempts)),
                _ => None,
            })
            .collect()
    }

    pub fn parked_keys_snapshot(&self) -> HashSet<Bytes> {
        self.keys.iter()
            .filter(|(_, ks)| ks.poisoned)
            .map(|(k, _)| k.clone())
            .collect()
    }

    // --- Internal ---

    fn try_advance_floor(&mut self) {
        while self.ack_floor + 1 < self.next_deliver_seq {
            let next = self.ack_floor + 1;
            match self.msgs.get(&next) {
                Some(MsgState::Pending { .. }) | Some(MsgState::Redeliver { .. }) => break,
                _ => self.ack_floor += 1,
            }
        }
    }

    pub fn ensure_active_consumer(&self, consumer_id: &str, generation: u64) -> Result<(), String> {
        if generation != self.generation {
            return Err("FENCED".to_string());
        }
        if !self.members.contains_key(consumer_id) {
            return Err("NOT_MEMBER".to_string());
        }
        Ok(())
    }

    fn issue_delivery(&mut self, consumer_id: &str, msg: Message) -> Option<Message> {
        // Already in DLT — skip
        if matches!(self.msgs.get(&msg.seq), Some(MsgState::Dlt(_))) {
            return None;
        }

        // Check poisoned key + per-key ordering in a single lookup
        if let Some(key) = &msg.key {
            if let Some(ks) = self.keys.get_mut(key) {
                if ks.poisoned {
                    self.move_to_dlt(msg.seq, "auto-parked (poisoned key)".to_string(), 0, msg.key.clone());
                    self.try_advance_floor();
                    return None;
                }
                // Check in-flight (per-key ordering)
                if let Some(in_flight_seq) = ks.in_flight {
                    if in_flight_seq != msg.seq {
                        ks.blocked.insert(msg.seq);
                        return None;
                    }
                    // Same seq: redelivery of the in-flight message — allow it through.
                }
            }
        }

        // Check max deliveries
        let next_attempt = match self.msgs.get(&msg.seq) {
            Some(MsgState::Redeliver { attempts, .. }) => *attempts,
            Some(MsgState::Pending { delivery_count, .. }) => *delivery_count,
            _ => 0,
        }.saturating_add(1);

        if next_attempt > self.max_deliveries {
            self.park_msg(&msg);
            self.try_advance_floor();
            return None;
        }

        // Claim key in-flight
        if let Some(key) = &msg.key {
            self.keys.entry(key.clone()).or_default().in_flight = Some(msg.seq);
        }

        let now = Instant::now();
        self.deadlines.push(msg.seq, Reverse(now + self.ack_wait));
        self.redeliver_idx.remove(&msg.seq);
        self.msgs.insert(msg.seq, MsgState::Pending {
            consumer_id: consumer_id.to_string(),
            delivery_count: next_attempt,
            key: msg.key.clone(),
        });
        self.pending_count += 1;
        Some(msg)
    }

    fn move_to_dlt(&mut self, seq: u64, reason: String, attempts: u32, key: Option<Bytes>) {
        self.msgs.insert(seq, MsgState::Dlt(DltEntry { reason, attempts, key: key.clone() }));
        self.redeliver_idx.remove(&seq);

        if let Some(k) = &key {
            *self.dlt_key_counts.entry(k.clone()).or_insert(0) += 1;
            if let Some(ks) = self.keys.get_mut(k) {
                ks.in_flight = None;
                ks.poisoned = true;
                // Move all blocked messages for this key to DLT
                for blocked_seq in ks.blocked.iter() {
                    self.msgs.insert(*blocked_seq, MsgState::Dlt(DltEntry {
                        reason: "auto-parked (poisoned key)".to_string(),
                        attempts: 0,
                        key: key.clone(),
                    }));
                    *self.dlt_key_counts.entry(k.clone()).or_insert(0) += 1;
                }
                ks.blocked.clear();
            }
        }
    }

    fn park_msg(&mut self, msg: &Message) {
        let attempts = match self.msgs.get(&msg.seq) {
            Some(MsgState::Redeliver { attempts, .. }) => *attempts,
            Some(MsgState::Pending { delivery_count, .. }) => *delivery_count,
            _ => 0,
        };
        self.move_to_dlt(msg.seq, format!("max_deliveries exceeded ({})", self.max_deliveries), attempts, msg.key.clone());
    }

    fn release_seq(&mut self, seq: u64, delivery_count: u32, key: Option<Bytes>) {
        if delivery_count >= self.max_deliveries {
            self.move_to_dlt(seq, format!("max_deliveries exceeded ({})", self.max_deliveries), delivery_count, key);
        } else {
            // Keep key in-flight to preserve per-key ordering during redelivery.
            // issue_delivery allows re-delivery of the same seq via in_flight == seq check.
            self.msgs.insert(seq, MsgState::Redeliver {
                attempts: delivery_count,
            });
            self.redeliver_idx.insert(seq);
        }
    }

    fn release_consumer(&mut self, consumer_id: &str) {
        let seqs: Vec<(u64, u32, Option<Bytes>)> = self.msgs.iter()
            .filter_map(|(seq, state)| match state {
                MsgState::Pending { consumer_id: cid, delivery_count, key, .. } if cid == consumer_id =>
                    Some((*seq, *delivery_count, key.clone())),
                _ => None,
            })
            .collect();

        for (seq, delivery_count, key) in seqs {
            self.remove_pending(seq);
            self.release_seq(seq, delivery_count, key);
        }

        self.try_advance_floor();
    }

    // --- DLT Operations ---

    pub fn peek_dlt(&self, limit: usize, offset: usize) -> Vec<(u64, DltEntry)> {
        self.msgs.iter()
            .filter_map(|(seq, state)| match state {
                MsgState::Dlt(e) => Some((*seq, e.clone())),
                _ => None,
            })
            .skip(offset)
            .take(limit)
            .collect()
    }

    /// Move a message from DLT back to redeliver queue. Returns true if a key was unblocked.
    pub fn move_to_stream(&mut self, seq: u64) -> Result<bool, String> {
        let entry = match self.msgs.remove(&seq) {
            Some(MsgState::Dlt(e)) => e,
            _ => return Err("seq not in DLT".to_string()),
        };

        self.msgs.insert(seq, MsgState::Redeliver {
            attempts: 0,
        });
        self.redeliver_idx.insert(seq);

        let key_unblocked = self.release_dlt_key(&entry.key);
        Ok(key_unblocked)
    }

    /// Remove a message from DLT permanently. Returns true if a key was unblocked.
    pub fn delete_dlt(&mut self, seq: u64) -> Result<bool, String> {
        let entry = match self.msgs.remove(&seq) {
            Some(MsgState::Dlt(e)) => e,
            _ => return Err("seq not in DLT".to_string()),
        };

        let key_unblocked = self.release_dlt_key(&entry.key);
        Ok(key_unblocked)
    }

    /// Clear all DLT entries and parked keys. Returns count of removed entries.
    pub fn purge_dlt(&mut self) -> usize {
        let dlt_seqs: Vec<u64> = self.msgs.iter()
            .filter_map(|(seq, s)| if matches!(s, MsgState::Dlt(_)) { Some(*seq) } else { None })
            .collect();
        let count = dlt_seqs.len();
        for seq in dlt_seqs {
            self.msgs.remove(&seq);
        }
        self.dlt_key_counts.clear();
        for ks in self.keys.values_mut() {
            ks.poisoned = false;
        }
        self.keys.retain(|_, ks| ks.in_flight.is_some() || !ks.blocked.is_empty());
        count
    }

    /// Decrement a key's DLT references and un-poison it when the last entry is removed.
    fn release_dlt_key(&mut self, key: &Option<Bytes>) -> bool {
        let Some(k) = key else { return false };
        let no_dlt_entries = match self.dlt_key_counts.get_mut(k) {
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            Some(_) => {
                self.dlt_key_counts.remove(k);
                true
            }
            None => true,
        };
        if no_dlt_entries {
            let mut remove_key_state = false;
            let was_poisoned = if let Some(ks) = self.keys.get_mut(k) {
                let was_poisoned = ks.poisoned;
                ks.poisoned = false;
                remove_key_state = ks.in_flight.is_none() && ks.blocked.is_empty();
                was_poisoned
            } else {
                false
            };
            if remove_key_state {
                self.keys.remove(k);
            }
            return was_poisoned;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::brokers::stream::domain::message::Message;
    use bytes::Bytes;

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

    fn fill_log(count: usize) -> Vec<Message> {
        (1..=count).map(|i| make_msg(i as u64, None)).collect()
    }

    // Test helpers
    fn is_pending(g: &ConsumerGroup, seq: u64) -> bool {
        matches!(g.msgs.get(&seq), Some(MsgState::Pending { .. }))
    }
    fn is_redeliver(g: &ConsumerGroup, seq: u64) -> bool {
        matches!(g.msgs.get(&seq), Some(MsgState::Redeliver { .. }))
    }
    fn is_dlt(g: &ConsumerGroup, seq: u64) -> bool {
        matches!(g.msgs.get(&seq), Some(MsgState::Dlt(_)))
    }
    fn pending_count(g: &ConsumerGroup) -> usize {
        g.msgs.values().filter(|s| matches!(s, MsgState::Pending { .. })).count()
    }
    fn redeliver_count(g: &ConsumerGroup) -> usize {
        g.redeliver_idx.len()
    }
    fn dlt_count(g: &ConsumerGroup) -> usize {
        g.msgs.values().filter(|s| matches!(s, MsgState::Dlt(_))).count()
    }
    fn redeliver_seqs(g: &ConsumerGroup) -> Vec<u64> {
        g.redeliver_idx.iter().copied().collect()
    }
    fn is_key_poisoned(g: &ConsumerGroup, key: &Bytes) -> bool {
        g.keys.get(key).map_or(false, |ks| ks.poisoned)
    }
    fn is_key_blocked(g: &ConsumerGroup, key: &Bytes, seq: u64) -> bool {
        g.keys.get(key).map_or(false, |ks| ks.blocked.contains(&seq))
    }
    fn has_blocked_key(g: &ConsumerGroup, key: &Bytes) -> bool {
        g.keys.get(key).map_or(false, |ks| !ks.blocked.is_empty())
    }
    fn insert_redeliver(g: &mut ConsumerGroup, seq: u64) {
        g.msgs.insert(seq, MsgState::Redeliver { attempts: 0 });
        g.redeliver_idx.insert(seq);
    }
    fn insert_dlt(g: &mut ConsumerGroup, seq: u64, entry: DltEntry) {
        if let Some(k) = &entry.key {
            g.keys.entry(k.clone()).or_default().poisoned = true;
            *g.dlt_key_counts.entry(k.clone()).or_insert(0) += 1;
        }
        g.msgs.insert(seq, MsgState::Dlt(entry));
    }
    fn insert_parked_key(g: &mut ConsumerGroup, key: Bytes) {
        g.keys.entry(key).or_default().poisoned = true;
    }
    fn insert_pending(g: &mut ConsumerGroup, seq: u64, consumer_id: String, delivered_at: Instant, delivery_count: u32, key: Option<Bytes>) {
        g.deadlines.push(seq, Reverse(delivered_at + g.ack_wait));
        g.msgs.insert(seq, MsgState::Pending { consumer_id, delivery_count, key });
        g.pending_count += 1;
    }

    // === Basic ack/floor tests ===

    #[test]
    fn test_ack_advances_floor_when_contiguous() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        let msgs = g.fetch(&consumer, gen, 3, &log, 1).unwrap();
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
    fn test_ack_removes_idle_key_state() {
        let mut group = make_group(100, 30000, 5);
        let key = Bytes::from("one-shot-key");
        let log = vec![make_msg(1, Some(key.clone()))];
        let consumer = group.add_member("conn1".to_string());
        let generation = group.generation();

        group.fetch(&consumer, generation, 1, &log, 1).unwrap();
        assert!(group.keys.contains_key(&key));

        group.ack(&consumer, generation, 1).unwrap();

        assert!(!group.keys.contains_key(&key));
    }

    #[test]
    fn test_ack_out_of_order_floor_stays_at_gap() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        let msgs = g.fetch(&consumer, gen, 3, &log, 1).unwrap();
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

        g.fetch(&c1, gen, 1, &log, 1).unwrap();

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
        let msgs = g.fetch(&consumer, gen, 10, &log, 1).unwrap();
        assert_eq!(msgs.len(), 3);
        assert!(g.is_backpressured());

        // Further fetch returns empty
        let msgs2 = g.fetch(&consumer, gen, 10, &log, 1).unwrap();
        assert!(msgs2.is_empty());

        // Ack one → can fetch one more
        g.ack(&consumer, gen, 1).unwrap();
        let msgs3 = g.fetch(&consumer, gen, 10, &log, 1).unwrap();
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
        let msgs = g.fetch(&consumer, gen, 2, &log, 1).unwrap();
        assert_eq!(msgs.len(), 2);

        // Simulate redelivery of seq 1 (e.g. from release_consumer)
        g.release_consumer(&consumer);
        assert!(is_redeliver(&g, 1));
        assert!(is_redeliver(&g, 2));

        // Next fetch should serve redelivered 1, 2 before fresh 3
        let msgs2 = g.fetch(&consumer, gen, 3, &log, 1).unwrap();
        assert_eq!(msgs2[0].seq, 1);
        assert_eq!(msgs2[1].seq, 2);
        assert_eq!(msgs2[2].seq, 3);
    }

    #[test]
    fn test_redeliver_no_duplicates() {
        let mut g = make_group(100, 30000, 5);
        let consumer = g.add_member("conn1".to_string());

        let now = Instant::now();
        insert_pending(&mut g, 1, consumer.clone(), now, 1, None);

        g.release_consumer(&consumer);
        assert_eq!(redeliver_count(&g), 1);

        // release_seq again should not duplicate
        g.release_seq(1, 1, None);
        assert_eq!(redeliver_count(&g), 1);
    }

    #[test]
    fn test_move_to_stream_inserts_in_order() {
        let mut g = make_group(100, 30000, 5);

        insert_dlt(&mut g, 5, DltEntry { reason: "test".to_string(), attempts: 1, key: None });
        insert_dlt(&mut g, 3, DltEntry { reason: "test".to_string(), attempts: 1, key: None });
        insert_dlt(&mut g, 7, DltEntry { reason: "test".to_string(), attempts: 1, key: None });

        g.move_to_stream(5).unwrap();
        g.move_to_stream(3).unwrap();
        g.move_to_stream(7).unwrap();

        let seqs = redeliver_seqs(&g);
        assert_eq!(seqs, vec![3, 5, 7]);
    }

    // === check_redelivery / deadlines tests ===

    #[test]
    fn test_check_redelivery_no_expired() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 3, &log, 1).unwrap();

        // Immediately check → nothing expired
        assert!(!g.check_redelivery());
        assert_eq!(pending_count(&g), 3);
    }

    #[test]
    fn test_check_redelivery_with_expired() {
        let mut g = make_group(100, 50, 5); // 50ms ack_wait
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 3, &log, 1).unwrap();
        assert_eq!(pending_count(&g), 3);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(60));

        assert!(g.check_redelivery());
        // Messages should be moved to redeliver
        assert_eq!(pending_count(&g), 0);
        assert_eq!(redeliver_count(&g), 3);
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
        g.fetch(&consumer, gen, 3, &log, 1).unwrap();
        let total_in_deadlines: usize = g.deadlines.len();
        assert_eq!(total_in_deadlines, 3);

        // Ack seq 2
        g.ack(&consumer, gen, 2).unwrap();
        let total_in_deadlines: usize = g.deadlines.len();
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

        g.fetch(&consumer, gen, 5, &log, 1).unwrap();
        assert_eq!(pending_count(&g), 5);

        // Clamp head to 4 → pending 1,2,3 should be removed
        g.clamp_head(4);
        assert!(!is_pending(&g, 1));
        assert!(!is_pending(&g, 2));
        assert!(!is_pending(&g, 3));
        assert!(is_pending(&g, 4));
        assert!(is_pending(&g, 5));
        assert_eq!(g.ack_floor, 3); // head-1
    }

    #[test]
    fn test_clamp_head_removes_stale_redeliver() {
        let mut g = make_group(100, 30000, 5);
        insert_redeliver(&mut g, 1);
        insert_redeliver(&mut g, 2);
        insert_redeliver(&mut g, 5);
        insert_redeliver(&mut g, 8);

        g.clamp_head(5);
        assert!(!is_redeliver(&g, 1));
        assert!(!is_redeliver(&g, 2));
        assert!(is_redeliver(&g, 5));
        assert!(is_redeliver(&g, 8));
    }

    #[test]
    fn test_clamp_head_removes_stale_dlt() {
        let mut g = make_group(100, 30000, 5);
        insert_dlt(&mut g, 1, DltEntry { reason: "test".to_string(), attempts: 1, key: None });
        insert_dlt(&mut g, 3, DltEntry { reason: "test".to_string(), attempts: 1, key: None });
        insert_dlt(&mut g, 7, DltEntry { reason: "test".to_string(), attempts: 1, key: None });

        g.clamp_head(5);
        assert!(!is_dlt(&g, 1));
        assert!(!is_dlt(&g, 3));
        assert!(is_dlt(&g, 7));
    }

    #[test]
    fn test_clamp_head_idempotent() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(10);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 3, &log, 1).unwrap();
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
        insert_dlt(&mut g, 5, DltEntry { reason: "r5".to_string(), attempts: 1, key: None });
        insert_dlt(&mut g, 1, DltEntry { reason: "r1".to_string(), attempts: 1, key: None });
        insert_dlt(&mut g, 3, DltEntry { reason: "r3".to_string(), attempts: 1, key: None });

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
            insert_dlt(&mut g, i, DltEntry { reason: format!("r{}", i), attempts: 1, key: None });
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
        insert_dlt(&mut g, 1, DltEntry { reason: "r".to_string(), attempts: 1, key: None });
        insert_dlt(&mut g, 2, DltEntry { reason: "r".to_string(), attempts: 1, key: None });
        insert_parked_key(&mut g, Bytes::from("key1"));

        let count = g.purge_dlt();
        assert_eq!(count, 2);
        assert_eq!(dlt_count(&g), 0);
        assert!(!g.keys.values().any(|ks| ks.poisoned));
    }

    #[test]
    fn test_delete_dlt_removes_entry() {
        let mut g = make_group(100, 30000, 5);
        insert_dlt(&mut g, 1, DltEntry { reason: "r".to_string(), attempts: 1, key: None });
        insert_dlt(&mut g, 2, DltEntry { reason: "r".to_string(), attempts: 1, key: None });

        assert!(g.delete_dlt(1).is_ok());
        assert!(!is_dlt(&g, 1));
        assert!(is_dlt(&g, 2));

        let err = g.delete_dlt(99).unwrap_err();
        assert_eq!(err, "seq not in DLT");
    }

    #[test]
    fn test_delete_dlt_unparks_key_when_no_more_entries() {
        let mut g = make_group(100, 30000, 5);
        let key = Bytes::from("key1");
        insert_dlt(&mut g, 1, DltEntry { reason: "r".to_string(), attempts: 1, key: Some(key.clone()) });
        insert_parked_key(&mut g, key.clone());

        // Delete the only entry with this key → key should be unparked
        let unblocked = g.delete_dlt(1).unwrap();
        assert!(unblocked);
        assert!(!is_key_poisoned(&g, &key));
    }

    #[test]
    fn test_delete_dlt_keeps_key_when_other_entries_exist() {
        let mut g = make_group(100, 30000, 5);
        let key = Bytes::from("key1");
        insert_dlt(&mut g, 1, DltEntry { reason: "r".to_string(), attempts: 1, key: Some(key.clone()) });
        insert_dlt(&mut g, 2, DltEntry { reason: "r".to_string(), attempts: 1, key: Some(key.clone()) });
        insert_parked_key(&mut g, key.clone());

        let unblocked = g.delete_dlt(1).unwrap();
        assert!(!unblocked);
        assert!(is_key_poisoned(&g, &key));
    }

    // === Per-key ordering tests ===

    #[test]
    fn test_per_key_ordering_blocks_second_message() {
        let mut g = make_group(100, 30000, 5);
        let key = Bytes::from("key-A");
        let log = vec![
            make_msg(1, Some(key.clone())),
            make_msg(2, Some(key.clone())),
        ];

        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Fetch both — only first should be delivered, second blocked
        let msgs = g.fetch(&consumer, gen, 10, &log, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq, 1);

        // seq 2 should be in blocked_by_key
        assert!(is_key_blocked(&g, &key, 2));
    }

    #[test]
    fn test_per_key_ordering_ack_unblocks() {
        let mut g = make_group(100, 30000, 5);
        let key = Bytes::from("key-A");
        let log = vec![
            make_msg(1, Some(key.clone())),
            make_msg(2, Some(key.clone())),
            make_msg(3, Some(key.clone())),
        ];

        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 10, &log, 1).unwrap();
        assert!(has_blocked_key(&g, &key));

        // Ack seq 1 → seq 2 should be unblocked into redeliver, seq 3 still blocked
        let key_unblocked = g.ack(&consumer, gen, 1).unwrap();
        assert!(key_unblocked);
        assert!(is_redeliver(&g, 2));
        // seq 3 is still blocked
        assert!(is_key_blocked(&g, &key, 3));

        // Ack seq 2 → seq 3 should be unblocked
        // First re-fetch to deliver seq 2 from redeliver
        g.fetch(&consumer, gen, 10, &log, 1).unwrap();
        let key_unblocked2 = g.ack(&consumer, gen, 2).unwrap();
        assert!(key_unblocked2);
        assert!(is_redeliver(&g, 3));
        assert!(!has_blocked_key(&g, &key));
    }

    // === Fencing / generation tests ===

    #[test]
    fn test_fenced_generation_rejected() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&consumer, gen, 1, &log, 1).unwrap();

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
        let err = g.fetch("fake-consumer", gen, 1, &log, 1).unwrap_err();
        assert_eq!(err, "NOT_MEMBER");
    }

    // === release_consumer tests ===

    #[test]
    fn test_release_consumer_redelivers_pending() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let c1 = g.add_member("conn1".to_string());
        let gen = g.generation();

        g.fetch(&c1, gen, 3, &log, 1).unwrap();
        assert_eq!(pending_count(&g), 3);

        // Remove member → pending should go to redeliver
        g.remove_member(&c1);
        assert_eq!(pending_count(&g), 0);
        assert_eq!(redeliver_count(&g), 3);
    }

    #[test]
    fn test_release_consumer_only_affects_that_consumer() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(10);
        let c1 = g.add_member("conn1".to_string());
        let c2 = g.add_member("conn2".to_string());
        let gen = g.generation();

        g.fetch(&c1, gen, 2, &log, 1).unwrap();
        g.fetch(&c2, gen, 2, &log, 1).unwrap();
        assert_eq!(pending_count(&g), 4);

        // Remove c1 → only c1's messages should be redelivered
        g.remove_member(&c1);
        assert_eq!(pending_count(&g), 2); // c2's messages still pending
        assert_eq!(redeliver_count(&g), 2); // c1's messages in redeliver
    }

    // === try_advance_floor edge cases ===

    #[test]
    fn test_floor_advances_past_redeliver_gap() {
        let mut g = make_group(100, 30000, 5);
        let log = fill_log(5);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Deliver 1,2,3
        g.fetch(&consumer, gen, 3, &log, 1).unwrap();

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
        g.fetch(&consumer, gen, 1, &log, 1).unwrap();
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
            BTreeMap::new(),
        );

        assert_eq!(g.ack_floor, 4); // max(3, 5-1)
        assert_eq!(g.next_deliver_seq, 5);
        assert!(is_dlt(&g, 5));
        assert!(is_key_poisoned(&g, &key));
        assert_eq!(pending_count(&g), 0);
        assert_eq!(redeliver_count(&g), 0);
        assert!(g.deadlines.is_empty());
    }

    #[test]
    fn test_per_key_dlt_restore_seek_lifecycle() {
        let key_a = Bytes::from("key-A");
        let key_b = Bytes::from("key-B");
        let mut log = vec![
            make_msg(1, Some(key_a.clone())),
            make_msg(2, Some(key_b.clone())),
            make_msg(3, Some(key_a.clone())),
            make_msg(4, None),
            make_msg(5, Some(key_b.clone())),
            make_msg(6, Some(key_a.clone())),
        ];
        let mut group = make_group(100, 0, 1);
        let consumer = group.add_member("conn-before-restart".to_string());
        let generation = group.generation();

        let initial = group.fetch(&consumer, generation, 10, &log, 1).unwrap();
        assert_eq!(initial.iter().map(|message| message.seq).collect::<Vec<_>>(), vec![1, 2, 4]);
        group.ack(&consumer, generation, 2).unwrap();
        group.ack(&consumer, generation, 4).unwrap();

        assert!(group.check_redelivery());
        assert_eq!(group.peek_dlt(10, 0).into_iter().map(|(seq, _)| seq).collect::<Vec<_>>(), vec![1, 3, 6]);
        assert!(is_key_poisoned(&group, &key_a));
        assert!(!is_key_poisoned(&group, &key_b));

        let key_b_followup = group.fetch(&consumer, generation, 10, &log, 1).unwrap();
        assert_eq!(key_b_followup.iter().map(|message| message.seq).collect::<Vec<_>>(), vec![5]);
        group.ack(&consumer, generation, 5).unwrap();

        let mut group = ConsumerGroup::restore(
            "test-group".to_string(),
            group.ack_floor,
            1,
            group.max_ack_pending,
            group.ack_wait,
            group.max_deliveries,
            group.dlt_snapshot(),
            group.parked_keys_snapshot(),
            group.redeliver_snapshot(),
        );
        assert_eq!(group.ack_floor, 6);
        assert_eq!(dlt_count(&group), 3);
        assert!(is_key_poisoned(&group, &key_a));

        log.push(make_msg(7, Some(key_a.clone())));
        log.push(make_msg(8, Some(key_b.clone())));
        log.push(make_msg(9, None));
        let consumer = group.add_member("conn-after-restart".to_string());
        let generation = group.generation();
        let after_restore = group.fetch(&consumer, generation, 10, &log, 1).unwrap();
        assert_eq!(after_restore.iter().map(|message| message.seq).collect::<Vec<_>>(), vec![8, 9]);
        assert_eq!(dlt_count(&group), 4);
        assert!(is_dlt(&group, 7));
        group.ack(&consumer, generation, 8).unwrap();
        group.ack(&consumer, generation, 9).unwrap();

        group.seek_beginning(1);
        assert_eq!(dlt_count(&group), 0);
        assert!(!is_key_poisoned(&group, &key_a));
        assert_eq!(group.ack_floor, 0);

        let consumer = group.add_member("conn-after-seek".to_string());
        let generation = group.generation();
        let after_seek = group.fetch(&consumer, generation, 10, &log, 1).unwrap();
        assert_eq!(after_seek.iter().map(|message| message.seq).collect::<Vec<_>>(), vec![1, 2, 4, 9]);

        let mut delivered_key_a = 1;
        for expected_seq in [3, 6, 7] {
            group.ack(&consumer, generation, delivered_key_a).unwrap();
            let next = group.fetch(&consumer, generation, 10, &log, 1).unwrap();
            assert_eq!(next.iter().map(|message| message.seq).collect::<Vec<_>>(), vec![expected_seq]);
            delivered_key_a = expected_seq;
        }
        assert_eq!(dlt_count(&group), 0);
    }

    // === Invariant: pending ↔ deadlines sync ===

    #[test]
    fn test_invariant_pending_deadlines_count_match() {
        let mut g = make_group(100, 50, 5);
        let log = fill_log(10);
        let consumer = g.add_member("conn1".to_string());
        let gen = g.generation();

        // Deliver 5 messages
        g.fetch(&consumer, gen, 5, &log, 1).unwrap();
        let total_deadlines: usize = g.deadlines.len();
        assert_eq!(pending_count(&g), total_deadlines);

        // Ack some
        g.ack(&consumer, gen, 2).unwrap();
        g.ack(&consumer, gen, 4).unwrap();
        let total_deadlines: usize = g.deadlines.len();
        assert_eq!(pending_count(&g), total_deadlines);

        // Timeout redelivery
        std::thread::sleep(Duration::from_millis(60));
        g.check_redelivery();
        let total_deadlines: usize = g.deadlines.len();
        assert_eq!(pending_count(&g), total_deadlines);
        assert_eq!(pending_count(&g), 0);
        assert_eq!(total_deadlines, 0);
    }

    // === Invariant: redeliver no duplicates ===

    #[test]
    fn test_invariant_redeliver_no_duplicates_after_clamp() {
        let mut g = make_group(100, 30000, 5);
        insert_redeliver(&mut g, 1);
        insert_redeliver(&mut g, 3);
        insert_redeliver(&mut g, 5);
        insert_redeliver(&mut g, 7);

        g.clamp_head(4);
        // All elements should be unique
        let seqs = redeliver_seqs(&g);
        let unique: std::collections::HashSet<u64> = seqs.iter().copied().collect();
        assert_eq!(seqs.len(), unique.len());
        assert!(seqs.iter().all(|s| *s >= 4));
    }

    // === fetch_plan tests ===

    #[test]
    fn test_fetch_plan_prefers_redeliver() {
        let mut g = make_group(100, 30000, 5);
        g.next_deliver_seq = 10;
        insert_redeliver(&mut g, 5);

        let plan = g.fetch_plan(1, 10);
        assert_eq!(plan[0], 5); // redeliver first
        assert_eq!(plan[1], 10); // then fresh
    }

    #[test]
    fn test_fetch_plan_uses_next_deliver_when_redeliver_empty() {
        let mut g = make_group(100, 30000, 5);
        g.next_deliver_seq = 10;

        let plan = g.fetch_plan(1, 3);
        assert_eq!(plan, vec![10, 11, 12]);
    }

    #[test]
    fn test_fetch_plan_clamps_to_head() {
        let mut g = make_group(100, 30000, 5);
        g.next_deliver_seq = 1;

        let plan = g.fetch_plan(5, 3);
        assert_eq!(plan, vec![5, 6, 7]);
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use crate::brokers::stream::domain::message::Message;
    use bytes::Bytes;
    use std::time::Instant;

    fn make_msg(seq: u64, key: Option<Bytes>) -> Message {
        Message { seq, timestamp: 0, key, payload: Bytes::from(format!("msg-{}", seq)) }
    }
    fn fill_log(count: usize) -> Vec<Message> {
        (1..=count).map(|i| make_msg(i as u64, None)).collect()
    }
    fn fill_log_keyed(count: usize, key: Bytes) -> Vec<Message> {
        (1..=count).map(|i| make_msg(i as u64, Some(key.clone()))).collect()
    }

    #[test]
    fn bench_fetch_and_ack_1000() {
        let n = 1000;
        let log = fill_log(n);
        let mut g = ConsumerGroup::new("bench".into(), 1, 10000, Duration::from_secs(60), 5);
        let consumer = g.add_member("conn".into());
        let gen = g.generation();

        let start = Instant::now();
        for i in (0..n).step_by(100) {
            let batch = &log[i..(i + 100).min(n)];
            g.fetch(&consumer, gen, 100, batch, 1).unwrap();
        }
        let fetch_dur = start.elapsed();

        let start = Instant::now();
        for seq in 1..=n as u64 {
            g.ack(&consumer, gen, seq).unwrap();
        }
        let ack_dur = start.elapsed();

        println!("\n=== bench_fetch_and_ack_1000 ===");
        println!("fetch {n} msgs (batch=100): {:?}", fetch_dur);
        println!("ack  {n} msgs: {:?}", ack_dur);
        println!("total: {:?}", fetch_dur + ack_dur);
    }

    #[test]
    fn bench_fetch_with_redeliver_500() {
        let n = 500;
        let log = fill_log(n);
        let mut g = ConsumerGroup::new("bench".into(), 1, 10000, Duration::from_secs(60), 5);
        let consumer = g.add_member("conn".into());
        let gen = g.generation();

        g.fetch(&consumer, gen, n, &log, 1).unwrap();
        g.remove_member(&consumer);

        let consumer = g.add_member("conn".into());
        let gen = g.generation();

        let start = Instant::now();
        g.fetch(&consumer, gen, n, &log, 1).unwrap();
        let dur = start.elapsed();

        println!("\n=== bench_fetch_with_redeliver_500 ===");
        println!("fetch {n} redelivered msgs: {:?}", dur);
    }

    #[test]
    fn bench_clamp_head_with_pending() {
        let n = 5000;
        let log = fill_log(n);
        let mut g = ConsumerGroup::new("bench".into(), 1, 100000, Duration::from_secs(60), 5);
        let consumer = g.add_member("conn".into());
        let gen = g.generation();

        for i in (0..n).step_by(100) {
            let batch = &log[i..(i + 100).min(n)];
            g.fetch(&consumer, gen, 100, batch, 1).unwrap();
        }

        let start = Instant::now();
        g.clamp_head(n as u64 / 2);
        let dur = start.elapsed();

        println!("\n=== bench_clamp_head_with_pending ===");
        println!("clamp_head {n} pending → {}: {:?}", n / 2, dur);
    }

    #[test]
    fn bench_check_redelivery_1000() {
        let n = 1000;
        let log = fill_log(n);
        let mut g = ConsumerGroup::new("bench".into(), 1, 100000, Duration::from_millis(1), 100);
        let consumer = g.add_member("conn".into());
        let gen = g.generation();

        for i in (0..n).step_by(100) {
            let batch = &log[i..(i + 100).min(n)];
            g.fetch(&consumer, gen, 100, batch, 1).unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));

        let start = Instant::now();
        g.check_redelivery();
        let dur = start.elapsed();

        println!("\n=== bench_check_redelivery_1000 ===");
        println!("check_redelivery {n} expired: {:?}", dur);
    }

    #[test]
    fn bench_per_key_ordering_stress() {
        let n = 1000;
        let key = Bytes::from("stress-key");
        let log = fill_log_keyed(n, key);
        let mut g = ConsumerGroup::new("bench".into(), 1, 100000, Duration::from_secs(60), 100);
        let consumer = g.add_member("conn".into());
        let gen = g.generation();

        let start = Instant::now();
        let mut fetched = 0;
        while fetched < n {
            let batch = &log[fetched..(fetched + 100).min(n)];
            let msgs = g.fetch(&consumer, gen, 100, batch, 1).unwrap();
            for msg in &msgs {
                g.ack(&consumer, gen, msg.seq).unwrap();
                fetched += 1;
            }
        }
        let dur = start.elapsed();

        println!("\n=== bench_per_key_ordering_stress ===");
        println!("fetch+ack {n} sequential per-key: {:?}", dur);
    }

    #[test]
    fn bench_dlt_snapshot_500() {
        let n = 500;
        let mut dlt = BTreeMap::new();
        for i in 1..=n {
            dlt.insert(i as u64, DltEntry {
                reason: "test".into(),
                attempts: 3,
                key: Some(Bytes::from(format!("key-{}", i))),
            });
        }
        let parked: HashSet<Bytes> = dlt.values().filter_map(|e| e.key.clone()).collect();
        let g = ConsumerGroup::restore(
            "bench".into(),
            0,
            n as u64 + 1,
            100000,
            Duration::from_secs(60),
            5,
            dlt,
            parked,
            BTreeMap::new(),
        );

        let start = Instant::now();
        let _snap = g.dlt_snapshot();
        let snap_dur = start.elapsed();

        let start = Instant::now();
        let _parked = g.parked_keys_snapshot();
        let parked_dur = start.elapsed();

        println!("\n=== bench_dlt_snapshot_500 ===");
        println!("dlt_snapshot({n} entries): {:?}", snap_dur);
        println!("parked_keys_snapshot({n} keys): {:?}", parked_dur);
    }
}

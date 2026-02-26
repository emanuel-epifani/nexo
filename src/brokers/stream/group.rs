//! Consumer Group: JetStream-like per-message ack tracking
//!
//! Each group tracks:
//! - ack_floor: highest seq such that ALL seqs 1..=ack_floor are acked
//! - pending: messages delivered but not yet acked
//! - redeliver: messages that need to be redelivered (nack or timeout)

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::brokers::stream::message::Message;

pub struct PendingMsg {
    pub client_id: String,
    pub delivered_at: Instant,
    pub delivery_count: u32,
}

pub struct ConsumerGroup {
    pub id: String,
    // === Persistent state (saved to disk) ===
    pub ack_floor: u64,
    // === Volatile state (rebuilt on restart) ===
    pub next_deliver_seq: u64,
    pub pending: HashMap<u64, PendingMsg>,
    pub redeliver: VecDeque<u64>,
    // === Config ===
    pub max_ack_pending: usize,
    pub ack_wait: Duration,
    // === Member tracking (for disconnect cleanup) ===
    pub members: HashSet<String>,
}

impl ConsumerGroup {
    pub fn new(id: String, max_ack_pending: usize, ack_wait: Duration) -> Self {
        Self {
            id,
            ack_floor: 0,
            next_deliver_seq: 1, // sequences start at 1
            pending: HashMap::new(),
            redeliver: VecDeque::new(),
            max_ack_pending,
            ack_wait,
            members: HashSet::new(),
        }
    }

    /// Restore from persisted ack_floor (on warm start)
    pub fn restore(id: String, ack_floor: u64, max_ack_pending: usize, ack_wait: Duration) -> Self {
        Self {
            id,
            ack_floor,
            next_deliver_seq: ack_floor + 1,
            pending: HashMap::new(),
            redeliver: VecDeque::new(),
            max_ack_pending,
            ack_wait,
            members: HashSet::new(),
        }
    }

    /// Fetch messages for a client. Serves redeliver queue first, then fresh messages.
    pub fn fetch(&mut self, client_id: &str, limit: usize, log: &VecDeque<Message>, ram_start_seq: u64) -> Vec<Message> {
        if self.pending.len() >= self.max_ack_pending {
            return vec![]; // backpressure
        }

        let budget = limit.min(self.max_ack_pending - self.pending.len());
        let mut result = Vec::with_capacity(budget);

        // 1. Redeliver first
        while result.len() < budget {
            if let Some(seq) = self.redeliver.pop_front() {
                if let Some(msg) = Self::read_from_log(log, ram_start_seq, seq) {
                    let delivery_count = self.pending.get(&seq).map(|p| p.delivery_count).unwrap_or(0) + 1;
                    self.pending.insert(seq, PendingMsg {
                        client_id: client_id.to_string(),
                        delivered_at: Instant::now(),
                        delivery_count,
                    });
                    result.push(msg);
                }
            } else {
                break;
            }
        }

        // 2. Fresh messages
        while result.len() < budget {
            let seq = self.next_deliver_seq;
            if let Some(msg) = Self::read_from_log(log, ram_start_seq, seq) {
                self.next_deliver_seq += 1;
                self.pending.insert(seq, PendingMsg {
                    client_id: client_id.to_string(),
                    delivered_at: Instant::now(),
                    delivery_count: 1,
                });
                result.push(msg);
            } else {
                break; // no more messages in log
            }
        }

        result
    }

    /// Acknowledge a message. Removes from pending and tries to advance ack_floor.
    pub fn ack(&mut self, seq: u64) -> Result<(), String> {
        if self.pending.remove(&seq).is_none() {
            return Err(format!("seq {} not pending", seq));
        }
        self.try_advance_floor();
        Ok(())
    }

    /// Negative acknowledge: move message back to redeliver queue.
    pub fn nack(&mut self, seq: u64) -> Result<(), String> {
        if self.pending.remove(&seq).is_none() {
            return Err(format!("seq {} not pending", seq));
        }
        self.redeliver.push_back(seq);
        Ok(())
    }

    /// Check for expired pending messages and move them to redeliver queue.
    pub fn check_redelivery(&mut self) {
        let now = Instant::now();
        let expired: Vec<u64> = self.pending.iter()
            .filter(|(_, msg)| now.duration_since(msg.delivered_at) > self.ack_wait)
            .map(|(seq, _)| *seq)
            .collect();

        for seq in expired {
            if let Some(msg) = self.pending.remove(&seq) {
                tracing::debug!("[Group:{}] Redelivery timeout seq={} (attempts={})", self.id, seq, msg.delivery_count);
                self.redeliver.push_back(seq);
            }
        }
    }

    /// Seek to a target position. Clears all pending/redeliver state.
    pub fn seek_beginning(&mut self) {
        self.ack_floor = 0;
        self.next_deliver_seq = 1;
        self.pending.clear();
        self.redeliver.clear();
    }

    pub fn seek_end(&mut self, last_seq: u64) {
        self.ack_floor = last_seq;
        self.next_deliver_seq = last_seq + 1;
        self.pending.clear();
        self.redeliver.clear();
    }

    // --- Members ---

    pub fn add_member(&mut self, client_id: String) {
        self.members.insert(client_id);
    }

    pub fn remove_member(&mut self, client_id: &str) -> bool {
        if self.members.remove(client_id) {
            // Move any pending messages from this client back to redeliver
            let seqs: Vec<u64> = self.pending.iter()
                .filter(|(_, msg)| msg.client_id == client_id)
                .map(|(seq, _)| *seq)
                .collect();
            for seq in seqs {
                self.pending.remove(&seq);
                self.redeliver.push_back(seq);
            }
            return true;
        }
        false
    }

    pub fn is_member(&self, client_id: &str) -> bool {
        self.members.contains(client_id)
    }

    // --- Internal ---

    fn try_advance_floor(&mut self) {
        while self.ack_floor + 1 < self.next_deliver_seq
            && !self.pending.contains_key(&(self.ack_floor + 1))
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
}

//! Topic: Pure Logic Struct (No Actors, No Channels)
//! Single append-only log per topic (no partitions).

use crate::brokers::stream::message::Message;
use crate::brokers::stream::commands::{StreamCreateOptions, RetentionOptions};
use crate::config::SystemStreamConfig;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub persistence_path: String,
    pub max_segment_size: u64,
    pub retention: RetentionOptions,
    pub retention_check_ms: u64,
    pub default_flush_ms: u64,
    pub ram_soft_limit: usize,
    pub ram_hard_limit: usize,
    pub eviction_interval_ms: u64,
    pub max_ack_pending: usize,
    pub ack_wait_ms: u64,
    pub actor_channel_capacity: usize,
}



impl TopicConfig {
    pub fn from_options(opts: StreamCreateOptions, sys: &SystemStreamConfig) -> Self {
        let retention = match opts.retention {
            Some(r) => RetentionOptions {
                max_age_ms: r.max_age_ms.map_or(
                    Some(sys.default_retention_age_ms),
                    |v| if v == 0 { None } else { Some(v) }
                ),
                max_bytes: r.max_bytes.map_or(
                    Some(sys.default_retention_bytes),
                    |v| if v == 0 { None } else { Some(v) }
                ),
            },
            None => RetentionOptions {
                max_age_ms: Some(sys.default_retention_age_ms),
                max_bytes: Some(sys.default_retention_bytes),
            }
        };

        Self {
            persistence_path: sys.persistence_path.clone(),
            max_segment_size: sys.max_segment_size,
            retention,
            retention_check_ms: sys.retention_check_interval_ms,
            default_flush_ms: sys.default_flush_ms,
            ram_soft_limit: sys.ram_soft_limit,
            ram_hard_limit: sys.ram_hard_limit,
            eviction_interval_ms: sys.eviction_interval_ms,
            max_ack_pending: sys.max_ack_pending,
            ack_wait_ms: sys.ack_wait_ms,
            actor_channel_capacity: sys.actor_channel_capacity,
        }
    }
}

pub struct TopicState {
    pub name: String,
    // Single log
    pub log: VecDeque<Message>,
    pub next_seq: u64,
    pub start_seq: u64,       // first seq available (after retention)
    pub ram_start_seq: u64,   // first seq in RAM window
    pub persisted_seq: u64,   // last seq flushed to disk
    // Config
    pub ram_soft_limit: usize,
}

impl TopicState {
    pub fn new(name: String, ram_soft_limit: usize) -> Self {
        Self {
            name,
            log: VecDeque::new(),
            next_seq: 1, // sequences start at 1 (0 = "nothing processed")
            start_seq: 1,
            ram_start_seq: 1,
            persisted_seq: 0,
            ram_soft_limit,
        }
    }

    pub fn restore(name: String, ram_soft_limit: usize, messages: VecDeque<Message>) -> Self {
        let next_seq = messages.back().map(|m| m.seq + 1).unwrap_or(1);
        let ram_start_seq = messages.front().map(|m| m.seq).unwrap_or(next_seq);
        let start_seq = ram_start_seq; // will be refined with segment index

        Self {
            name,
            log: messages,
            next_seq,
            start_seq,
            ram_start_seq,
            persisted_seq: next_seq.saturating_sub(1), // assume all recovered data is persisted
            ram_soft_limit,
        }
    }

    pub fn append(&mut self, payload: Bytes) -> (u64, u64) {
        let seq = self.next_seq;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if self.log.is_empty() {
            self.ram_start_seq = seq;
        }

        self.log.push_back(Message {
            seq,
            timestamp,
            payload,
        });
        self.next_seq += 1;

        (seq, timestamp)
    }

    pub fn read(&self, from_seq: u64, limit: usize) -> Vec<Message> {
        let from_seq = from_seq.max(1); // sequences start at 1
        // Hot read: from RAM
        if from_seq >= self.ram_start_seq && !self.log.is_empty() {
            let idx = (from_seq - self.ram_start_seq) as usize;
            if idx < self.log.len() {
                return self.log.iter().skip(idx).take(limit).cloned().collect();
            }
        }
        // Cold read is handled by the actor (segment files)
        Vec::new()
    }

    /// Evict old messages from RAM front (only if persisted to disk)
    pub fn evict(&mut self) {
        while self.log.len() > self.ram_soft_limit {
            if let Some(front) = self.log.front() {
                if front.seq <= self.persisted_seq {
                    if let Some(removed) = self.log.pop_front() {
                        self.ram_start_seq = removed.seq + 1;
                    }
                } else {
                    break; // don't evict unpersisted data
                }
            } else {
                break;
            }
        }
    }
}

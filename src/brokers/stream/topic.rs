//! Topic: Pure Logic Struct (No Actors, No Channels)
//! Contains N partitions.
//! Now prepared for Actor usage (sync methods, pass-through logic).

use crate::brokers::stream::message::Message;
use std::collections::{VecDeque, BTreeMap};
use std::sync::{Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use tokio::sync::Notify;

pub struct TopicState {
    pub name: String,
    pub partitions: Vec<PartitionState>,
}

impl TopicState {
    pub fn new(name: String, partitions_count: u32) -> Self {
        let partitions = (0..partitions_count)
            .map(|id| PartitionState::new(id))
            .collect();
        
        Self { name, partitions }
    }

    pub fn publish(&mut self, partition_id: u32, payload: Bytes) -> u64 {
        if let Some(p) = self.partitions.get_mut(partition_id as usize) {
            p.append(payload)
        } else {
            0
        }
    }

    pub fn read(&self, partition_id: u32, offset: u64, limit: usize) -> Vec<Message> {
        if let Some(p) = self.partitions.get(partition_id as usize) {
            p.read(offset, limit)
        } else {
            Vec::new()
        }
    }

    pub fn get_partitions_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn get_high_watermark(&self, partition_id: u32) -> u64 {
        if let Some(p) = self.partitions.get(partition_id as usize) {
            p.next_offset
        } else {
            0
        }
    }
}

pub struct PartitionState {
    pub id: u32,
    log: VecDeque<Message>,
    next_offset: u64,
    start_offset: u64,
    // Note: Notify is hard to keep in pure state if we want to be serializable,
    // but for now it's fine as runtime state.
    waiters: BTreeMap<u64, Vec<Weak<Notify>>>,
}

impl PartitionState {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            log: VecDeque::new(),
            next_offset: 0,
            start_offset: 0,
            waiters: BTreeMap::new(),
        }
    }

    pub fn append(&mut self, payload: Bytes) -> u64 {
        let offset = self.next_offset;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.log.push_back(Message {
            offset,
            timestamp,
            payload,
        });
        self.next_offset += 1;

        // Wake waiters logic
        // (In pure state we might return a "WaitersToNotify" list to the actor,
        // but keeping it here for simplicity is okay for now)
        let mut future_waiters = self.waiters.split_off(&(offset + 1));
        
        for (_, batch) in self.waiters.iter() {
            for weak in batch {
                if let Some(notify) = weak.upgrade() {
                    notify.notify_one();
                }
            }
        }
        
        self.waiters = future_waiters;
        
        offset
    }

    pub fn read(&self, offset: u64, limit: usize) -> Vec<Message> {
        if offset < self.start_offset {
            self.log.iter().take(limit).cloned().collect()
        } else {
            let idx = (offset - self.start_offset) as usize;
            if idx >= self.log.len() {
                Vec::new()
            } else {
                self.log.iter().skip(idx).take(limit).cloned().collect()
            }
        }
    }
}

use std::sync::RwLock;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::sync::atomic::{AtomicUsize, Ordering};
use bytes::Bytes;
use crate::brokers::stream::partition::Partition;

#[derive(Clone, Debug)]
pub struct TopicConfig {
    pub partitions: u32,
    // Future: retention_ms, retention_bytes
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            partitions: 4,
        }
    }
}

impl TopicConfig {
    pub fn merge_defaults(&mut self) {
        if self.partitions == 0 {
            self.partitions = 4;
        }
    }
}

pub struct Topic {
    pub name: String,
    pub partitions: Vec<RwLock<Partition>>,
    pub config: TopicConfig,
    // For round-robin when no key is provided
    next_partition_idx: AtomicUsize, 
}

impl Topic {
    pub fn new(name: String, config: TopicConfig) -> Self {
        let mut partitions = Vec::with_capacity(config.partitions as usize);
        for i in 0..config.partitions {
            partitions.push(RwLock::new(Partition::new(i)));
        }

        // TODO: Start Background Retention Thread here
        // Spawn a tokio task that periodically checks partitions
        // and removes messages older than config.retention_ms

        Self {
            name,
            partitions,
            config,
            next_partition_idx: AtomicUsize::new(0),
        }
    }

    pub fn publish(&self, payload: Bytes, key: Option<String>) -> u64 {
        // 1. Choose Partition ID
        let partition_idx = if let Some(k) = &key {
            // Hash Key
            let mut hasher = DefaultHasher::new();
            k.hash(&mut hasher);
            (hasher.finish() % self.partitions.len() as u64) as usize
        } else {
            // Round Robin
            self.next_partition_idx.fetch_add(1, Ordering::Relaxed) % self.partitions.len()
        };

        // 2. Append (Lock scope reduced to single partition)
        if let Some(part_lock) = self.partitions.get(partition_idx) {
            let mut part = part_lock.write().unwrap();
            part.append(payload, key)
        } else {
            // Should never happen if logic is correct
            0
        }
    }

    pub fn read(&self, partition_id: u32, offset: u64, limit: usize) -> Option<Vec<crate::brokers::stream::message::Message>> {
        if let Some(part_lock) = self.partitions.get(partition_id as usize) {
            let part = part_lock.read().unwrap();
            Some(part.read(offset, limit))
        } else {
            None // Partition not found
        }
    }
}


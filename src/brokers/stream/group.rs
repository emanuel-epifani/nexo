//! Consumer Group: Tracks committed offset and connected clients
//! Simplified: No partitions, no rebalancing, just offset tracking

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::brokers::stream::snapshot::GroupSummary;

pub struct ConsumerGroup {
    pub id: String,
    pub topic: String,
    /// Single committed offset for the entire group
    pub committed_offset: AtomicU64,
    /// Connected clients (for tracking/dashboard)
    pub members: DashMap<String, ()>,
}

impl ConsumerGroup {
    pub fn new(id: String, topic: String) -> Self {
        Self {
            id,
            topic,
            committed_offset: AtomicU64::new(0),
            members: DashMap::new(),
        }
    }
    
    pub fn add_member(&self, client_id: String) {
        self.members.insert(client_id, ());
    }
    
    pub fn remove_member(&self, client_id: &str) {
        self.members.remove(client_id);
    }
    
    pub fn commit(&self, offset: u64) {
        // Only advance forward (prevent regression)
        self.committed_offset.fetch_max(offset, Ordering::SeqCst);
    }
    
    pub fn get_committed_offset(&self) -> u64 {
        self.committed_offset.load(Ordering::SeqCst)
    }
    
    pub fn get_snapshot(&self, high_watermark: u64) -> GroupSummary {
        let committed = self.get_committed_offset();
        let pending = if high_watermark > committed {
            high_watermark - committed
        } else {
            0
        };
        
        GroupSummary {
            name: self.id.clone(),
            pending_messages: pending,
            connected_clients: self.members.len(),
        }
    }
}

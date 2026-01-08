//! Consumer Group: Tracks committed offset and connected clients
//! Simplified: No partitions, no rebalancing, just offset tracking

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::brokers::stream::snapshot::{GroupSummary, MemberSummary};

pub struct ConsumerGroup {
    pub id: String,
    pub topic: String,
    /// Single committed offset for the entire group
    pub committed_offset: AtomicU64,
    /// Connected clients and their local offset (if we were tracking per-client, but we track group offset)
    /// Actually, for the simplified view "Members", we just track who is connected.
    /// In a real Kafka group, all members share the group offset.
    /// But if we want to show lag PER MEMBER, we need to know where each member is.
    /// In this simplified architecture, all members of a group consume from the same shared offset.
    /// So they all effectively have the same offset (the group offset).
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
        let lag = if high_watermark > committed {
            high_watermark - committed
        } else {
            0
        };
        
        // In this simple model, all members share the same group offset
        let members_summary: Vec<MemberSummary> = self.members.iter().map(|kv| {
            MemberSummary {
                client_id: kv.key().clone(),
                current_offset: committed,
                lag,
            }
        }).collect();
        
        GroupSummary {
            name: self.id.clone(),
            members: members_summary,
        }
    }
}

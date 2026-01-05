use dashmap::DashMap;
use crate::brokers::stream::snapshot::{GroupSummary, MemberDetail};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub client_id: String,
    // Which partitions is this member reading?
    pub assigned_partitions: Vec<u32>, 
}

pub struct ConsumerGroup {
    pub id: String,
    pub topic: String,
    // Generation ID (Epoch) for Fencing
    pub generation_id: AtomicU64,
    // Committed offsets: PartitionID -> Offset
    pub committed_offsets: DashMap<u32, u64>,
    // Active members: ClientID -> MemberInfo
    pub members: DashMap<String, ConsumerGroupMember>,
}

impl ConsumerGroup {
    pub fn new(id: String, topic: String) -> Self {
        Self {
            id,
            topic,
            generation_id: AtomicU64::new(0),
            committed_offsets: DashMap::new(),
            members: DashMap::new(),
        }
    }

    pub fn add_member(&self, client_id: String) {
        self.members.insert(client_id.clone(), ConsumerGroupMember {
            client_id,
            assigned_partitions: Vec::new(),
        });
    }

    pub fn remove_member(&self, client_id: &str) {
        self.members.remove(client_id);
    }
    
    /// Redistributes partitions among current members deterministically
    /// Returns the new Generation ID
    pub fn rebalance(&self, num_partitions: u32) -> u64 {
        // Increment Generation ID
        let new_gen_id = self.generation_id.fetch_add(1, Ordering::SeqCst) + 1;

        if self.members.is_empty() {
            return new_gen_id;
        }

        // 1. Get all member IDs and sort them to ensure deterministic assignment
        let mut member_ids: Vec<String> = self.members.iter().map(|m| m.key().clone()).collect();
        member_ids.sort();

        // 2. Calculate assignments (Round Robin)
        // Map MemberID -> List of Partitions
        let mut assignments: std::collections::HashMap<String, Vec<u32>> = std::collections::HashMap::new();
        
        // Ensure all members have an entry (even if empty)
        for m_id in &member_ids {
            assignments.insert(m_id.clone(), Vec::new());
        }
        
        for p_id in 0..num_partitions {
            let member_idx = (p_id as usize) % member_ids.len();
            let member_id = &member_ids[member_idx];
            
            assignments.get_mut(member_id).unwrap().push(p_id);
        }

        // 3. Apply assignments
        for mut member_ref in self.members.iter_mut() {
            let client_id = member_ref.key();
            let new_partitions = assignments.remove(client_id).unwrap_or_default();
            member_ref.assigned_partitions = new_partitions;
        }
        
        new_gen_id
    }
    
    pub fn commit(&self, partition_id: u32, offset: u64) {
        self.committed_offsets.insert(partition_id, offset);
    }
    
    pub fn get_committed_offset(&self, partition_id: u32) -> u64 {
        self.committed_offsets.get(&partition_id).map(|v| *v.value()).unwrap_or(0)
    }

    pub fn get_snapshot(&self, topic_partition_offsets: &std::collections::HashMap<u32, u64>) -> GroupSummary {
        let mut pending_messages = 0;
        
        for (pid, max_offset) in topic_partition_offsets {
             let committed = self.get_committed_offset(*pid);
             if *max_offset > committed {
                 pending_messages += *max_offset - committed;
             }
        }

        let members_detail: Vec<MemberDetail> = self.members.iter().map(|m| {
            MemberDetail {
                client_id: m.key().clone(),
                partitions_assigned: m.value().assigned_partitions.clone(),
            }
        }).collect();

        GroupSummary {
            name: self.id.clone(),
            pending_messages,
            connected_clients: self.members.len(),
            members: members_detail,
        }
    }
}

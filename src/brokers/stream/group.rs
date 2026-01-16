//! Consumer Group: Pure Logic (No Arc/Mutex)
//! Tracks members and offsets.
//! Handles Partition Assignment Logic (Rebalancing).

use std::collections::HashMap;
use crate::brokers::stream::snapshot::{GroupSummary, MemberSummary};

#[derive(Clone, Debug)]
pub struct MemberInfo {
    pub client_id: String,
    // Partitions assigned to this member
    pub partitions: Vec<u32>,
}

pub struct ConsumerGroup {
    pub id: String,
    pub topic: String,
    pub members: HashMap<String, MemberInfo>,
    // Generation ID (Epoch) - Monotonically increasing on every rebalance
    pub generation_id: u64,
    // Last committed offsets per partition (PartitionID -> Offset)
    pub committed_offsets: HashMap<u32, u64>,
}

impl ConsumerGroup {
    pub fn new(id: String, topic: String) -> Self {
        Self {
            id,
            topic,
            members: HashMap::new(),
            generation_id: 0,
            committed_offsets: HashMap::new(),
        }
    }

    pub fn add_member(&mut self, client_id: String) {
        if !self.members.contains_key(&client_id) {
            self.members.insert(client_id.clone(), MemberInfo {
                client_id,
                partitions: Vec::new(),
            });
            // Trigger implicit rebalance logic later (caller must call rebalance)
        }
    }

    pub fn remove_member(&mut self, client_id: &str) -> bool {
        if self.members.remove(client_id).is_some() {
            // Trigger implicit rebalance logic later (caller must call rebalance)
            return true;
        }
        false
    }

    pub fn commit(&mut self, partition: u32, offset: u64) {
        // Monotonic commit: only increase
        let current = self.committed_offsets.entry(partition).or_insert(0);
        if offset > *current {
            *current = offset;
        }
    }

    pub fn get_committed_offset(&self, partition: u32) -> u64 {
        *self.committed_offsets.get(&partition).unwrap_or(&0)
    }

    /// REBALANCE ALGORITHM (Round Robin / Range)
    /// Redistributes partitions among current members.
    /// Increments generation_id.
    pub fn rebalance(&mut self, total_partitions: u32) {
        if self.members.is_empty() {
            self.generation_id += 1;
            return;
        }

        // 1. Sort members to ensure deterministic assignment
        let mut member_ids: Vec<String> = self.members.keys().cloned().collect();
        member_ids.sort();

        let member_count = member_ids.len() as u32;
        let partitions_per_member = total_partitions / member_count;
        let remainder = total_partitions % member_count;

        // 2. Clear current assignments
        for m in self.members.values_mut() {
            m.partitions.clear();
        }

        // 3. Distribute
        let mut current_partition = 0;
        for (i, member_id) in member_ids.iter().enumerate() {
            // Give extra partition to the first 'remainder' members
            let extra = if (i as u32) < remainder { 1 } else { 0 };
            let count = partitions_per_member + extra;

            if let Some(member) = self.members.get_mut(member_id) {
                for _ in 0..count {
                    member.partitions.push(current_partition);
                    current_partition += 1;
                }
            }
        }

        // 4. New Era
        self.generation_id += 1;
    }

    pub fn is_member(&self, client_id: &str) -> bool {
        self.members.contains_key(client_id)
    }

    pub fn get_assignments(&self, client_id: &str) -> Option<&Vec<u32>> {
        self.members.get(client_id).map(|m| &m.partitions)
    }

    pub fn validate_epoch(&self, generation_id: u64) -> bool {
        self.generation_id == generation_id
    }

    pub fn get_snapshot(&self, high_watermarks: &HashMap<u32, u64>) -> GroupSummary {
        let members_summary = self.members.values().map(|m| {
            // Sum lag of all assigned partitions
            let mut current_offset_sum = 0;
            let mut lag_sum = 0;
            
            for &p_id in &m.partitions {
                let committed = self.get_committed_offset(p_id);
                let hw = *high_watermarks.get(&p_id).unwrap_or(&0);
                
                current_offset_sum += committed;
                if hw > committed {
                    lag_sum += hw - committed;
                }
            }

            MemberSummary {
                client_id: m.client_id.clone(),
                current_offset: current_offset_sum,
                lag: lag_sum,
            }
        }).collect();

        GroupSummary {
            name: self.id.clone(),
            members: members_summary,
        }
    }
}

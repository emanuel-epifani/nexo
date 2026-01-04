use dashmap::DashMap;

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub client_id: String,
    // Which partitions is this member reading?
    pub assigned_partitions: Vec<u32>, 
}

pub struct ConsumerGroup {
    pub id: String,
    pub topic: String,
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
    pub fn rebalance(&self, num_partitions: u32) {
        if self.members.is_empty() {
            return;
        }

        // 1. Get all member IDs and sort them to ensure deterministic assignment
        let mut member_ids: Vec<String> = self.members.iter().map(|m| m.key().clone()).collect();
        member_ids.sort();

        // 2. Calculate assignments (Round Robin)
        // Map MemberID -> List of Partitions
        let mut assignments: std::collections::HashMap<String, Vec<u32>> = std::collections::HashMap::new();
        
        for p_id in 0..num_partitions {
            let member_idx = (p_id as usize) % member_ids.len();
            let member_id = &member_ids[member_idx];
            
            assignments.entry(member_id.clone())
                .or_default()
                .push(p_id);
        }

        // 3. Apply assignments
        // We iterate all members to ensure those who lost partitions get cleared
        for mut member_ref in self.members.iter_mut() {
            let client_id = member_ref.key();
            let new_partitions = assignments.remove(client_id).unwrap_or_default();
            member_ref.assigned_partitions = new_partitions;
        }
    }
    
    pub fn commit(&self, partition_id: u32, offset: u64) {
        self.committed_offsets.insert(partition_id, offset);
    }
    
    pub fn get_committed_offset(&self, partition_id: u32) -> u64 {
        // Default to 0 if no commit found (Start from beginning)
        // Future: Configurable 'auto.offset.reset' (earliest/latest)
        self.committed_offsets.get(&partition_id).map(|v| *v.value()).unwrap_or(0)
    }
}

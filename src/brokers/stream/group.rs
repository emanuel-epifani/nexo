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
        // Simple logic: add member.
        // The actual rebalancing will be triggered by the Manager.
        self.members.insert(client_id.clone(), ConsumerGroupMember {
            client_id,
            assigned_partitions: Vec::new(),
        });
    }

    pub fn remove_member(&self, client_id: &str) {
        self.members.remove(client_id);
    }
    
    pub fn commit(&self, partition_id: u32, offset: u64) {
        self.committed_offsets.insert(partition_id, offset);
    }
    
    pub fn get_committed_offset(&self, partition_id: u32) -> u64 {
        self.committed_offsets.get(&partition_id).map(|v| v.value().clone()).unwrap_or(0)
    }
}


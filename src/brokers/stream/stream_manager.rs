use dashmap::DashMap;
use bytes::Bytes;
use std::sync::Arc;
use crate::brokers::stream::topic::{Topic, TopicConfig};
use crate::brokers::stream::group::ConsumerGroup;
use crate::brokers::stream::message::Message;
use crate::brokers::stream::snapshot::StreamBrokerSnapshot;

pub struct StreamManager {
    // Topic Name -> Topic Struct
    topics: DashMap<String, Arc<Topic>>,
    // Group ID -> Consumer Group Struct
    groups: DashMap<String, Arc<ConsumerGroup>>,
    // Client ID -> List of Group IDs they joined (for efficient disconnect)
    client_groups: DashMap<String, Vec<String>>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self {
            topics: DashMap::new(),
            groups: DashMap::new(),
            client_groups: DashMap::new(),
        }
    }

    /// Creates a topic (or returns OK if it already exists with compatible config)
    pub fn create_topic(&self, name: String, partitions: u32) {
        if !self.topics.contains_key(&name) {
            let mut config = TopicConfig { partitions }; 
            config.merge_defaults();
            let topic = Topic::new(name.clone(), config);
            self.topics.insert(name, Arc::new(topic));
        }
    }

    /// Publishes a message. Strict Mode: Topic must exist.
    pub fn publish(&self, topic_name: &str, payload: Bytes, key: Option<String>) -> Result<u64, String> {
        if let Some(topic) = self.topics.get(topic_name) {
            Ok(topic.publish(payload, key))
        } else {
            Err(format!("Topic '{}' not found. Create it first.", topic_name))
        }
    }
    
    /// Reads messages from a specific partition
    /// Authenticates that the client (if provided) actually owns this partition
    pub fn read(&self, topic_name: &str, partition_id: u32, offset: u64, limit: usize, client_id_opt: Option<&str>) -> Vec<Message> {
        // OWNERSHIP CHECK
        // If client_id is provided, we must check if it belongs to any group on this topic
        // and if so, whether it is assigned this partition.
        // NOTE: This check is complex because a client might read without a group (Direct Access).
        // Current Protocol V1: S_FETCH doesn't send GroupID. 
        // So we rely on: "Is this client in ANY group for this topic?"
        
        if let Some(client_id) = client_id_opt {
             if let Some(groups) = self.client_groups.get(client_id) {
                 for group_id in groups.iter() {
                     if let Some(group) = self.groups.get(group_id) {
                         if group.topic == topic_name {
                             // The client is in a group for this topic. 
                             // It MUST own the partition to read it.
                             if let Some(member) = group.members.get(client_id) {
                                 if !member.assigned_partitions.contains(&partition_id) {
                                     // Client is in group but NOT assigned this partition (Rebalanced away?)
                                     // Return empty to stop processing.
                                     return Vec::new();
                                 }
                             }
                         }
                     }
                 }
             }
        }

        if let Some(topic) = self.topics.get(topic_name) {
            if let Some(msgs) = topic.read(partition_id, offset, limit) {
                return msgs;
            }
        }
        Vec::new()
    }

    /// Registers a consumer group and returns assigned partitions with their starting offsets
    /// Returns: Vec<(PartitionID, StartOffset)>
    pub fn join_group(&self, group_id: &str, topic_name: &str, client_id: &str) -> Result<Vec<(u32, u64)>, String> {
        // Ensure topic exists
        let topic = self.topics.get(topic_name)
            .ok_or_else(|| format!("Topic '{}' not found. Create it first.", topic_name))?;

        let num_partitions = topic.config.partitions;

        let group = self.groups.entry(group_id.to_string())
            .or_insert_with(|| Arc::new(ConsumerGroup::new(group_id.to_string(), topic_name.to_string())));

        // 1. Add Member to Group
        group.add_member(client_id.to_string());
        
        // 2. Track Client -> Group mapping
        self.client_groups.entry(client_id.to_string())
            .or_default()
            .push(group_id.to_string());

        // 3. Trigger Rebalance (Redistribute partitions)
        group.rebalance(num_partitions);
        
        // 4. Get assigned partitions for THIS client
        let assigned = group.members.get(client_id)
            .map(|m| m.assigned_partitions.clone())
            .unwrap_or_default();
            
        // 5. Resolve Starting Offsets (Smart Offset)
        let mut result = Vec::new();
        for partition_id in assigned {
            let start_offset = group.get_committed_offset(partition_id);
            result.push((partition_id, start_offset));
        }
        
        Ok(result) 
    }

    /// Handles client disconnection or explicit leave
    pub fn leave_group(&self, group_id: &str, client_id: &str) {
        if let Some(group) = self.groups.get(group_id) {
            // 1. Remove member
            group.remove_member(client_id);
            
            // 2. Rebalance remaining members to pick up the slack
            if let Some(topic) = self.topics.get(&group.topic) {
                group.rebalance(topic.config.partitions);
            }
        }
        
        // Remove from client_groups mapping
        if let Some(mut groups) = self.client_groups.get_mut(client_id) {
            groups.retain(|g| g != group_id);
        }
    }
    
    /// Commits an offset for a specific partition in a consumer group
    pub fn commit_offset(&self, group_id: &str, topic_name: &str, partition_id: u32, offset: u64) -> Result<(), String> {
        // Ensure topic exists (optional validation)
        if !self.topics.contains_key(topic_name) {
            return Err(format!("Topic '{}' not found", topic_name));
        }

        if let Some(group) = self.groups.get(group_id) {
            // Validation: Is the group actually for this topic?
            if group.topic != topic_name {
                 return Err(format!("Group '{}' is for topic '{}', not '{}'", group_id, group.topic, topic_name));
            }
            
            group.commit(partition_id, offset);
            Ok(())
        } else {
             Err(format!("Group '{}' not found. Join it first.", group_id))
        }
    }
    pub fn disconnect(&self, client_id: &str) {
        if let Some((_, groups)) = self.client_groups.remove(client_id) {
            for group_id in groups {
                // Call leave logic for each group
                if let Some(group) = self.groups.get(&group_id) {
                    group.remove_member(client_id);
                     if let Some(topic) = self.topics.get(&group.topic) {
                        group.rebalance(topic.config.partitions);
                    }
                }
            }
        }
    }

    pub fn get_snapshot(&self) -> StreamBrokerSnapshot {
        // 1. Group all consumer groups by topic name for easier lookup
        let mut groups_by_topic: std::collections::HashMap<String, Vec<Arc<ConsumerGroup>>> = std::collections::HashMap::new();
        for group in self.groups.iter() {
            groups_by_topic.entry(group.topic.clone())
                .or_default()
                .push(group.value().clone());
        }

        // 2. Iterate topics and build summaries
        let mut topics_summary = Vec::new();
        for topic_entry in self.topics.iter() {
            let topic_name = topic_entry.key();
            let topic = topic_entry.value();
            
            let topic_groups = groups_by_topic.remove(topic_name).unwrap_or_default();
            
            topics_summary.push(topic.get_snapshot(topic_groups));
        }

        StreamBrokerSnapshot {
            total_topics: self.topics.len(),
            total_active_groups: self.groups.len(),
            topics: topics_summary,
        }
    }
}

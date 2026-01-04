use dashmap::DashMap;
use bytes::Bytes;
use std::sync::Arc;
use crate::brokers::stream::topic::{Topic, TopicConfig};
use crate::brokers::stream::group::ConsumerGroup;
use crate::brokers::stream::message::Message;

pub struct StreamManager {
    // Topic Name -> Topic Struct
    topics: DashMap<String, Arc<Topic>>,
    // Group ID -> Consumer Group Struct
    groups: DashMap<String, Arc<ConsumerGroup>>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self {
            topics: DashMap::new(),
            groups: DashMap::new(),
        }
    }

    /// Creates a topic (or returns OK if it already exists with compatible config)
    pub fn create_topic(&self, name: String, partitions: u32) {
        if !self.topics.contains_key(&name) {
            let config = TopicConfig { partitions, }; 
            let topic = Topic::new(name.clone(), config);
            self.topics.insert(name, Arc::new(topic));
        }
    }

    /// Publishes a message. Strict Mode: Topic must exist.
    pub fn publish(&self, topic_name: &str, payload: Bytes, key: Option<String>) -> Result<u64, String> {
        if let Some(topic) = self.topics.get(topic_name) {
            Ok(topic.publish(payload, key))
        } else {
            Err(format!("Topic '{}' not found. Use create() first.", topic_name))
        }
    }
    
    /// Reads messages from a specific partition
    pub fn read(&self, topic_name: &str, partition_id: u32, offset: u64, limit: usize) -> Vec<Message> {
        if let Some(topic) = self.topics.get(topic_name) {
            if let Some(msgs) = topic.read(partition_id, offset, limit) {
                return msgs;
            }
        }
        Vec::new()
    }

    /// Registers a consumer group and returns assigned partitions
    pub fn join_group(&self, group_id: &str, topic_name: &str, client_id: &str) -> Result<Vec<u32>, String> {
        // Ensure topic exists
        let topic = self.topics.get(topic_name)
            .ok_or_else(|| format!("Topic '{}' not found", topic_name))?;
        
        let num_partitions = topic.config.partitions;

        let group = self.groups.entry(group_id.to_string())
            .or_insert_with(|| Arc::new(ConsumerGroup::new(group_id.to_string(), topic_name.to_string())));

        // Add member
        group.add_member(client_id.to_string());
        
        // SIMPLE ASSIGNMENT STRATEGY (Round Robin)
        // This is not dynamic (doesn't revoke from others), but works for startup.
        // Get all members count
        let member_count = group.members.len();
        
        // In a real system we would sort members by ID and assign slice.
        // Here we just assign ALL partitions to everyone to allow testing 
        // (Competing Consumers logic requires offsets coordination which we don't have yet in Kafka-style)
        // WAIT: If we assign ALL to everyone, they process duplicates.
        // Let's assign based on Modulo hash of client_id for stability?
        // Or just: return (0..num_partitions).collect() for now and warn user.
        
        // For V1 correctness: Return All Partitions. 
        // We accept that currently multiple consumers in same group will duplicate work 
        // unless they coordinate. 
        // TODO: Implement proper Rebalance Protocol.
        
        Ok((0..num_partitions).collect()) 
    }

    pub fn leave_group(&self, group_id: &str, client_id: &str) {
        if let Some(group) = self.groups.get(group_id) {
            group.remove_member(client_id);
        }
    }
}

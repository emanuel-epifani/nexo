//! Stream Manager: Orchestrates Topics and Consumer Groups
//! Uses MPSC Actor pattern - each topic runs in its own Tokio task

use dashmap::DashMap;
use bytes::Bytes;
use std::sync::Arc;
use crate::brokers::stream::topic::TopicHandle;
use crate::brokers::stream::group::ConsumerGroup;
use crate::brokers::stream::message::Message;
use crate::brokers::stream::snapshot::StreamBrokerSnapshot;

const TOPIC_CHANNEL_BUFFER: usize = 10_000;

/// Session Guard for Stream
/// Automatically handles cleanup when dropped (RAII pattern)
pub struct StreamSession {
    client_id: String,
    manager: Arc<StreamManager>,
}

impl Drop for StreamSession {
    fn drop(&mut self) {
        self.manager.disconnect(&self.client_id);
    }
}

pub struct StreamManager {
    /// Topic Name -> Topic Handle (actor communication)
    topics: DashMap<String, TopicHandle>,
    /// Group ID -> Consumer Group
    groups: DashMap<String, Arc<ConsumerGroup>>,
    /// Client ID -> List of Group IDs (for disconnect cleanup)
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
    
    /// Registers a client session for lifecycle management
    pub fn register_session(self: &Arc<Self>, client_id: String) -> StreamSession {
        StreamSession {
            client_id,
            manager: self.clone(),
        }
    }
    
    /// Creates a topic (idempotent - ignores if exists)
    pub fn create_topic(&self, name: String) {
        if !self.topics.contains_key(&name) {
            let handle = TopicHandle::new(name.clone(), TOPIC_CHANNEL_BUFFER);
            self.topics.insert(name, handle);
        }
    }
    
    /// Publishes a message to a topic
    pub async fn publish(&self, topic_name: &str, payload: Bytes, key: Option<String>) -> Result<u64, String> {
        let topic = self.topics.get(topic_name)
            .ok_or_else(|| format!("Topic '{}' not found. Create it first.", topic_name))?;
        
        topic.publish(payload, key).await
    }
    
    /// Reads messages from a topic starting at offset
    pub async fn read(&self, topic_name: &str, offset: u64, limit: usize) -> Vec<Message> {
        if let Some(topic) = self.topics.get(topic_name) {
            topic.read(offset, limit).await
        } else {
            Vec::new()
        }
    }
    
    /// Joins a consumer group - returns starting offset
    pub async fn join_group(&self, group_id: &str, topic_name: &str, client_id: &str) -> Result<u64, String> {
        // Ensure topic exists
        if !self.topics.contains_key(topic_name) {
            return Err(format!("Topic '{}' not found. Create it first.", topic_name));
        }
        
        // Get or create group
        let group = self.groups.entry(group_id.to_string())
            .or_insert_with(|| Arc::new(ConsumerGroup::new(group_id.to_string(), topic_name.to_string())));
        
        // Validate group is for this topic
        if group.topic != topic_name {
            return Err(format!("Group '{}' is for topic '{}', not '{}'", group_id, group.topic, topic_name));
        }
        
        // Add member
        group.add_member(client_id.to_string());
        
        // Track client -> group mapping
        self.client_groups.entry(client_id.to_string())
            .or_default()
            .push(group_id.to_string());
        
        // Return committed offset as starting point
        Ok(group.get_committed_offset())
    }
    
    /// Commits an offset for a consumer group
    pub fn commit_offset(&self, group_id: &str, topic_name: &str, offset: u64, client_id: &str) -> Result<(), String> {
        // Validate topic exists
        if !self.topics.contains_key(topic_name) {
            return Err(format!("Topic '{}' not found", topic_name));
        }
        
        let group = self.groups.get(group_id)
            .ok_or_else(|| format!("Group '{}' not found. Join it first.", group_id))?;
        
        // Validate group is for this topic
        if group.topic != topic_name {
            return Err(format!("Group '{}' is for topic '{}', not '{}'", group_id, group.topic, topic_name));
        }
        
        // Validate client is member
        if !group.members.contains_key(client_id) {
            return Err(format!("Client '{}' is not a member of group '{}'", client_id, group_id));
        }
        
        group.commit(offset);
        Ok(())
    }
    
    /// Handles client disconnection
    pub fn disconnect(&self, client_id: &str) {
        if let Some((_, groups)) = self.client_groups.remove(client_id) {
            for group_id in groups {
                if let Some(group) = self.groups.get(&group_id) {
                    group.remove_member(client_id);
                }
            }
        }
    }
    
    /// Get topic handle for advanced operations (e.g., wait_for_data)
    pub fn get_topic(&self, name: &str) -> Option<TopicHandle> {
        self.topics.get(name).map(|t| t.clone())
    }
    
    /// Dashboard snapshot
    pub async fn get_snapshot(&self) -> StreamBrokerSnapshot {
        let mut topics_summary = Vec::new();
        
        // Collect topic names first to avoid holding DashMap refs across await
        let topic_names: Vec<String> = self.topics.iter()
            .map(|t| t.key().clone())
            .collect();
        
        for topic_name in topic_names {
            if let Some(topic) = self.topics.get(&topic_name) {
                let (total_messages, high_watermark) = topic.get_stats().await;
                
                // Get groups for this topic
                let group_summaries: Vec<_> = self.groups.iter()
                    .filter(|g| g.topic == topic_name)
                    .map(|g| g.get_snapshot(high_watermark))
                    .collect();
                
                topics_summary.push(crate::brokers::stream::snapshot::TopicSummary {
                    name: topic_name,
                    total_messages,
                    consumer_groups: group_summaries,
                });
            }
        }
        
        StreamBrokerSnapshot {
            total_topics: self.topics.len(),
            total_active_groups: self.groups.len(),
            topics: topics_summary,
        }
    }
}

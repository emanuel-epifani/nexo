use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
pub struct StreamBrokerSnapshot {
    pub total_topics: usize,
    pub total_active_groups: usize,
    pub topics: Vec<TopicSummary>,
}

#[derive(Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Serialize)]
pub struct PartitionInfo {
    pub id: u32,
    pub messages: Vec<MessagePreview>,
    pub groups: Vec<ConsumerGroupSummary>,
    pub last_offset: u64,
}

#[derive(Serialize)]
pub struct ConsumerGroupSummary {
    pub id: String,
    pub committed_offset: u64,
}

#[derive(Serialize)]
pub struct MessagePreview {
    pub offset: u64,
    pub timestamp: String,
    pub payload: Value,
}

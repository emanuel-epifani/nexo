use serde::Serialize;

#[derive(Serialize)]
pub struct StreamBrokerSnapshot {
    pub total_topics: usize,
    pub total_active_groups: usize,
    pub topics: Vec<TopicSummary>,
}

#[derive(Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub total_messages: u64,
    pub consumer_groups: Vec<GroupSummary>,
}

#[derive(Serialize)]
pub struct GroupSummary {
    pub name: String,
    pub pending_messages: u64,
    pub connected_clients: usize,
}

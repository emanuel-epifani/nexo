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
    pub partitions_count: usize,
    pub retention_ms: u64, // o "Infinite"
    pub total_messages: u64, // Somma dei messaggi in tutte le partizioni
    pub consumer_groups: Vec<GroupSummary>,
}

#[derive(Serialize)]
pub struct GroupSummary {
    pub name: String,
    pub pending_messages: u64, // La famosa "LAG": (Total msg - Last Committed)
    pub connected_clients: usize,
    pub members: Vec<MemberDetail>,
}

#[derive(Serialize)]
pub struct MemberDetail {
    pub client_id: String,
    pub partitions_assigned: Vec<u32>,
}

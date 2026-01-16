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
    pub partitions: u32,
    pub size_bytes: u64,
    pub groups: Vec<GroupSummary>,
}

#[derive(Serialize)]
pub struct GroupSummary {
    pub name: String,
    pub members: Vec<MemberSummary>,
}

#[derive(Serialize)]
pub struct MemberSummary {
    pub client_id: String,
    pub current_offset: u64,
    pub lag: u64,
}

use serde::Serialize;

#[derive(Serialize)]
pub struct PubSubBrokerSnapshot {
    pub active_clients: usize,
    pub topic_tree: TopicNodeSnapshot,
}

#[derive(Serialize)]
pub struct TopicNodeSnapshot {
    pub name: String, // "kitchen", "+", "#"
    pub subscribers: usize,
    pub retained_msg: bool,
    pub children: Vec<TopicNodeSnapshot>,
}

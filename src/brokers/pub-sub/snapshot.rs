use serde::Serialize;

#[derive(Serialize)]
pub struct PubSubBrokerSnapshot {
    pub active_clients: usize,
    pub topic_tree: TopicNodeSnapshot,
    pub wildcard_subscriptions: Vec<WildcardSubscription>,
}

#[derive(Serialize)]
pub struct WildcardSubscription {
    pub pattern: String,
    pub client_id: String,
}

#[derive(Serialize)]
pub struct TopicNodeSnapshot {
    pub name: String,
    pub full_path: String,
    pub subscribers: usize,
    pub retained_value: Option<String>,
    pub children: Vec<TopicNodeSnapshot>,
}

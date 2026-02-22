use serde::{Serialize, Deserialize};
use serde_json::Value;

#[derive(Serialize)]
pub struct PubSubBrokerSnapshot {
    pub active_clients: usize,
    pub topics: Vec<TopicSnapshot>,
    pub wildcards: WildcardSubscriptions,
}

#[derive(Serialize)]
pub struct WildcardSubscriptions {
    pub multi_level: Vec<WildcardSubscription>,
    pub single_level: Vec<WildcardSubscription>,
}

#[derive(Serialize)]
pub struct WildcardSubscription {
    pub pattern: String,
    pub client_id: String,
}

#[derive(Serialize)]
pub struct TopicSnapshot {
    pub full_path: String,
    pub subscribers: usize,
    pub retained_value: Option<Value>,
}

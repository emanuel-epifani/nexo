//! PubSub introspection types: neutral snapshots consumed by any read-only
//! adapter (dashboard HTTP, future CLI, metrics, ...).

use bytes::Bytes;

pub struct PubSubSnapshot {
    pub active_clients: usize,
    pub total_topics: usize,
    pub topics: Vec<TopicSnapshot>,
    pub wildcards: WildcardSubscriptions,
}

#[derive(Clone)]
pub struct TopicSnapshot {
    pub full_path: String,
    pub subscribers: usize,
    pub retained_payload: Option<Bytes>,
}

#[derive(Clone)]
pub struct WildcardSubscriptions {
    pub multi_level: Vec<WildcardSubscription>,
    pub single_level: Vec<WildcardSubscription>,
}

#[derive(Clone)]
pub struct WildcardSubscription {
    pub pattern: String,
    pub client_id: String,
}

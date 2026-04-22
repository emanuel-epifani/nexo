//! Stream introspection types: neutral snapshots consumed by any read-only
//! adapter (dashboard HTTP, future CLI, metrics, ...).

use crate::brokers::stream::domain::topic::TopicConfig;

pub struct StreamSnapshot {
    pub topics: Vec<TopicSnapshot>,
}

pub struct TopicSnapshot {
    pub name: String,
    pub last_seq: u64,
    pub groups: Vec<ConsumerGroupSnapshot>,
    pub config: TopicConfig,
}

pub struct ConsumerGroupSnapshot {
    pub id: String,
    pub ack_floor: u64,
    pub pending_count: usize,
}

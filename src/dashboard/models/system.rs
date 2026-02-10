use serde::Serialize;
use super::store::StoreBrokerSnapshot;
use super::queues::QueueSummary;
use super::pubsub::PubSubBrokerSnapshot;
use super::stream::StreamBrokerSnapshot;

#[derive(Serialize)]
pub struct SystemSnapshot {
    pub brokers: BrokersSnapshot,
}

#[derive(Serialize)]
pub struct BrokersSnapshot {
    pub store: StoreBrokerSnapshot,
    pub queue: Vec<QueueSummary>,
    pub pubsub: PubSubBrokerSnapshot,
    pub stream: StreamBrokerSnapshot,
}

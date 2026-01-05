use serde::Serialize;
use crate::brokers::stream::snapshot::StreamBrokerSnapshot;
use crate::brokers::queues::snapshot::QueueBrokerSnapshot;
use crate::brokers::store::snapshot::StoreBrokerSnapshot;
use crate::brokers::pub_sub::snapshot::PubSubBrokerSnapshot;

#[derive(Serialize)]
pub struct SystemSnapshot {
    pub uptime_seconds: u64,
    pub server_time: String,
    pub brokers: BrokersSnapshot,
}

#[derive(Serialize)]
pub struct BrokersSnapshot {
    pub stream: StreamBrokerSnapshot,
    pub queue: QueueBrokerSnapshot,
    pub store: StoreBrokerSnapshot,
    pub pubsub: PubSubBrokerSnapshot,
}


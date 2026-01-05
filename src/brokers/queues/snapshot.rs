use serde::Serialize;

#[derive(Serialize)]
pub struct QueueBrokerSnapshot {
    pub queues: Vec<QueueSummary>,
}

#[derive(Serialize)]
pub struct QueueSummary {
    pub name: String,
    pub pending_count: usize, // waiting_for_dispatch
    pub inflight_count: usize, // waiting_for_ack
    pub scheduled_count: usize, // waiting_for_time
    pub consumers_waiting: usize,
}


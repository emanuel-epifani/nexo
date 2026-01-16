use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
pub struct QueueBrokerSnapshot {
    pub queues: Vec<QueueSummary>,
}

#[derive(Serialize)]
pub struct QueueSummary {
    pub name: String,
    pub pending_count: usize, 
    pub inflight_count: usize, 
    pub scheduled_count: usize, 
    pub consumers_waiting: usize,
    pub messages: Vec<MessageSummary>, 
}

#[derive(Serialize)]
pub struct MessageSummary {
    pub id: Uuid,
    pub payload_preview: String,
    pub state: String, // "Pending", "InFlight", "Scheduled"
    pub priority: u8,
    pub attempts: u32,
    pub next_delivery_at: Option<String>,
}

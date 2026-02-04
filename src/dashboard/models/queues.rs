use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Serialize)]
pub struct QueueBrokerSnapshot {
    pub active_queues: Vec<QueueSummary>,
    pub dlq_queues: Vec<QueueSummary>,
}

#[derive(Serialize)]
pub struct QueueSummary {
    pub name: String,
    pub pending: Vec<MessageSummary>,
    pub inflight: Vec<MessageSummary>,
    pub scheduled: Vec<MessageSummary>,
}

#[derive(Serialize)]
pub struct MessageSummary {
    pub id: Uuid,
    pub payload: Value,
    pub state: String, // "Pending", "InFlight", "Scheduled"
    pub priority: u8,
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_delivery_at: Option<String>,
}

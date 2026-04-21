//! Queue introspection types: neutral domain snapshots consumed by any
//! read-only adapter (dashboard HTTP, future CLI, metrics, ...).
//!
//! Rules:
//! - No `Serialize`/`Deserialize`.
//! - No `serde_json::Value`.
//! - Only raw domain primitives (`Bytes`, `u64`, `Uuid`, enums).

use bytes::Bytes;
use uuid::Uuid;

use crate::brokers::queue::queue::QueueConfig;

pub struct QueueSnapshot {
    pub name: String,
    pub pending: usize,
    pub inflight: usize,
    pub scheduled: usize,
    pub dlq: usize,
    pub config: QueueConfig,
}

pub enum MessageStateTag {
    Pending,
    InFlight,
    Scheduled,
}

pub struct QueueMessagePreview {
    pub id: Uuid,
    pub payload: Bytes,
    pub state: MessageStateTag,
    pub priority: u8,
    pub attempts: u32,
    pub next_delivery_at_ms: Option<u64>,
}

//! Stored/consumer-facing value types for the queue domain.

use bytes::Bytes;
use uuid::Uuid;

/// A queue message as surfaced by `consume`/`pop`: carries the delivery
/// fencing token the client must echo back on ack/nack.
#[derive(Debug, Clone)]
pub struct Message {
    pub id: Uuid,
    pub payload: Bytes,
    pub priority: u8,
    pub attempts: u32,
    pub created_at: u64,
    /// Lease expiry (ms) while in-flight; `0` while ready.
    pub visible_at: u64,
    /// FIFO/priority ordering key; survives restart via the `queues` counters.
    pub ready_seq: u64,
    /// Fences this specific delivery; changes on every (re)delivery.
    pub delivery_token: u64,
    /// Last failure reason, kept across requeues for diagnostics.
    pub failure_reason: Option<String>,
}

/// One item inside a push batch.
#[derive(Debug)]
pub struct PushItem {
    pub payload: Bytes,
    pub priority: u8,
}

/// One dead-lettered entry surfaced by `peek_dlq`.
#[derive(Debug, Clone)]
pub struct DlqMessage {
    pub id: Uuid,
    pub payload: Bytes,
    pub priority: u8,
    pub attempts: u32,
    pub created_at: u64,
    pub failed_at: u64,
    /// FIFO position inside the DLQ; survives restart via `queues.next_dlq_seq`.
    pub dlq_seq: u64,
    pub failure_reason: String,
}

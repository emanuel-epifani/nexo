//! Stored/consumer-facing value types for the stream domain.

use bytes::Bytes;

/// A stored stream event as exposed by `read` (no delivery metadata).
#[derive(Debug, Clone)]
pub struct Message {
    pub seq: u64,
    pub timestamp: u64,
    /// Empty means the producer did not supply a key.
    pub key: Bytes,
    pub payload: Bytes,
}

/// One item inside a publish batch.
#[derive(Debug, Clone)]
pub struct PubItem {
    /// Empty key means keyless.
    pub key: Bytes,
    pub payload: Bytes,
}

/// One leased batch returned by `fetch`: consecutive original positions of a
/// single key, one replay event, or one keyless event. `receipt` fences the
/// lease and is echoed back on batch updates.
#[derive(Debug, Clone)]
pub struct DeliveryBatch {
    pub receipt: [u8; 16],
    pub messages: Vec<Message>,
}

/// One fetch result: the lease deadline horizon plus the granted batches.
#[derive(Debug, Clone)]
pub struct FetchPage {
    pub lease_ms: u64,
    pub batches: Vec<DeliveryBatch>,
}

#[derive(Debug, Clone, Copy)]
pub struct FetchLimits {
    pub max_messages: usize,
    pub max_key_messages: usize,
    pub max_key_batches: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchDisposition {
    Checkpoint,
    Retry,
    Release,
}

#[derive(Debug, Clone, Copy)]
pub struct BatchUpdate {
    pub receipt: [u8; 16],
    pub completed: u32,
    pub disposition: BatchDisposition,
}

/// `connection_id` binds the lease to its TCP session, `consumer_id` is the
/// member inside the group epoch, `epoch` is the group's opaque active epoch
/// id fencing pre-seek identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerIdentity {
    pub connection_id: String,
    pub consumer_id: String,
    pub epoch: [u8; 16],
}

/// One parked entry surfaced by `peek_dls`.
#[derive(Debug, Clone)]
pub struct DlsEntry {
    pub seq: u64,
    pub reason: String,
    pub attempts: u32,
    /// Empty for keyless entries.
    pub key: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_clones_are_independent() {
        let msg = Message {
            seq: 7,
            timestamp: 42,
            key: Bytes::from_static(b"k"),
            payload: Bytes::from_static(b"p"),
        };
        let mut copy = msg.clone();
        copy.seq = 9;
        assert_eq!(msg.seq, 7);
    }
}

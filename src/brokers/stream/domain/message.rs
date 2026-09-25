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

/// A leased delivery returned by `fetch`: the receipt fences this lease and
/// must be echoed back by the consumer on ACK.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub message: Message,
    pub receipt: [u8; 16],
}

/// Durable membership identity used by fetch/ack/leave.
/// `connection_id` binds the lease to its TCP session, `consumer_id` is the
/// member inside the group epoch, `generation` fences pre-seek identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerIdentity {
    pub connection_id: String,
    pub consumer_id: String,
    pub generation: u64,
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

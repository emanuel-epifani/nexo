//! Stream domain primitives: on-disk scalar encodings and state enums.
//!
//! SQLite INTEGER is signed 64-bit; protocol sequences/positions span the full
//! u64 range, so every wire-orderable u64 column is stored as an 8-byte
//! big-endian BLOB (byte order = numeric order for the whole u64 domain).
//! Row-ids, tickets, counters and timestamps are INTEGER columns instead.

use crate::brokers::BrokerError;



/// Canonical per-event storage charge: fixed record overhead (framing,
/// sequence, timestamp and length fields) plus key and payload bytes.
/// Charged at insert and subtracted verbatim at retention, so the same
/// function must price both sides.
pub const EVENT_LOGICAL_OVERHEAD_BYTES: u64 = 26;

/// Single shared database file inside the stream persistence root.
pub const DB_FILE_NAME: &str = "streams.sqlite3";
/// Advisory process-ownership lock kept next to the database.
pub const LOCK_FILE_NAME: &str = "streams.lock";
/// On-disk schema version persisted in `schema_meta`.
pub const SCHEMA_VERSION: i64 = 1;

#[inline]
pub fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64, BrokerError> {
    <[u8; 8]>::try_from(bytes)
        .map(u64::from_be_bytes)
        .map_err(|_| BrokerError::storage("Corrupt storage: u64 column is not 8 bytes"))
}

/// Fixed per-event logical size used by retention accounting.
#[inline]
pub fn event_logical_bytes(key_len: usize, payload_len: usize) -> u64 {
    EVENT_LOGICAL_OVERHEAD_BYTES + key_len as u64 + payload_len as u64
}

/// Encoded size of one item inside a FETCH response:
/// seq u64 + receipt 16 + timestamp u64 + key_len u16 + key + payload_len u32 + payload.
#[inline]
pub fn fetch_item_encoded_bytes(key_len: usize, payload_len: usize) -> u64 {
    38 + key_len as u64 + payload_len as u64
}

/// Lane scheduling states for `key_lanes.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum LaneState {
    /// No current candidate: cursor is at the resolved frontier.
    Empty = 0,
    /// One eligible head (original or requested replay) with a fair ticket.
    Ready = 1,
    /// Head leased to a consumer: receipt/owner/connection/deadline are set.
    Leased = 2,
    /// Key blocked by unresolved DLS members.
    Parked = 3,
}

impl LaneState {
    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::Empty),
            1 => Some(Self::Ready),
            2 => Some(Self::Leased),
            3 => Some(Self::Parked),
            _ => None,
        }
    }
}

/// Where a lane head / keyless delivery comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum Origin {
    /// Next event after the lane's original cursor.
    Original = 0,
    /// Explicit replay of a former DLS member.
    Replay = 1,
}

/// Why a DLS range exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum DlsReason {
    /// Head exhausted `max_deliveries`.
    MaxDeliveries = 0,
    /// Successor auto-parked behind a poisoned key / parked replay intent.
    AutoParked = 1,
}

impl DlsReason {
    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::MaxDeliveries),
            1 => Some(Self::AutoParked),
            _ => None,
        }
    }

    pub fn describe(self, max_deliveries: u32) -> String {
        match self {
            Self::MaxDeliveries => format!("max_deliveries exceeded ({})", max_deliveries),
            Self::AutoParked => "auto-parked (poisoned key)".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_blob_codec_preserves_full_domain_order() {
        let mut values = vec![0u64, 1, 2, u32::MAX as u64, i64::MAX as u64];
        values.push(i64::MAX as u64 + 1);
        values.push(u64::MAX - 1);
        values.push(u64::MAX);

        for &v in &values {
            assert_eq!(decode_u64(&encode_u64(v)).unwrap(), v);
        }
        let mut sorted = values.clone();
        sorted.sort();
        assert_eq!(values, sorted);
        for pair in values.windows(2) {
            assert!(encode_u64(pair[0]) < encode_u64(pair[1]));
        }
    }

    #[test]
    fn u64_blob_decode_rejects_wrong_lengths() {
        for len in [0usize, 1, 7, 9, 16] {
            assert!(decode_u64(&vec![0u8; len]).is_err());
        }
    }

    #[test]
    fn lane_state_roundtrip() {
        for state in [
            LaneState::Empty,
            LaneState::Ready,
            LaneState::Leased,
            LaneState::Parked,
        ] {
            assert_eq!(LaneState::from_i64(state as i64), Some(state));
        }
        assert_eq!(LaneState::from_i64(4), None);
    }
}

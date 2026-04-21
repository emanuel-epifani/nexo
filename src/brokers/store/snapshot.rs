//! Store introspection types: neutral snapshot consumed by any read-only
//! adapter (dashboard HTTP, future CLI, metrics, ...).

use bytes::Bytes;
use std::time::Instant;

pub struct StoreSnapshot {
    pub entries: Vec<KeyEntry>,
    pub total: usize,
}

pub struct KeyEntry {
    pub key: String,
    pub payload: Bytes,
    pub expires_at: Option<Instant>,
}

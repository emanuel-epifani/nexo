//! PubSub Retained Message: Last Value Caching with TTL support

use std::time::{Instant, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RetainedMessage {
    pub(crate) data: Bytes,
    #[serde(skip)]
    pub(crate) expires_at: Option<Instant>,
    pub(crate) expires_at_unix: Option<u64>,
}

impl RetainedMessage {
    pub(crate) fn new(data: Bytes, ttl_seconds: Option<u64>) -> Self {
        let expires_at = ttl_seconds.map(|secs| Instant::now() + std::time::Duration::from_secs(secs));
        let expires_at_unix = ttl_seconds.map(|secs| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + secs
        });
        Self { data, expires_at, expires_at_unix }
    }
    
    pub(crate) fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |exp| Instant::now() >= exp)
    }
    
    pub(crate) fn from_persisted(data: Bytes, expires_at_unix: Option<u64>) -> Self {
        let expires_at = expires_at_unix.and_then(|unix_ts| {
            let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if unix_ts > now_unix {
                let remaining = unix_ts - now_unix;
                Some(Instant::now() + std::time::Duration::from_secs(remaining))
            } else {
                None
            }
        });
        Self { data, expires_at, expires_at_unix }
    }
}

//! PubSub Retained Message: Last Value Caching with TTL support

use std::time::{Instant, SystemTime, UNIX_EPOCH};
use bytes::Bytes;

#[derive(Clone)]
pub(crate) struct RetainedMessage {
    pub(crate) data: Bytes,
    pub(crate) expires_at: Option<Instant>,
    pub(crate) expires_at_unix: Option<u64>,
}

impl RetainedMessage {
    pub(crate) fn new(data: Bytes, ttl_seconds: Option<u32>) -> Self {
        let expires_at = ttl_seconds.map(|secs| Instant::now() + std::time::Duration::from_secs(secs as u64));
        let expires_at_unix = ttl_seconds.map(|secs| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() + secs as u64
        });
        Self { data, expires_at, expires_at_unix }
    }

    pub(crate) fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at {
            Instant::now() >= exp
        } else {
            self.expires_at_unix.map_or(false, |unix| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() >= unix
            })
        }
    }

    pub(crate) fn from_persisted(data: Bytes, expires_at_unix: Option<u64>) -> Self {
        let expires_at = expires_at_unix.and_then(|unix_ts| {
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if unix_ts > now_unix {
                let remaining = ((unix_ts - now_unix).min(u32::MAX as u64)) as u32;
                Some(Instant::now() + std::time::Duration::from_secs(remaining as u64))
            } else {
                None
            }
        });
        Self { data, expires_at, expires_at_unix }
    }
}

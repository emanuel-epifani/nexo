use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use crate::brokers::store::config::StoreConfig;
use bytes::Bytes;

#[derive(Clone, Debug)]
struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
}

pub struct Map {
    inner: Arc<DashMap<String, Entry>>,
}

impl Map {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        let inner = Arc::new(DashMap::new());

        // Weak reference for the cleanup task
        // This prevents the task from keeping the store domain alive if the StoreManager is dropped
        let weak_inner = Arc::downgrade(&inner);
        let cleanup_interval = config.cleanup_interval_secs;

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(cleanup_interval));
            loop {
                interval.tick().await;

                match weak_inner.upgrade() {
                    Some(map) => {
                        let now = Instant::now();
                        map.retain(|_, entry: &mut Entry| {
                            if let Some(expiry) = entry.expires_at {
                                return expiry > now;
                            }
                            true
                        });
                    }
                    None => {
                        break;
                    }
                }
            }
        });

        Self { inner }
    }

    pub fn set(&self, key: String, value: Bytes, ttl: Option<u64>) -> Result<(), String> {
        let expires_at = match ttl {
            None => None,
            Some(0) => return Err("ttl must be greater than 0".into()),
            Some(secs) => Some(Instant::now() + Duration::from_secs(secs)),
        };

        self.inner.insert(key, Entry {
            value,
            expires_at,
        });
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        if let Some(entry) = self.inner.get(key) {
            if let Some(expiry) = entry.expires_at {
                if Instant::now() >= expiry {
                    return None;
                }
            }
            return Some(entry.value.clone());
        }
        None
    }

    pub fn del(&self, key: &str) -> bool {
        self.inner.remove(key).is_some()
    }

    pub fn incr(&self, key: &str, delta: i64) -> Result<Bytes, String> {
        use dashmap::mapref::entry::Entry as DashEntry;

        let entry = self.inner.entry(key.to_string());
        match entry {
            DashEntry::Vacant(v) => {
                let new_val = delta;
                v.insert(Entry {
                    value: Bytes::from(new_val.to_string()),
                    expires_at: None,
                });
                Ok(Bytes::from(new_val.to_string()))
            }
            DashEntry::Occupied(mut o) => {
                // Check TTL expiry
                if let Some(expiry) = o.get().expires_at {
                    if Instant::now() >= expiry {
                        let new_val = delta;
                        o.insert(Entry {
                            value: Bytes::from(new_val.to_string()),
                            expires_at: None,
                        });
                        return Ok(Bytes::from(new_val.to_string()));
                    }
                }
                // Parse existing value as i64, handling optional DataType prefix
                let raw = &o.get().value;
                let current: i64 = parse_i64(raw)
                    .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                let new_val = current.checked_add(delta)
                    .ok_or_else(|| "increment would overflow".to_string())?;

                // Preserve original format: if value had DataType prefix, keep it
                let new_val_str = new_val.to_string();
                let had_prefix = raw.len() > 1 && matches!(raw[0], 0x00 | 0x01 | 0x02);
                let stored_bytes = if had_prefix {
                    let mut buf = Vec::with_capacity(new_val_str.len() + 1);
                    buf.push(raw[0]);
                    buf.extend_from_slice(new_val_str.as_bytes());
                    Bytes::from(buf)
                } else {
                    Bytes::from(new_val_str.clone())
                };
                let expires_at = o.get().expires_at;
                o.insert(Entry {
                    value: stored_bytes,
                    expires_at,
                });
                // Response is always raw number bytes (no prefix)
                Ok(Bytes::from(new_val_str))
            }
        }
    }

}

fn parse_i64(raw: &[u8]) -> Option<i64> {
    if raw.is_empty() {
        return None;
    }
    // Try raw parse first (Rust API tests store raw bytes)
    if let Ok(s) = std::str::from_utf8(raw) {
        if let Ok(n) = s.parse::<i64>() {
            return Some(n);
        }
    }
    // Try with DataType prefix (SDK stores prefixed bytes)
    match raw[0] {
        0x00 | 0x01 | 0x02 => std::str::from_utf8(&raw[1..]).ok()?.parse::<i64>().ok(),
        _ => None,
    }
}

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

    pub fn clear_all(&self) -> usize {
        let count = self.inner.len();
        self.inner.clear();
        count
    }

    pub fn clear_with_prefix(&self, prefix: &str) -> usize {
        let mut count = 0;
        self.inner.retain(|k, _| {
            if k.starts_with(prefix) {
                count += 1;
                false
            } else {
                true
            }
        });
        count
    }

    pub fn incr(&self, key: &str, delta: i64) -> Result<Bytes, String> {
        use dashmap::mapref::entry::Entry as DashEntry;

        const INT_PREFIX: u8 = 0x03;

        fn encode_int(val: i64) -> Bytes {
            let mut buf = vec![INT_PREFIX];
            buf.extend_from_slice(&val.to_be_bytes());
            Bytes::from(buf)
        }

        let entry = self.inner.entry(key.to_string());
        match entry {
            DashEntry::Vacant(v) => {
                let new_val = delta;
                v.insert(Entry {
                    value: encode_int(new_val),
                    expires_at: None,
                });
                Ok(encode_int(new_val))
            }
            DashEntry::Occupied(mut o) => {
                // Check TTL expiry
                if let Some(expiry) = o.get().expires_at {
                    if Instant::now() >= expiry {
                        let new_val = delta;
                        o.insert(Entry {
                            value: encode_int(new_val),
                            expires_at: None,
                        });
                        return Ok(encode_int(new_val));
                    }
                }
                // Parse existing value — must be INT type
                let raw = &o.get().value;
                let current: i64 = parse_i64(raw)
                    .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                let new_val = current.checked_add(delta)
                    .ok_or_else(|| "increment would overflow".to_string())?;

                let expires_at = o.get().expires_at;
                o.insert(Entry {
                    value: encode_int(new_val),
                    expires_at,
                });
                Ok(encode_int(new_val))
            }
        }
    }

}

fn parse_i64(raw: &[u8]) -> Option<i64> {
    if raw.len() == 9 && raw[0] == 0x03 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&raw[1..]);
        Some(i64::from_be_bytes(arr))
    } else {
        None
    }
}

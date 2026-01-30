pub(crate) mod commands;

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Bytes;
use tokio::time;

use crate::brokers::store::types::{Entry, Value};
use crate::config::Config;

#[derive(Clone)]
pub struct MapStore {
    inner: Arc<DashMap<String, Entry>>,
}

#[derive(Debug, Clone)]
pub struct MapValue(pub Bytes);

impl MapStore {
    pub fn new() -> Self {
        let inner = Arc::new(DashMap::new());

        // Weak reference for the cleanup thread
        // This prevents the thread from keeping the map alive if the StoreManager is dropped
        let weak_inner = Arc::downgrade(&inner);
        let cleanup_interval = Config::global().store.cleanup_interval_secs;

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(cleanup_interval));
            loop {
                interval.tick().await;

                // Try to upgrade the weak pointer
                match weak_inner.upgrade() {
                    Some(map) => {
                        // Manager is alive, proceed with cleanup
                        let now = Instant::now();
                        map.retain(|_, entry: &mut Entry| {
                            if let Some(expiry) = entry.expires_at {
                                return expiry > now;
                            }
                            true
                        });
                    }
                    None => {
                        // Manager has been dropped, stop the cleanup task
                        break;
                    }
                }
            }
        });

        Self { inner }
    }

    pub fn set(&self, key: String, value: Bytes, ttl: Option<u64>) {
        let expires_at = match ttl {
            Some(0) | None => {
                // Use default TTL from config
                Some(Instant::now() + Duration::from_secs(Config::global().store.default_ttl_secs))
            }
            Some(secs) => Some(Instant::now() + Duration::from_secs(secs)),
        };

        self.inner.insert(key, Entry {
            value: Value::Map(MapValue(value)),
            expires_at,
        });
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        if let Some(entry) = self.inner.get(key) {
            if let Some(expiry) = entry.expires_at {
                if Instant::now() > expiry {
                    return None; // Lazy expiration could happen here (delete on read)
                }
            }
            if let Value::Map(MapValue(val)) = &entry.value {
                return Some(val.clone());
            }
        }
        None
    }

    pub fn del(&self, key: &str) -> bool {
        self.inner.remove(key).is_some()
    }

    /// Used for snapshots and iterating
    pub fn iter(&self) -> dashmap::iter::Iter<String, Entry> {
        self.inner.iter()
    }
}

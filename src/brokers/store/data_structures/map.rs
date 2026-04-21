use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use crate::brokers::store::config::StoreConfig;
use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: MapValue,
    pub expires_at: Option<Instant>,
}

#[derive(Clone)]
pub struct MapStore {
    inner: Arc<DashMap<String, Entry>>,
    config: Arc<StoreConfig>,
}

#[derive(Debug, Clone)]
pub struct MapValue(pub Bytes);

impl MapStore {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        let inner = Arc::new(DashMap::new());

        // Weak reference for the cleanup thread
        // This prevents the thread from keeping the data_structures alive if the StoreManager is dropped
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

        Self { inner, config }
    }

    pub fn set(&self, key: String, value: Bytes, ttl: Option<u64>) {
        let expires_at = match ttl {
            Some(0) | None => {
                Some(Instant::now() + Duration::from_secs(self.config.default_ttl_secs))
            }
            Some(secs) => Some(Instant::now() + Duration::from_secs(secs)),
        };

        self.inner.insert(key, Entry {
            value: MapValue(value),
            expires_at,
        });
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        if let Some(entry) = self.inner.get(key) {
            if let Some(expiry) = entry.expires_at {
                if Instant::now() > expiry {
                    return None;
                }
            }
            let MapValue(val) = &entry.value;
            return Some(val.clone());
        }
        None
    }

    pub fn del(&self, key: &str) -> bool {
        self.inner.remove(key).is_some()
    }

    pub fn iter(&self) -> dashmap::iter::Iter<'_, String, Entry> {
        self.inner.iter()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

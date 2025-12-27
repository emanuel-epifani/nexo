//! KV Manager: In-memory key-value store with TTL support
use dashmap::DashMap;
use std::time::{Duration, Instant};
use bytes::Bytes;
use std::sync::Arc;
use tokio::time;

#[derive(Clone, Debug)]
struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
}

pub struct KvManager {
    store: Arc<DashMap<String, Entry>>,
}

impl KvManager {
    pub fn new() -> Self {
        let store = Arc::new(DashMap::new());
        
        // Background cleanup task (Cron)
        let store_cleanup = Arc::clone(&store);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let now = Instant::now();
                // retain() is efficient: it only locks buckets as it iterates
                store_cleanup.retain(|_, entry: &mut Entry| {
                    if let Some(expiry) = entry.expires_at {
                        return expiry > now;
                    }
                    true
                });
            }
        });

        Self { store }
    }

    pub fn set(&self, key: String, value: Bytes, ttl: Option<u64>) -> Result<(), String> {
        let expires_at = ttl.map(|secs| Instant::now() + Duration::from_secs(secs));
        let entry = Entry { value, expires_at };
        self.store.insert(key, entry);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, String> {
        if let Some(entry_ref) = self.store.get(key) {
            let entry = entry_ref.value();
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() > expires_at {
                    // Return None immediately if expired, cleanup will happen later
                    return Ok(None);
                }
            }
            return Ok(Some(entry.value.clone()));
        }
        Ok(None)
    }

    pub fn del(&self, key: &str) -> Result<bool, String> {
        let removed = self.store.remove(key).is_some();
        Ok(removed)
    }
}

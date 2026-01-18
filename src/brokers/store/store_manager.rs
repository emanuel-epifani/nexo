//! Store Manager: In-memory data store orchestrator

use dashmap::DashMap;
use std::time::{Duration, Instant};
use bytes::Bytes;
use std::sync::Arc;
use tokio::time;

use crate::brokers::store::types::{Entry, Value};
use crate::brokers::store::structures::kv::{KvOperations, KvValue};

pub struct StoreManager {
    store: Arc<DashMap<String, Entry>>,
}

impl StoreManager {
    pub fn new() -> Self {
        let store = Arc::new(DashMap::new());
        
        let store_cleanup = Arc::clone(&store);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let now = Instant::now();
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
        KvOperations::set(&self.store, key, value, ttl);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, String> {
        Ok(KvOperations::get(&self.store, key))
    }

    pub fn del(&self, key: &str) -> Result<bool, String> {
        Ok(KvOperations::del(&self.store, key))
    }

    pub fn get_snapshot(&self) -> crate::dashboard::models::store::StoreBrokerSnapshot {
        let mut keys_detail = Vec::new();
        let mut expiring = 0;
        
        for entry in self.store.iter() {
            let val = entry.value();
            if val.expires_at.is_some() {
                expiring += 1;
            }
            
            let value_preview = match &val.value {
                Value::Kv(KvValue(b)) => {
                    if let Ok(s) = std::str::from_utf8(b) {
                        if s.len() > 50 {
                            format!("{}...", &s[0..50])
                        } else {
                            s.to_string()
                        }
                    } else {
                        format!("[Binary {} bytes]", b.len())
                    }
                }
            };

            let expires_at_str = val.expires_at
                .map(|inst| {
                    let now = Instant::now();
                    let sys_now = std::time::SystemTime::now();
                    if inst > now {
                        let dur = inst - now;
                        let future_sys = sys_now + dur;
                        chrono::DateTime::<chrono::Utc>::from(future_sys).to_rfc3339()
                    } else {
                        "Expired".to_string()
                    }
                });

            keys_detail.push(crate::dashboard::models::store::KeyDetail {
                key: entry.key().clone(),
                value_preview,
                created_at: None,
                expires_at: expires_at_str,
            });
        }
        
        crate::dashboard::models::store::StoreBrokerSnapshot {
            total_keys: self.store.len(),
            expiring_keys: expiring,
            keys: keys_detail,
        }
    }
}

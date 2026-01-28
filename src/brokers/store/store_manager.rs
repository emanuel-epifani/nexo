//! Store Manager: In-memory data store orchestrator

use dashmap::DashMap;
use std::time::{Duration, Instant};
use bytes::Bytes;
use std::sync::Arc;
use tokio::time;

use crate::brokers::store::types::{Entry, Value};
use crate::brokers::store::structures::kv::{KvOperations, KvValue};
use crate::config::Config;

pub struct StoreManager {
    store: Arc<DashMap<String, Entry>>,
}

impl StoreManager {
    pub fn new() -> Self {
        let store = Arc::new(DashMap::new());
        
        let store_cleanup = Arc::clone(&store);
        let cleanup_interval = Config::global().store.cleanup_interval_secs;
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(cleanup_interval));
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
        let expires_at = match ttl {
            Some(0) | None => {
                // Use default TTL from config
                Some(Instant::now() + Duration::from_secs(Config::global().store.default_ttl_secs))
            }
            Some(secs) => Some(Instant::now() + Duration::from_secs(secs)),
        };

        KvOperations::set(&self.store, key, value, expires_at);
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
        let now = Instant::now();
        
        for entry in self.store.iter() {
            let val = entry.value();
            
            // Skip expired keys
            if let Some(expiry) = val.expires_at {
                if expiry <= now {
                    continue;
                }
            }
            
            let value = match &val.value {
                Value::Kv(KvValue(b)) => {
                    if b.is_empty() {
                        "[Empty]".to_string()
                    } else {
                        let data_type = b[0];  // First byte = DataType (0=RAW, 1=STRING, 2=JSON)
                        let content = &b[1..]; // Rest = actual data
                        
                        match data_type {
                            0 => format!("0x{}", hex::encode(content)),     // RAW → hex
                            1 => String::from_utf8_lossy(content).to_string(), // STRING → text
                            2 => String::from_utf8_lossy(content).to_string(), // JSON → text
                            _ => format!("[Unknown type: {}] 0x{}", data_type, hex::encode(content)),
                        }
                    }
                }
            };

            let expires_at_str = {
                let sys_now = std::time::SystemTime::now();
                let dur = val.expires_at.unwrap() - now;
                let future_sys = sys_now + dur;
                chrono::DateTime::<chrono::Utc>::from(future_sys).to_rfc3339()
            };

            keys_detail.push(crate::dashboard::models::store::KeyDetail {
                key: entry.key().clone(),
                value,
                expires_at: expires_at_str,
            });
        }
        
        crate::dashboard::models::store::StoreBrokerSnapshot {
            keys: keys_detail,
        }
    }
}

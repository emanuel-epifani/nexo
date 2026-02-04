//! Store Manager: In-memory data store orchestrator

use crate::brokers::store::map::MapStore;
use crate::brokers::store::types::{Entry, Value};
use crate::brokers::store::map::MapValue;
use std::time::Instant;
use crate::config::StoreConfig;

pub struct StoreManager {
    pub map: MapStore,
}

impl StoreManager {
    pub fn new(config: StoreConfig) -> Self {
        Self {
            map: MapStore::new(config),
        }
    }

    pub fn get_snapshot(&self) -> crate::dashboard::models::store::StoreBrokerSnapshot {
        let mut keys_detail = Vec::new();
        let now = Instant::now();
        
        for entry in self.map.iter() {
            let val = entry.value();
            
            // Skip expired keys (double check, though MapStore handles this on get)
            if let Some(expiry) = val.expires_at {
                if expiry <= now {
                    continue;
                }
            }
            
            let value = match &val.value {
                Value::Map(MapValue(b)) => {
                    let data_type = b[0];
                    let content = &b[1..];
                    
                    match data_type {
                        2 => serde_json::from_slice(content).unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(content).to_string())),
                        0 => serde_json::Value::String(format!("0x{}", hex::encode(content))),
                        _ => serde_json::Value::String(String::from_utf8_lossy(content).to_string()),
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

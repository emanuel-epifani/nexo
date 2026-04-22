//! Store Manager: In-memory data store orchestrator

use crate::brokers::store::domain::map::{MapStore, MapValue};
use crate::brokers::store::config::StoreConfig;
use crate::brokers::store::snapshot::{KeyEntry, StoreSnapshot};
use std::sync::Arc;
use std::time::Instant;

pub struct StoreManager {
    pub map: MapStore,
}

impl StoreManager {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        Self {
            map: MapStore::new(config),
        }
    }

    pub fn scan(&self, limit: usize, offset: usize, filter: Option<String>) -> StoreSnapshot {
        let total = self.map.len();
        let now = Instant::now();

        let entries = self.map.iter()
            .filter(|entry| {
                if let Some(ref f) = filter {
                    entry.key().contains(f)
                } else {
                    true
                }
            })
            .skip(offset)
            .take(limit)
            .filter_map(|entry| {
                let val = entry.value();
                if let Some(expiry) = val.expires_at {
                    if expiry <= now {
                        return None;
                    }
                }
                let MapValue(payload) = val.value.clone();
                Some(KeyEntry {
                    key: entry.key().clone(),
                    payload,
                    expires_at: val.expires_at,
                })
            })
            .collect();

        StoreSnapshot { entries, total }
    }
}

//! Store Manager: In-memory data store orchestrator

use crate::brokers::store::map::MapStore;
use crate::brokers::store::types::{Entry, Value};
use crate::brokers::store::map::MapValue;
use std::time::Instant;
use crate::config::StoreConfig;
use crate::server::protocol::payload_to_dashboard_value;

pub struct StoreManager {
    pub map: MapStore,
}

impl StoreManager {
    pub fn new(config: StoreConfig) -> Self {
        Self {
            map: MapStore::new(config),
        }
    }
}

pub struct ScanResult {
    pub items: Vec<(String, Value, Option<Instant>)>,
    pub total: usize,
}

impl StoreManager {
    pub fn scan(&self, limit: usize, offset: usize, filter: Option<String>) -> ScanResult {
        let total = self.map.len();
        
        let items = self.map.iter()
            .filter(|entry| {
                if let Some(ref f) = filter {
                    entry.key().contains(f)
                } else {
                    true
                }
            })
            .skip(offset)
            .take(limit)
            .map(|entry| {
                let val = entry.value();
                (entry.key().clone(), val.value.clone(), val.expires_at)
            })
            .collect();

        ScanResult { items, total }
    }
}

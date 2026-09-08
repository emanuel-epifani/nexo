//! Store Manager: In-memory data store orchestrator

use crate::brokers::store::config::StoreConfig;
use crate::brokers::store::domain::map::Map;
use std::sync::Arc;

pub struct StoreManager {
    pub map: Map,
}

impl StoreManager {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        Self {
            map: Map::new(config),
        }
    }
}

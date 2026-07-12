//! Store Manager: In-memory data store orchestrator

use crate::brokers::store::domain::map::MapStore;
use crate::brokers::store::config::StoreConfig;
use std::sync::Arc;

pub struct StoreManager {
    pub map: MapStore,
}

impl StoreManager {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        Self {
            map: MapStore::new(config),
        }
    }

}

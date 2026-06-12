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

}

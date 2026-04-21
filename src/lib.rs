#![allow(dead_code, unused_imports, unused_variables)]

pub mod transport;
pub mod brokers;
pub mod config;

use std::sync::Arc;
use std::time::Instant;
use crate::brokers::store::StoreManager;
use crate::brokers::queue::QueueManager;
use crate::brokers::pub_sub::PubSubManager;
use crate::brokers::stream::StreamManager;
use crate::config::Config;

// ========================================
// ENGINE (The Singleton)
// ========================================

#[derive(Clone)]
pub struct NexoEngine {
    pub store: Arc<StoreManager>,
    pub queue: Arc<QueueManager>,
    pub pubsub: Arc<PubSubManager>,
    pub stream: Arc<StreamManager>,
    pub start_time: Instant,
}

impl NexoEngine {
    pub fn new(config: &Config) -> Self {
        let pubsub = Arc::new(PubSubManager::new(Arc::new(config.pubsub.clone())));
        
        Self {
            store: Arc::new(StoreManager::new(Arc::new(config.store.clone()))),
            queue: Arc::new(QueueManager::new(Arc::new(config.queue.clone()))),
            pubsub,
            stream: Arc::new(StreamManager::new(Arc::new(config.stream.clone()))),
            start_time: Instant::now(),
        }
    }


}

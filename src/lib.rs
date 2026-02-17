#![allow(dead_code, unused_imports, unused_variables)]

pub mod server;
pub mod brokers;
pub mod utils;
pub mod dashboard;
pub mod config;

use std::sync::Arc;
use std::time::Instant;
use crate::brokers::store::StoreManager;
use crate::brokers::queue::QueueManager;
use crate::brokers::pub_sub::PubSubManager;
use crate::brokers::stream::StreamManager;
use crate::dashboard::models::system::SystemSnapshot;
use crate::dashboard::models::system::BrokersSnapshot;
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
        let pubsub = Arc::new(PubSubManager::new(config.pubsub.clone()));
        
        // Start background cleanup task for expired retained messages
        let pubsub_clone = pubsub.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.tick().await; // Skip first immediate tick
            loop {
                interval.tick().await;
                pubsub_clone.cleanup_expired_retained().await;
            }
        });
        
        Self {
            store: Arc::new(StoreManager::new(config.store.clone())),
            queue: Arc::new(QueueManager::new(config.queue.clone())),
            pubsub,
            stream: Arc::new(StreamManager::new(config.stream.clone())),
            start_time: Instant::now(),
        }
    }


}

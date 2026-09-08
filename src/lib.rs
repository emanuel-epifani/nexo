#![deny(warnings)]

pub mod brokers;
pub mod config;
pub mod protocol;
pub mod transport;

use crate::brokers::pub_sub::PubSubManager;
use crate::brokers::queue::QueueManager;
use crate::brokers::store::StoreManager;
use crate::brokers::stream::StreamManager;
use crate::config::Config;
use std::sync::Arc;

// ========================================
// ENGINE (The Singleton)
// ========================================

#[derive(Clone)]
pub struct NexoEngine {
    pub store: Arc<StoreManager>,
    pub queue: Arc<QueueManager>,
    pub pubsub: Arc<PubSubManager>,
    pub stream: Arc<StreamManager>,
}

impl NexoEngine {
    pub async fn new(config: &Config) -> Self {
        let pubsub = Arc::new(PubSubManager::new(Arc::new(config.pubsub.clone())));

        Self {
            store: Arc::new(StoreManager::new(Arc::new(config.store.clone()))),
            queue: Arc::new(QueueManager::new(Arc::new(config.queue.clone()))),
            pubsub,
            stream: Arc::new(StreamManager::new(Arc::new(config.stream.clone())).await),
        }
    }

    pub async fn shutdown(&self) {
        self.queue.shutdown().await;
        self.stream.shutdown().await;
        self.pubsub.shutdown();
    }
}

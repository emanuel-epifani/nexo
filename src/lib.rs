pub mod brokers;
pub mod config;
pub mod durable;
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
        // PubSub startup is fail-closed: a corrupt/locked/legacy retained
        // store must stop the engine instead of silently opening empty state.
        let pubsub = Arc::new(
            PubSubManager::new(Arc::new(config.pubsub.clone()))
                .expect("FATAL: pubsub engine failed to start"),
        );

        Self {
            store: Arc::new(StoreManager::new(Arc::new(config.store.clone()))),
            // Queue startup is fail-closed: a corrupt/locked/legacy store
            // must stop the engine instead of silently opening empty state.
            queue: Arc::new(
                QueueManager::new(Arc::new(config.queue.clone()))
                    .await
                    .expect("FATAL: queue engine failed to start"),
            ),
            pubsub,
            // Stream startup is fail-closed: a corrupt/locked/legacy store
            // must stop the engine instead of silently opening empty state.
            stream: Arc::new(
                StreamManager::new(Arc::new(config.stream.clone()))
                    .await
                    .expect("FATAL: stream engine failed to start"),
            ),
        }
    }

    pub async fn shutdown(&self) {
        self.queue.shutdown().await;
        self.stream.shutdown().await;
        self.pubsub.shutdown();
    }
}

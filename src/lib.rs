#![allow(dead_code, unused_imports, unused_variables)]

pub mod server;
pub mod brokers;
pub mod utils;
pub mod dashboard;
pub mod config;

use std::sync::Arc;
use std::time::Instant;
use crate::brokers::store::StoreManager;
use crate::brokers::queues::QueueManager;
use crate::brokers::pub_sub::PubSubManager;
use crate::brokers::stream::StreamManager;
use crate::dashboard::models::system::SystemSnapshot;
use crate::dashboard::models::system::BrokersSnapshot;

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
    pub fn new() -> Self {
        Self {
            store: Arc::new(StoreManager::new()),
            queue: Arc::new(QueueManager::new()),
            pubsub: Arc::new(PubSubManager::new()),
            stream: Arc::new(StreamManager::new()),
            start_time: Instant::now(),
        }
    }

    pub async fn get_global_snapshot(&self) -> SystemSnapshot {
        SystemSnapshot {
            brokers: BrokersSnapshot {
                store: self.store.get_snapshot(),
                queue: self.queue.get_snapshot().await,
                pubsub: self.pubsub.get_snapshot().await,
                stream: self.stream.get_snapshot().await,
            },
        }
    }
}

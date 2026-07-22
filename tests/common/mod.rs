#![allow(dead_code)]

use std::sync::Arc;
use nexo::brokers::queue::QueueManager;
use nexo::brokers::store::StoreManager;
use nexo::brokers::pub_sub::PubSubManager;
use nexo::config::Config;
use tempfile::TempDir;

// ==========================================
// SETUP HELPERS
// ==========================================

pub(crate) async fn setup_queue_manager() -> (Arc<QueueManager>, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();
    
    let mut config = Config::global().queue.clone();
    config.persistence_path = path;
    
    let manager = Arc::new(QueueManager::new(Arc::new(config)));
    (manager, temp_dir)
}

pub(crate) async fn setup_pubsub_manager() -> (Arc<PubSubManager>, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();
    
    let mut config = Config::global().pubsub.clone();
    config.persistence_path = path;
    
    let manager = Arc::new(PubSubManager::new(Arc::new(config)));
    (manager, temp_dir)
}

pub(crate) async fn setup_store_manager() -> (StoreManager, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::global().store.clone();
    let manager = StoreManager::new(Arc::new(config));
    (manager, temp_dir)
}


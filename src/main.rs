mod server;
mod features;
// mod debug;
mod utils;

use std::sync::Arc;
use crate::features::kv::KvManager;
use crate::features::queue::QueueManager;
use crate::features::topic::TopicManager;
use crate::features::stream::StreamManager;

// ========================================
// ENGINE (The Singleton)
// ========================================

/// The central brain of the server.
/// Holds references to all state managers.
/// This struct is cheap to clone (all fields are Arcs).
#[derive(Clone)]
pub struct NexoEngine {
    pub kv: Arc<KvManager>,
    pub queue: Arc<QueueManager>,
    pub topic: Arc<TopicManager>,
    pub stream: Arc<StreamManager>,
}

impl NexoEngine {
    /// Initialize all managers
    pub fn new() -> Self {
        Self {
            kv: Arc::new(KvManager::new()),
            queue: Arc::new(QueueManager::new()),
            topic: Arc::new(TopicManager::new()),
            stream: Arc::new(StreamManager::new()),
        }
    }
}

// ========================================
// MAIN ENTRY POINT
// ========================================

#[tokio::main]
async fn main() {
    // 1. Initialize the Engine
    // This creates all the data structures in memory.
    let engine = NexoEngine::new();
    
    println!("🚀 Nexo Server v0.1 Starting...");
    println!("📦 Managers initialized: KV, Queue, MQTT, Stream");

    // 2. Start the Network Layer
    // We pass the engine so the network handler can use it.
    server::network::start(engine).await;
}

#![allow(dead_code, unused_imports, unused_variables)]

mod server;
mod features;
// mod debug;
mod utils;

use std::sync::Arc;
use tokio::net::TcpListener;
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
    dotenv::dotenv().ok();

    let engine = NexoEngine::new();
    
    let host = std::env::var("NEXO_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("NEXO_PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("{}:{}", host, port);

    println!("🚀 Nexo Server v0.1 Starting...");
    println!("📦 Managers initialized: KV, Queue, MQTT, Stream");

    let listener = TcpListener::bind(&addr)
        .await
        .expect(&format!("Failed to bind to {}", addr));

    println!("[Server] Nexo listening on {}", addr);

    loop {
        let (socket, addr) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        let engine_clone = engine.clone();

        println!("[Server] New connection from {}", addr);

        tokio::spawn(async move {
            if let Err(e) = server::network::handle_connection(socket, engine_clone).await {
                eprintln!("[Server] Error handling connection from {}: {}", addr, e);
            }
            println!("[Server] Connection closed from {}", addr);
        });
    }
}

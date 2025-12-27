#![allow(dead_code, unused_imports, unused_variables)]

mod server;
mod brokers;
mod utils;

use std::sync::Arc;
use tokio::net::TcpListener;
use crate::brokers::kv::KvManager;
use crate::brokers::queue::QueueManager;
use crate::brokers::topic::TopicManager;
use crate::brokers::stream::StreamManager;

// ========================================
// ENGINE (The Singleton)
// ========================================

#[derive(Clone)]
pub struct NexoEngine {
    pub kv: Arc<KvManager>,
    pub queue: Arc<QueueManager>,
    pub topic: Arc<TopicManager>,
    pub stream: Arc<StreamManager>,
}

impl NexoEngine {
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

    println!("🚀 Nexo Server v0.2 Starting...");
    println!("📦 Brokers initialized: KV, Queue, Topic, Stream");

    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");

    println!("[Server] Nexo listening on {}", addr);

    loop {
        let (socket, client_addr) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        let engine_clone = engine.clone();

        println!("[Server] New connection from {}", client_addr);

        tokio::spawn(async move {
            if let Err(e) = server::socket_network::handle_connection(socket, engine_clone).await {
                eprintln!("[Server] Error from {}: {}", client_addr, e);
            }
            println!("[Server] Connection closed from {}", client_addr);
        });
    }
}

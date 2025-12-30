#![allow(dead_code, unused_imports, unused_variables)]

mod server;
mod brokers;
mod utils;

use std::sync::Arc;
use tokio::net::TcpListener;
use crate::brokers::kv::KvManager;
use crate::brokers::queues::QueueManager;
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
        let queue_manager = Arc::new(QueueManager::new());
        queue_manager.set_self(queue_manager.clone());
        
        Self {
            kv: Arc::new(KvManager::new()),
            queue: queue_manager,
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

    // Init Tracing (NEXO_LOG)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_env("NEXO_LOG")
                .add_directive(tracing::Level::ERROR.into()) // Default fallback
        )
        .compact()
        .with_target(true)
        .init();

    let engine = NexoEngine::new();
    
    let host = std::env::var("NEXO_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("NEXO_PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("{}:{}", host, port);

    tracing::info!("NEXO_LOG = {}", std::env::var("NEXO_LOG").unwrap_or_else(|_| "INFO".to_string()));

    tracing::info!(host = %host, port = %port, "🚀 Nexo Server v0.2 Starting...");
    tracing::info!("📦 Brokers initialized: KV, Queue, Topic, Stream");

    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");

    tracing::info!(address = %addr, "Nexo listening");

    loop {
        let (socket, client_addr) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        let engine_clone = engine.clone();

        tracing::info!(client = %client_addr, "New connection accepted");

        tokio::spawn(async move {
            if let Err(e) = server::socket_network::handle_connection(socket, engine_clone).await {
                tracing::error!(client = %client_addr, error = %e, "Connection error");
            }
            tracing::debug!(client = %client_addr, "Connection closed");
        });
    }
}

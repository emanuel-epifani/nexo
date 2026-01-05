#![allow(dead_code, unused_imports, unused_variables)]

mod server;
mod brokers;
mod utils;
mod system_snapshot;

use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use crate::brokers::store::StoreManager;
use crate::brokers::queues::QueueManager;
use crate::brokers::pub_sub::PubSubManager;
use crate::brokers::stream::StreamManager;

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

impl Default for NexoEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl NexoEngine {
    pub fn new() -> Self {
        let queue_manager = Arc::new(QueueManager::new());
        queue_manager.set_self(queue_manager.clone());
        
        Self {
            store: Arc::new(StoreManager::new()),
            queue: queue_manager,
            pubsub: Arc::new(PubSubManager::new()),
            stream: Arc::new(StreamManager::new()),
            start_time: Instant::now(),
        }
    }

    pub async fn get_global_snapshot(&self) -> system_snapshot::SystemSnapshot {
        system_snapshot::SystemSnapshot {
            uptime_seconds: self.start_time.elapsed().as_secs(),
            server_time: chrono::Local::now().to_rfc3339(),
            brokers: system_snapshot::BrokersSnapshot {
                store: self.store.get_snapshot(),
                queue: self.queue.get_snapshot(),
                pubsub: self.pubsub.get_snapshot(),
                stream: self.stream.get_snapshot().await,
            },
        }
    }
}

// ========================================
// MAIN ENTRY POINT
// ========================================

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    // Init Tracing (logging)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_env("NEXO_LOG")
        )
        .compact()
        .with_target(false)
        .without_time()
        .init();
    // -- LOGGING CONFIGURATION --
    match std::env::var("NEXO_LOG") {
        Ok(val) => tracing::info!("NEXO_LOG found: '{}'", val),
        Err(_) => tracing::warn!("NEXO_LOG NOT FOUND!"),
    }



    let engine = NexoEngine::new();
    
    let host = std::env::var("NEXO_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("NEXO_PORT").unwrap_or_else(|_| "7654".to_string());
    let dashboard_port = std::env::var("NEXO_DASHBOARD_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .unwrap_or(8080);

    let addr = format!("{}:{}", host, port);

    let engine_clone_for_dashboard = engine.clone();
    tokio::spawn(async move {
        server::dashboard_api::start_dashboard_server(engine_clone_for_dashboard, dashboard_port).await;
    });

    tracing::info!(host = %host, port = %port, "🚀 Nexo Server v0.2 Starting...");

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

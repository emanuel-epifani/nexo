#![allow(dead_code, unused_imports, unused_variables)]

mod server;
pub mod brokers;
mod utils;
pub mod dashboard;
pub mod config;

use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use crate::brokers::store::StoreManager;
use crate::brokers::queues::QueueManager;
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
    pub config: Arc<Config>,
    pub store: Arc<StoreManager>,
    pub queue: Arc<QueueManager>,
    pub pubsub: Arc<PubSubManager>,
    pub stream: Arc<StreamManager>,
    pub start_time: Instant,
}

impl NexoEngine {
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);
        
        Self {
            store: Arc::new(StoreManager::new(config.store.clone())),
            queue: Arc::new(QueueManager::new(config.queue.clone())),
            pubsub: Arc::new(PubSubManager::new(config.pubsub.clone())),
            stream: Arc::new(StreamManager::new(config.stream.clone())),
            start_time: Instant::now(),
            config,
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

// ========================================
// MAIN ENTRY POINT
// ========================================

#[tokio::main]
async fn main() {
    let config = Config::load();

    // Init Tracing (logging)
    tracing_subscriber::fmt()
        .with_env_filter(&config.server.log_level)
        .compact()
        .with_target(false)
        .without_time()
        .init();

    tracing::info!("--- CONFIGURATION LOADED ---");
    tracing::info!("{:#?}", config);
    tracing::info!("----------------------------");

    let engine = NexoEngine::new(config.clone());
    
    let addr = format!("{}:{}", config.server.host, config.server.port);

    let engine_clone_for_dashboard = engine.clone();
    tokio::spawn(async move {
        dashboard::server::start_dashboard_server(engine_clone_for_dashboard, config.server.dashboard_port).await;
    });

    tracing::info!(host = %config.server.host, port = %config.server.port, "🚀 Nexo Server v0.2 Starting...");

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

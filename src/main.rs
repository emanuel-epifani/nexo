#![allow(dead_code, unused_imports, unused_variables)]

use nexo::config::Config;
use nexo::NexoEngine;
use nexo::server;
use nexo::dashboard;
use tokio::net::TcpListener;

// ========================================
// MAIN ENTRY POINT
// ========================================

#[tokio::main]
async fn main() {
    let config = Config::global();

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

    let engine = NexoEngine::new();
    
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

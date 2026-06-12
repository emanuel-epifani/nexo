#![allow(dead_code, unused_imports, unused_variables)]

use nexo::config::Config;
use nexo::NexoEngine;
use nexo::transport::tcp;
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
        .init();

    tracing::debug!("--- CONFIGURATION LOADED ---");
    tracing::debug!("{:#?}", config);
    tracing::debug!("----------------------------");

    let engine = NexoEngine::new(&config).await;

    let addr = format!("{}:{}", config.server.host, config.server.port);

    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");

    tracing::info!(
        host = %config.server.host,
        tcp_port = config.server.port,
        "Nexo ready"
    );

    loop {
        let (socket, client_addr) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        let engine_clone = engine.clone();

        tracing::debug!(client = %client_addr, "New connection accepted");

        tokio::spawn(async move {
            if let Err(e) = tcp::connection::handle_connection(socket, engine_clone).await {
                tracing::error!(client = %client_addr, error = %e, "Connection error");
            }
            tracing::debug!(client = %client_addr, "Connection closed");
        });
    }
}

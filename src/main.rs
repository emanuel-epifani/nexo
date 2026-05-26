#![allow(dead_code, unused_imports, unused_variables)]

use nexo::config::Config;
use nexo::NexoEngine;
use nexo::transport::{tcp, http};
use tokio::net::TcpListener;

// ========================================
// MAIN ENTRY POINT
// ========================================

fn parse_mode() -> bool {
    // Returns true when the dashboard should be started (i.e. `nexo dev`).
    // No-arg or `serve` -> false. Anything else -> usage + exit.
    match std::env::args().nth(1).as_deref() {
        None | Some("serve") => false,
        Some("dev") => true,
        Some(other) => {
            eprintln!("nexo: unknown subcommand '{}'", other);
            eprintln!("usage:");
            eprintln!("  nexo serve   start server (dashboard OFF) — default");
            eprintln!("  nexo dev     start server with dashboard (development)");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() {
    let dashboard_enabled = parse_mode();
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
    let engine_clone_for_dashboard = engine.clone();

    if dashboard_enabled {
        tokio::spawn(async move {
            http::router::start_http_server(engine_clone_for_dashboard, config.server.dashboard_port).await;
        });
    }

    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");

    let dashboard_url = dashboard_enabled
        .then(|| format!("http://{}:{}", config.server.host, config.server.dashboard_port));

    tracing::info!(
        host = %config.server.host,
        tcp_port = config.server.port,
        dashboard = dashboard_url.as_deref().unwrap_or("off"),
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

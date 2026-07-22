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

    let server_config = config.server.clone();

    tokio::select! {
        result = accept_loop(&listener, &engine, &server_config) => {
            tracing::error!("Accept loop exited unexpectedly: {:?}", result);
        }
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received, gracefully stopping...");
            engine.shutdown().await;
            tracing::info!("Nexo stopped gracefully");
        }
    }
}

async fn accept_loop(
    listener: &TcpListener,
    engine: &NexoEngine,
    server_config: &nexo::config::ServerConfig,
) -> Result<(), std::io::Error> {
    loop {
        let (socket, client_addr) = listener.accept().await?;

        let engine_clone = engine.clone();
        let server_config_clone = server_config.clone();

        tracing::debug!(client = %client_addr, "New connection accepted");

        tokio::spawn(async move {
            if let Err(e) = tcp::connection::handle_connection(socket, engine_clone, server_config_clone).await {
                tracing::error!(client = %client_addr, error = %e, "Connection error");
            }
            tracing::debug!(client = %client_addr, "Connection closed");
        });
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

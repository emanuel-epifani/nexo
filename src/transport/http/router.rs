use axum::Router;
use tower_http::compression::CompressionLayer;
use crate::NexoEngine;
use crate::transport::http::assets::static_handler;

pub async fn start_http_server(engine: NexoEngine, port: u16) {
    let app = Router::new()
        .merge(crate::brokers::store::http::routes())
        .merge(crate::brokers::queue::http::routes())
        .merge(crate::brokers::stream::http::routes())
        .merge(crate::brokers::pub_sub::http::routes())
        .layer(CompressionLayer::new())
        .fallback(static_handler)
        .with_state(engine);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("🌐 Dashboard available at http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.expect("Failed to bind dashboard port");

    axum::serve(listener, app).await.expect("Failed to start dashboard server");
}

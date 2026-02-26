use axum::{
    routing::get,
    Router,
    response::IntoResponse,
    http::{header, Uri, StatusCode},
    body::Body,
};
use tower_http::compression::CompressionLayer;
use rust_embed::RustEmbed;
use crate::NexoEngine;
use crate::dashboard::pubsub::get_pubsub;
use crate::dashboard::queue::{get_queue, get_queue_messages};
use crate::dashboard::stream::{get_stream, get_stream_messages};

// Embed the frontend build directory
#[derive(RustEmbed)]
#[folder = "dashboard/dist/"]
struct Assets;



pub async fn start_dashboard_server(engine: NexoEngine, port: u16) {
    let app = Router::new()
        .route("/api/store", get(crate::dashboard::store::get_store_handler))
        .route("/api/queue", get(get_queue))
        .route("/api/queue/{name}/messages", get(get_queue_messages))
        .route("/api/stream", get(get_stream))
        .route("/api/stream/{topic}/messages", get(get_stream_messages))
        .route("/api/pubsub", get(get_pubsub))
        .layer(CompressionLayer::new())
        .fallback(static_handler)
        .with_state(engine);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("🌐 Dashboard available at http://{}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("Failed to bind dashboard port");
    
    axum::serve(listener, app).await.expect("Failed to start dashboard server");
}




// Handler for serving embedded static files (SPA support)
async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();
    
    if path.is_empty() {
        path = "index.html".to_string();
    }

    match Assets::get(&path) {
        Some(content) => {
            let mime = mime_guess::from_path(&path).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], Body::from(content.data)).into_response()
        }
        None => {
            // SPA Fallback: if file not found, serve index.html
             match Assets::get("index.html") {
                Some(content) => {
                    let mime = mime_guess::from_path("index.html").first_or_octet_stream();
                    ([(header::CONTENT_TYPE, mime.as_ref())], Body::from(content.data)).into_response()
                }
                None => (StatusCode::NOT_FOUND, "Dashboard not found (index.html missing)").into_response()
             }
        }
    }
}

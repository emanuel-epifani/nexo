use axum::{
    routing::get,
    Router,
    response::{IntoResponse},
    extract::{State, Path, Query},
    http::{header, Uri, StatusCode},
    body::Body,
};
use serde::Deserialize;
use tower_http::compression::CompressionLayer;
use rust_embed::RustEmbed;
use crate::NexoEngine;
use crate::dashboard::dashboard_queue::{PaginatedMessages, PaginatedDlqMessages, get_queue, get_queue_messages};

// Embed the frontend build directory
#[derive(RustEmbed)]
#[folder = "dashboard/dist/"]
struct Assets;

#[derive(Deserialize)]
pub struct QueueMessagesQuery {
    pub state: String,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub search: Option<String>,
}

pub async fn start_dashboard_server(engine: NexoEngine, port: u16) {
    let app = Router::new()
        .route("/api/store", get(crate::dashboard::dashboard_store::get_store_handler))
        .route("/api/queue", get(get_queue))
        .route("/api/queue/{name}/messages", get(get_queue_messages))
        .route("/api/stream", get(get_stream))
        .route("/api/pubsub", get(get_pubsub))
        .layer(CompressionLayer::new())
        .fallback(static_handler)
        .with_state(engine);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("🌐 Dashboard available at http://{}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("Failed to bind dashboard port");
    
    axum::serve(listener, app).await.expect("Failed to start dashboard server");
}



async fn get_stream(State(engine): State<NexoEngine>) -> impl IntoResponse {
    let snapshot = engine.stream.get_snapshot().await;
    axum::Json(snapshot)
}

async fn get_pubsub(State(engine): State<NexoEngine>) -> impl IntoResponse {
    let snapshot = engine.pubsub.get_snapshot().await;
    axum::Json(snapshot)
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

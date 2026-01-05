use axum::{
    routing::get,
    Router,
    response::{IntoResponse, Response},
    extract::State,
    http::{StatusCode, header, Uri},
    body::Body,
};
use rust_embed::RustEmbed;
use crate::NexoEngine;

// Embed the frontend build directory
#[derive(RustEmbed)]
#[folder = "dashboard-web/dist/"] 
struct Assets;

pub async fn start_dashboard_server(engine: NexoEngine, port: u16) {
    let app = Router::new()
        .route("/api/state", get(get_state))
        .fallback(static_handler)
        .with_state(engine);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("🌐 Dashboard available at http://{}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("Failed to bind dashboard port");
    
    // Axum 0.8+ serve syntax
    axum::serve(listener, app).await.expect("Failed to start dashboard server");
}

async fn get_state(State(engine): State<NexoEngine>) -> impl IntoResponse {
    let snapshot = engine.get_global_snapshot();
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


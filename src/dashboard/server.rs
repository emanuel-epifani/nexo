use axum::{
    Router,
    response::IntoResponse,
    http::{header, Uri, StatusCode},
    body::Body,
};
use tower_http::compression::CompressionLayer;
use rust_embed::RustEmbed;
use crate::NexoEngine;

#[derive(RustEmbed)]
#[folder = "dashboard/dist/"]
struct Assets;

pub async fn start_dashboard_server(engine: NexoEngine, port: u16) {
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

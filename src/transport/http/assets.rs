use axum::{
    response::IntoResponse,
    http::{header, Uri, StatusCode},
    body::Body,
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "dashboard/dist/"]
struct Assets;

pub async fn static_handler(uri: Uri) -> impl IntoResponse {
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

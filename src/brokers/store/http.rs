//! Store broker HTTP surface.

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::brokers::store::snapshot::StoreSnapshot;
use crate::transport::http::payload::payload_to_json_value;
use crate::NexoEngine;

// ==========================================
// DTOs
// ==========================================

#[derive(Serialize)]
pub struct StoreBrokerSnapshot {
    pub keys: Vec<KeyDetail>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

#[derive(Serialize)]
pub struct KeyDetail {
    pub key: String,
    pub value: Value,
    pub exp_at: String,
}

#[derive(Deserialize)]
pub struct StoreQueryParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub search: Option<String>,
}

// ==========================================
// HANDLERS
// ==========================================

async fn get_store(
    State(engine): State<NexoEngine>,
    Query(params): Query<StoreQueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(500);
    let offset = params.offset.unwrap_or(0);
    let filter = params.search;

    let StoreSnapshot { entries, total } = engine.store.scan(limit, offset, filter);
    let now = std::time::Instant::now();

    let keys = entries
        .into_iter()
        .map(|entry| {
            let exp_at = match entry.expires_at {
                Some(expiry) => {
                    let sys_now = std::time::SystemTime::now();
                    let dur = expiry.saturating_duration_since(now);
                    let future_sys = sys_now + dur;
                    chrono::DateTime::<chrono::Utc>::from(future_sys).to_rfc3339()
                }
                None => "Never".to_string(),
            };
            KeyDetail {
                key: entry.key,
                value: payload_to_json_value(&entry.payload),
                exp_at,
            }
        })
        .collect();

    axum::Json(StoreBrokerSnapshot { keys, total, offset, limit })
}

// ==========================================
// ROUTES
// ==========================================

pub fn routes() -> Router<NexoEngine> {
    Router::new().route("/api/store", get(get_store))
}

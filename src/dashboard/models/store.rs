use serde::{Deserialize, Serialize};
use serde_json::Value;
use axum::{
    response::IntoResponse,
    extract::{State, Query},
};
use crate::NexoEngine;
use crate::brokers::store::types::Value as StoreValue;
use crate::brokers::store::map::MapValue;
use crate::server::protocol::payload_to_dashboard_value;
use std::time::Instant;

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
    pub exp_at: String, // ISO8601
}

#[derive(Deserialize)]
pub struct StoreQueryParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub search: Option<String>,
}

/// Handler for GET /api/store
pub async fn get_store_handler(
    State(engine): State<NexoEngine>, 
    Query(params): Query<StoreQueryParams>
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(500);
    let offset = params.offset.unwrap_or(0);
    let filter = params.search;

    let scan_result = engine.store.scan(limit, offset, filter);
    let mut keys_detail = Vec::new();
    let now = Instant::now();

    for (key, val, expires_at) in scan_result.items {
        // Skip expired keys
        if let Some(expiry) = expires_at {
            if expiry <= now {
                continue;
            }
        }
        
        let value = match &val {
            StoreValue::Map(MapValue(b)) => {
                payload_to_dashboard_value(b)
            }
        };

        let expires_at_str = if let Some(expiry) = expires_at {
            let sys_now = std::time::SystemTime::now();
            let dur: std::time::Duration = expiry.saturating_duration_since(now);
            let future_sys = sys_now + dur;
            chrono::DateTime::<chrono::Utc>::from(future_sys).to_rfc3339()
        } else {
             "Never".to_string()
        };

        keys_detail.push(KeyDetail {
            key,
            value,
            exp_at: expires_at_str,
        });
    }

    let snapshot = StoreBrokerSnapshot {
        keys: keys_detail,
        total: scan_result.total,
        offset,
        limit,
    };

    axum::Json(snapshot)
}

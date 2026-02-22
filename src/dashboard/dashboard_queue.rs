use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;
use crate::dashboard::server::QueueMessagesQuery;
use crate::NexoEngine;

#[derive(Serialize)]
pub struct QueueSummary {
    pub name: String,
    pub pending: usize,
    pub inflight: usize,
    pub scheduled: usize,
    pub dlq: usize,
}

#[derive(Serialize)]
pub struct PaginatedMessages {
    pub messages: Vec<MessageSummary>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct PaginatedDlqMessages {
    pub messages: Vec<DlqMessageSummary>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct DlqMessageSummary {
    pub id: Uuid,
    pub payload: Value,
    pub attempts: u32,
    pub failure_reason: Option<String>,
    pub created_at: u64,
}

#[derive(Serialize)]
pub struct MessageSummary {
    pub id: Uuid,
    pub payload: Value,
    pub state: String, // "Pending", "InFlight", "Scheduled"
    pub priority: u8,
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_delivery_at: Option<String>,
}


pub async fn get_queue(State(engine): State<NexoEngine>) -> impl IntoResponse {
    let snapshot = engine.queue.get_snapshot().await;
    axum::Json(snapshot)
}

pub async fn get_queue_messages(
    State(engine): State<NexoEngine>,
    Path(name): Path<String>,
    Query(query): Query<QueueMessagesQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).min(1000); // Hard limit 1000
    let offset = query.offset.unwrap_or(0);
    let state_filter = query.state.to_lowercase();
    let search = query.search;

    if state_filter == "dlq" {
        match engine.queue.peek_dlq(&name, limit, offset).await {
            Ok((total, dlq_msgs)) => {
                let messages = dlq_msgs.into_iter().map(|msg| crate::dashboard::dashboard_queue::DlqMessageSummary {
                    id: msg.id,
                    payload: crate::dashboard::utils::payload_to_dashboard_value(&msg.payload),
                    attempts: msg.attempts,
                    failure_reason: Some(msg.failure_reason),
                    created_at: msg.created_at,
                }).collect();

                axum::Json(serde_json::to_value(PaginatedDlqMessages {
                    messages,
                    total,
                }).unwrap()).into_response()
            },
            Err(e) => (StatusCode::NOT_FOUND, e).into_response()
        }
    } else {
        match engine.queue.get_messages(name, state_filter, offset, limit, search).await {
            Some((total, messages)) => {
                axum::Json(serde_json::to_value(PaginatedMessages {
                    messages,
                    total,
                }).unwrap()).into_response()
            },
            None => (StatusCode::NOT_FOUND, "Queue not found").into_response()
        }
    }
}
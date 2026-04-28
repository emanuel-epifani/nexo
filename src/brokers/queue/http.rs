//! Queue broker HTTP surface: DTOs (with `Serialize`), mapping from domain
//! snapshots, axum handlers, and the `routes()` sub-router.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::brokers::queue::domain::dlq::DlqMessage;
use crate::brokers::queue::domain::queue::QueueConfig;
use crate::brokers::queue::snapshot::{MessageStateTag, QueueMessagePreview, QueueSnapshot};
use crate::transport::http::payload::payload_to_json_value;
use crate::NexoEngine;

// ==========================================
// DTOs
// ==========================================

#[derive(Serialize)]
pub struct QueueSummary {
    pub name: String,
    pub pending: usize,
    pub inflight: usize,
    pub dlq: usize,
    pub config: QueueConfig,
}

impl From<QueueSnapshot> for QueueSummary {
    fn from(s: QueueSnapshot) -> Self {
        Self {
            name: s.name,
            pending: s.pending,
            inflight: s.inflight,
            dlq: s.dlq,
            config: s.config,
        }
    }
}

#[derive(Serialize)]
pub struct MessageSummary {
    pub id: Uuid,
    pub payload: Value,
    pub state: String,
    pub priority: u8,
    pub attempts: u32,
}

impl From<QueueMessagePreview> for MessageSummary {
    fn from(m: QueueMessagePreview) -> Self {
        let state = match m.state {
            MessageStateTag::Pending => "pending".to_string(),
            MessageStateTag::InFlight => "inflight".to_string(),
        };
        Self {
            id: m.id,
            payload: payload_to_json_value(&m.payload),
            state,
            priority: m.priority,
            attempts: m.attempts,
        }
    }
}

#[derive(Serialize)]
pub struct DlqMessageSummary {
    pub id: Uuid,
    pub payload: Value,
    pub attempts: u32,
    pub failure_reason: Option<String>,
    pub created_at: u64,
}

impl From<DlqMessage> for DlqMessageSummary {
    fn from(m: DlqMessage) -> Self {
        Self {
            id: m.id,
            payload: payload_to_json_value(&m.payload),
            attempts: m.attempts,
            failure_reason: Some(m.failure_reason),
            created_at: m.created_at,
        }
    }
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

#[derive(Deserialize)]
pub struct QueueMessagesQuery {
    pub state: String,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub search: Option<String>,
}

// ==========================================
// HANDLERS
// ==========================================

async fn get_queue(State(engine): State<NexoEngine>) -> impl IntoResponse {
    let snap = engine.queue.get_snapshot().await;
    let dto: Vec<QueueSummary> = snap.into_iter().map(Into::into).collect();
    axum::Json(dto)
}

async fn get_queue_messages(
    State(engine): State<NexoEngine>,
    Path(name): Path<String>,
    Query(query): Query<QueueMessagesQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).min(1000);
    let offset = query.offset.unwrap_or(0);
    let state_filter = query.state.to_lowercase();
    let search = query.search;

    if state_filter == "dlq" {
        match engine.queue.peek_dlq(&name, limit, offset).await {
            Ok((total, dlq_msgs)) => {
                let messages: Vec<DlqMessageSummary> = dlq_msgs.into_iter().map(Into::into).collect();
                axum::Json(PaginatedDlqMessages { messages, total }).into_response()
            }
            Err(e) => (StatusCode::NOT_FOUND, e).into_response(),
        }
    } else {
        match engine.queue.get_messages(name, state_filter, offset, limit, search).await {
            Some((total, previews)) => {
                let messages: Vec<MessageSummary> = previews.into_iter().map(Into::into).collect();
                axum::Json(PaginatedMessages { messages, total }).into_response()
            }
            None => (StatusCode::NOT_FOUND, "Queue not found").into_response(),
        }
    }
}

// ==========================================
// ROUTES
// ==========================================

pub fn routes() -> Router<NexoEngine> {
    Router::new()
        .route("/api/queue", get(get_queue))
        .route("/api/queue/{name}/messages", get(get_queue_messages))
}



//! Stream broker HTTP surface.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::brokers::stream::snapshot::{ConsumerGroupSnapshot, StreamSnapshot, TopicSnapshot};
use crate::brokers::stream::topic::TopicConfig;
use crate::transport::http::payload::payload_to_json_value;
use crate::NexoEngine;

const STREAM_PAGE_SIZE: usize = 50;
const STREAM_MAX_PAGE_SIZE: usize = 500;

// ==========================================
// DTOs
// ==========================================

#[derive(Serialize)]
pub struct StreamBrokerSnapshot {
    pub topics: Vec<TopicSummary>,
}

impl From<StreamSnapshot> for StreamBrokerSnapshot {
    fn from(s: StreamSnapshot) -> Self {
        Self { topics: s.topics.into_iter().map(Into::into).collect() }
    }
}

#[derive(Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub last_seq: u64,
    pub groups: Vec<ConsumerGroupSummary>,
    pub config: TopicConfig,
}

impl From<TopicSnapshot> for TopicSummary {
    fn from(t: TopicSnapshot) -> Self {
        Self {
            name: t.name,
            last_seq: t.last_seq,
            groups: t.groups.into_iter().map(Into::into).collect(),
            config: t.config,
        }
    }
}

#[derive(Serialize)]
pub struct ConsumerGroupSummary {
    pub id: String,
    pub ack_floor: u64,
    pub pending_count: usize,
}

impl From<ConsumerGroupSnapshot> for ConsumerGroupSummary {
    fn from(g: ConsumerGroupSnapshot) -> Self {
        Self { id: g.id, ack_floor: g.ack_floor, pending_count: g.pending_count }
    }
}

#[derive(Serialize)]
pub struct MessagePreview {
    pub seq: u64,
    pub timestamp: String,
    pub payload: Value,
}

#[derive(Serialize)]
pub struct StreamMessages {
    pub messages: Vec<MessagePreview>,
    pub from_seq: u64,
    pub limit: usize,
    pub last_seq: u64,
}

#[derive(Deserialize)]
pub struct StreamMessagesQuery {
    pub from: Option<u64>,
    pub limit: Option<usize>,
}

// ==========================================
// HANDLERS
// ==========================================

async fn get_stream(State(engine): State<NexoEngine>) -> impl IntoResponse {
    let snap = engine.stream.get_snapshot().await;
    axum::Json(StreamBrokerSnapshot::from(snap))
}

async fn get_stream_messages(
    State(engine): State<NexoEngine>,
    Path(topic): Path<String>,
    Query(query): Query<StreamMessagesQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(STREAM_PAGE_SIZE).min(STREAM_MAX_PAGE_SIZE);

    let snap = engine.stream.get_snapshot().await;
    let topic_info = snap.topics.iter().find(|t| t.name == topic);

    let last_seq = match topic_info {
        Some(t) => t.last_seq,
        None => return (StatusCode::NOT_FOUND, "Topic not found").into_response(),
    };

    let from_seq = query.from.unwrap_or_else(|| last_seq.saturating_sub(limit as u64).max(1));

    let messages = engine.stream.read(&topic, from_seq, limit).await
        .into_iter()
        .rev()
        .map(|msg| MessagePreview {
            seq: msg.seq,
            timestamp: chrono::DateTime::from_timestamp_millis(msg.timestamp as i64)
                .unwrap_or_default()
                .to_rfc3339(),
            payload: payload_to_json_value(&msg.payload),
        })
        .collect();

    axum::Json(StreamMessages { messages, from_seq, limit, last_seq }).into_response()
}

// ==========================================
// ROUTES
// ==========================================

pub fn routes() -> Router<NexoEngine> {
    Router::new()
        .route("/api/stream", get(get_stream))
        .route("/api/stream/{topic}/messages", get(get_stream_messages))
}

use axum::extract::{State, Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::NexoEngine;

const STREAM_PAGE_SIZE: usize = 50;
const STREAM_MAX_PAGE_SIZE: usize = 500;

#[derive(Serialize)]
pub struct StreamBrokerSnapshot {
    pub topics: Vec<TopicSummary>,
}

#[derive(Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub last_seq: u64,
    pub groups: Vec<ConsumerGroupSummary>,
}

#[derive(Serialize)]
pub struct ConsumerGroupSummary {
    pub id: String,
    pub ack_floor: u64,
    pub pending_count: usize,
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

pub async fn get_stream(State(engine): State<NexoEngine>) -> impl IntoResponse {
    let snapshot = engine.stream.get_snapshot().await;
    axum::Json(snapshot)
}

pub async fn get_stream_messages(
    State(engine): State<NexoEngine>,
    Path(topic): Path<String>,
    Query(query): Query<StreamMessagesQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(STREAM_PAGE_SIZE).min(STREAM_MAX_PAGE_SIZE);

    let snapshot = engine.stream.get_snapshot().await;
    let topic_info = snapshot.topics.iter().find(|t| t.name == topic);

    let last_seq = match topic_info {
        Some(t) => t.last_seq,
        None => return (StatusCode::NOT_FOUND, "Topic not found").into_response(),
    };

    let from_seq = query.from.unwrap_or_else(|| last_seq.saturating_sub(limit as u64).max(1));

    let messages_raw = engine.stream.read(&topic, from_seq, limit).await;

    let messages = messages_raw.into_iter().map(|msg| MessagePreview {
        seq: msg.seq,
        timestamp: chrono::DateTime::from_timestamp_millis(msg.timestamp as i64)
            .unwrap_or_default()
            .to_rfc3339(),
        payload: crate::dashboard::utils::payload_to_dashboard_value(&msg.payload),
    }).collect();

    axum::Json(StreamMessages { messages, from_seq, limit, last_seq }).into_response()
}

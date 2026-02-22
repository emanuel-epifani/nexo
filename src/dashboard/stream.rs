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
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Serialize)]
pub struct PartitionInfo {
    pub id: u32,
    pub groups: Vec<ConsumerGroupSummary>,
    pub last_offset: u64,
}

#[derive(Serialize)]
pub struct ConsumerGroupSummary {
    pub id: String,
    pub committed_offset: u64,
}

#[derive(Serialize)]
pub struct MessagePreview {
    pub offset: u64,
    pub timestamp: String,
    pub payload: Value,
}

#[derive(Serialize)]
pub struct StreamMessages {
    pub messages: Vec<MessagePreview>,
    pub from_offset: u64,
    pub limit: usize,
    pub last_offset: u64,
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
    Path((topic, partition)): Path<(String, u32)>,
    Query(query): Query<StreamMessagesQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(STREAM_PAGE_SIZE).min(STREAM_MAX_PAGE_SIZE);

    let snapshot = engine.stream.get_snapshot().await;
    let partition_info = snapshot.topics.iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.id == partition));

    let last_offset = match partition_info {
        Some(p) => p.last_offset,
        None => return (StatusCode::NOT_FOUND, "Topic or partition not found").into_response(),
    };

    // Default: load the last N messages
    let from_offset = query.from.unwrap_or_else(|| last_offset.saturating_sub(limit as u64));

    let messages_raw = engine.stream.read_partition(&topic, partition, from_offset, limit).await;

    let messages = messages_raw.into_iter().map(|msg| MessagePreview {
        offset: msg.offset,
        timestamp: chrono::DateTime::from_timestamp_millis(msg.timestamp as i64)
            .unwrap_or_default()
            .to_rfc3339(),
        payload: crate::dashboard::utils::payload_to_dashboard_value(&msg.payload),
    }).collect();

    axum::Json(StreamMessages { messages, from_offset, limit, last_offset }).into_response()
}

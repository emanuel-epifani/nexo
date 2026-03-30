use axum::extract::{State, Query};
use axum::response::IntoResponse;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::NexoEngine;
use crate::dashboard::utils::payload_to_dashboard_value;

const PUBSUB_PAGE_SIZE: usize = 50;
const PUBSUB_MAX_PAGE_SIZE: usize = 500;

#[derive(Serialize)]
pub struct PubSubBrokerSnapshot {
    pub active_clients: usize,
    pub total_topics: usize,
    pub topics: Vec<TopicSnapshot>,
    pub wildcards: WildcardSubscriptions,
}

#[derive(Serialize, Clone)]
pub struct WildcardSubscriptions {
    pub multi_level: Vec<WildcardSubscription>,
    pub single_level: Vec<WildcardSubscription>,
}

#[derive(Serialize, Clone)]
pub struct WildcardSubscription {
    pub pattern: String,
    pub client_id: String,
}

#[derive(Serialize, Clone)]
pub struct TopicSnapshot {
    pub full_path: String,
    pub subscribers: usize,
    pub retained_value: Option<Value>,
}

#[derive(Clone)]
pub struct RawPubSubBrokerSnapshot {
    pub active_clients: usize,
    pub total_topics: usize,
    pub topics: Vec<RawTopicSnapshot>,
    pub wildcards: WildcardSubscriptions,
}

#[derive(Clone)]
pub struct RawTopicSnapshot {
    pub full_path: String,
    pub subscribers: usize,
    pub retained_payload: Option<Bytes>,
}

#[derive(Deserialize)]
pub struct PubSubQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub search: Option<String>,
}

pub async fn get_pubsub(
    State(engine): State<NexoEngine>,
    Query(query): Query<PubSubQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(PUBSUB_PAGE_SIZE).min(PUBSUB_MAX_PAGE_SIZE);
    let offset = query.offset.unwrap_or(0);
    let snapshot = engine.pubsub.scan_topics(limit, offset, query.search.as_deref());
    let snapshot = PubSubBrokerSnapshot {
        active_clients: snapshot.active_clients,
        total_topics: snapshot.total_topics,
        topics: snapshot.topics.into_iter().map(|topic| TopicSnapshot {
            full_path: topic.full_path,
            subscribers: topic.subscribers,
            retained_value: topic.retained_payload.as_ref().map(|payload| payload_to_dashboard_value(payload)),
        }).collect(),
        wildcards: snapshot.wildcards,
    };
    axum::Json(snapshot)
}
//! PubSub broker HTTP surface.

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::brokers::pub_sub::snapshot::{PubSubSnapshot, WildcardSubscription, WildcardSubscriptions};
use crate::transport::http::payload::payload_to_json_value;
use crate::NexoEngine;

const PUBSUB_PAGE_SIZE: usize = 50;
const PUBSUB_MAX_PAGE_SIZE: usize = 500;

// ==========================================
// DTOs
// ==========================================

#[derive(Serialize)]
pub struct PubSubBrokerSnapshot {
    pub active_clients: usize,
    pub total_topics: usize,
    pub topics: Vec<TopicSummary>,
    pub wildcards: WildcardSubscriptionsDto,
}

#[derive(Serialize, Clone)]
pub struct TopicSummary {
    pub full_path: String,
    pub subscribers: usize,
    pub retained_value: Option<Value>,
}

#[derive(Serialize, Clone)]
pub struct WildcardSubscriptionsDto {
    pub multi_level: Vec<WildcardSubscriptionDto>,
    pub single_level: Vec<WildcardSubscriptionDto>,
}

#[derive(Serialize, Clone)]
pub struct WildcardSubscriptionDto {
    pub pattern: String,
    pub client_id: String,
}

impl From<WildcardSubscription> for WildcardSubscriptionDto {
    fn from(w: WildcardSubscription) -> Self {
        Self { pattern: w.pattern, client_id: w.client_id }
    }
}

impl From<WildcardSubscriptions> for WildcardSubscriptionsDto {
    fn from(w: WildcardSubscriptions) -> Self {
        Self {
            multi_level: w.multi_level.into_iter().map(Into::into).collect(),
            single_level: w.single_level.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<PubSubSnapshot> for PubSubBrokerSnapshot {
    fn from(s: PubSubSnapshot) -> Self {
        Self {
            active_clients: s.active_clients,
            total_topics: s.total_topics,
            topics: s.topics.into_iter().map(|topic| TopicSummary {
                full_path: topic.full_path,
                subscribers: topic.subscribers,
                retained_value: topic.retained_payload.as_ref().map(|p| payload_to_json_value(p)),
            }).collect(),
            wildcards: s.wildcards.into(),
        }
    }
}

#[derive(Deserialize)]
pub struct PubSubQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub search: Option<String>,
}

// ==========================================
// HANDLERS
// ==========================================

async fn get_pubsub(
    State(engine): State<NexoEngine>,
    Query(query): Query<PubSubQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(PUBSUB_PAGE_SIZE).min(PUBSUB_MAX_PAGE_SIZE);
    let offset = query.offset.unwrap_or(0);
    let snap = engine.pubsub.scan_topics(limit, offset, query.search.as_deref());
    axum::Json(PubSubBrokerSnapshot::from(snap))
}

// ==========================================
// ROUTES
// ==========================================

pub fn routes() -> Router<NexoEngine> {
    Router::new().route("/api/pubsub", get(get_pubsub))
}

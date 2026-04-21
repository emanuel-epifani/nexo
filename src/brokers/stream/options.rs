//! Stream option types shared between the manager and the TCP adapter.
//! `RetentionOptions` is also persisted inside `TopicConfig` (hence `Serialize`).

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RetentionOptions {
    pub max_age_ms: Option<u64>,
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamCreateOptions {
    pub retention: Option<RetentionOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeekTarget {
    Beginning,
    End,
}

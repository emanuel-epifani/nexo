//! Stream option types shared between the manager and the TCP adapter.
//! `RetentionOptions` is also persisted inside `StreamConfig` (hence `Serialize`).

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionOptions {
    pub max_age_ms: Option<u64>,
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Default, Clone)]
pub struct StreamCreateOptions {
    pub retention: Option<RetentionOptions>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SeekTarget {
    Beginning,
    End,
}

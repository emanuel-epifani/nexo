//! Queue definition: pure domain logic for queue configuration.

use serde::{Deserialize, Serialize};

use crate::brokers::queue::config::SystemQueueConfig;
use crate::brokers::queue::options::QueueCreateOptions;

/// Authoritative per-queue configuration, persisted as JSON inside the
/// shared queue database at provision time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub visibility_timeout_ms: u64,
    pub max_deliveries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDefinition {
    pub name: String,
    pub config: QueueConfig,
}

impl QueueConfig {
    pub fn from_options(opts: QueueCreateOptions, sys: &SystemQueueConfig) -> Self {
        Self {
            visibility_timeout_ms: opts
                .visibility_timeout_ms
                .unwrap_or(sys.visibility_timeout_ms),
            max_deliveries: opts.max_deliveries.unwrap_or(sys.max_deliveries),
        }
    }
}

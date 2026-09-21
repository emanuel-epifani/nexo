//! Stream definition: Pure domain logic for stream configuration.

use crate::brokers::stream::config::SystemStreamConfig;
use crate::brokers::stream::options::{RetentionOptions, StreamCreateOptions};
use serde::{Deserialize, Serialize};

/// Authoritative per-stream configuration, persisted as JSON inside the
/// shared stream database at provision time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub retention: RetentionOptions,
    pub max_ack_pending: usize,
    pub ack_wait_ms: u64,
    pub max_deliveries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDefinition {
    pub name: String,
    pub config: StreamConfig,
}

impl StreamConfig {
    pub fn from_options(opts: StreamCreateOptions, sys: &SystemStreamConfig) -> Self {
        let retention = match opts.retention {
            Some(r) => RetentionOptions {
                max_age_ms: r
                    .max_age_ms
                    .map_or(Some(sys.default_retention_age_ms), |v| {
                        if v == 0 {
                            None
                        } else {
                            Some(v)
                        }
                    }),
                max_bytes: r.max_bytes.map_or(Some(sys.default_retention_bytes), |v| {
                    if v == 0 {
                        None
                    } else {
                        Some(v)
                    }
                }),
            },
            None => RetentionOptions {
                max_age_ms: Some(sys.default_retention_age_ms),
                max_bytes: Some(sys.default_retention_bytes),
            },
        };

        Self {
            retention,
            max_ack_pending: sys.max_ack_pending,
            ack_wait_ms: sys.ack_wait_ms,
            max_deliveries: sys.max_deliveries,
        }
    }
}

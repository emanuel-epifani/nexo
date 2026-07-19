//! Topic: Pure Logic Struct (No Actors, No Channels)
//! Single append-only log per topic (no partitions).

use crate::brokers::stream::options::{StreamCreateOptions, RetentionOptions};
use crate::brokers::stream::config::SystemStreamConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub max_segment_size: u64,
    pub retention: RetentionOptions,
    pub max_ack_pending: usize,
    pub ack_wait_ms: u64,
    pub max_deliveries: u32,
}

impl TopicConfig {
    pub fn from_options(opts: StreamCreateOptions, sys: &SystemStreamConfig) -> Self {
        let retention = match opts.retention {
            Some(r) => RetentionOptions {
                max_age_ms: r.max_age_ms.map_or(
                    Some(sys.default_retention_age_ms),
                    |v| if v == 0 { None } else { Some(v) }
                ),
                max_bytes: r.max_bytes.map_or(
                    Some(sys.default_retention_bytes),
                    |v| if v == 0 { None } else { Some(v) }
                ),
            },
            None => RetentionOptions {
                max_age_ms: Some(sys.default_retention_age_ms),
                max_bytes: Some(sys.default_retention_bytes),
            }
        };

        Self {
            max_segment_size: sys.max_segment_size,
            retention,
            max_ack_pending: sys.max_ack_pending,
            ack_wait_ms: sys.ack_wait_ms,
            max_deliveries: sys.max_deliveries,
        }
    }
}

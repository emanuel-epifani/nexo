//! PubSub option types shared between the manager and the TCP adapter.

use serde::Deserialize;

use crate::brokers::pub_sub::config::PubSubConfig;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubPublishOptions {
    pub retain: Option<bool>,
    pub ttl: Option<u64>,
}

/// Resolved publish configuration after merging client options with system defaults
#[derive(Debug)]
pub struct PubSubPublishConfig {
    pub retain: bool,
    pub ttl_seconds: u64,
}

impl PubSubPublishConfig {
    pub fn from_options(opts: PubSubPublishOptions, sys: &PubSubConfig) -> Self {
        Self {
            retain: opts.retain.unwrap_or(false),
            ttl_seconds: opts.ttl.unwrap_or(sys.default_retained_ttl_seconds),
        }
    }
}

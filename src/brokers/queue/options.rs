//! Queue option types shared between the manager (domain API) and the TCP
//! adapter (wire parsing). They live here (not in `tcp.rs`) because the
//! manager API consumes them directly.

use serde::Deserialize;

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueueCreateOptions {
    pub visibility_timeout_ms: Option<u64>,
    pub max_retries: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueuePushOptions {
    pub priority: Option<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueConsumeOptions {
    pub batch_size: Option<usize>,
    pub wait_ms: Option<u64>,
}

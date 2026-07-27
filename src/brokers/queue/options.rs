//! Queue option types shared between the manager (domain API) and the TCP
//! adapter (wire parsing). They live here (not in `tcp.rs`) because the
//! manager API consumes them directly.

#[derive(Debug, Default, Clone)]
pub struct QueueCreateOptions {
    pub visibility_timeout_ms: Option<u64>,
    pub max_deliveries: Option<u32>,
}

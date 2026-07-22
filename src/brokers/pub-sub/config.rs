#[derive(Debug, Clone)]
pub struct PubSubConfig {
    pub persistence_path: String,
    pub default_retained_ttl_seconds: u32,
    pub cleanup_interval_seconds: u64,
    pub retained_flush_ms: u64,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            persistence_path: "./data/pubsub".to_string(),
            default_retained_ttl_seconds: 3600,
            cleanup_interval_seconds: 60,
            retained_flush_ms: 500,
        }
    }
}

impl PubSubConfig {
    pub fn load() -> Self {
        let default = Self::default();
        Self {
            persistence_path: crate::config::get_env("PUBSUB_ROOT_PERSISTENCE_PATH", default.persistence_path),
            default_retained_ttl_seconds: crate::config::get_env("PUBSUB_DEFAULT_RETAINED_TTL_SECS", default.default_retained_ttl_seconds),
            cleanup_interval_seconds: crate::config::get_env("PUBSUB_CLEANUP_INTERVAL_SECS", default.cleanup_interval_seconds),
            retained_flush_ms: crate::config::get_env("PUBSUB_RETAINED_FLUSH_MS", default.retained_flush_ms),
        }
    }
}

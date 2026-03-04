use std::env;

#[derive(Debug, Clone)]
pub struct PubSubConfig {
    pub actor_channel_capacity: usize,
    pub persistence_path: String,
    pub default_retained_ttl_seconds: u64,
    pub cleanup_interval_seconds: u64,
    pub retained_flush_ms: u64,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            actor_channel_capacity: 10000,
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
            actor_channel_capacity: get_env("PUBSUB_ACTOR_CHAN_CAP", default.actor_channel_capacity),
            persistence_path: get_env_str("PUBSUB_ROOT_PERSISTENCE_PATH", &default.persistence_path),
            default_retained_ttl_seconds: get_env("PUBSUB_DEFAULT_RETAINED_TTL_SECS", default.default_retained_ttl_seconds),
            cleanup_interval_seconds: get_env("PUBSUB_CLEANUP_INTERVAL_SECS", default.cleanup_interval_seconds),
            retained_flush_ms: get_env("PUBSUB_RETAINED_FLUSH_MS", default.retained_flush_ms),
        }
    }
}

fn get_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(default)
}

fn get_env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

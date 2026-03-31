use std::env;

#[derive(Debug, Clone)]
pub struct SystemQueueConfig {
    // CREATE config
    pub visibility_timeout_ms: u64,
    pub max_retries: u32,
    pub ttl_ms: u64,
    // PUSH config
    pub default_batch_size: usize,
    pub default_wait_ms: u64,
    // PERSISTENCE config
    pub persistence_path: String,
    pub default_flush_ms: u64,
    pub writer_batch_size: usize,
}

impl Default for SystemQueueConfig {
    fn default() -> Self {
        Self {
            visibility_timeout_ms: 30000,
            max_retries: 5,
            ttl_ms: 604800000,
            default_batch_size: 10,
            default_wait_ms: 0,
            persistence_path: "./data/queues".to_string(),
            default_flush_ms: 100,
            writer_batch_size: 50000,
        }
    }
}

impl SystemQueueConfig {
    pub fn load() -> Self {
        let default = Self::default();
        Self {
            visibility_timeout_ms: get_env("QUEUE_VISIBILITY_MS", default.visibility_timeout_ms),
            max_retries:           get_env("QUEUE_MAX_RETRIES", default.max_retries),
            ttl_ms:                get_env("QUEUE_TTL_MS", default.ttl_ms),
            default_batch_size:    get_env("QUEUE_DEFAULT_BATCH_SIZE", default.default_batch_size),
            default_wait_ms:       get_env("QUEUE_DEFAULT_WAIT_MS", default.default_wait_ms),
            persistence_path:      get_env_str("QUEUE_ROOT_PERSISTENCE_PATH", &default.persistence_path),
            default_flush_ms:      get_env("QUEUE_DEFAULT_FLUSH_MS", default.default_flush_ms),
            writer_batch_size:     get_env("QUEUE_WRITER_BATCH_SIZE", default.writer_batch_size),
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

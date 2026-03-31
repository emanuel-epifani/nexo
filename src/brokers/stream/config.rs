use std::env;

#[derive(Debug, Clone)]
pub struct SystemStreamConfig {
    pub persistence_path: String,
    pub default_flush_ms: u64,
    pub max_segment_size: u64,
    pub retention_check_interval_ms: u64,
    pub default_retention_bytes: u64,
    pub default_retention_age_ms: u64,
    pub eviction_interval_ms: u64,
    pub ram_soft_limit: usize,
    pub ram_hard_limit: usize,
    pub max_ack_pending: usize,
    pub max_open_files: usize,
    pub ack_wait_ms: u64,
}

impl Default for SystemStreamConfig {
    fn default() -> Self {
        Self {
            persistence_path: "./data/streams".to_string(),
            default_flush_ms: 50,
            max_segment_size: 104857600, // 100MB
            retention_check_interval_ms: 600000,  // 10 minutes
            default_retention_bytes: 1073741824, // 1GB
            default_retention_age_ms: 604800000, // 7 days
            eviction_interval_ms: 500,
            ram_soft_limit: 1000,
            ram_hard_limit: 20000,
            max_ack_pending: 10000,
            max_open_files: 256,
            ack_wait_ms: 30000, // 30 seconds
        }
    }
}

impl SystemStreamConfig {
    pub fn load() -> Self {
        let default = Self::default();
        Self {
            persistence_path:            get_env_str("STREAM_ROOT_PERSISTENCE_PATH", &default.persistence_path),
            default_flush_ms:            get_env("STREAM_DEFAULT_FLUSH_MS", default.default_flush_ms),
            max_segment_size:            get_env("STREAM_MAX_SEGMENT_SIZE", default.max_segment_size),
            retention_check_interval_ms: get_env("STREAM_RETENTION_CHECK_MS", default.retention_check_interval_ms),
            default_retention_bytes:     get_env("STREAM_DEFAULT_RETENTION_BYTES", default.default_retention_bytes),
            default_retention_age_ms:    get_env("STREAM_DEFAULT_RETENTION_AGE_MS", default.default_retention_age_ms),
            eviction_interval_ms:        get_env("STREAM_EVICTION_INTERVAL_MS", default.eviction_interval_ms),
            ram_soft_limit:              get_env("STREAM_RAM_SOFT_LIMIT", default.ram_soft_limit),
            ram_hard_limit:              get_env("STREAM_RAM_HARD_LIMIT", default.ram_hard_limit),
            max_ack_pending:             get_env("STREAM_MAX_ACK_PENDING", default.max_ack_pending),
            max_open_files:              get_env("STREAM_MAX_OPEN_FILES", default.max_open_files),
            ack_wait_ms:                 get_env("STREAM_ACK_WAIT_MS", default.ack_wait_ms),
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

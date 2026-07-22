#[derive(Debug, Clone)]
pub struct SystemStreamConfig {
    pub persistence_path: String,
    pub default_flush_ms: u64,
    pub max_segment_size: u64,
    pub retention_check_interval_ms: u64,
    pub default_retention_bytes: u64,
    pub default_retention_age_ms: u64,
    pub max_ack_pending: usize,
    pub max_open_files: usize,
    pub ack_wait_ms: u64,
    pub max_deliveries: u32,
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
            max_ack_pending: 10000,
            max_open_files: 256,
            ack_wait_ms: 30000, // 30 seconds
            max_deliveries: 5,
        }
    }
}

impl SystemStreamConfig {
    pub fn load() -> Self {
        let default = Self::default();
        Self {
            persistence_path:            crate::config::get_env("STREAM_ROOT_PERSISTENCE_PATH", default.persistence_path),
            default_flush_ms:            crate::config::get_env("STREAM_DEFAULT_FLUSH_MS", default.default_flush_ms),
            max_segment_size:            crate::config::get_env("STREAM_MAX_SEGMENT_SIZE", default.max_segment_size),
            retention_check_interval_ms: crate::config::get_env("STREAM_RETENTION_CHECK_MS", default.retention_check_interval_ms),
            default_retention_bytes:     crate::config::get_env("STREAM_DEFAULT_RETENTION_BYTES", default.default_retention_bytes),
            default_retention_age_ms:    crate::config::get_env("STREAM_DEFAULT_RETENTION_AGE_MS", default.default_retention_age_ms),
            max_ack_pending:             crate::config::get_env("STREAM_MAX_ACK_PENDING", default.max_ack_pending),
            max_open_files:              crate::config::get_env("STREAM_MAX_OPEN_FILES", default.max_open_files),
            ack_wait_ms:                 crate::config::get_env("STREAM_ACK_WAIT_MS", default.ack_wait_ms),
            max_deliveries:              crate::config::get_env("STREAM_MAX_DELIVERIES", default.max_deliveries),
        }
    }
}

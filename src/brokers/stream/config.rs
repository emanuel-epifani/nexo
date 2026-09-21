#[derive(Debug, Clone)]
pub struct SystemStreamConfig {
    /// Root directory containing the shared stream database (`streams.sqlite3`)
    /// and the process-ownership lock file.
    pub persistence_path: String,
    /// Bounded command channel between submitters and the SQLite writer
    /// (message-count bound).
    pub storage_queue_capacity: usize,
    /// Byte bound on queued command payloads; submitters apply backpressure
    /// once in-flight command bytes exceed this.
    pub storage_queue_max_bytes: usize,
    /// Max encoded size of one FETCH response (sum of per-item wire bytes plus
    /// the 4-byte count header). Clamped to the server `max_payload_size` when
    /// the engine is assembled.
    pub fetch_response_bytes: usize,
    /// Periodic retention pass interval.
    pub retention_check_interval_ms: u64,
    pub default_retention_bytes: u64,
    pub default_retention_age_ms: u64,
    /// Default per-group cap on outstanding (leased-but-unacked) deliveries.
    /// Persisted per stream at provision time; later default changes never
    /// rewrite existing streams.
    pub max_ack_pending: usize,
    pub ack_wait_ms: u64,
    pub max_deliveries: u32,
}

impl Default for SystemStreamConfig {
    fn default() -> Self {
        Self {
            persistence_path: "./data/streams".to_string(),
            storage_queue_capacity: 16384,
            storage_queue_max_bytes: 268435456, // 256MB
            fetch_response_bytes: 10485760,     // 10MB
            retention_check_interval_ms: 600000, // 10 minutes
            default_retention_bytes: 1073741824, // 1GB
            default_retention_age_ms: 604800000, // 7 days
            max_ack_pending: 10000,
            ack_wait_ms: 30000, // 30 seconds
            max_deliveries: 5,
        }
    }
}

impl SystemStreamConfig {
    pub fn load() -> Self {
        let default = Self::default();
        Self {
            persistence_path: crate::config::get_env(
                "STREAM_ROOT_PERSISTENCE_PATH",
                default.persistence_path,
            ),
            storage_queue_capacity: crate::config::get_env(
                "STREAM_STORAGE_QUEUE_CAPACITY",
                default.storage_queue_capacity,
            ),
            storage_queue_max_bytes: crate::config::get_env(
                "STREAM_STORAGE_QUEUE_MAX_BYTES",
                default.storage_queue_max_bytes,
            ),
            fetch_response_bytes: crate::config::get_env(
                "STREAM_FETCH_RESPONSE_BYTES",
                default.fetch_response_bytes,
            ),
            retention_check_interval_ms: crate::config::get_env(
                "STREAM_RETENTION_CHECK_MS",
                default.retention_check_interval_ms,
            ),
            default_retention_bytes: crate::config::get_env(
                "STREAM_DEFAULT_RETENTION_BYTES",
                default.default_retention_bytes,
            ),
            default_retention_age_ms: crate::config::get_env(
                "STREAM_DEFAULT_RETENTION_AGE_MS",
                default.default_retention_age_ms,
            ),
            max_ack_pending: crate::config::get_env(
                "STREAM_MAX_ACK_PENDING",
                default.max_ack_pending,
            ),
            ack_wait_ms: crate::config::get_env("STREAM_ACK_WAIT_MS", default.ack_wait_ms),
            max_deliveries: crate::config::get_env("STREAM_MAX_DELIVERIES", default.max_deliveries),
        }
    }
}

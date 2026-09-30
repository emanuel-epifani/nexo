#[derive(Debug, Clone)]
pub struct SystemQueueConfig {
    /// Root directory containing the shared queue database (`queues.sqlite3`)
    /// and the process-ownership lock file.
    pub persistence_path: String,
    /// Bounded command channel between submitters and the SQLite writer
    /// (message-count bound).
    pub storage_queue_capacity: usize,
    /// Byte bound on queued command payloads; submitters apply backpressure
    /// once in-flight command bytes exceed this.
    pub storage_queue_max_bytes: usize,
    /// Defaults used when a `create_queue` omits the option. Persisted per
    /// queue at provision time; later default changes never rewrite existing
    /// queues.
    pub visibility_timeout_ms: u64,
    pub max_deliveries: u32,
    /// Defaults applied when a `consume` request omits batch/wait.
    pub default_batch_size: usize,
    pub default_wait_ms: u64,
}

impl Default for SystemQueueConfig {
    fn default() -> Self {
        Self {
            persistence_path: "./data/queues".to_string(),
            storage_queue_capacity: 16384,
            storage_queue_max_bytes: 268435456, // 256MB
            visibility_timeout_ms: 30000,
            max_deliveries: 5,
            default_batch_size: 10,
            default_wait_ms: 0,
        }
    }
}

impl SystemQueueConfig {
    pub fn load() -> Self {
        let default = Self::default();
        Self {
            persistence_path: crate::config::get_env(
                "QUEUE_ROOT_PERSISTENCE_PATH",
                default.persistence_path,
            ),
            storage_queue_capacity: crate::config::get_env(
                "QUEUE_STORAGE_QUEUE_CAPACITY",
                default.storage_queue_capacity,
            ),
            storage_queue_max_bytes: crate::config::get_env(
                "QUEUE_STORAGE_QUEUE_MAX_BYTES",
                default.storage_queue_max_bytes,
            ),
            visibility_timeout_ms: crate::config::get_env(
                "QUEUE_VISIBILITY_MS",
                default.visibility_timeout_ms,
            ),
            max_deliveries: crate::config::get_env("QUEUE_MAX_DELIVERIES", default.max_deliveries),
            default_batch_size: crate::config::get_env(
                "QUEUE_DEFAULT_BATCH_SIZE",
                default.default_batch_size,
            ),
            default_wait_ms: crate::config::get_env(
                "QUEUE_DEFAULT_WAIT_MS",
                default.default_wait_ms,
            ),
        }
    }
}

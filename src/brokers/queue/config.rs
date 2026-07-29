#[derive(Debug, Clone)]
pub struct SystemQueueConfig {
    pub visibility_timeout_ms: u64,
    pub max_deliveries: u32,
    pub default_batch_size: usize,
    pub default_wait_ms: u64,
    pub persistence_path: String,
    pub default_flush_ms: u64,
    pub writer_batch_size: usize,
    pub storage_channel_capacity: usize,
}

impl Default for SystemQueueConfig {
    fn default() -> Self {
        Self {
            visibility_timeout_ms: 30000,
            max_deliveries: 5,
            default_batch_size: 10,
            default_wait_ms: 0,
            persistence_path: "./data/queues".to_string(),
            default_flush_ms: 100,
            writer_batch_size: 50000,
            storage_channel_capacity: 65536,
        }
    }
}

impl SystemQueueConfig {
    pub fn load() -> Self {
        let default = Self::default();
        Self {
            visibility_timeout_ms: crate::config::get_env("QUEUE_VISIBILITY_MS", default.visibility_timeout_ms),
            max_deliveries:       crate::config::get_env("QUEUE_MAX_DELIVERIES", default.max_deliveries),
            default_batch_size:    crate::config::get_env("QUEUE_DEFAULT_BATCH_SIZE", default.default_batch_size),
            default_wait_ms:       crate::config::get_env("QUEUE_DEFAULT_WAIT_MS", default.default_wait_ms),
            persistence_path:         crate::config::get_env("QUEUE_ROOT_PERSISTENCE_PATH", default.persistence_path),
            default_flush_ms:         crate::config::get_env("QUEUE_DEFAULT_FLUSH_MS", default.default_flush_ms),
            writer_batch_size:        crate::config::get_env("QUEUE_WRITER_BATCH_SIZE", default.writer_batch_size),
            storage_channel_capacity: crate::config::get_env("QUEUE_STORAGE_CHANNEL_CAPACITY", default.storage_channel_capacity),
        }
    }
}

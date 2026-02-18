use std::env;
use std::sync::OnceLock;

static CONFIG: OnceLock<Config> = OnceLock::new();

// --- CONFIG AGGREGATOR ---

#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub store: StoreConfig,
    pub queue: SystemQueueConfig,
    pub pubsub: PubSubConfig,
    pub stream: SystemStreamConfig,
}

impl Config {
    pub fn global() -> &'static Config {
        CONFIG.get_or_init(Self::load)
    }

    fn load() -> Self {
        dotenv::dotenv().ok();
        Self {
            server: ServerConfig::load(),
            store: StoreConfig::load(),
            queue: SystemQueueConfig::load(),
            pubsub: PubSubConfig::load(),
            stream: SystemStreamConfig::load(),
        }
    }
}

// --- MODULES ---

// SERVER
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub dashboard_port: u16,
    pub log_level: String,
    pub dashboard_enabled: bool,
    pub network_buffer_read_size: usize,
    pub network_buffer_write_size: usize,
    pub channel_capacity_socket_write: usize,
}

impl ServerConfig {
    fn load() -> Self {
        let env_mode = get_env::<String>("NEXO_ENV", "dev");

        Self {
            host:           get_env("SERVER_HOST", "0.0.0.0"),
            port:           get_env("SERVER_PORT", "7654"),
            dashboard_port: get_env("DASHBOARD_PORT", "8080"),
            log_level:      get_env("NEXO_LOG", "error"),
            dashboard_enabled: env_mode != "prod",
            network_buffer_read_size: get_env("NETWORK_BUFFER_READ_SIZE", "65536"), // 64KB
            network_buffer_write_size: get_env("NETWORK_BUFFER_WRITE_SIZE", "16384"), // 16KB
            channel_capacity_socket_write: get_env("CHANNEL_CAPACITY_SOCKET_WRITE", "1024"),
        }
    }
}

// STORE
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub cleanup_interval_secs: u64,
    pub default_ttl_secs: u64,
}

impl StoreConfig {
    fn load() -> Self {
        Self {
            cleanup_interval_secs: get_env("STORE_CLEANUP_INTERVAL_SECS", "60"),
            default_ttl_secs:      get_env("STORE_TTL_SECS", "3600"),
        }
    }
}

// QUEUE
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
    pub actor_channel_capacity: usize,
    pub writer_channel_capacity: usize,
    pub writer_batch_size: usize,
}

impl SystemQueueConfig {
    fn load() -> Self {
        Self {
            visibility_timeout_ms: get_env("QUEUE_VISIBILITY_MS", "30000"),
            max_retries:           get_env("QUEUE_MAX_RETRIES", "5"),
            ttl_ms:                get_env("QUEUE_TTL_MS", "604800000"),
            default_batch_size:    get_env("QUEUE_DEFAULT_BATCH_SIZE", "10"),
            default_wait_ms:       get_env("QUEUE_DEFAULT_WAIT_MS", "0"),
            persistence_path:      get_env("QUEUE_ROOT_PERSISTENCE_PATH", "./data/queues"),
            default_flush_ms:      get_env("QUEUE_DEFAULT_FLUSH_MS", "200"),
            actor_channel_capacity: get_env("QUEUE_ACTOR_CHAN_CAP", "10000"),
            writer_channel_capacity: get_env("QUEUE_WRITER_CHAN_CAP", "10000"),
            writer_batch_size:     get_env("QUEUE_WRITER_BATCH_SIZE", "50000"),
        }
    }
}

// PUBSUB
#[derive(Debug, Clone)]
pub struct PubSubConfig {
    pub actor_channel_capacity: usize,
    pub persistence_path: String,
    pub default_retained_ttl_seconds: u64,
    pub cleanup_interval_seconds: u64,
}

impl PubSubConfig {
    fn load() -> Self {
        Self {
            actor_channel_capacity: get_env("PUBSUB_ACTOR_CHAN_CAP", "10000"),
            persistence_path: get_env("PUBSUB_ROOT_PERSISTENCE_PATH", "./data/pubsub"),
            default_retained_ttl_seconds: get_env("PUBSUB_DEFAULT_RETAINED_TTL_SECS", "3600"),
            cleanup_interval_seconds: get_env("PUBSUB_CLEANUP_INTERVAL_SECS", "60"),
        }
    }
}

// STREAM
#[derive(Debug, Clone)]
pub struct SystemStreamConfig {
    pub default_partitions: u32,
    pub max_partitions: u32,
    pub actor_channel_capacity: usize,
    pub persistence_path: String,
    pub default_flush_ms: u64,
    pub compaction_threshold: u64,
    pub max_segment_size: u64,
    pub retention_check_interval_ms: u64,
    pub default_retention_bytes: u64,
    pub default_retention_age_ms: u64,
    pub max_ram_messages: usize,
    pub writer_channel_capacity: usize,
    pub writer_batch_size: usize,
    pub eviction_interval_ms: u64,
    pub eviction_batch_size: usize,
    pub ram_soft_limit: usize,
    pub ram_hard_limit: usize,
}

impl SystemStreamConfig {
    fn load() -> Self {
        Self {
            default_partitions:          get_env("STREAM_PARTITIONS", "4"),
            max_partitions:              get_env("STREAM_MAX_PARTITIONS", "32"),
            actor_channel_capacity:      get_env("STREAM_ACTOR_CHAN_CAP", "10000"),
            persistence_path:            get_env("STREAM_ROOT_PERSISTENCE_PATH", "./data/streams"),
            default_flush_ms:            get_env("STREAM_DEFAULT_FLUSH_MS", "50"),
            compaction_threshold:        get_env("STREAM_COMPACTION_THRESHOLD", "10000"),
            max_segment_size:            get_env("STREAM_MAX_SEGMENT_SIZE", "104857600"), // 100MB
            retention_check_interval_ms: get_env("STREAM_RETENTION_CHECK_MS", "600000"),  // 10 minutes
            default_retention_bytes:     get_env("STREAM_DEFAULT_RETENTION_BYTES", "1073741824"), // 1GB
            default_retention_age_ms:    get_env("STREAM_DEFAULT_RETENTION_AGE_MS", "604800000"), // 7 days
            max_ram_messages:            get_env("STREAM_MAX_RAM_MESSAGES", "20000"), // 2x default channel
            writer_channel_capacity:     get_env("STREAM_WRITER_CHAN_CAP", "10000"),
            writer_batch_size:           get_env("STREAM_WRITER_BATCH_SIZE", "5000"),
            eviction_interval_ms:        get_env("STREAM_EVICTION_INTERVAL_MS", "500"),
            eviction_batch_size:         get_env("STREAM_EVICTION_BATCH_SIZE", "10000"),
            ram_soft_limit:              get_env("STREAM_RAM_SOFT_LIMIT", "1000"),
            ram_hard_limit:              get_env("STREAM_RAM_HARD_LIMIT", "20000"),
        }
    }
}

// --- PRIVATE HELPER ---

fn get_env<T: std::str::FromStr>(key: &str, default: &str) -> T {
    env::var(key)
        .unwrap_or_else(|_| default.to_string())
        .parse()
        .map_err(|_| format!("Config error: {} must be valid", key))
        .unwrap()
}

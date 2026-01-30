use std::env;
use std::sync::OnceLock;

static CONFIG: OnceLock<Config> = OnceLock::new();

// --- CONFIG AGGREGATOR ---

#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub store: StoreConfig,
    pub queue: QueueConfig,
    pub pubsub: PubSubConfig,
    pub stream: StreamConfig,
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
            queue: QueueConfig::load(),
            pubsub: PubSubConfig::load(),
            stream: StreamConfig::load(),
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
}

impl ServerConfig {
    fn load() -> Self {
        Self {
            host:           get_env("SERVER_HOST", "127.0.0.1"),
            port:           get_env("SERVER_PORT", "7654"),
            dashboard_port: get_env("DASHBOARD_PORT", "8080"),
            log_level:      get_env("LOG_LEVEL", "info"),
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
pub struct QueueConfig {
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
}

impl QueueConfig {
    fn load() -> Self {
        Self {
            visibility_timeout_ms: get_env("QUEUE_VISIBILITY_MS", "30000"),
            max_retries:           get_env("QUEUE_MAX_RETRIES", "5"),
            ttl_ms:                get_env("QUEUE_TTL_MS", "604800000"),
            default_batch_size:    get_env("QUEUE_DEFAULT_BATCH_SIZE", "10"),
            default_wait_ms:       get_env("QUEUE_DEFAULT_WAIT_MS", "0"),
            persistence_path:      get_env("QUEUE_ROOT_PERSISTENCE_PATH", "./data/queues"),
            default_flush_ms:      get_env("QUEUE_DEFAULT_FLUSH_MS", "100"),
        }
    }
}

// PUBSUB
#[derive(Debug, Clone)]
pub struct PubSubConfig {
    pub actor_channel_capacity: usize,
}

impl PubSubConfig {
    fn load() -> Self {
        Self {
            actor_channel_capacity: get_env("PUBSUB_ACTOR_CHAN_CAP", "10000"),
        }
    }
}

// STREAM
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub default_partitions: u32,
    pub actor_channel_capacity: usize,
    pub persistence_path: String,
    pub default_flush_ms: u64,
    pub compaction_threshold: u64,
    pub max_segment_size: u64,
}

impl StreamConfig {
    fn load() -> Self {
        Self {
            default_partitions:     get_env("STREAM_PARTITIONS", "4"),
            actor_channel_capacity: get_env("STREAM_ACTOR_CHAN_CAP", "10000"),
            persistence_path:       get_env("STREAM_ROOT_PERSISTENCE_PATH", "./data/streams"),
            default_flush_ms:       get_env("STREAM_DEFAULT_FLUSH_MS", "100"),
            compaction_threshold:   get_env("STREAM_COMPACTION_THRESHOLD", "10000"),
            max_segment_size:       get_env("STREAM_MAX_SEGMENT_SIZE", "104857600"), // 100MB
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

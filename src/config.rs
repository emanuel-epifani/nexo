use crate::brokers::stream::config::SystemStreamConfig;
use crate::brokers::queue::config::SystemQueueConfig;
use crate::brokers::pub_sub::config::PubSubConfig;
use crate::brokers::store::config::StoreConfig;
use std::env;
use std::sync::OnceLock;

static CONFIG: OnceLock<Config> = OnceLock::new();

// --- SHARED HELPERS ---

pub(crate) fn get_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(default)
}

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
    pub log_level: String,
    pub max_payload_size: usize,
    pub channel_capacity_socket_write: usize,
}

impl ServerConfig {
    fn load() -> Self {
        Self {
            host:           get_env("SERVER_HOST", "127.0.0.1".to_string()),
            port:           get_env("SERVER_SOCKET_TCP_PORT", 7654u16),
            log_level:      get_env("NEXO_LOG", "info".to_string()),
            max_payload_size: get_env("MAX_PAYLOAD_SIZE", 10485760usize), // 10MB
            channel_capacity_socket_write: get_env("CHANNEL_CAPACITY_SOCKET_WRITE", 1024usize),
        }
    }
}

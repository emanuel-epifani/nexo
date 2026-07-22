#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub cleanup_interval_secs: u64,
    pub default_ttl_secs: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            cleanup_interval_secs: 60,
            default_ttl_secs: 3600,
        }
    }
}

impl StoreConfig {
    pub fn load() -> Self {
        let default = Self::default();
        Self {
            cleanup_interval_secs: crate::config::get_env("STORE_CLEANUP_INTERVAL_SECS", default.cleanup_interval_secs),
            default_ttl_secs: crate::config::get_env("STORE_TTL_SECS", default.default_ttl_secs),
        }
    }
}

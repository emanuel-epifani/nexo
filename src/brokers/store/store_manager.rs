//! Store Manager: In-memory data store with multiple data structures
//! Supports KV (default), Hash, List, Set - predisposed for future expansion

use dashmap::DashMap;
use std::time::{Duration, Instant};
use bytes::Bytes;
use std::sync::Arc;
use tokio::time;

// ========================================
// VALUE TYPES - Predisposed for future structures
// ========================================

/// Represents the different data types that can be stored.
/// Currently only Bytes (KV) is implemented, others are placeholders.
#[derive(Clone, Debug)]
pub enum Value {
    /// Simple key-value (default) - stores raw bytes
    Bytes(Bytes),
    // Future: Hash, List, Set
    // Hash(DashMap<String, Bytes>),
    // List(VecDeque<Bytes>),
    // Set(HashSet<Bytes>),
}

impl Value {
    /// Extract bytes if this is a Bytes variant
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Value::Bytes(b) => Some(b),
            // _ => None, // Uncomment when adding more variants
        }
    }
}

// ========================================
// ENTRY - Wrapper with TTL support
// ========================================

#[derive(Clone, Debug)]
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

// ========================================
// STORE MANAGER
// ========================================

pub struct StoreManager {
    store: Arc<DashMap<String, Entry>>,
}

impl StoreManager {
    pub fn new() -> Self {
        let store = Arc::new(DashMap::new());
        
        // Background cleanup task (Cron)
        let store_cleanup = Arc::clone(&store);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let now = Instant::now();
                store_cleanup.retain(|_, entry: &mut Entry| {
                    if let Some(expiry) = entry.expires_at {
                        return expiry > now;
                    }
                    true
                });
            }
        });

        Self { store }
    }

    // ========================================
    // KV OPERATIONS
    // ========================================

    pub fn set(&self, key: String, value: Bytes, ttl: Option<u64>) -> Result<(), String> {
        // Default TTL: 1 hour (3600 seconds) if not specified to prevent memory leaks
        let ttl_secs = ttl.unwrap_or(3600);
        let expires_at = Some(Instant::now() + Duration::from_secs(ttl_secs));
        
        let entry = Entry { 
            value: Value::Bytes(value), 
            expires_at 
        };
        self.store.insert(key, entry);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, String> {
        if let Some(entry_ref) = self.store.get(key) {
            let entry = entry_ref.value();
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() > expires_at {
                    return Ok(None);
                }
            }
            // Extract bytes from Value enum
            if let Some(bytes) = entry.value.as_bytes() {
                return Ok(Some(bytes.clone()));
            }
        }
        Ok(None)
    }

    pub fn del(&self, key: &str) -> Result<bool, String> {
        let removed = self.store.remove(key).is_some();
        Ok(removed)
    }

    // ========================================
    // FUTURE: HASH OPERATIONS
    // ========================================
    // pub fn hset(&self, key: &str, field: &str, value: Bytes) -> Result<(), String>
    // pub fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, String>
    // pub fn hincrby(&self, key: &str, field: &str, delta: i64) -> Result<i64, String>

    // ========================================
    // FUTURE: LIST OPERATIONS  
    // ========================================
    // pub fn lpush(&self, key: &str, value: Bytes) -> Result<usize, String>
    // pub fn rpop(&self, key: &str) -> Result<Option<Bytes>, String>

    // ========================================
    // FUTURE: SET OPERATIONS
    // ========================================
    // pub fn sadd(&self, key: &str, member: Bytes) -> Result<bool, String>
    // pub fn sismember(&self, key: &str, member: &Bytes) -> Result<bool, String>
}

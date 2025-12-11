//! KV Manager: In-memory key-value store with TTL support

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

// ========================================
// TYPES
// ========================================

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

pub struct KvManager {
    store: Arc<RwLock<HashMap<String, Entry>>>,
}

// ========================================
// IMPLEMENTATION
// ========================================

impl KvManager {
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set key to value with optional TTL (seconds)
    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<u64>) -> Result<(), String> {
        let expires_at = ttl.map(|secs| Instant::now() + Duration::from_secs(secs));

        let entry = Entry { value, expires_at };

        self.store
            .write()
            .map_err(|e| format!("Lock error: {}", e))?
            .insert(key, entry);

        Ok(())
    }

    /// Get value by key (None if not exists or expired)
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let store = self
            .store
            .read()
            .map_err(|e| format!("Lock error: {}", e))?;

        if let Some(entry) = store.get(key) {
            // Check if expired
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() > expires_at {
                    return Ok(None);
                }
            }
            Ok(Some(entry.value.clone()))
        } else {
            Ok(None)
        }
    }

    /// Delete key (returns true if existed)
    pub fn del(&self, key: &str) -> Result<bool, String> {
        let removed = self
            .store
            .write()
            .map_err(|e| format!("Lock error: {}", e))?
            .remove(key)
            .is_some();

        Ok(removed)
    }
}

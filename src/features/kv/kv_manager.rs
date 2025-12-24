//! KV Manager: In-memory key-value store with TTL support
use dashmap::DashMap;
use std::time::{Duration, Instant};

// ========================================
// TYPES
// ========================================

#[derive(Clone, Debug)]
struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

pub struct KvManager {
    // DashMap: Concurrent HashMap. 
    // Non serve Mutex esterno, DashMap gestisce lo sharding dei lock internamente.
    store: DashMap<String, Entry>,
}

// ========================================
// IMPLEMENTATION
// ========================================

impl KvManager {
    pub fn new() -> Self {
        Self {
            store: DashMap::new(),
        }
    }

    /// Set key to value with optional TTL (seconds)
    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<u64>) -> Result<(), String> {
        let expires_at = ttl.map(|secs| Instant::now() + Duration::from_secs(secs));

        let entry = Entry { value, expires_at };

        // DashMap handle locks internally per bucket
        self.store.insert(key, entry);

        Ok(())
    }

    /// Get value by key (None if not exists or expired)
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        // DashMap::get returns a Ref which holds a read lock
        if let Some(entry_ref) = self.store.get(key) {
            let entry = entry_ref.value();
            
            // Check if expired
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() > expires_at {
                    // Lazy deletion: we could delete it here, but it requires upgrading lock or re-locking
                    // For now, just return None. 
                    // To do proper expiration, we would use a dedicated clearer task.
                    return Ok(None);
                }
            }
            return Ok(Some(entry.value.clone()));
        }
        
        Ok(None)
    }

    /// Delete key (returns true if existed)
    pub fn del(&self, key: &str) -> Result<bool, String> {
        let removed = self.store.remove(key).is_some();
        Ok(removed)
    }
}

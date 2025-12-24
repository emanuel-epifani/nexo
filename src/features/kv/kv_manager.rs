//! KV Manager: In-memory key-value store with TTL support
use dashmap::DashMap;
use std::time::{Duration, Instant};
use std::convert::TryInto;
use crate::server::protocol::{OP_KV_SET, OP_KV_GET, OP_KV_DEL, Response};

// ========================================
// COMMAND PATTERN
// ========================================

#[derive(Debug)]
pub enum KvCommand {
    Set { key: String, value: Vec<u8>, ttl: Option<u64> },
    Get { key: String },
    Del { key: String },
}

impl KvCommand {
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_KV_SET => {
                // Payload: [KeyLen: 4 bytes] [Key bytes] [Value bytes]
                if payload.len() < 4 {
                    return Err("Payload too short".to_string());
                }
                let key_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + key_len {
                    return Err("Invalid frame format".to_string());
                }

                let key_bytes = &payload[4..4 + key_len];
                let value = payload[4 + key_len..].to_vec();

                let key = std::str::from_utf8(key_bytes)
                    .map_err(|_| "Key must be UTF-8".to_string())?
                    .to_string();

                Ok(KvCommand::Set { key, value, ttl: None })
            },
            OP_KV_GET => {
                let key = std::str::from_utf8(payload)
                    .map_err(|_| "Key must be UTF-8".to_string())?
                    .to_string();
                Ok(KvCommand::Get { key })
            },
            OP_KV_DEL => {
                let key = std::str::from_utf8(payload)
                    .map_err(|_| "Key must be UTF-8".to_string())?
                    .to_string();
                Ok(KvCommand::Del { key })
            },
            _ => Err(format!("Unknown KV Opcode: 0x{:02X}", opcode)),
        }
    }
}

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

    /// Execute a parsed command
    pub fn execute(&self, cmd: KvCommand) -> Result<Response, String> {
        match cmd {
            KvCommand::Set { key, value, ttl } => {
                self.set(key, value, ttl)?;
                Ok(Response::Ok)
            },
            KvCommand::Get { key } => {
                match self.get(&key)? {
                    Some(val) => Ok(Response::Data(val)),
                    None => Ok(Response::Null),
                }
            },
            KvCommand::Del { key } => {
                self.del(&key)?;
                Ok(Response::Ok)
            }
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

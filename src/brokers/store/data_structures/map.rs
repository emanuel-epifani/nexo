use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use crate::brokers::store::config::StoreConfig;
use crate::config::Config;
use bytes::Bytes;
use crate::server::protocol::cursor::PayloadCursor;
use crate::server::protocol::ParseError;
use serde::Deserialize;

pub const OP_MAP_SET: u8 = 0x02;
pub const OP_MAP_GET: u8 = 0x03;
pub const OP_MAP_DEL: u8 = 0x04;

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: MapValue,
    pub expires_at: Option<Instant>,
}


#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MapSetOptions {
    pub ttl: Option<u64>,
}

#[derive(Debug)]
pub enum MapCommand {
    /// SET: [KeyLen:4][Key][JSONLen:4][JSON][Val...]
    Set {
        key: String,
        options: MapSetOptions,
        value: Bytes,
    },
    /// GET: [KeyLen:4][Key]
    Get {
        key: String,
    },
    /// DEL: [KeyLen:4][Key]
    Del {
        key: String,
    },
}

impl MapCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_MAP_SET => {
                let key = cursor.read_string()?;
                let json_str = cursor.read_string()?;

                let options: MapSetOptions = serde_json::from_str(&json_str)
                    .map_err(|e| ParseError::Invalid(format!("Invalid JSON options: {}", e)))?;

                let value = cursor.read_remaining();
                Ok(Self::Set { key, options, value })
            }
            OP_MAP_GET => {
                let key = cursor.read_string()?;
                Ok(Self::Get { key })
            }
            OP_MAP_DEL => {
                let key = cursor.read_string()?;
                Ok(Self::Del { key })
            }
            _ => Err(ParseError::Invalid(format!("Unknown Map opcode: 0x{:02X}", opcode))),
        }
    }
}

#[derive(Clone)]
pub struct MapStore {
    inner: Arc<DashMap<String, Entry>>,
    config: Arc<StoreConfig>,
}

#[derive(Debug, Clone)]
pub struct MapValue(pub Bytes);

impl MapStore {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        let inner = Arc::new(DashMap::new());

        // Weak reference for the cleanup thread
        // This prevents the thread from keeping the data_structures alive if the StoreManager is dropped
        let weak_inner = Arc::downgrade(&inner);
        let cleanup_interval = config.cleanup_interval_secs;

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(cleanup_interval));
            loop {
                interval.tick().await;

                // Try to upgrade the weak pointer
                match weak_inner.upgrade() {
                    Some(map) => {
                        // Manager is alive, proceed with cleanup
                        let now = Instant::now();
                        map.retain(|_, entry: &mut Entry| {
                            if let Some(expiry) = entry.expires_at {
                                return expiry > now;
                            }
                            true
                        });
                    }
                    None => {
                        // Manager has been dropped, stop the cleanup task
                        break;
                    }
                }
            }
        });

        Self { inner, config }
    }

    pub fn set(&self, key: String, value: Bytes, ttl: Option<u64>) {
        let expires_at = match ttl {
            Some(0) | None => {
                // Use default TTL from config
                Some(Instant::now() + Duration::from_secs(self.config.default_ttl_secs))
            }
            Some(secs) => Some(Instant::now() + Duration::from_secs(secs)),
        };

        self.inner.insert(key, Entry {
            value: MapValue(value),
            expires_at,
        });
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        if let Some(entry) = self.inner.get(key) {
            if let Some(expiry) = entry.expires_at {
                if Instant::now() > expiry {
                    return None; // Lazy expiration could happen here (delete on read)
                }
            }
            let MapValue(val) = &entry.value;
            return Some(val.clone());
        }
        None
    }

    pub fn del(&self, key: &str) -> bool {
        self.inner.remove(key).is_some()
    }

    /// Used for snapshots and iterating
    pub fn iter(&self) -> dashmap::iter::Iter<'_, String, Entry> {
        self.inner.iter()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

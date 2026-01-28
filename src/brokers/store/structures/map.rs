use bytes::Bytes;
use dashmap::DashMap;
use std::time::{Duration, Instant};
use crate::brokers::store::types::{Entry, Value};

#[derive(Clone, Debug)]
pub struct MapValue(pub Bytes);

pub struct MapOperations;

impl MapOperations {
    pub fn set(store: &DashMap<String, Entry>, key: String, value: Bytes, expires_at: Option<Instant>) {
        let entry = Entry { 
            value: Value::Map(MapValue(value)), 
            expires_at 
        };
        store.insert(key, entry);
    }

    pub fn get(store: &DashMap<String, Entry>, key: &str) -> Option<Bytes> {
        if let Some(entry_ref) = store.get(key) {
            let entry = entry_ref.value();
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() > expires_at {
                    return None;
                }
            }
            if let Value::Map(MapValue(bytes)) = &entry.value {
                return Some(bytes.clone());
            }
        }
        None
    }

    pub fn del(store: &DashMap<String, Entry>, key: &str) -> bool {
        store.remove(key).is_some()
    }
}

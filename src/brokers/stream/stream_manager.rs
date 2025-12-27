//! Stream Manager: Append-only logs for topics
use dashmap::DashMap;
use std::sync::RwLock;
use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct LogMessage {
    pub id: u64,
    pub payload: Bytes,
}

pub struct StreamManager {
    topics: DashMap<String, RwLock<Vec<LogMessage>>>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self {
            topics: DashMap::new(),
        }
    }

    pub fn append(&self, topic: String, payload: Bytes) -> u64 {
        let entry = self.topics.entry(topic).or_insert_with(|| RwLock::new(Vec::new()));
        let mut stream = entry.write().unwrap();
        let id = stream.len() as u64;
        stream.push(LogMessage { id, payload });
        id
    }

    pub fn read(&self, topic: &str, start_offset: usize) -> Vec<LogMessage> {
        if let Some(entry) = self.topics.get(topic) {
            let stream = entry.read().unwrap();
            if start_offset < stream.len() {
                return stream[start_offset..].to_vec();
            }
        }
        Vec::new()
    }
}

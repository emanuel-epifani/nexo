//! Queue Manager: Message queue functionality
use dashmap::DashMap;
use std::sync::Mutex;
use std::collections::VecDeque;
use bytes::Bytes;

pub struct QueueManager {
    queues: DashMap<String, Mutex<VecDeque<Bytes>>>,
}

impl QueueManager {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
        }
    }

    pub fn push(&self, queue_name: String, value: Bytes) {
        let entry = self.queues.entry(queue_name).or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut queue = entry.lock().unwrap();
        queue.push_back(value);
    }

    pub fn pop(&self, queue_name: &str) -> Option<Bytes> {
        if let Some(entry) = self.queues.get(queue_name) {
            let mut queue = entry.lock().unwrap();
            return queue.pop_front();
        }
        None
    }
}

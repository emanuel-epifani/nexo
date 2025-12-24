//! Queue Manager: Message queue functionality
use dashmap::DashMap;
use std::sync::Mutex;
use std::collections::VecDeque;

pub struct QueueManager {
    // DashMap separa le code (Coda A non blocca Coda B).
    // Mutex protegge l'ordine FIFO dentro la singola coda.
    queues: DashMap<String, Mutex<VecDeque<Vec<u8>>>>,
}

impl QueueManager {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
        }
    }

    /// Push a message to the back of the queue
    pub fn push(&self, queue_name: String, value: Vec<u8>) {
        let entry = self.queues.entry(queue_name).or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut queue = entry.lock().unwrap();
        queue.push_back(value);
    }

    /// Pop a message from the front of the queue
    pub fn pop(&self, queue_name: &str) -> Option<Vec<u8>> {
        if let Some(entry) = self.queues.get(queue_name) {
            let mut queue = entry.lock().unwrap();
            return queue.pop_front();
        }
        None
    }
}

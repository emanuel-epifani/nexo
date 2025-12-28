//! Queue Manager: Message queue functionality
use dashmap::DashMap;
use std::sync::Arc;
use bytes::Bytes;
use uuid::Uuid;
use tokio::sync::oneshot;
use super::{Queue, Message};

use once_cell::sync::OnceCell;

pub struct QueueManager {
    queues: DashMap<String, Arc<Queue>>,
    self_ref: OnceCell<Arc<QueueManager>>,
}

impl QueueManager {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
            self_ref: OnceCell::new(),
        }
    }

    pub fn set_self(&self, self_arc: Arc<QueueManager>) {
        let _ = self.self_ref.set(self_arc);
    }

    pub fn push(&self, queue_name: String, value: Bytes, priority: u8, delay_ms: Option<u64>) {
        let queue = self.get_or_create(queue_name);
        queue.push(value, priority, delay_ms);
    }

    pub fn pop(&self, queue_name: &str) -> Option<Message> {
        if let Some(queue) = self.queues.get(queue_name) {
            return queue.pop();
        }
        None
    }

    pub fn ack(&self, queue_name: &str, id: Uuid) -> bool {
        if let Some(queue) = self.queues.get(queue_name) {
            return queue.ack(id);
        }
        false
    }

    pub fn consume(&self, queue_name: String) -> oneshot::Receiver<Message> {
        let queue = self.get_or_create(queue_name);
        queue.consume()
    }

    pub fn get_or_create(&self, queue_name: String) -> Arc<Queue> {
        self.queues.entry(queue_name.clone())
            .or_insert_with(|| {
                let queue = Arc::new(Queue::new(queue_name));
                if let Some(manager_arc) = self.self_ref.get() {
                    queue.clone().start_reaper(manager_arc.clone());
                }
                queue
            })
            .value()
            .clone()
    }

    // This method should be called after QueueManager is wrapped in an Arc
    pub fn start_reapers(self: Arc<Self>) {
        // This is a bit tricky since new queues can be created at runtime.
        // We'll probably want the get_or_create to start the reaper.
    }
}

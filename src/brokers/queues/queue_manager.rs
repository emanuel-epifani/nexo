//! Queue Manager: Message queues functionality
use dashmap::DashMap;
use std::sync::Arc;
use bytes::Bytes;
use uuid::Uuid;
use tokio::sync::oneshot;

use once_cell::sync::OnceCell;
use crate::brokers::queues::{Message, Queue, QueueConfig};

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

    pub fn push(&self, queue_name: String, value: Bytes, priority: u8, delay_ms: Option<u64>, auto_create: bool) -> Result<(), String> {
        let queue = if auto_create {
            self.get_or_create(queue_name)
        } else {
            self.get(queue_name.as_str())
                .ok_or_else(|| format!("Queue '{}' not found. Create it first.", queue_name))?
        };
        queue.push(value, priority, delay_ms);
        Ok(())
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

    pub fn consume(&self, queue_name: String) -> Result<oneshot::Receiver<Message>, String> {
        let queue = self.get(queue_name.as_str())
            .ok_or_else(|| format!("Queue '{}' not found. Create it first.", queue_name))?;
        Ok(queue.consume())
    }

    pub fn declare_queue(&self, queue_name: String, config: QueueConfig) -> Arc<Queue> {
        self.queues.entry(queue_name.clone())
            .or_insert_with(|| {
                let queue = Arc::new(Queue::new(queue_name, config));
                if let Some(manager_arc) = self.self_ref.get() {
                    queue.clone().start_reaper(manager_arc.clone());
                }
                queue
            })
            .value()
            .clone()
    }

    pub fn get_or_create(&self, queue_name: String) -> Arc<Queue> {
        self.declare_queue(queue_name, QueueConfig::default())
    }

    pub fn get(&self, queue_name: &str) -> Option<Arc<Queue>> {
        self.queues.get(queue_name).map(|q| q.value().clone())
    }

    // This method should be called after QueueManager is wrapped in an Arc
    pub fn start_reapers(self: Arc<Self>) {
        // Queues created at runtime automatically start their reaper in declare_queue
    }
}

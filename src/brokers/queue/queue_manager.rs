//! Queue Manager: Router and lifecycle manager for Queue Actors

use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use uuid::Uuid;

use crate::brokers::queue::queue::{QueueConfig, Message};
use crate::brokers::queue::actor::{QueueActor, QueueActorCommand};
use crate::brokers::queue::commands::{QueueCreateOptions, PersistenceOptions};
use crate::dashboard::queue::QueueSummary;
use crate::config::SystemQueueConfig;
use crate::brokers::queue::dlq::{DlqMessage, DlqState};

// ==========================================
// QUEUE MANAGER (Router)
// ==========================================

#[derive(Clone)]
pub struct QueueManager {
    queue_actors: Arc<DashMap<String, mpsc::Sender<QueueActorCommand>>>,
    config: SystemQueueConfig,
}

impl QueueManager {
    pub fn new(system_config: SystemQueueConfig) -> Self {
        let queue_actors = Arc::new(DashMap::new());
        let manager = Self { 
            queue_actors: queue_actors.clone(),
            config: system_config.clone(),
        };

        // WARM START: Discover and restore queues from filesystem
        let persistence_path = std::path::PathBuf::from(&system_config.persistence_path);
        if persistence_path.exists() {
            if let Ok(entries) = std::fs::read_dir(&persistence_path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                            if filename.ends_with(".db") && !filename.ends_with(".db-wal") && !filename.ends_with(".db-shm") {
                                let queue_name = filename.trim_end_matches(".db").to_string();
                                let config = QueueConfig::from_options(QueueCreateOptions::default(), &system_config);
                                let actor_tx = manager.spawn_queue_actor(
                                    queue_name.clone(),
                                    config,
                                    &system_config
                                );
                                queue_actors.insert(queue_name.clone(), actor_tx);
                                tracing::info!("[QueueManager] Warm start: Restored queue '{}'", queue_name);
                            }
                        }
                    }
                }
            }
        }

        manager
    }

    fn spawn_queue_actor(
        &self,
        name: String,
        config: QueueConfig,
        system_config: &SystemQueueConfig,
    ) -> mpsc::Sender<QueueActorCommand> {
        let (tx, rx) = mpsc::channel(system_config.actor_channel_capacity);
        
        // Spawn actor with path from system_config
        let path = std::path::PathBuf::from(&system_config.persistence_path);
        let actor = QueueActor::new(name, config.clone(), path, rx);
        tokio::spawn(actor.run());
        
        tx
    }

    // --- Public API ---

    pub async fn create_queue(&self, name: String, options: QueueCreateOptions) -> Result<(), String> {
        use dashmap::mapref::entry::Entry;
        
        match self.queue_actors.entry(name.clone()) {
            Entry::Occupied(_) => Ok(()), // Already exists
            Entry::Vacant(v) => {
                let config = QueueConfig::from_options(options, &self.config);
                let actor_tx = self.spawn_queue_actor(
                    name.clone(), 
                    config, 
                    &self.config
                );
                v.insert(actor_tx);
                Ok(())
            }
        }
    }

    pub async fn delete_queue(&self, name: String) -> Result<(), String> {
        if let Some((_, actor_tx)) = self.queue_actors.remove(&name) {
            // 1. Stop Actor
            let (stop_tx, stop_rx) = oneshot::channel();
            if actor_tx.send(QueueActorCommand::Stop { reply: stop_tx }).await.is_ok() {
                let _ = stop_rx.await;
            }
        }
        
        // 2. Delete Persistence
        // Use system_config path
        let base_path = std::path::PathBuf::from(&self.config.persistence_path);
        let db_path = base_path.join(format!("{}.db", name));
        let wal_path = base_path.join(format!("{}.db-wal", name));
        let shm_path = base_path.join(format!("{}.db-shm", name));

        // Best effort cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
        let _ = std::fs::remove_file(shm_path);

        Ok(())
    }

    pub async fn push(&self, queue_name: String, payload: Bytes, priority: u8, delay_ms: Option<u64>) -> Result<(), String> {
        let actor = self.get_actor(&queue_name)
            .ok_or_else(|| format!("Queue '{}' not found. Create it first.", queue_name))?;
        
        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::Push { payload, priority, delay_ms, reply: tx })
            .await
            .map_err(|_| "Actor closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())?;
        Ok(())
    }

    pub async fn pop(&self, queue_name: &str) -> Option<Message> {
        let actor = self.get_actor(queue_name)?;
        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::Pop { reply: tx }).await.ok()?;
        rx.await.ok()?
    }

    pub async fn ack(&self, queue_name: &str, id: Uuid) -> bool {
        if let Some(actor) = self.get_actor(queue_name) {
            let (tx, rx) = oneshot::channel();
            if actor.send(QueueActorCommand::Ack { id, reply: tx }).await.is_ok() {
                return rx.await.unwrap_or(false);
            }
        }
        false
    }

    pub async fn nack(&self, queue_name: &str, id: Uuid, reason: String) -> bool {
        if let Some(actor) = self.get_actor(queue_name) {
            let (tx, rx) = oneshot::channel();
            if actor.send(QueueActorCommand::Nack { id, reason, reply: tx }).await.is_ok() {
                return rx.await.unwrap_or(false);
            }
        }
        false
    }

    pub async fn consume_batch(&self, queue_name: String, max: Option<usize>, wait_ms: Option<u64>) -> Result<Vec<Message>, String> {
        let actor = self.get_actor(&queue_name)
            .ok_or_else(|| format!("Queue '{}' not found. Create it first.", queue_name))?;
        
        let max_val = max.unwrap_or(self.config.default_batch_size);
        let wait_val = wait_ms.unwrap_or(self.config.default_wait_ms);

        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::ConsumeBatch { max: max_val, wait_ms: wait_val, reply: tx })
            .await
            .map_err(|_| "Actor closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())
    }

    pub async fn get_snapshot(&self) -> Vec<QueueSummary> {
        let mut queues = Vec::new();
        
        for entry in self.queue_actors.iter() {
            let actor_tx = entry.value();
            let (tx, rx) = oneshot::channel();
            if actor_tx.send(QueueActorCommand::GetSnapshot { reply: tx }).await.is_ok() {
                if let Ok(summary) = rx.await {
                    queues.push(summary);
                }
            }
        }
        queues
    }

    pub async fn get_messages(&self, queue_name: String, state_filter: String, offset: usize, limit: usize, search: Option<String>) -> Option<(usize, Vec<crate::dashboard::queue::MessageSummary>)> {
        if let Some(actor_tx) = self.get_actor(&queue_name) {
            let (tx, rx) = oneshot::channel();
            if actor_tx.send(QueueActorCommand::GetMessages { state_filter, offset, limit, search, reply: tx }).await.is_ok() {
                return rx.await.ok();
            }
        }
        None
    }

    pub async fn exists(&self, name: &str) -> bool {
        self.queue_actors.contains_key(name)
    }

    // --- DLQ Operations ---

    pub async fn move_to_dlq(&self, queue_name: String, payload: Bytes, priority: u8) {
        let actor_tx = if let Some(tx) = self.get_actor(&queue_name) {
            tx
        } else {
            let options = QueueCreateOptions {
                visibility_timeout_ms: None,
                max_retries: None,
                ttl_ms: None,
                persistence: Some(PersistenceOptions::FileSync),
            };
            let config = QueueConfig::from_options(options, &self.config);
            
            let tx = self.spawn_queue_actor(
                queue_name.clone(),
                config,
                &self.config
            );
            self.queue_actors.insert(queue_name, tx.clone());
            tx
        };
        
        let (reply_tx, _) = oneshot::channel();
        let _ = actor_tx.send(QueueActorCommand::Push {
            payload,
            priority,
            delay_ms: None,
            reply: reply_tx,
        }).await;
    }

    /// Peek messages from DLQ without consuming them
    pub async fn peek_dlq(&self, queue_name: &str, limit: usize, offset: usize) -> Result<(usize, Vec<DlqMessage>), String> {
        let actor = self.get_actor(queue_name)
            .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;
        
        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::PeekDLQ { limit, offset, reply: tx })
            .await
            .map_err(|_| "Actor closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())
    }

    /// Move a message from DLQ back to main queue (replay/retry)
    pub async fn move_to_queue(&self, queue_name: &str, message_id: Uuid) -> Result<bool, String> {
        let actor = self.get_actor(queue_name)
            .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;
        
        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::MoveToQueue { message_id, reply: tx })
            .await
            .map_err(|_| "Actor closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())
    }

    /// Delete a specific message from DLQ
    pub async fn delete_dlq(&self, queue_name: &str, message_id: Uuid) -> Result<bool, String> {
        let actor = self.get_actor(queue_name)
            .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;
        
        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::DeleteDLQ { message_id, reply: tx })
            .await
            .map_err(|_| "Actor closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())
    }

    /// Purge all messages from DLQ
    pub async fn purge_dlq(&self, queue_name: &str) -> Result<usize, String> {
        let actor = self.get_actor(queue_name)
            .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;
        
        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::PurgeDLQ { reply: tx })
            .await
            .map_err(|_| "Actor closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())
    }

    #[inline]
    fn get_actor(&self, name: &str) -> Option<mpsc::Sender<QueueActorCommand>> {
        self.queue_actors.get(name).map(|r| r.value().clone())
    }
}


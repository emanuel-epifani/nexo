//! Queue Manager: Router and lifecycle manager for Queue Actors

use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use uuid::Uuid;
use std::collections::HashMap;

use crate::brokers::queues::queue::{QueueConfig, Message};
use crate::brokers::queues::actor::{QueueActor, QueueActorCommand};
use crate::brokers::queues::persistence::types::PersistenceMode;
use crate::config::Config;
use crate::dashboard::models::queues::QueueBrokerSnapshot;

// ==========================================
// MANAGER COMMANDS
// ==========================================

pub enum ManagerCommand {
    CreateQueue {
        name: String,
        config: QueueConfig,
        reply: oneshot::Sender<Result<(), String>>,
    },
    GetQueueActor {
        name: String,
        reply: oneshot::Sender<Option<mpsc::Sender<QueueActorCommand>>>,
    },
    MoveToDLQ {
        queue_name: String,
        payload: Bytes,
        priority: u8,
    },
    DeleteQueue {
        name: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    GetSnapshot {
        reply: oneshot::Sender<QueueBrokerSnapshot>,
    },
}

// ==========================================
// QUEUE MANAGER (Router)
// ==========================================

#[derive(Clone)]
pub struct QueueManager {
    tx: mpsc::Sender<ManagerCommand>,
}

impl QueueManager {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(256);
        let manager_tx = tx.clone();
        
        tokio::spawn(Self::run_manager_loop(rx, manager_tx));
        
        Self { tx }
    }

    async fn run_manager_loop(
        mut rx: mpsc::Receiver<ManagerCommand>,
        manager_tx: mpsc::Sender<ManagerCommand>,
    ) {
        let mut actors: HashMap<String, mpsc::Sender<QueueActorCommand>> = HashMap::new();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                ManagerCommand::CreateQueue { name, config, reply } => {
                    if !actors.contains_key(&name) {
                        let actor_tx = Self::spawn_queue_actor(
                            name.clone(), 
                            config, 
                            manager_tx.clone()
                        );
                        actors.insert(name, actor_tx);
                    }
                    let _ = reply.send(Ok(()));
                }
                
                ManagerCommand::GetQueueActor { name, reply } => {
                    let _ = reply.send(actors.get(&name).cloned());
                }
                
                ManagerCommand::MoveToDLQ { queue_name, payload, priority } => {
                    let actor_tx = if let Some(tx) = actors.get(&queue_name) {
                        tx.clone()
                    } else {
                        let global_config = &Config::global().queue;
                        let config = QueueConfig {
                            visibility_timeout_ms: global_config.visibility_timeout_ms,
                            max_retries: global_config.max_retries,
                            ttl_ms: global_config.ttl_ms,
                            persistence: PersistenceMode::Memory, // Default to memory for implcit DLQ
                        };
                        let tx = Self::spawn_queue_actor(
                            queue_name.clone(),
                            config,
                            manager_tx.clone()
                        );
                        actors.insert(queue_name, tx.clone());
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

                ManagerCommand::DeleteQueue { name, reply } => {
                    if let Some(actor_tx) = actors.remove(&name) {
                        // 1. Stop Actor
                        let (stop_tx, stop_rx) = oneshot::channel();
                        if actor_tx.send(QueueActorCommand::Stop { reply: stop_tx }).await.is_ok() {
                            let _ = stop_rx.await;
                        }
                    }
                    
                    // 2. Delete Persistence
                    // We need to construct the path manually here or ask Config.
                    // Ideally QueueStore::destroy(name) would be better but Manager knows path too.
                    let base_path = std::path::PathBuf::from(&Config::global().queue.persistence_path);
                    let db_path = base_path.join(format!("{}.db", name));
                    let wal_path = base_path.join(format!("{}.db-wal", name));
                    let shm_path = base_path.join(format!("{}.db-shm", name));

                    // Best effort cleanup
                    let _ = std::fs::remove_file(db_path);
                    let _ = std::fs::remove_file(wal_path);
                    let _ = std::fs::remove_file(shm_path);

                    let _ = reply.send(Ok(()));
                }
                
                ManagerCommand::GetSnapshot { reply } => {
                    let mut active_queues = Vec::new();
                    let mut dlq_queues = Vec::new();
                    
                    for (_, actor_tx) in &actors {
                        let (tx, rx) = oneshot::channel();
                        if actor_tx.send(QueueActorCommand::GetSnapshot { reply: tx }).await.is_ok() {
                            if let Ok(summary) = rx.await {
                                if summary.name.ends_with("_dlq") {
                                    dlq_queues.push(summary);
                                } else {
                                    active_queues.push(summary);
                                }
                            }
                        }
                    }
                    let _ = reply.send(QueueBrokerSnapshot { active_queues, dlq_queues });
                }
            }
        }
    }

    fn spawn_queue_actor(
        name: String,
        config: QueueConfig,
        manager_tx: mpsc::Sender<ManagerCommand>,
    ) -> mpsc::Sender<QueueActorCommand> {
        let (tx, rx) = mpsc::channel(256);
        
        // Spawn actor
        let actor = QueueActor::new(name, config.clone(), rx, manager_tx);
        tokio::spawn(actor.run());
        
        tx
    }

    // --- Public API ---

    pub async fn declare_queue(&self, name: String, config: QueueConfig) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ManagerCommand::CreateQueue { name, config, reply: tx })
            .await
            .map_err(|_| "Manager closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())?
    }

    pub async fn delete_queue(&self, name: String) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ManagerCommand::DeleteQueue { name, reply: tx })
            .await
            .map_err(|_| "Manager closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())?
    }

    pub async fn push(&self, queue_name: String, payload: Bytes, priority: u8, delay_ms: Option<u64>) -> Result<(), String> {
        let actor = self.get_actor(&queue_name).await
            .ok_or_else(|| format!("Queue '{}' not found. Create it first.", queue_name))?;
        
        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::Push { payload, priority, delay_ms, reply: tx })
            .await
            .map_err(|_| "Actor closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())?;
        Ok(())
    }

    pub async fn pop(&self, queue_name: &str) -> Option<Message> {
        let actor = self.get_actor(queue_name).await?;
        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::Pop { reply: tx }).await.ok()?;
        rx.await.ok()?
    }

    pub async fn ack(&self, queue_name: &str, id: Uuid) -> bool {
        if let Some(actor) = self.get_actor(queue_name).await {
            let (tx, rx) = oneshot::channel();
            if actor.send(QueueActorCommand::Ack { id, reply: tx }).await.is_ok() {
                return rx.await.unwrap_or(false);
            }
        }
        false
    }

    pub async fn consume_batch(&self, queue_name: String, max: Option<usize>, wait_ms: Option<u64>) -> Result<Vec<Message>, String> {
        let actor = self.get_actor(&queue_name).await
            .ok_or_else(|| format!("Queue '{}' not found. Create it first.", queue_name))?;
        
        let max_val = max.unwrap_or(Config::global().queue.default_batch_size);
        let wait_val = wait_ms.unwrap_or(Config::global().queue.default_wait_ms);

        let (tx, rx) = oneshot::channel();
        actor.send(QueueActorCommand::ConsumeBatch { max: max_val, wait_ms: wait_val, reply: tx })
            .await
            .map_err(|_| "Actor closed".to_string())?;
        rx.await.map_err(|_| "No reply".to_string())
    }

    pub async fn get_snapshot(&self) -> QueueBrokerSnapshot {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(ManagerCommand::GetSnapshot { reply: tx }).await.is_ok() {
            rx.await.unwrap_or(QueueBrokerSnapshot { active_queues: vec![], dlq_queues: vec![] })
        } else {
            QueueBrokerSnapshot { active_queues: vec![], dlq_queues: vec![] }
        }
    }

    pub async fn exists(&self, name: &str) -> bool {
        self.get_actor(name).await.is_some()
    }

    async fn get_actor(&self, name: &str) -> Option<mpsc::Sender<QueueActorCommand>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ManagerCommand::GetQueueActor { 
            name: name.to_string(), 
            reply: tx 
        }).await.ok()?;
        rx.await.ok()?
    }
}

//! Queue Manager: Actor-based message queue system
//! 
//! Architecture: Actor per Queue
//! - QueueManager is a router that maps queue names to QueueActor handles
//! - Each QueueActor owns its state and processes commands sequentially
//! - DLQ handled via ManagerCommand::MoveToDLQ (no circular refs)

use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use uuid::Uuid;
use std::path::PathBuf;
use tracing::{error, info};

use crate::brokers::queues::queue::{QueueState, QueueConfig, Message};
use crate::brokers::queues::persistence::{QueueStore, types::{StorageOp, PersistenceMode}};
use crate::config::Config;
use crate::dashboard::models::queues::{QueueBrokerSnapshot, QueueSummary};

// ==========================================
// ACTOR COMMANDS
// ==========================================

pub enum QueueActorCommand {
    Push {
        payload: Bytes,
        priority: u8,
        delay_ms: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    Pop {
        reply: oneshot::Sender<Option<Message>>,
    },
    Ack {
        id: Uuid,
        reply: oneshot::Sender<bool>,
    },
    ConsumeBatch {
        max: usize,
        wait_ms: u64,
        reply: oneshot::Sender<Vec<Message>>,
    },
    GetSnapshot {
        reply: oneshot::Sender<QueueSummary>,
    },
    ProcessExpired,
}

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
    GetSnapshot {
        reply: oneshot::Sender<QueueBrokerSnapshot>,
    },
}

// ==========================================
// QUEUE ACTOR
// ==========================================

struct QueueActor {
    name: String,
    state: QueueState,
    config: QueueConfig,
    store: QueueStore,
    rx: mpsc::Receiver<QueueActorCommand>,
    manager_tx: mpsc::Sender<ManagerCommand>,
}

impl QueueActor {
    fn new(
        name: String,
        config: QueueConfig,
        rx: mpsc::Receiver<QueueActorCommand>,
        manager_tx: mpsc::Sender<ManagerCommand>,
    ) -> Self {
        let base_path = PathBuf::from(&Config::global().queue.persistence_path);
        
        // Ensure directory exists
        if let Err(e) = std::fs::create_dir_all(&base_path) {
            error!("Failed to create queue data directory at {:?}: {}", base_path, e);
        }

        let db_path = base_path.join(format!("{}.db", name));
        let store = QueueStore::new(db_path, config.persistence.clone());

        Self {
            name,
            state: QueueState::new(),
            config,
            store,
            rx,
            manager_tx,
        }
    }

    async fn run(mut self) {
        // RECOVERY PHASE
        match self.store.recover() {
            Ok(messages) => {
                let count = messages.len();
                if count > 0 {
                    for msg in messages {
                        self.state.push(msg);
                    }
                    info!("Queue '{}': Recovered {} messages from storage", self.name, count);
                }
            }
            Err(e) => {
                error!("Queue '{}': Persistence recovery failed: {}", self.name, e);
            }
        }

        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                QueueActorCommand::Push { payload, priority, delay_ms, reply } => {
                    let msg = Message::new(payload, priority, delay_ms);
                    self.state.push(msg.clone());
                    
                    // Persist
                    let _ = self.store.execute(StorageOp::Insert(msg)).await;
                    
                    let _ = reply.send(());
                }
                
                QueueActorCommand::Pop { reply } => {
                    let (msg_opt, _) = self.state.pop(self.config.visibility_timeout_ms);
                    
                    if let Some(msg) = &msg_opt {
                        // Persist visibility update
                        let _ = self.store.execute(StorageOp::UpdateState { 
                            id: msg.id, 
                            visible_at: msg.visible_at, 
                            attempts: msg.attempts 
                        }).await;
                    }

                    let _ = reply.send(msg_opt);
                }
                
                QueueActorCommand::Ack { id, reply } => {
                    let result = self.state.ack(id);
                    if result {
                        // Persist delete
                        let _ = self.store.execute(StorageOp::Delete(id)).await;
                    }
                    let _ = reply.send(result);
                }
                
                QueueActorCommand::ConsumeBatch { max, wait_ms, reply } => {
                    let (msgs, _) = self.state.take_batch(max, self.config.visibility_timeout_ms);
                    
                    if !msgs.is_empty() {
                        // Persist all visibility updates
                        for msg in &msgs {
                            let _ = self.store.execute(StorageOp::UpdateState { 
                                id: msg.id, 
                                visible_at: msg.visible_at, 
                                attempts: msg.attempts 
                            }).await;
                        }
                        let _ = reply.send(msgs);
                    } else {
                        // Wait with timeout, then retry
                        tokio::time::sleep(std::time::Duration::from_millis(wait_ms.min(100))).await;
                        let (msgs, _) = self.state.take_batch(max, self.config.visibility_timeout_ms);
                        
                        for msg in &msgs {
                            let _ = self.store.execute(StorageOp::UpdateState { 
                                id: msg.id, 
                                visible_at: msg.visible_at, 
                                attempts: msg.attempts 
                            }).await;
                        }
                        let _ = reply.send(msgs);
                    }
                }
                
                QueueActorCommand::GetSnapshot { reply } => {
                    let snapshot = self.state.get_snapshot(&self.name);
                    let _ = reply.send(snapshot);
                }
                
                QueueActorCommand::ProcessExpired => {
                    let (requeued, dlq_msgs) = self.state.process_expired(self.config.max_retries);
                    
                    // Persist Requeued (Update State to Ready/0)
                    for msg in requeued {
                        let _ = self.store.execute(StorageOp::UpdateState {
                            id: msg.id,
                            visible_at: 0, // Ready
                            attempts: msg.attempts
                        }).await;
                    }

                    // Persist DLQ Moves (Delete from current DB, Push to new Queue)
                    for msg in dlq_msgs {
                        // 1. Delete from current Queue DB
                        let _ = self.store.execute(StorageOp::Delete(msg.id)).await;

                        // 2. Push to DLQ Queue (via Manager)
                        let dlq_name = format!("{}_dlq", self.name);
                        let _ = self.manager_tx.send(ManagerCommand::MoveToDLQ {
                            queue_name: dlq_name,
                            payload: msg.payload,
                            priority: msg.priority,
                        }).await;
                    }
                }
            }
        }
    }
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
        
        // Spawn pulse loop for ProcessExpired
        let pulse_tx = tx.clone();
        let visibility_timeout = config.visibility_timeout_ms;
        tokio::spawn(async move {
            let check_interval = std::time::Duration::from_millis(
                (visibility_timeout / 4).max(50).min(1000)
            );
            loop {
                tokio::time::sleep(check_interval).await;
                if pulse_tx.send(QueueActorCommand::ProcessExpired).await.is_err() {
                    break;
                }
            }
        });
        
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

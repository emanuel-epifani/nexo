//! Stream Manager: Actor-based Router
//!
//! Routes commands to per-topic TopicActor instances.
//! Single-log model: no partitions, no rebalancing.

use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use crate::brokers::stream::topic::TopicConfig;
use crate::brokers::stream::message::Message;
use crate::brokers::stream::commands::{StreamCreateOptions, SeekTarget};
use crate::brokers::stream::actor::{TopicCommand, TopicActor};
use crate::brokers::stream::persistence::{StorageManager, StorageCommand};
use crate::config::SystemStreamConfig;
use crate::dashboard::stream::StreamBrokerSnapshot;

// ==========================================
// MANAGER COMMANDS
// ==========================================

pub enum ManagerCommand {
    GetTopicActor {
        topic: String,
        reply: oneshot::Sender<Option<mpsc::Sender<TopicCommand>>>,
    },
    CreateTopic {
        name: String,
        options: StreamCreateOptions,
        reply: oneshot::Sender<Result<(), String>>,
    },
    DeleteTopic {
        name: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    DisconnectClient {
        client_id: String,
        reply: oneshot::Sender<()>,
    },
    GetSnapshot {
        reply: oneshot::Sender<StreamBrokerSnapshot>,
    }
}

// ==========================================
// STREAM MANAGER
// ==========================================

#[derive(Clone)]
pub struct StreamManager {
    tx: mpsc::Sender<ManagerCommand>,
}

impl StreamManager {
    pub fn new(config: SystemStreamConfig) -> Self {
        let (tx, mut rx) = mpsc::channel(100);
        let system_config = config.clone();

        tokio::spawn(async move {
            let mut actors = HashMap::<String, mpsc::Sender<TopicCommand>>::new();
            
            // SPAWN STORAGE MANAGER
            let (storage_tx, storage_rx) = mpsc::channel(50_000); // Backpressure protection buffer
            let max_open_files = 1024; // TODO: move to config
            let max_segment_size = system_config.max_segment_size;
            
            let storage_manager = StorageManager::new(
                system_config.persistence_path.clone(),
                storage_rx,
                max_open_files,
                system_config.default_flush_ms,
                max_segment_size,
            );
            tokio::spawn(storage_manager.run());

            // WARM START: Discover and restore topics from filesystem
            let persistence_path = std::path::PathBuf::from(&system_config.persistence_path);
            if persistence_path.exists() {
                if let Ok(entries) = std::fs::read_dir(&persistence_path) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(topic_name) = path.file_name().and_then(|n| n.to_str()) {
                                let topic_config = TopicConfig::from_options(StreamCreateOptions::default(), &system_config);
                                let (t_tx, t_rx) = mpsc::channel(topic_config.actor_channel_capacity);
                                
                                let config_clone = topic_config.clone();
                                let actor = TopicActor::new(
                                    topic_name.to_string(),
                                    t_rx,
                                    t_tx.clone(),
                                    topic_config,
                                    storage_tx.clone(),
                                ).await;
                                tokio::spawn(actor.run(config_clone));
                                actors.insert(topic_name.to_string(), t_tx);
                                tracing::info!("[StreamManager] Warm start: Restored topic '{}'", topic_name);
                            }
                        }
                    }
                }
            }
            
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    ManagerCommand::CreateTopic { name, options, reply } => {
                        if !actors.contains_key(&name) {
                            let topic_config = TopicConfig::from_options(options, &system_config);
                            let (t_tx, t_rx) = mpsc::channel(topic_config.actor_channel_capacity);
                            
                            tracing::info!("[StreamManager] Creating topic '{}' with config {:?}", name, topic_config);
                            
                            let config_clone = topic_config.clone();
                            let actor = TopicActor::new(
                                name.clone(),
                                t_rx,
                                t_tx.clone(),
                                topic_config,
                                storage_tx.clone(),
                            ).await;
                            tokio::spawn(actor.run(config_clone));
                            actors.insert(name, t_tx);
                        }
                        let _ = reply.send(Ok(()));
                    },
                    ManagerCommand::DeleteTopic { name, reply } => {
                        if let Some(actor_tx) = actors.remove(&name) {
                            let (stop_tx, stop_rx) = oneshot::channel();
                            if actor_tx.send(TopicCommand::Stop { reply: stop_tx }).await.is_ok() {
                                let _ = stop_rx.await;
                            }
                        }

                        let base_path = std::path::PathBuf::from(&system_config.persistence_path);
                        let topic_path = base_path.join(&name);
                        if topic_path.exists() {
                            let _ = std::fs::remove_dir_all(topic_path);
                        }

                        let _ = reply.send(Ok(()));
                    },
                    ManagerCommand::GetTopicActor { topic, reply } => {
                        let _ = reply.send(actors.get(&topic).cloned());
                    },
                    ManagerCommand::DisconnectClient { client_id, reply } => {
                        tracing::info!("[StreamManager] Disconnecting client: {}", client_id);
                        let mut waits = Vec::new();
                        for actor in actors.values() {
                            let (tx, rx) = oneshot::channel();
                            if actor.send(TopicCommand::LeaveGroup { client_id: client_id.clone(), reply: tx }).await.is_ok() {
                                waits.push(rx);
                            }
                        }
                        for rx in waits {
                            let _ = rx.await;
                        }
                        let _ = reply.send(());
                    },
                    ManagerCommand::GetSnapshot { reply } => {
                        let mut summaries = Vec::new();
                        for (_name, actor) in &actors {
                            let (s_tx, s_rx) = oneshot::channel();
                            let _ = actor.send(TopicCommand::GetSnapshot { reply: s_tx }).await;
                            if let Ok(summary) = s_rx.await {
                                summaries.push(summary);
                            }
                        }
                        let _ = reply.send(StreamBrokerSnapshot { topics: summaries });
                    }
                }
            }
        });

        Self { tx }
    }

    // --- Public API ---

    pub async fn create_topic(&self, name: String, options: StreamCreateOptions) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ManagerCommand::CreateTopic { name, options, reply: tx }).await.map_err(|_| "Server closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn delete_topic(&self, name: String) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ManagerCommand::DeleteTopic { name, reply: tx }).await.map_err(|_| "Server closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    async fn get_actor(&self, topic: &str) -> Option<mpsc::Sender<TopicCommand>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ManagerCommand::GetTopicActor { topic: topic.to_string(), reply: tx }).await;
        rx.await.ok().flatten()
    }

    pub async fn publish(&self, topic: &str, payload: Bytes) -> Result<u64, String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Publish { payload, reply: tx }).await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn read(&self, topic: &str, from_seq: u64, limit: usize) -> Vec<Message> {
        if let Some(actor) = self.get_actor(topic).await {
            let (tx, rx) = oneshot::channel();
            let _ = actor.send(TopicCommand::Read { from_seq, limit, reply: tx }).await;
            rx.await.unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    pub async fn fetch(&self, group: &str, client: &str, limit: usize, topic: &str) -> Result<Vec<Message>, String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Fetch {
            group_id: group.to_string(),
            client_id: client.to_string(),
            limit,
            reply: tx,
        }).await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn ack(&self, group: &str, topic: &str, seq: u64) -> Result<(), String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Ack { group_id: group.to_string(), seq, reply: tx })
            .await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn nack(&self, group: &str, topic: &str, seq: u64) -> Result<(), String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Nack { group_id: group.to_string(), seq, reply: tx })
            .await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn seek(&self, group: &str, topic: &str, target: SeekTarget) -> Result<(), String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Seek { group_id: group.to_string(), target, reply: tx })
            .await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn join_group(&self, group: &str, topic: &str, client: &str) -> Result<u64, String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::JoinGroup {
            group_id: group.to_string(),
            client_id: client.to_string(),
            reply: tx,
        }).await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn disconnect(&self, client_id: String) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ManagerCommand::DisconnectClient { client_id, reply: tx }).await;
        let _ = rx.await;
    }

    pub async fn get_snapshot(&self) -> StreamBrokerSnapshot {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ManagerCommand::GetSnapshot { reply: tx }).await;
        rx.await.unwrap_or(StreamBrokerSnapshot { topics: vec![] })
    }

    pub async fn exists(&self, name: &str) -> bool {
        self.get_actor(name).await.is_some()
    }
}

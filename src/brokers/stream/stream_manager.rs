//! Stream Manager: Actor-based Router
//! 
//! Structure:
//! - StreamManager (Router): Maps topic names to TopicActor handles.
//! - TopicActor (1 per Topic): Owns TopicState, handles Pub/Sub/Groups sequentially.
//! 
//! CLEAN ARCHITECTURE V0: All public methods are async and await actor confirmation.

use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use crate::brokers::stream::topic::TopicConfig;
use crate::brokers::stream::message::Message;
use crate::brokers::stream::commands::StreamCreateOptions;
use crate::brokers::stream::actor::{TopicCommand, TopicActor};
use crate::config::SystemStreamConfig;
use crate::dashboard::dashboard_stream::StreamBrokerSnapshot;

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
        reply: oneshot::Sender<Result<(), String>>, // Ack
    },
    DeleteTopic {
        name: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    DisconnectClient {
        client_id: String,
        reply: oneshot::Sender<()>, // Ack
    },
    GetSnapshot {
        reply: oneshot::Sender<StreamBrokerSnapshot>,
    }
}

// ==========================================
// STREAM MANAGER (The Router)
// ==========================================

#[derive(Clone)]
pub struct StreamManager {
    tx: mpsc::Sender<ManagerCommand>,
    config: SystemStreamConfig,
}

impl StreamManager {
    pub fn new(config: SystemStreamConfig) -> Self {
        let (tx, mut rx) = mpsc::channel(100);
        let actor_capacity = config.actor_channel_capacity;
        let system_config = config.clone();

        tokio::spawn(async move {
            let mut actors = HashMap::<String, mpsc::Sender<TopicCommand>>::new();
            
            // WARM START: Discover and restore topics from filesystem
            let persistence_path = std::path::PathBuf::from(&system_config.persistence_path);
            if persistence_path.exists() {
                if let Ok(entries) = std::fs::read_dir(&persistence_path) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(topic_name) = path.file_name().and_then(|n| n.to_str()) {
                                let topic_config = TopicConfig::from_options(StreamCreateOptions::default(), &system_config);
                                let (t_tx, t_rx) = mpsc::channel(actor_capacity);
                                let eviction_interval = topic_config.eviction_interval_ms;
                                
                                let actor = TopicActor::new(
                                    topic_name.to_string(),
                                    t_rx,
                                    t_tx.clone(),
                                    topic_config
                                );
                                tokio::spawn(actor.run(eviction_interval));
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
                        let topic_config = TopicConfig::from_options(options, &system_config);
                        
                        // Fail Fast: Hard Limit on Partitions to prevent FD exhaustion
                        if topic_config.partitions > system_config.max_partitions {
                            let _ = reply.send(Err(format!("Too much partitions requested ({}). Allowed max {} partitions for each topic.", topic_config.partitions, system_config.max_partitions)));
                            continue;
                        }

                        if !actors.contains_key(&name) {
                            let (t_tx, t_rx) = mpsc::channel(actor_capacity); 
                            let eviction_interval = topic_config.eviction_interval_ms;
                            
                            tracing::info!("[StreamManager] Creating topic '{}' with config {:?}", name, topic_config);
                            
                            let actor = TopicActor::new(
                                name.clone(), 
                                t_rx,
                                t_tx.clone(),
                                topic_config
                            );
                            tokio::spawn(actor.run(eviction_interval));
                            actors.insert(name, t_tx);
                        }
                        let _ = reply.send(Ok(()));
                    },
                    ManagerCommand::DeleteTopic { name, reply } => {
                        if let Some(actor_tx) = actors.remove(&name) {
                            // 1. Stop Actor
                            let (stop_tx, stop_rx) = oneshot::channel();
                            if actor_tx.send(TopicCommand::Stop { reply: stop_tx }).await.is_ok() {
                                let _ = stop_rx.await;
                            }
                        }

                        // 2. Delete Persistence
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
                        // Wait for all rebalances to complete
                        for rx in waits {
                            let _ = rx.await;
                        }
                        tracing::info!("[StreamManager] Client {} fully disconnected and groups rebalanced", client_id);
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
                         let _ = reply.send(StreamBrokerSnapshot {
                             total_topics: actors.len(),
                             total_active_groups: 0, 
                             topics: summaries,
                         });
                    }
                }
            }
        });

        Self { tx, config }
    }

    // --- Public API ---

    pub fn register_session(&self, client_id: String) -> StreamSession {
        StreamSession {
            client_id,
            manager: self.clone(),
        }
    }

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

    pub async fn publish(&self, topic: &str, options: crate::brokers::stream::commands::StreamPublishOptions, payload: Bytes) -> Result<u64, String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Publish { options, payload, reply: tx }).await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn read(&self, topic: &str, offset: u64, limit: usize) -> Vec<Message> {
        if let Some(actor) = self.get_actor(topic).await {
            let (tx, rx) = oneshot::channel();
            let _ = actor.send(TopicCommand::Fetch { 
                group_id: None, client_id: None, generation_id: None,
                partition: 0, offset, limit, reply: tx 
            }).await;
            
            match rx.await {
                Ok(Ok(msgs)) => msgs,
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        }
    }

    pub async fn fetch_group(&self, group: &str, client: &str, gen_id: u64, partition: u32, offset: u64, limit: usize, topic: &str) -> Result<Vec<Message>, String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Fetch {
            group_id: Some(group.to_string()),
            client_id: Some(client.to_string()),
            generation_id: Some(gen_id),
            partition,
            offset,
            limit,
            reply: tx
        }).await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn join_group(&self, group: &str, topic: &str, client: &str) -> Result<(u64, Vec<u32>, HashMap<u32, u64>), String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::JoinGroup { 
            group_id: group.to_string(), 
            client_id: client.to_string(), 
            reply: tx 
        }).await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn commit_offset(&self, group: &str, topic: &str, partition: u32, offset: u64, client: &str, gen_id: u64) -> Result<(), String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Commit { 
            group_id: group.to_string(), 
            partition, 
            offset, 
            client_id: client.to_string(), 
            generation_id: gen_id, 
            reply: tx 
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
         rx.await.unwrap_or(StreamBrokerSnapshot {
             total_topics: 0, total_active_groups: 0, topics: vec![]
         })
    }

    pub async fn exists(&self, name: &str) -> bool {
        self.get_actor(name).await.is_some()
    }
}

pub struct StreamSession {
    #[allow(dead_code)]
    client_id: String,
    #[allow(dead_code)]
    manager: StreamManager,
}

// Note: Disconnect is now called explicitly in socket_network.rs before drop.
// This ensures rebalance completes synchronously before the connection handler returns.

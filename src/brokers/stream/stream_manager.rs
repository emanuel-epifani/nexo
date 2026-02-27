//! Stream Manager: Multi-Topic Router
//!
//! Routes commands to per-topic TopicActor instances.
//! Uses DashMap for high-concurrency lookups (Address Book).
//! Single-log model: no partitions, no rebalancing.

use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use tracing::{info, info_span};

use crate::brokers::stream::topic::TopicConfig;
use crate::brokers::stream::message::Message;
use crate::brokers::stream::commands::{StreamCreateOptions, SeekTarget};
use crate::brokers::stream::actor::{TopicCommand, TopicActor};
use crate::brokers::stream::persistence::{StorageManager, StorageCommand};
use crate::config::SystemStreamConfig;
use crate::dashboard::stream::StreamBrokerSnapshot;

// ==========================================
// STREAM MANAGER
// ==========================================

#[derive(Clone)]
pub struct StreamManager {
    actors: Arc<DashMap<String, mpsc::Sender<TopicCommand>>>,
    storage_tx: mpsc::Sender<StorageCommand>,
    config: SystemStreamConfig,
}

impl StreamManager {
    pub fn new(config: SystemStreamConfig) -> Self {
        let actors = Arc::new(DashMap::new());
        let (storage_tx, storage_rx) = mpsc::channel(50_000); // Backpressure protection buffer
        
        // SPAWN STORAGE MANAGER
        let max_open_files = 1024; // TODO: move to config
        let storage_manager = StorageManager::new(
            config.persistence_path.clone(),
            storage_rx,
            max_open_files,
            config.default_flush_ms,
            config.max_segment_size,
        );
        tokio::spawn(storage_manager.run());

        let manager = Self {
            actors: actors.clone(),
            storage_tx: storage_tx.clone(),
            config: config.clone(),
        };

        // WARM START: Discover and restore topics from filesystem
        let persistence_path = std::path::PathBuf::from(&config.persistence_path);
        if persistence_path.exists() {
            if let Ok(entries) = std::fs::read_dir(&persistence_path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        if let Some(topic_name) = path.file_name().and_then(|n| n.to_str()) {
                            let topic_config = TopicConfig::from_options(StreamCreateOptions::default(), &config);
                            let (t_tx, t_rx) = mpsc::channel(topic_config.actor_channel_capacity);
                            
                            let name = topic_name.to_string();
                            let config_clone = topic_config.clone();
                            let st_tx = storage_tx.clone();
                            let self_tx = t_tx.clone();
                            let t_name = name.clone();

                            // Instantiate and run actor
                            tokio::spawn(async move {
                                let actor = TopicActor::new(
                                    t_name,
                                    t_rx,
                                    self_tx,
                                    config_clone.clone(),
                                    st_tx,
                                ).await;
                                actor.run(config_clone).await;
                            });

                            actors.insert(name.clone(), t_tx);
                            info!("[StreamManager] Warm start: Restored topic '{}'", name);
                        }
                    }
                }
            }
        }

        manager
    }

    // --- Public API ---

    pub async fn create_topic(&self, name: String, options: StreamCreateOptions) -> Result<(), String> {
        use dashmap::mapref::entry::Entry;

        match self.actors.entry(name.clone()) {
            Entry::Occupied(_) => Ok(()), // Already exists
            Entry::Vacant(v) => {
                let topic_config = TopicConfig::from_options(options, &self.config);
                let (t_tx, t_rx) = mpsc::channel(topic_config.actor_channel_capacity);
                
                info!("[StreamManager] Creating topic '{}'", name);
                
                let config_clone = topic_config.clone();
                let storage_tx = self.storage_tx.clone();
                let self_tx = t_tx.clone();
                let topic_name = name.clone();

                tokio::spawn(async move {
                    let actor = TopicActor::new(
                        topic_name,
                        t_rx,
                        self_tx,
                        config_clone.clone(),
                        storage_tx,
                    ).await;
                    actor.run(config_clone).await;
                });

                v.insert(t_tx);
                Ok(())
            }
        }
    }

    pub async fn delete_topic(&self, name: String) -> Result<(), String> {
        if let Some((_, actor_tx)) = self.actors.remove(&name) {
            let (del_tx, del_rx) = oneshot::channel();
            if actor_tx.send(TopicCommand::Delete { reply: del_tx }).await.is_ok() {
                let _ = del_rx.await;
            }
        }
        Ok(())
    }

    #[inline]
    fn get_actor(&self, topic: &str) -> Option<mpsc::Sender<TopicCommand>> {
        self.actors.get(topic).map(|r| r.value().clone())
    }

    pub async fn publish(&self, topic: &str, payload: Bytes) -> Result<u64, String> {
        let actor = self.get_actor(topic).ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Publish { payload, reply: tx }).await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn read(&self, topic: &str, from_seq: u64, limit: usize) -> Vec<Message> {
        if let Some(actor) = self.get_actor(topic) {
            let (tx, rx) = oneshot::channel();
            let _ = actor.send(TopicCommand::Read { from_seq, limit, reply: tx }).await;
            rx.await.unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    pub async fn fetch(&self, group: &str, client: &str, limit: usize, topic: &str) -> Result<Vec<Message>, String> {
        let actor = self.get_actor(topic).ok_or("Topic not found")?;
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
        let actor = self.get_actor(topic).ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Ack { group_id: group.to_string(), seq, reply: tx })
            .await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn nack(&self, group: &str, topic: &str, seq: u64) -> Result<(), String> {
        let actor = self.get_actor(topic).ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Nack { group_id: group.to_string(), seq, reply: tx })
            .await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn seek(&self, group: &str, topic: &str, target: SeekTarget) -> Result<(), String> {
        let actor = self.get_actor(topic).ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Seek { group_id: group.to_string(), target, reply: tx })
            .await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn join_group(&self, group: &str, topic: &str, client: &str) -> Result<u64, String> {
        let actor = self.get_actor(topic).ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::JoinGroup {
            group_id: group.to_string(),
            client_id: client.to_string(),
            reply: tx,
        }).await.map_err(|_| "Actor closed")?;
        rx.await.map_err(|_| "No reply")?
    }

    pub async fn disconnect(&self, client_id: String) {
        info!("[StreamManager] Disconnecting client: {}", client_id);
        let mut waits = Vec::new();
        // Since we are iterating all actors, this might take time if there are many.
        // But DashMap allows parallel iteration if needed. For now, simple loop.
        for entry in self.actors.iter() {
            let actor = entry.value();
            let (tx, rx) = oneshot::channel();
            if actor.send(TopicCommand::LeaveGroup { client_id: client_id.clone(), reply: tx }).await.is_ok() {
                waits.push(rx);
            }
        }
        for rx in waits {
            let _ = rx.await;
        }
    }

    pub async fn get_snapshot(&self) -> StreamBrokerSnapshot {
        let mut summaries = Vec::new();
        for entry in self.actors.iter() {
            let actor = entry.value();
            let (s_tx, s_rx) = oneshot::channel();
            let _ = actor.send(TopicCommand::GetSnapshot { reply: s_tx }).await;
            if let Ok(summary) = s_rx.await {
                summaries.push(summary);
            }
        }
        StreamBrokerSnapshot { topics: summaries }
    }

    pub async fn exists(&self, name: &str) -> bool {
        self.actors.contains_key(name)
    }
}

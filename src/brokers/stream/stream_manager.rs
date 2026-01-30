//! Stream Manager: Architecture Refactor -> Actor Model
//! 
//! Structure:
//! - StreamManager (Router): Maps topic names to TopicActor handles.
//! - TopicActor (1 per Topic): Owns TopicState, handles Pub/Sub/Groups sequentially.
//! 
//! CLEAN ARCHITECTURE V0: All public methods are async and await actor confirmation.

use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use crate::brokers::stream::topic::TopicState;
use crate::brokers::stream::group::ConsumerGroup;
use crate::brokers::stream::message::Message;
use crate::dashboard::models::stream::StreamBrokerSnapshot;
use crate::config::Config;
use crate::brokers::stream::commands::{StreamCreateOptions, StreamPublishOptions};
use crate::brokers::stream::persistence::{recover_topic, StreamWriter, WriterCommand, StreamStorageOp};
use crate::brokers::stream::persistence::types::PersistenceMode;


// ==========================================
// COMMANDS (The Internal Protocol)
// ==========================================

pub enum TopicCommand {
    Publish {
        options: StreamPublishOptions,
        payload: Bytes,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    Fetch {
        group_id: Option<String>,
        client_id: Option<String>,
        generation_id: Option<u64>,
        partition: u32,
        offset: u64,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<Message>, String>>,
    },
    JoinGroup {
        group_id: String,
        client_id: String,
        reply: oneshot::Sender<Result<(u64, Vec<u32>, HashMap<u32, u64>), String>>,
    },
    Commit {
        group_id: String,
        partition: u32,
        offset: u64,
        client_id: String,
        generation_id: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    LeaveGroup {
        client_id: String,
        reply: oneshot::Sender<()>, // Ack needed for clean shutdown
    },
    GetSnapshot {
        reply: oneshot::Sender<crate::dashboard::models::stream::TopicSummary>,
    }
}

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
    DisconnectClient {
        client_id: String,
        reply: oneshot::Sender<()>, // Ack
    },
    GetSnapshot {
        reply: oneshot::Sender<StreamBrokerSnapshot>,
    }
}

// ==========================================
// TOPIC ACTOR (The Engine)
// ==========================================

struct TopicActor {
    state: TopicState,
    groups: HashMap<String, ConsumerGroup>,
    client_map: HashMap<String, Vec<String>>, 
    rx: mpsc::Receiver<TopicCommand>,
    last_partition: u32,
    persistence_mode: PersistenceMode,
    writer_tx: mpsc::Sender<WriterCommand>,
}

impl TopicActor {
    fn new(name: String, partitions: u32, rx: mpsc::Receiver<TopicCommand>, mode: PersistenceMode) -> Self {
        // 1. Recovery
        let recovered = recover_topic(&name, partitions);
        let state = TopicState::restore(name.clone(), partitions, recovered.partitions_data);
        
        let mut groups = HashMap::new();
        // Restore Groups
        for (gid, (gen_id, committed)) in recovered.groups_data {
            let mut g = ConsumerGroup::new(gid.clone(), name.clone());
            g.generation_id = gen_id;
            g.committed_offsets = committed;
            groups.insert(gid, g);
        }

        // 2. Spawn Writer
        let (w_tx, w_rx) = mpsc::channel(1000);
        let writer = StreamWriter::new(name.clone(), partitions, mode.clone(), w_rx);
        tokio::spawn(writer.run());

        Self {
            state,
            groups,
            client_map: HashMap::new(),
            rx,
            last_partition: 0,
            persistence_mode: mode,
            writer_tx: w_tx,
        }
    }

    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                TopicCommand::Publish { options, payload, reply } => {
                    let p_count = self.state.get_partitions_count() as u32;
                    
                    let partition = if let Some(key) = &options.key {
                        // Key-based partitioning (simple hash)
                        use std::hash::{Hash, Hasher};
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        key.hash(&mut hasher);
                        (hasher.finish() % (p_count as u64)) as u32
                    } else {
                        // Round-robin
                        let p = self.last_partition % p_count;
                        self.last_partition = (self.last_partition + 1) % p_count;
                        p
                    };
                    
                    let (offset, timestamp) = self.state.publish(partition, payload.clone());
                    
                    // Persistence
                    let (tx, rx) = if let PersistenceMode::Sync = self.persistence_mode {
                        let (tx, rx) = oneshot::channel();
                        (Some(tx), Some(rx))
                    } else {
                        (None, None)
                    };

                    let op = StreamStorageOp::Append {
                        partition,
                        offset,
                        timestamp,
                        payload,
                    };

                    let _ = self.writer_tx.send(WriterCommand { op, reply: tx }).await;

                    if let Some(wait_rx) = rx {
                        match wait_rx.await {
                            Ok(Ok(_)) => { let _ = reply.send(Ok(offset)); },
                            Ok(Err(e)) => { let _ = reply.send(Err(e)); },
                            Err(_) => { let _ = reply.send(Err("Writer died".to_string())); }
                        }
                    } else {
                        let _ = reply.send(Ok(offset));
                    }
                },
                TopicCommand::Fetch { group_id, client_id, generation_id, partition, offset, limit, reply } => {
                    if let (Some(gid), Some(cid), Some(gen_id)) = (group_id, client_id, generation_id) {
                        if let Some(group) = self.groups.get(&gid) {
                            if !group.validate_epoch(gen_id) || !group.is_member(&cid) {
                                let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                                continue;
                            }
                            if let Some(assignments) = group.get_assignments(&cid) {
                                if !assignments.contains(&partition) {
                                    let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                                    continue;
                                }
                            } else {
                                let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                                continue;
                            }
                        } else {
                            let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                            continue;
                        }
                    }
                    let msgs = self.state.read(partition, offset, limit);
                    tracing::debug!("[TopicActor:{}] Fetch: P{} off={} lim={} -> got {} msgs", self.state.name, partition, offset, limit, msgs.len());
                    let _ = reply.send(Ok(msgs));
                },
                TopicCommand::JoinGroup { group_id, client_id, reply } => {
                    tracing::info!("[TopicActor:{}] JoinGroup: Group={}, Client={}", self.state.name, group_id, client_id);
                    let g = self.groups.entry(group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new(group_id.clone(), self.state.name.clone()));
                    
                    let is_new = !g.is_member(&client_id);
                    g.add_member(client_id.clone());
                    
                    let groups_list = self.client_map.entry(client_id.clone()).or_default();
                    if !groups_list.contains(&group_id) {
                        groups_list.push(group_id.clone());
                    }
                    
                    if is_new {
                        tracing::info!("[TopicActor:{}] NEW member. Triggering rebalance.", self.state.name);
                        g.rebalance(self.state.get_partitions_count() as u32);
                    } else {
                        tracing::info!("[TopicActor:{}] EXISTING member. Current Gen: {}", self.state.name, g.generation_id);
                    }

                    let assigned = g.get_assignments(&client_id).cloned().unwrap_or_default();
                    let mut start_offsets = HashMap::new();
                    for &pid in &assigned {
                        let off = g.get_committed_offset(pid);
                        start_offsets.insert(pid, off);
                        tracing::info!("[TopicActor:{}]   -> Assignment: P{} start_at={}", self.state.name, pid, off);
                    }
                    
                    let _ = reply.send(Ok((g.generation_id, assigned, start_offsets)));
                },
                TopicCommand::Commit { group_id, partition, offset, client_id, generation_id, reply } => {
                    if let Some(g) = self.groups.get_mut(&group_id) {
                        if !g.validate_epoch(generation_id) || !g.is_member(&client_id) {
                             let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                        } else {
                            if let Some(assign) = g.get_assignments(&client_id) {
                                if assign.contains(&partition) {
                                    g.commit(partition, offset);
                                    
                                    // Persistence
                                    // Commits are usually async even in sync mode? 
                                    // Actually kafka commits can be sync or async.
                                    // Here let's follow the mode.
                                    
                                    let (tx, rx) = if let PersistenceMode::Sync = self.persistence_mode {
                                        let (tx, rx) = oneshot::channel();
                                        (Some(tx), Some(rx))
                                    } else {
                                        (None, None)
                                    };

                                    let op = StreamStorageOp::Commit {
                                        group: group_id.clone(),
                                        partition,
                                        offset,
                                        generation_id,
                                    };

                                    let _ = self.writer_tx.send(WriterCommand { op, reply: tx }).await;

                                    if let Some(wait_rx) = rx {
                                        match wait_rx.await {
                                            Ok(Ok(_)) => { let _ = reply.send(Ok(())); },
                                            Ok(Err(e)) => { let _ = reply.send(Err(e)); },
                                            Err(_) => { let _ = reply.send(Err("Writer died".to_string())); }
                                        }
                                    } else {
                                        let _ = reply.send(Ok(()));
                                    }

                                } else {
                                    let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                                }
                            } else {
                                let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                            }
                        }
                    } else {
                        let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                    }
                },
                TopicCommand::LeaveGroup { client_id, reply } => {
                     tracing::info!("[TopicActor:{}] LeaveGroup Req: Client={}", self.state.name, client_id);
                     if let Some(g_ids) = self.client_map.remove(&client_id) {
                         tracing::info!("[TopicActor:{}] Found client in groups: {:?}", self.state.name, g_ids);
                         for gid in g_ids {
                             if let Some(g) = self.groups.get_mut(&gid) {
                                 if g.remove_member(&client_id) {
                                     tracing::info!("[TopicActor:{}] Client {} removed from Group {}. Triggering rebalance.", self.state.name, client_id, gid);
                                     g.rebalance(self.state.get_partitions_count() as u32);
                                 } else {
                                     tracing::warn!("[TopicActor:{}] Client {} NOT found in Group members {}", self.state.name, client_id, gid);
                                 }
                             }
                         }
                     } else {
                         tracing::warn!("[TopicActor:{}] Client {} NOT found in client_map", self.state.name, client_id);
                     }
                     let _ = reply.send(());
                },
                    TopicCommand::GetSnapshot { reply } => {
                        let mut partitions_info = Vec::new();
                        
                        for (i, partition) in self.state.partitions.iter().enumerate() {
                            let p_id = i as u32;

                            // 1. Groups Info
                            let mut groups_info = Vec::new();
                            for group in self.groups.values() {
                                // Get committed offset for this partition (defaults to 0 if not found)
                                let committed = group.get_committed_offset(p_id);
                                
                                // We include the group if it exists on this topic
                                groups_info.push(crate::dashboard::models::stream::ConsumerGroupSummary {
                                    id: group.id.clone(),
                                    committed_offset: committed,
                                });
                            }
                            
                            // 2. Messages (Full Content)
                            let messages_preview: Vec<crate::dashboard::models::stream::MessagePreview> = partition
                                .log
                                .iter()
                                .rev()
                                .map(|msg| {
                                    // Skip first byte (datatype indicator)
                                    let payload_bytes = if msg.payload.len() > 0 {
                                        &msg.payload[1..]
                                    } else {
                                        &msg.payload[..]
                                    };

                                    // Try to parse as JSON, fallback to String
                                    let payload_json: serde_json::Value = serde_json::from_slice(payload_bytes)
                                        .unwrap_or_else(|_| {
                                            serde_json::Value::String(String::from_utf8_lossy(payload_bytes).to_string())
                                        });

                                    crate::dashboard::models::stream::MessagePreview {
                                        offset: msg.offset,
                                        timestamp: chrono::DateTime::from_timestamp_millis(msg.timestamp as i64)
                                            .unwrap_or_default()
                                            .to_rfc3339(),
                                        payload: payload_json,
                                    }
                                })
                                .collect();
                            
                            partitions_info.push(crate::dashboard::models::stream::PartitionInfo {
                                id: p_id,
                                messages: messages_preview,
                                groups: groups_info,
                                last_offset: partition.next_offset,
                            });
                        }
                        
                        let summary = crate::dashboard::models::stream::TopicSummary {
                             name: self.state.name.clone(),
                             partitions: partitions_info,
                        };
                        let _ = reply.send(summary);
                    }
            }
        }
    }
}

// ==========================================
// STREAM MANAGER (The Router)
// ==========================================

#[derive(Clone)]
pub struct StreamManager {
    tx: mpsc::Sender<ManagerCommand>,
}

impl StreamManager {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(100);
        let actor_capacity = Config::global().stream.actor_channel_capacity;
        let default_partitions = Config::global().stream.default_partitions;

        tokio::spawn(async move {
            let mut actors = HashMap::<String, mpsc::Sender<TopicCommand>>::new();
            
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    ManagerCommand::CreateTopic { name, options, reply } => {
                        if !actors.contains_key(&name) {
                            let partitions = options.partitions.unwrap_or(default_partitions);
                            let persistence: PersistenceMode = options.persistence.into();
                            
                            let (t_tx, t_rx) = mpsc::channel(actor_capacity); 
                            tracing::info!("[StreamManager] Creating topic '{}' with {} partitions, mode {:?}", name, partitions, persistence);
                            let actor = TopicActor::new(name.clone(), partitions, t_rx, persistence);
                            tokio::spawn(actor.run());
                            actors.insert(name, t_tx);
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

        Self { tx }
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

    async fn get_actor(&self, topic: &str) -> Option<mpsc::Sender<TopicCommand>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ManagerCommand::GetTopicActor { topic: topic.to_string(), reply: tx }).await;
        rx.await.ok().flatten()
    }

    pub async fn publish(&self, topic: &str, options: StreamPublishOptions, payload: Bytes) -> Result<u64, String> {
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

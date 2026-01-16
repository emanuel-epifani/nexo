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

// ==========================================
// COMMANDS (The Internal Protocol)
// ==========================================

pub enum TopicCommand {
    Publish {
        key: Option<String>,
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
        partitions: u32,
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
}

impl TopicActor {
    fn new(name: String, partitions: u32, rx: mpsc::Receiver<TopicCommand>) -> Self {
        Self {
            state: TopicState::new(name, partitions),
            groups: HashMap::new(),
            client_map: HashMap::new(),
            rx,
        }
    }

    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                TopicCommand::Publish { key: _, payload, reply } => {
                    let partition = 0; // TODO: Hashing
                    let offset = self.state.publish(partition, payload);
                    let _ = reply.send(Ok(offset));
                },
                TopicCommand::Fetch { group_id, client_id, generation_id, partition, offset, limit, reply } => {
                    if let (Some(gid), Some(cid), Some(gen_id)) = (group_id, client_id, generation_id) {
                        if let Some(group) = self.groups.get(&gid) {
                            // UNIFIED CHECK
                            if !group.validate_epoch(gen_id) || !group.is_member(&cid) {
                                let _ = reply.send(Err("REBALANCE_NEEDED".to_string()));
                                continue;
                            }
                            // Assignment Check
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
                    let _ = reply.send(Ok(msgs));
                },
                TopicCommand::JoinGroup { group_id, client_id, reply } => {
                    let g = self.groups.entry(group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new(group_id.clone(), self.state.name.clone()));
                    
                    g.add_member(client_id.clone());
                    self.client_map.entry(client_id.clone()).or_default().push(group_id);
                    g.rebalance(self.state.get_partitions_count() as u32);

                    let assigned = g.get_assignments(&client_id).cloned().unwrap_or_default();
                    let mut start_offsets = HashMap::new();
                    for &pid in &assigned {
                        start_offsets.insert(pid, g.get_committed_offset(pid));
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
                                    let _ = reply.send(Ok(()));
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
                     if let Some(g_ids) = self.client_map.remove(&client_id) {
                         for gid in g_ids {
                             if let Some(g) = self.groups.get_mut(&gid) {
                                 if g.remove_member(&client_id) {
                                     g.rebalance(self.state.get_partitions_count() as u32);
                                 }
                             }
                         }
                     }
                     let _ = reply.send(());
                },
                    TopicCommand::GetSnapshot { reply } => {
                        let mut hw_marks = HashMap::new();
                        for i in 0..self.state.get_partitions_count() {
                            hw_marks.insert(i as u32, self.state.get_high_watermark(i as u32));
                        }
                        let summary = crate::dashboard::models::stream::TopicSummary {
                             name: self.state.name.clone(),
                             partitions: self.state.get_partitions_count() as u32,
                             size_bytes: 0, 
                             groups: self.groups.values().map(|g| g.get_snapshot(&hw_marks)).collect(),
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

        tokio::spawn(async move {
            let mut actors = HashMap::<String, mpsc::Sender<TopicCommand>>::new();
            
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    ManagerCommand::CreateTopic { name, partitions, reply } => {
                        if !actors.contains_key(&name) {
                            let (t_tx, t_rx) = mpsc::channel(10_000); 
                            let actor = TopicActor::new(name.clone(), partitions, t_rx);
                            tokio::spawn(actor.run());
                            actors.insert(name, t_tx);
                        }
                        let _ = reply.send(Ok(()));
                    },
                    ManagerCommand::GetTopicActor { topic, reply } => {
                        let _ = reply.send(actors.get(&topic).cloned());
                    },
                    ManagerCommand::DisconnectClient { client_id, reply } => {
                        // Broadcast disconnect with ack
                        for actor in actors.values() {
                            let (tx, rx) = oneshot::channel();
                            // We don't await individual leaves to speed up loop, 
                            // but we could join them if strict ordering needed.
                            let _ = actor.send(TopicCommand::LeaveGroup { client_id: client_id.clone(), reply: tx }).await;
                            // Fire and forget waiting for sub-actors for now
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

    pub async fn create_topic(&self, name: String) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ManagerCommand::CreateTopic { name, partitions: 1, reply: tx }).await.map_err(|_| "Server closed")?;
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
        actor.send(TopicCommand::Publish { key: None, payload, reply: tx }).await.map_err(|_| "Actor closed")?;
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

    pub async fn commit_offset(&self, group: &str, topic: &str, offset: u64, client: &str, gen_id: u64) -> Result<(), String> {
        let actor = self.get_actor(topic).await.ok_or("Topic not found")?;
        let (tx, rx) = oneshot::channel();
        actor.send(TopicCommand::Commit { 
            group_id: group.to_string(), 
            partition: 0, 
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
}

pub struct StreamSession {
    client_id: String,
    manager: StreamManager,
}

impl Drop for StreamSession {
    fn drop(&mut self) {
        // Fire and forget disconnect on drop (cannot await in drop)
        let m = self.manager.clone();
        let c = self.client_id.clone();
        tokio::spawn(async move {
            m.disconnect(c).await;
        });
    }
}

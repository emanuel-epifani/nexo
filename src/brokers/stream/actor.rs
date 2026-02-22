//! Stream Topic Actor: Async actor owning a single topic's state
//! 
//! Each topic has its own actor that handles Pub/Sub/Groups sequentially.
//! The StreamManager routes commands to the appropriate TopicActor.

use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use crate::brokers::stream::topic::{TopicState, TopicConfig};
use crate::brokers::stream::message::Message;
use crate::brokers::stream::group::ConsumerGroup;
use crate::brokers::stream::commands::StreamPublishOptions;
use crate::brokers::stream::persistence::{recover_topic, StreamWriter, WriterCommand, StreamStorageOp};
use crate::brokers::stream::persistence::types::PersistenceMode;
use crate::brokers::stream::persistence::writer::Segment;
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
        reply: oneshot::Sender<()>,
    },
    GetSnapshot {
        reply: oneshot::Sender<crate::dashboard::stream::TopicSummary>,
    },
    PersistenceAck {
        partition: u32,
        last_persisted_offset: u64,
    },
    SegmentRotated {
        partition: u32,
        segment: Segment,
    },
    EvictOldMessages,
    Stop {
        reply: oneshot::Sender<()>,
    }
}

// ==========================================
// TOPIC ACTOR (The Engine)
// ==========================================

pub(crate) struct TopicActor {
    state: TopicState,
    groups: HashMap<String, ConsumerGroup>,
    client_map: HashMap<String, Vec<String>>, 
    rx: mpsc::Receiver<TopicCommand>,
    last_partition: u32,
    persistence_mode: PersistenceMode,
    writer_tx: mpsc::Sender<WriterCommand>,
    self_tx: mpsc::Sender<TopicCommand>,
    eviction_batch_size: usize,
}

impl TopicActor {
    pub(crate) fn new(
        name: String, 
        rx: mpsc::Receiver<TopicCommand>,
        self_tx: mpsc::Sender<TopicCommand>,
        config: TopicConfig,
    ) -> Self {
        // 1. Recovery (SYNCHRONOUS DIRECTORY CREATION)
        let persistence_path = PathBuf::from(&config.persistence_path).join(&name);
        if let Err(e) = std::fs::create_dir_all(&persistence_path) {
            tracing::error!("FATAL: Failed to create topic directory at {:?}: {}", persistence_path, e);
            // We proceed, but the writer will likely fail. At least we logged it early.
        }

        let recovered = recover_topic(&name, config.partitions, PathBuf::from(config.persistence_path.clone()));
        let state = TopicState::restore(
            name.clone(), 
            config.partitions, 
            config.max_ram_messages,
            config.ram_soft_limit,
            config.ram_hard_limit,
            recovered.partitions_data
        );
        
        let mut groups = HashMap::new();
        // Restore Groups
        for (gid, (gen_id, committed)) in recovered.groups_data {
            let mut g = ConsumerGroup::new(gid.clone(), name.clone());
            g.generation_id = gen_id;
            g.committed_offsets = committed;
            groups.insert(gid, g);
        }

        // 2. Spawn Writer
        let (w_tx, w_rx) = mpsc::channel(config.writer_channel_capacity);
        let writer = StreamWriter::new(
            name.clone(), 
            config.partitions, 
            config.persistence_mode.clone(), 
            w_rx,
            PathBuf::from(config.persistence_path),
            config.compaction_threshold,
            config.max_segment_size,
            config.retention,
            config.retention_check_ms,
            config.writer_batch_size,
            self_tx.clone(),
        );
        tokio::spawn(writer.run());

        Self {
            state,
            groups,
            client_map: HashMap::new(),
            rx,
            last_partition: 0,
            persistence_mode: config.persistence_mode,
            writer_tx: w_tx,
            self_tx,
            eviction_batch_size: config.eviction_batch_size,
        }
    }

    pub(crate) async fn run(mut self, eviction_interval_ms: u64) {
        // Spawn eviction timer task
        let eviction_tx = self.self_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(eviction_interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let _ = eviction_tx.send(TopicCommand::EvictOldMessages).await;
            }
        });

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
                                groups_info.push(crate::dashboard::stream::ConsumerGroupSummary {
                                    id: group.id.clone(),
                                    committed_offset: committed,
                                });
                            }
                            
                            partitions_info.push(crate::dashboard::stream::PartitionInfo {
                                id: p_id,
                                groups: groups_info,
                                last_offset: partition.next_offset,
                            });
                        }
                        
                        let summary = crate::dashboard::stream::TopicSummary {
                             name: self.state.name.clone(),
                             partitions: partitions_info,
                        };
                        let _ = reply.send(summary);
                    },
                TopicCommand::PersistenceAck { partition, last_persisted_offset } => {
                    if let Some(p) = self.state.partitions.get_mut(partition as usize) {
                        p.persisted_offset = last_persisted_offset;
                    }
                },
                TopicCommand::SegmentRotated { partition, segment } => {
                    if let Some(p) = self.state.partitions.get_mut(partition as usize) {
                        p.segments.push(segment);
                        p.segments.sort_by_key(|s| s.start_offset);
                    }
                },
                TopicCommand::EvictOldMessages => {
                    for partition in &mut self.state.partitions {
                        let target_evict = partition.log.len().saturating_sub(partition.soft_limit);
                        let evict_count = target_evict.min(self.eviction_batch_size);
                        
                        for _ in 0..evict_count {
                            let should_evict = if let Some(front) = partition.log.front() {
                                front.offset < partition.persisted_offset
                            } else {
                                false
                            };
                            
                            if should_evict {
                                if let Some(removed) = partition.log.pop_front() {
                                    partition.ram_start_offset = removed.offset + 1;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                },
                TopicCommand::Stop { reply } => {
                    let _ = reply.send(());
                    break;
                }
            }
        }
    }
}

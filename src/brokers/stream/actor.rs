//! Stream Topic Actor: Async actor owning a single topic's state
//!
//! High-level Brain: delegates ALL disk operations to StorageManager.
//! Handles: Publish, Fetch, Ack, Nack, Seek, Join, Leave, Eviction.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use tracing::{error, info, warn};

use crate::brokers::stream::topic::{TopicState, TopicConfig};
use crate::brokers::stream::message::Message;
use crate::brokers::stream::group::ConsumerGroup;
use crate::brokers::stream::commands::SeekTarget;
use crate::brokers::stream::persistence::{
    recover_topic,
    storage::{StorageCommand, MessageToAppend},
};

// ==========================================
// COMMANDS (The Internal Protocol)
// ==========================================

pub enum TopicCommand {
    Publish {
        payload: Bytes,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    Fetch {
        group_id: String,
        client_id: String,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<Message>, String>>,
    },
    Ack {
        group_id: String,
        seq: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Nack {
        group_id: String,
        seq: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Seek {
        group_id: String,
        target: SeekTarget,
        reply: oneshot::Sender<Result<(), String>>,
    },
    JoinGroup {
        group_id: String,
        client_id: String,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    LeaveGroup {
        client_id: String,
        reply: oneshot::Sender<()>,
    },
    Read {
        from_seq: u64,
        limit: usize,
        reply: oneshot::Sender<Vec<Message>>,
    },
    GetSnapshot {
        reply: oneshot::Sender<crate::dashboard::stream::TopicSummary>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
    // ---- Internal Commands ----
    PersistedAck {
        seq: u64,
    }
}

// ==========================================
// TOPIC ACTOR
// ==========================================

pub(crate) struct TopicActor {
    state: TopicState,
    groups: HashMap<String, ConsumerGroup>,
    client_map: HashMap<String, Vec<String>>,
    rx: mpsc::Receiver<TopicCommand>,
    self_tx: mpsc::Sender<TopicCommand>, // Added self-reference for internal commands
    storage_tx: mpsc::Sender<StorageCommand>,
    // Groups state
    groups_dirty: bool,
    // Config
    max_segment_size: u64,
    retention: crate::brokers::stream::commands::RetentionOptions,
    max_ack_pending: usize,
    ack_wait: Duration,
}

impl TopicActor {
    pub(crate) async fn new(
        name: String,
        rx: mpsc::Receiver<TopicCommand>,
        self_tx: mpsc::Sender<TopicCommand>, // Require self_tx
        config: TopicConfig,
        storage_tx: mpsc::Sender<StorageCommand>,
    ) -> Self {
        let base_path = PathBuf::from(&config.persistence_path).join(&name);

        // Create directory
        if let Err(e) = fs::create_dir_all(&base_path) {
            error!("FATAL: Failed to create topic directory at {:?}: {}", base_path, e);
        }

        // Recovery
        let recovered = recover_topic(&name, PathBuf::from(config.persistence_path.clone())).await;
        let mut state = TopicState::restore(
            name.clone(),
            config.ram_soft_limit,
            recovered.messages,
        );

        // Restore segments index
        let segments = recovered.segments;
        if let Some(first) = segments.first() {
            state.start_seq = first.start_seq;
        }

        // Restore groups
        let mut groups = HashMap::new();
        for (gid, ack_floor) in recovered.groups_data {
            groups.insert(
                gid.clone(),
                ConsumerGroup::restore(gid, ack_floor, config.max_ack_pending, Duration::from_millis(config.ack_wait_ms)),
            );
        }

        // Remove local segments reference, as StorageManager handles filesystem indexing completely.

        Self {
            state,
            groups,
            client_map: HashMap::new(),
            rx,
            self_tx,
            storage_tx,
            groups_dirty: false,
            max_segment_size: config.max_segment_size,
            retention: config.retention,
            max_ack_pending: config.max_ack_pending,
            ack_wait: Duration::from_millis(config.ack_wait_ms),
        }
    }

    pub(crate) async fn run(mut self, config: TopicConfig) {
        info!("TopicActor '{}' started", self.state.name);
        
        // We will create a local clone of the actor's own TX channel 
        // to send itself InternalCommands (like PersistedAck)
        let self_tx = mpsc::channel::<TopicCommand>(10).0; 
        // Wait, TopicActor doesn't take its own Sender in `new`. 
        // Let's modify TopicActor::new or just pass it in. 
        // Actually, we can just pass the tx into `run` or `new` if we had it, but `StreamManager` owns `tx`.
        // Alternatively, we use `self.storage_tx`? No, StorageManager replies via oneshot!
        // So we can just poll those oneshots in the main `select!`, but that requires maintaining a Data Structure of Futures.
        // A much simpler actor-pattern is: we pass the parent's `mpsc::Sender<TopicCommand>` into the TopicActor so it can send messages to itself!

        let mut groups_save_timer = tokio::time::interval(Duration::from_millis(config.default_flush_ms * 10));
        groups_save_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Eviction timer
        let mut eviction_timer = tokio::time::interval(Duration::from_millis(config.eviction_interval_ms));
        eviction_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Retention timer
        let mut retention_timer = tokio::time::interval(Duration::from_millis(config.retention_check_ms));
        retention_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Redelivery check timer
        let mut redeliver_timer = tokio::time::interval(Duration::from_secs(5));
        redeliver_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            if self.handle_command(cmd).await {
                                break; // Stop command received
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                _ = groups_save_timer.tick() => {
                    // Periodically save groups if dirty
                    if self.groups_dirty {
                        self.save_groups().await;
                        self.groups_dirty = false;
                    }
                }
                _ = eviction_timer.tick() => {
                    self.state.evict();
                }
                _ = retention_timer.tick() => {
                    self.check_retention().await;
                }
                _ = redeliver_timer.tick() => {
                    for group in self.groups.values_mut() {
                        group.check_redelivery();
                    }
                }
            }
        }

        // Non-blocking groups save on shutdown (no disk writer to flush here since it's global)
        if self.groups_dirty {
            self.save_groups().await;
        }
        info!("TopicActor '{}' stopped", self.state.name);
    }

    /// Returns true if the actor should stop.
    async fn handle_command(&mut self, cmd: TopicCommand) -> bool {
        match cmd {
            TopicCommand::Publish { payload, reply } => {
                let (seq, timestamp) = self.state.append(payload.clone());

                let payload_clone = payload.clone();
                let topic_name = self.state.name.clone();
                let (ack_tx, ack_rx) = oneshot::channel();

                let storage_msg = StorageCommand::Append {
                    topic_name,
                    messages: vec![MessageToAppend {
                        seq,
                        timestamp,
                        payload: payload_clone,
                    }],
                    reply: ack_tx,
                };
                
                let _ = self.storage_tx.send(storage_msg).await;

                // Spawn a tiny task to await the ACK and send an Internal Command to self
                let self_tx = self.self_tx.clone();
                tokio::spawn(async move {
                    if let Ok(Ok(persisted_seq)) = ack_rx.await {
                        let _ = self_tx.send(TopicCommand::PersistedAck { seq: persisted_seq }).await;
                    }
                });

                // Rely on the Internal Command to update state! No fake synchronous update anymore.
                let _ = reply.send(Ok(seq));
            }

            TopicCommand::Fetch { group_id, client_id, limit, reply } => {
                let group = self.groups.entry(group_id.clone())
                    .or_insert_with(|| ConsumerGroup::new(group_id.clone(), self.max_ack_pending, self.ack_wait));

                if !group.is_member(&client_id) {
                    let _ = reply.send(Err("NOT_MEMBER".to_string()));
                    return false;
                }

                let msgs = group.fetch(&client_id, limit, &self.state.log, self.state.ram_start_seq);
                let _ = reply.send(Ok(msgs));
            }

            TopicCommand::Ack { group_id, seq, reply } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    match group.ack(seq) {
                        Ok(()) => {
                            self.groups_dirty = true;
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                        }
                    }
                } else {
                    let _ = reply.send(Err("Group not found".to_string()));
                }
            }

            TopicCommand::Nack { group_id, seq, reply } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    match group.nack(seq) {
                        Ok(()) => { let _ = reply.send(Ok(())); }
                        Err(e) => { let _ = reply.send(Err(e)); }
                    }
                } else {
                    let _ = reply.send(Err("Group not found".to_string()));
                }
            }

            TopicCommand::Seek { group_id, target, reply } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    match target {
                        SeekTarget::Beginning => group.seek_beginning(),
                        SeekTarget::End => group.seek_end(self.state.next_seq.saturating_sub(1)),
                    }
                    self.groups_dirty = true;
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err("Group not found".to_string()));
                }
            }

            TopicCommand::JoinGroup { group_id, client_id, reply } => {
                let group = self.groups.entry(group_id.clone())
                    .or_insert_with(|| ConsumerGroup::new(group_id.clone(), self.max_ack_pending, self.ack_wait));

                group.add_member(client_id.clone());

                let g_list = self.client_map.entry(client_id).or_default();
                if !g_list.contains(&group_id) {
                    g_list.push(group_id);
                }

                let _ = reply.send(Ok(group.ack_floor));
            }

            TopicCommand::LeaveGroup { client_id, reply } => {
                if let Some(g_ids) = self.client_map.remove(&client_id) {
                    for gid in g_ids {
                        if let Some(g) = self.groups.get_mut(&gid) {
                            g.remove_member(&client_id);
                        }
                    }
                }
                let _ = reply.send(());
            }

            TopicCommand::Read { from_seq, limit, reply } => {
                let msgs = self.state.read(from_seq, limit);
                // If hot read returned nothing, try cold read from segments via StorageManager
                if msgs.is_empty() {
                    let _ = self.storage_tx.send(StorageCommand::ColdRead {
                        topic_name: self.state.name.clone(),
                        from_seq,
                        limit,
                        reply, // We forward the reply oneshot directly to the StorageManager!
                    }).await;
                } else {
                    let _ = reply.send(msgs);
                }
            }

            TopicCommand::GetSnapshot { reply } => {
                let groups_info: Vec<_> = self.groups.values().map(|g| {
                    crate::dashboard::stream::ConsumerGroupSummary {
                        id: g.id.clone(),
                        ack_floor: g.ack_floor,
                        pending_count: g.pending.len(),
                    }
                }).collect();

                let summary = crate::dashboard::stream::TopicSummary {
                    name: self.state.name.clone(),
                    last_seq: self.state.next_seq.saturating_sub(1),
                    groups: groups_info,
                };
                let _ = reply.send(summary);
            }

            TopicCommand::PersistedAck { seq } => {
                if seq > self.state.persisted_seq {
                    self.state.persisted_seq = seq;
                }
            }

            TopicCommand::Stop { reply } => {
                let _ = reply.send(());
                return true;
            }
        }

        false
    }

    // ==========================================
    // PERSISTENCE HELPERS
    // ==========================================

    async fn save_groups(&self) {
        let groups_map: HashMap<String, u64> = self.groups.iter()
            .map(|(id, g)| (id.clone(), g.ack_floor))
            .collect();

        let _ = self.storage_tx.send(StorageCommand::SaveGroups {
            topic_name: self.state.name.clone(),
            groups_data: groups_map,
        }).await;
    }

    async fn check_retention(&mut self) {
        let _ = self.storage_tx.send(StorageCommand::ApplyRetention {
            topic_name: self.state.name.clone(),
            retention: self.retention.clone(),
            max_segment_size: self.max_segment_size,
        }).await;
    }
}

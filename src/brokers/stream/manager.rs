use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, watch, Mutex as AsyncMutex, RwLock};
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::brokers::stream::options::{SeekTarget, StreamCreateOptions};
use crate::brokers::stream::config::SystemStreamConfig;
use crate::brokers::stream::domain::group::ConsumerGroup;
use crate::brokers::stream::domain::message::Message;
use crate::brokers::stream::domain::persistence::{recover_topic, record_len, GroupPersistentState, Segment, StorageCommand, StorageManager};
use crate::brokers::stream::domain::topic::TopicConfig;

struct TopicShared {
    state: Mutex<TopicState>,
    append_gate: Arc<AsyncMutex<()>>,
    retention_gate: Arc<RwLock<()>>,
    wake_tx: watch::Sender<u64>,
}

#[derive(Clone)]
struct ConsumerBinding {
    group_id: String,
    consumer_id: String,
}

struct TopicState {
    head_seq: u64,
    next_seq: u64,
    index: BTreeMap<u64, u64>,
    groups: HashMap<String, ConsumerGroup>,
    client_map: HashMap<String, Vec<ConsumerBinding>>,
    groups_dirty: bool,
    full_config: TopicConfig,
    file_offset: u64,
    active_path: PathBuf,
    segments: Arc<Vec<Segment>>,
}

pub struct JoinGroupResult {
    pub ack_floor: u64,
    pub consumer_id: String,
    pub generation: u64,
}

pub struct StreamManager {
    topics: Arc<DashMap<String, Arc<TopicShared>>>,
    deleted_topics: Arc<DashMap<String, ()>>,
    storage_tx: mpsc::Sender<StorageCommand>,
    config: Arc<SystemStreamConfig>,
    cancel: CancellationToken,
}

impl StreamManager {
    pub async fn new(config: Arc<SystemStreamConfig>) -> Self {
        let topics = Arc::new(DashMap::new());
        let deleted_topics = Arc::new(DashMap::new());
        let (storage_tx, storage_rx) = mpsc::channel(config.storage_queue_capacity.max(1));

        let storage_manager = StorageManager::new(
            config.persistence_path.clone(),
            storage_rx,
            config.max_open_files,
        );
        tokio::spawn(storage_manager.run());

        let manager = Self {
            topics,
            deleted_topics,
            storage_tx,
            config,
            cancel: CancellationToken::new(),
        };

        manager.bootstrap_from_disk().await;
        manager.spawn_background_tasks();
        manager
    }

    pub async fn shutdown(&self) {
        self.cancel.cancel();

        // Final group state flush
        for (topic_name, topic_ref) in Self::collect_topics(&self.topics) {
            let groups_data = {
                let state = topic_ref.state.lock();
                state.groups.iter().map(|(id, group)| {
                    (id.clone(), GroupPersistentState {
                        ack_floor: group.ack_floor,
                        dlt_entries: group.dlt_snapshot(),
                        parked_keys: group.parked_keys_snapshot(),
                    })
                }).collect::<BTreeMap<_, _>>()
            };
            let _ = self.storage_tx.send(StorageCommand::SaveState { topic_name, groups: groups_data }).await;
        }

        // Shutdown storage manager (drains remaining commands then exits)
        let (tx, rx) = oneshot::channel();
        let _ = self.storage_tx.send(StorageCommand::Shutdown { reply: tx }).await;
        let _ = rx.await;
    }

    pub async fn create_topic(&self, name: String, options: StreamCreateOptions) -> Result<(), String> {
        self.deleted_topics.remove(&name);

        if self.topics.contains_key(&name) {
            return Ok(());
        }

        let base_path = PathBuf::from(&self.config.persistence_path).join(&name);
        let existed_on_disk = tokio::fs::metadata(&base_path).await.map(|meta| meta.is_dir()).unwrap_or(false);
        let topic_config = Self::load_topic_config(&base_path, options, &self.config).await;

        info!("[StreamManager] Creating topic '{}'", name);

        if !existed_on_disk {
            if let Err(e) = tokio::fs::create_dir_all(&base_path).await {
                tracing::error!("Failed to create topic directory at {:?}: {}", base_path, e);
            }
        }

        let config_path = base_path.join("config.json");
        let needs_write = !config_path.exists() || {
            match tokio::fs::read_to_string(&config_path).await {
                Ok(data) => serde_json::from_str::<TopicConfig>(&data).is_err(),
                Err(_) => true,
            }
        };
        if needs_write {
            if let Ok(data) = serde_json::to_string_pretty(&topic_config) {
                let _ = tokio::fs::write(&config_path, data).await;
            }
        }

        let shared = Self::build_topic_shared(name.clone(), topic_config, &self.config.persistence_path).await;

        use dashmap::mapref::entry::Entry;
        match self.topics.entry(name) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(v) => {
                v.insert(shared);
                Ok(())
            }
        }
    }

    pub async fn delete_topic(&self, name: String) -> Result<(), String> {
        self.deleted_topics.insert(name.clone(), ());

        let topic_path = PathBuf::from(&self.config.persistence_path).join(&name);
        if self.topics.remove(&name).is_some() || tokio::fs::metadata(&topic_path).await.map(|meta| meta.is_dir()).unwrap_or(false) {
            let (del_tx, del_rx) = oneshot::channel();
            let _ = self.storage_tx.send(StorageCommand::DropTopic {
                topic_name: name,
                reply: del_tx,
            }).await;
            let _ = del_rx.await;
        }
        Ok(())
    }

    pub async fn publish_batch(&self, topic: &str, items: Vec<(Option<Bytes>, Bytes)>) -> Result<Vec<u64>, String> {
        if items.is_empty() { return Ok(Vec::new()); }
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let append_guard = topic_ref.append_gate.clone().lock_owned().await;
        let permit = self.storage_tx.reserve().await
            .map_err(|_| "Storage unavailable".to_string())?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let n = items.len() as u64;

        let (first_seq, seqs, messages, file_path, offsets, file_offset_after, starts_new_segment) = {
            let state = topic_ref.state.lock();
            let first_seq = state.next_seq;
            let mut seqs = Vec::with_capacity(items.len());
            let mut messages = Vec::with_capacity(items.len());
            for (i, (key, payload)) in items.into_iter().enumerate() {
                let seq = first_seq + i as u64;
                seqs.push(seq);
                messages.push(Message { seq, timestamp, key, payload });
            }

            let bytes_len: u64 = messages.iter().map(|m| record_len(m.key.as_deref(), &m.payload)).sum();

            let write_offset = if state.file_offset > 0
                && state.file_offset + bytes_len > state.full_config.max_segment_size
            {
                0
            } else {
                state.file_offset
            };
            let starts_new_segment = write_offset == 0;
            let file_path = if starts_new_segment {
                let base_path = PathBuf::from(&self.config.persistence_path).join(topic);
                base_path.join(format!("{}.log", first_seq))
            } else {
                state.active_path.clone()
            };

            let mut current_offset = write_offset;
            let mut offsets = Vec::with_capacity(messages.len());
            for msg in &messages {
                offsets.push((msg.seq, current_offset));
                current_offset += record_len(msg.key.as_deref(), &msg.payload);
            }
            let file_offset_after = if current_offset >= state.full_config.max_segment_size {
                0
            } else {
                current_offset
            };
            (
                first_seq,
                seqs,
                messages,
                file_path,
                offsets,
                file_offset_after,
                starts_new_segment,
            )
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        let commit_topic = topic_ref.clone();
        let commit_path = file_path.clone();
        permit.send(StorageCommand::Append {
            file_path,
            messages,
            complete: Box::new(move |result| {
                let response = match result {
                    Ok(()) => {
                        {
                            let mut state = commit_topic.state.lock();
                            state.next_seq = first_seq + n;
                            state.file_offset = file_offset_after;
                            state.active_path = commit_path.clone();
                            if starts_new_segment {
                                let segments = Arc::make_mut(&mut state.segments);
                                if segments.last().map(|segment| &segment.path) != Some(&commit_path) {
                                    segments.push(Segment {
                                        path: commit_path,
                                        start_seq: first_seq,
                                    });
                                }
                            }
                            for (seq, offset) in offsets {
                                state.index.insert(seq, offset);
                            }
                        }
                        commit_topic.wake_tx.send_modify(|version| *version += 1);
                        Ok(())
                    }
                    Err(error) => Err(format!("Storage append failed: {}", error)),
                };
                let _ = reply_tx.send(response);
                drop(append_guard);
            }),
        });
        reply_rx.await
            .map_err(|_| "Storage append failed".to_string())??;
        Ok(seqs)
    }

    pub async fn publish(&self, topic: &str, key: Option<Bytes>, payload: Bytes) -> Result<u64, String> {
        let seqs = self.publish_batch(topic, vec![(key, payload)]).await?;
        Ok(seqs.into_iter().next().unwrap_or(0))
    }

    pub async fn peek_dlt(&self, topic: &str, group: &str, limit: usize, offset: usize) -> Result<Vec<(u64, String, u32, Option<Bytes>)>, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let state = topic_ref.state.lock();
        let group_ref = state.groups.get(group).ok_or("Group not found")?;
        let entries = group_ref.peek_dlt(limit, offset);
        Ok(entries.into_iter().map(|(seq, e)| (seq, e.reason, e.attempts, e.key)).collect())
    }

    pub async fn move_to_stream(&self, topic: &str, group: &str, seq: u64) -> Result<bool, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let key_unblocked = {
            let mut state = topic_ref.state.lock();
            let group_ref = state.groups.get_mut(group).ok_or("Group not found")?;
            let key_unblocked = group_ref.move_to_stream(seq)?;
            state.groups_dirty = true;
            key_unblocked
        };
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(key_unblocked)
    }

    pub async fn delete_dlt(&self, topic: &str, group: &str, seq: u64) -> Result<bool, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let key_unblocked = {
            let mut state = topic_ref.state.lock();
            let group_ref = state.groups.get_mut(group).ok_or("Group not found")?;
            let key_unblocked = group_ref.delete_dlt(seq)?;
            state.groups_dirty = true;
            key_unblocked
        };
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(key_unblocked)
    }

    pub async fn purge_dlt(&self, topic: &str, group: &str) -> Result<usize, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let count = {
            let mut state = topic_ref.state.lock();
            let group_ref = state.groups.get_mut(group).ok_or("Group not found")?;
            let count = group_ref.purge_dlt();
            state.groups_dirty = true;
            count
        };
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(count)
    }

    pub async fn read(&self, topic: &str, from_seq: u64, limit: usize) -> Result<Vec<Message>, String> {
        let Some(topic_ref) = self.get_topic(topic) else {
            return Ok(Vec::new());
        };
        let retention_guard = topic_ref.retention_gate.clone().read_owned().await;

        let (offsets, segments) = {
            let state = topic_ref.state.lock();
            let offsets = state.index.range(from_seq..)
                .take(limit)
                .map(|(seq, offset)| (*seq, *offset))
                .collect::<Vec<_>>();
            (offsets, state.segments.clone())
        };

        if offsets.is_empty() {
            return Ok(Vec::new());
        }

        let (tx, rx) = oneshot::channel();
        self.storage_tx.send(StorageCommand::ReadRange {
            segments,
            retention_guard,
            offsets,
            reply: tx,
        }).await.map_err(|_| "Storage unavailable".to_string())?;
        rx.await
            .map_err(|_| "Storage read failed".to_string())?
            .map_err(|error| format!("Storage read failed: {}", error))
    }

    pub async fn fetch(&self, group: &str, consumer_id: &str, generation: u64, limit: usize, topic: &str, wait_ms: u64) -> Result<Vec<Message>, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;

        let group_cancel = {
            let state = topic_ref.state.lock();
            match state.groups.get(group) {
                Some(g) => g.cancel_token(),
                None => return Err("Group not found".to_string()),
            }
        };

        if wait_ms == 0 {
            return self.try_fetch_once(&topic_ref, group, consumer_id, generation, limit, &group_cancel).await;
        }

        let deadline = Instant::now() + Duration::from_millis(wait_ms);
        let mut wake_rx = topic_ref.wake_tx.subscribe();

        loop {
            let wake_ver = *wake_rx.borrow();

            match self.try_fetch_once(&topic_ref, group, consumer_id, generation, limit, &group_cancel).await {
                Ok(messages) if !messages.is_empty() => return Ok(messages),
                Ok(_) => {}
                Err(e) if (e == "NOT_MEMBER" || e == "FENCED") && !self.is_active_member(&topic_ref, group, consumer_id) => {
                    return Ok(Vec::new());
                }
                Err(e) => return Err(e),
            }

            if *wake_rx.borrow() != wake_ver {
                continue;
            }

            if Instant::now() >= deadline {
                return Ok(Vec::new());
            }

            tokio::select! {
                _ = wake_rx.changed() => {}
                _ = sleep_until(deadline) => return Ok(Vec::new()),
                _ = group_cancel.cancelled() => return Ok(Vec::new()),
            }
        }
    }

    fn is_active_member(&self, topic_ref: &Arc<TopicShared>, group: &str, consumer_id: &str) -> bool {
        let state = topic_ref.state.lock();
        state.groups.get(group).map_or(false, |g| g.is_member(consumer_id))
    }

    pub async fn ack(&self, group: &str, topic: &str, consumer_id: &str, generation: u64, seq: u64) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        {
            let mut state = topic_ref.state.lock();
            let head_seq = state.head_seq;
            let Some(group_ref) = state.groups.get_mut(group) else {
                return Err("Group not found".to_string());
            };

            group_ref.clamp_head(head_seq);
            group_ref.ack(consumer_id, generation, seq)?;
            state.groups_dirty = true;
        }
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(())
    }

    pub async fn seek(&self, group: &str, topic: &str, target: SeekTarget) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        {
            let mut state = topic_ref.state.lock();
            let last_seq = state.next_seq.saturating_sub(1);
            let head_seq = state.head_seq;
            let max_ack_pending = state.full_config.max_ack_pending;
            let ack_wait = Duration::from_millis(state.full_config.ack_wait_ms);
            let max_deliveries = state.full_config.max_deliveries;
            let group_ref = state.groups.entry(group.to_string())
                .or_insert_with(|| ConsumerGroup::new(group.to_string(), head_seq, max_ack_pending, ack_wait, max_deliveries));

            match target {
                SeekTarget::Beginning => group_ref.seek_beginning(head_seq),
                SeekTarget::End => group_ref.seek_end(last_seq),
            }
            state.groups_dirty = true;
        }
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(())
    }

    pub async fn leave_group(&self, group: &str, topic: &str, consumer_id: &str, generation: u64) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let should_notify = {
            let mut state = topic_ref.state.lock();
            let Some(group_ref) = state.groups.get_mut(group) else {
                return Err("Group not found".to_string());
            };

            if group_ref.generation() != generation {
                return Err("FENCED".to_string());
            }

            let Some(connection_client_id) = group_ref.remove_member(consumer_id) else {
                return Err("NOT_MEMBER".to_string());
            };

            let mut remove_client_key = false;
            if let Some(bindings) = state.client_map.get_mut(&connection_client_id) {
                bindings.retain(|binding| !(binding.group_id == group && binding.consumer_id == consumer_id));
                remove_client_key = bindings.is_empty();
            }
            if remove_client_key {
                state.client_map.remove(&connection_client_id);
            }

            state.groups_dirty = true;
            true
        };
        if should_notify {
            topic_ref.wake_tx.send_modify(|v| *v += 1);
        }
        Ok(())
    }

    pub async fn join_group(&self, group: &str, topic: &str, connection_client_id: &str) -> Result<JoinGroupResult, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let mut state = topic_ref.state.lock();
        let head_seq = state.head_seq;
        let max_ack_pending = state.full_config.max_ack_pending;
        let ack_wait = Duration::from_millis(state.full_config.ack_wait_ms);
        let max_deliveries = state.full_config.max_deliveries;
        let client_id = connection_client_id.to_string();
        let group_id = group.to_string();
        let group_exists = state.groups.contains_key(&group_id);

        let (ack_floor, consumer_id, generation, was_clamped) = {
            let group_ref = state.groups.entry(group_id.clone())
                .or_insert_with(|| ConsumerGroup::new(group_id.clone(), head_seq, max_ack_pending, ack_wait, max_deliveries));
            let was_clamped = group_ref.clamp_head(head_seq);
            let consumer_id = group_ref.add_member(client_id.clone());
            (group_ref.ack_floor, consumer_id, group_ref.generation(), was_clamped)
        };

        state.client_map.entry(client_id).or_default().push(ConsumerBinding {
            group_id: group_id.clone(),
            consumer_id: consumer_id.clone(),
        });

        if !group_exists || was_clamped {
            state.groups_dirty = true;
        }

        Ok(JoinGroupResult {
            ack_floor,
            consumer_id,
            generation,
        })
    }

    pub async fn disconnect(&self, client_id: &str) {
        info!("[StreamManager] Disconnecting client: {}", client_id);
        for (_, topic_ref) in Self::collect_topics(&self.topics) {
            let mut should_notify = false;
            {
                let mut state = topic_ref.state.lock();
                if let Some(bindings) = state.client_map.remove(client_id) {
                    for binding in bindings {
                        if let Some(group_ref) = state.groups.get_mut(&binding.group_id) {
                            if group_ref.remove_member(&binding.consumer_id).is_some() {
                                should_notify = true;
                                state.groups_dirty = true;
                            }
                        }
                    }
                }
            }
            if should_notify {
                topic_ref.wake_tx.send_modify(|v| *v += 1);
            }
        }
    }

    pub async fn exists(&self, name: &str) -> bool {
        if self.topics.contains_key(name) {
            return true;
        }

        let path = PathBuf::from(&self.config.persistence_path).join(name);
        tokio::fs::metadata(path).await.map(|meta| meta.is_dir()).unwrap_or(false)
    }

    fn get_topic(&self, topic: &str) -> Option<Arc<TopicShared>> {
        self.topics.get(topic).map(|entry| entry.value().clone())
    }

    fn collect_topics(topics: &Arc<DashMap<String, Arc<TopicShared>>>) -> Vec<(String, Arc<TopicShared>)> {
        topics.iter().map(|entry| (entry.key().clone(), entry.value().clone())).collect()
    }

    async fn load_topic_config(base_path: &PathBuf, options: StreamCreateOptions, config: &SystemStreamConfig) -> TopicConfig {
        let config_path = base_path.join("config.json");
        if let Ok(data) = tokio::fs::read_to_string(&config_path).await {
            serde_json::from_str(&data).unwrap_or_else(|_| TopicConfig::from_options(options, config))
        } else {
            TopicConfig::from_options(options, config)
        }
    }

    async fn bootstrap_from_disk(&self) {
        let persistence_path = PathBuf::from(&self.config.persistence_path);
        if !persistence_path.exists() {
            return;
        }

        let mut entries = match tokio::fs::read_dir(&persistence_path).await {
            Ok(entries) => entries,
            Err(_) => return,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let Some(topic_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            let name = topic_name.to_string();
            if self.deleted_topics.contains_key(&name) {
                continue;
            }

            let topic_config = Self::load_topic_config(&path, StreamCreateOptions::default(), &self.config).await;

            let config_path = path.join("config.json");
            if !config_path.exists() {
                if let Ok(data) = serde_json::to_string_pretty(&topic_config) {
                    let _ = tokio::fs::write(&config_path, data).await;
                }
            }

            let topic_ref = Self::build_topic_shared(name.clone(), topic_config, &self.config.persistence_path).await;

            use dashmap::mapref::entry::Entry;
            match self.topics.entry(name.clone()) {
                Entry::Occupied(_) => {}
                Entry::Vacant(v) => {
                    v.insert(topic_ref);
                    info!("[StreamManager] Restored topic '{}'", name);
                }
            }
        }
    }

    async fn build_topic_shared(name: String, config: TopicConfig, persistence_path: &str) -> Arc<TopicShared> {
        let base_path = PathBuf::from(persistence_path).join(&name);
        if let Err(e) = tokio::fs::create_dir_all(&base_path).await {
            tracing::error!("Failed to create topic directory at {:?}: {}", base_path, e);
        }

        let recovered = recover_topic(&name, PathBuf::from(persistence_path)).await;
        let head_seq = recovered.head_seq.max(1);
        let next_seq = recovered.next_seq.max(head_seq);

        let ack_wait = Duration::from_millis(config.ack_wait_ms);
        let mut groups = HashMap::new();
        for (group_id, group_state) in recovered.groups_data {
            groups.insert(
                group_id.clone(),
                ConsumerGroup::restore(group_id, group_state.ack_floor, head_seq, config.max_ack_pending, ack_wait, config.max_deliveries, group_state.dlt_entries, group_state.parked_keys),
            );
        }

        let active_path = recovered.segments.last()
            .map(|segment| segment.path.clone())
            .unwrap_or_else(|| PathBuf::from(persistence_path).join(&name).join("1.log"));
        let segments = Arc::new(recovered.segments);

        Arc::new(TopicShared {
            state: Mutex::new(TopicState {
                head_seq,
                next_seq,
                index: recovered.index,
                groups,
                client_map: HashMap::new(),
                groups_dirty: false,
                full_config: config,
                file_offset: recovered.last_segment_size,
                active_path,
                segments,
            }),
            append_gate: Arc::new(AsyncMutex::new(())),
            retention_gate: Arc::new(RwLock::new(())),
            wake_tx: watch::channel(0u64).0,
        })
    }

    fn spawn_background_tasks(&self) {
        let cancel = self.cancel.clone();
        let topics = self.topics.clone();

        // Task 1: Periodic group state save
        let storage_tx = self.storage_tx.clone();
        let groups_interval_ms = self.config.default_flush_ms * 10;
        tokio::spawn({
            let cancel = cancel.clone();
            async move {
                let mut timer = tokio::time::interval(Duration::from_millis(groups_interval_ms));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = timer.tick() => {}
                    }
                    for (topic_name, topic_ref) in StreamManager::collect_topics(&topics) {
                        let groups_data = {
                            let mut state = topic_ref.state.lock();
                            if !state.groups_dirty {
                                None
                            } else {
                                state.groups_dirty = false;
                                Some(state.groups.iter().map(|(id, group)| {
                                    (id.clone(), GroupPersistentState {
                                        ack_floor: group.ack_floor,
                                        dlt_entries: group.dlt_snapshot(),
                                        parked_keys: group.parked_keys_snapshot(),
                                    })
                                }).collect::<BTreeMap<_, _>>())
                            }
                        };

                        if let Some(groups_data) = groups_data {
                            let _ = storage_tx.send(StorageCommand::SaveState {
                                topic_name,
                                groups: groups_data,
                            }).await;
                        }
                    }
                }
            }
        });

        // Task 2: Retention enforcement
        let topics = self.topics.clone();
        let storage_tx = self.storage_tx.clone();
        let retention_check_ms = self.config.retention_check_interval_ms;
        tokio::spawn({
            let cancel = cancel.clone();
            async move {
                let mut timer = tokio::time::interval(Duration::from_millis(retention_check_ms));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = timer.tick() => {}
                    }
                    for (topic_name, topic_ref) in StreamManager::collect_topics(&topics) {
                        let _retention_guard = topic_ref.retention_gate.write().await;
                        let retention = {
                            let state = topic_ref.state.lock();
                            state.full_config.retention.clone()
                        };

                        let (reply_tx, reply_rx) = oneshot::channel();
                        if storage_tx.send(StorageCommand::ApplyRetention {
                            topic_name,
                            retention,
                            reply: reply_tx,
                        }).await.is_err() {
                            continue;
                        }

                        let Ok(new_head_seq) = reply_rx.await else {
                            continue;
                        };

                        let mut should_notify = false;
                        {
                            let mut state = topic_ref.state.lock();
                            if new_head_seq != state.head_seq {
                                state.head_seq = new_head_seq;
                                // Trim index entries below new head_seq
                                let to_remove: Vec<u64> = state.index.range(..new_head_seq).map(|(k, _)| *k).collect();
                                for k in to_remove {
                                    state.index.remove(&k);
                                }
                                Arc::make_mut(&mut state.segments)
                                    .retain(|segment| segment.start_seq >= new_head_seq);
                                let mut groups_changed = false;
                                for group in state.groups.values_mut() {
                                    if group.clamp_head(new_head_seq) {
                                        groups_changed = true;
                                    }
                                }
                                if groups_changed {
                                    state.groups_dirty = true;
                                }
                                should_notify = true;
                            }
                        }
                        if should_notify {
                            topic_ref.wake_tx.send_modify(|v| *v += 1);
                        }
                    }
                }
            }
        });

        // Task 3: Redelivery check (ack timeouts)
        let topics = self.topics.clone();
        tokio::spawn({
            let cancel = cancel.clone();
            async move {
                let mut timer = tokio::time::interval(Duration::from_millis(100));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = timer.tick() => {}
                    }
                    for (_, topic_ref) in StreamManager::collect_topics(&topics) {
                        let mut should_notify = false;
                        {
                            let mut state = topic_ref.state.lock();
                            let mut groups_changed = false;
                            for group in state.groups.values_mut() {
                                if group.check_redelivery() {
                                    groups_changed = true;
                                    should_notify = true;
                                }
                            }
                            if groups_changed {
                                state.groups_dirty = true;
                            }
                        }
                        if should_notify {
                            topic_ref.wake_tx.send_modify(|v| *v += 1);
                        }
                    }
                }
            }
        });
    }
    /// Try to fetch messages for a consumer. Reads from storage via index if needed.
    async fn try_fetch_once(
        &self,
        topic_ref: &Arc<TopicShared>,
        group: &str,
        consumer_id: &str,
        generation: u64,
        limit: usize,
        group_cancel: &CancellationToken,
    ) -> Result<Vec<Message>, String> {
        let retention_guard = topic_ref.retention_gate.clone().read_owned().await;

        // 1. Compute fetch plan under lock (which seqs to read)
        let (plan, was_clamped) = {
            let mut state = topic_ref.state.lock();
            let head_seq = state.head_seq;
            let Some(group_ref) = state.groups.get_mut(group) else {
                return Err("Group not found".to_string());
            };

            group_ref.ensure_active_consumer(consumer_id, generation)?;

            let was_clamped = group_ref.clamp_head(head_seq);
            let backpressured = group_ref.is_backpressured();
            let plan = if backpressured {
                Vec::new()
            } else {
                group_ref.fetch_plan(head_seq, limit)
            };
            (plan, was_clamped)
        };

        if was_clamped {
            topic_ref.state.lock().groups_dirty = true;
        }

        if plan.is_empty() {
            return Ok(Vec::new());
        }

        // 2. Look up offsets in index (under lock, then released)
        let (offsets, segments) = {
            let state = topic_ref.state.lock();
            let offsets = plan.iter()
                .filter_map(|seq| state.index.get(seq).map(|off| (*seq, *off)))
                .collect::<Vec<_>>();
            (offsets, state.segments.clone())
        };

        if offsets.is_empty() {
            return Ok(Vec::new());
        }

        // 3. Read messages from storage (outside any lock)
        if group_cancel.is_cancelled() {
            return Ok(Vec::new());
        }

        let (tx, rx) = oneshot::channel();
        if self.storage_tx.send(StorageCommand::ReadRange {
            segments,
            retention_guard,
            offsets,
            reply: tx,
        }).await.is_err() {
            return Err("Storage read failed".to_string());
        }

        let messages = rx.await
            .map_err(|_| "Storage read failed".to_string())?
            .map_err(|error| format!("Storage read failed: {}", error))?;
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        // 4. Deliver messages to the group (under lock)
        let mut state = topic_ref.state.lock();
        let head_seq = state.head_seq;
        let Some(group_ref) = state.groups.get_mut(group) else {
            return Err("Group not found".to_string());
        };

        if group_cancel.is_cancelled() {
            return Ok(Vec::new());
        }

        group_ref.clamp_head(head_seq);
        let result = group_ref.fetch(consumer_id, generation, limit, &messages, head_seq)?;
        state.groups_dirty = true;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn accepted_append_commits_after_publisher_is_cancelled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut stream_config = SystemStreamConfig::default();
        stream_config.persistence_path = temp_dir.path().to_str().unwrap().to_string();
        let config = Arc::new(stream_config);
        let (storage_tx, mut storage_rx) = mpsc::channel(1);
        let topics = Arc::new(DashMap::new());
        let topic_name = "cancelled-publisher";
        let topic_config = TopicConfig::from_options(StreamCreateOptions::default(), &config);
        let topic_ref = StreamManager::build_topic_shared(
            topic_name.to_string(),
            topic_config,
            &config.persistence_path,
        )
        .await;
        topics.insert(topic_name.to_string(), topic_ref.clone());

        let manager = Arc::new(StreamManager {
            topics,
            deleted_topics: Arc::new(DashMap::new()),
            storage_tx,
            config,
            cancel: CancellationToken::new(),
        });

        let first_manager = manager.clone();
        let first_publish = tokio::spawn(async move {
            first_manager
                .publish(topic_name, None, Bytes::from_static(b"first"))
                .await
        });

        let first_command = storage_rx.recv().await.unwrap();
        first_publish.abort();
        assert!(first_publish.await.unwrap_err().is_cancelled());
        match first_command {
            StorageCommand::Append { complete, .. } => complete(Ok(())),
            _ => panic!("expected append command"),
        }

        let second_manager = manager.clone();
        let second_publish = tokio::spawn(async move {
            second_manager
                .publish(topic_name, None, Bytes::from_static(b"second"))
                .await
        });
        let second_command = storage_rx.recv().await.unwrap();
        match second_command {
            StorageCommand::Append { complete, .. } => complete(Ok(())),
            _ => panic!("expected append command"),
        }

        assert_eq!(second_publish.await.unwrap().unwrap(), 2);
        let state = topic_ref.state.lock();
        assert_eq!(state.next_seq, 3);
        assert_eq!(state.index.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
    }
}

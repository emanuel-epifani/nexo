use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::brokers::stream::options::{SeekTarget, StreamCreateOptions};
use crate::brokers::stream::config::SystemStreamConfig;
use crate::brokers::stream::domain::group::ConsumerGroup;
use crate::brokers::stream::domain::message::Message;
use crate::brokers::stream::domain::persistence::{recover_topic, record_len, GroupPersistentState, StorageCommand, StorageManager};
use crate::brokers::stream::domain::topic::TopicConfig;

struct TopicShared {
    state: Mutex<TopicState>,
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
}

pub struct JoinGroupResult {
    pub ack_floor: u64,
    pub consumer_id: String,
    pub generation: u64,
}

#[derive(Clone)]
pub struct StreamManager {
    topics: Arc<DashMap<String, Arc<TopicShared>>>,
    deleted_topics: Arc<DashMap<String, ()>>,
    storage_tx: mpsc::UnboundedSender<StorageCommand>,
    config: Arc<SystemStreamConfig>,
    cancel: CancellationToken,
}

impl StreamManager {
    pub async fn new(config: Arc<SystemStreamConfig>) -> Self {
        let topics = Arc::new(DashMap::new());
        let deleted_topics = Arc::new(DashMap::new());
        let (storage_tx, storage_rx) = mpsc::unbounded_channel();

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

    pub fn shutdown(&self) {
        self.cancel.cancel();
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
            });
            let _ = del_rx.await;
        }
        Ok(())
    }

    pub async fn publish_batch(&self, topic: &str, items: Vec<(Option<Bytes>, Bytes)>) -> Result<Vec<u64>, String> {
        if items.is_empty() { return Ok(Vec::new()); }
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let n = items.len() as u64;

        let (seqs, messages, file_path) = {
            let mut state = topic_ref.state.lock();
            let first_seq = state.next_seq;
            state.next_seq += n;
            let mut seqs = Vec::with_capacity(items.len());
            let mut messages = Vec::with_capacity(items.len());
            for (i, (key, payload)) in items.into_iter().enumerate() {
                let seq = first_seq + i as u64;
                seqs.push(seq);
                messages.push(Message { seq, timestamp, key, payload });
            }

            let bytes_len: u64 = messages.iter().map(|m| record_len(m.key.as_deref(), &m.payload)).sum();

            if state.file_offset + bytes_len > state.full_config.max_segment_size && state.file_offset > 0 {
                state.file_offset = 0;
            }

            if state.file_offset == 0 {
                let base_path = PathBuf::from(&self.config.persistence_path).join(topic);
                state.active_path = base_path.join(format!("{}.log", first_seq));
            }

            let mut current_offset = state.file_offset;
            for msg in &messages {
                state.index.insert(msg.seq, current_offset);
                current_offset += record_len(msg.key.as_deref(), &msg.payload);
            }
            state.file_offset = current_offset;

            // Post-write rollover: if the segment now exceeds max_segment_size,
            // force a new segment on the next publish
            if state.file_offset >= state.full_config.max_segment_size {
                state.file_offset = 0;
            }
            (seqs, messages, state.active_path.clone())
        };

        let _ = self.storage_tx.send(StorageCommand::Append {
            topic_name: topic.to_string(),
            file_path,
            messages,
        });

        topic_ref.wake_tx.send_modify(|v| *v += 1);
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

    pub async fn read(&self, topic: &str, from_seq: u64, limit: usize) -> Vec<Message> {
        let Some(topic_ref) = self.get_topic(topic) else {
            return Vec::new();
        };

        let offsets = {
            let state = topic_ref.state.lock();
            state.index.range(from_seq..)
                .take(limit)
                .map(|(seq, offset)| (*seq, *offset))
                .collect::<Vec<_>>()
        };

        if offsets.is_empty() {
            return Vec::new();
        }

        let (tx, rx) = oneshot::channel();
        let _ = self.storage_tx.send(StorageCommand::ReadRange {
            topic_name: topic.to_string(),
            offsets,
            reply: tx,
        });
        rx.await.unwrap_or_default()
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
            return self.try_fetch_once(&topic_ref, topic, group, consumer_id, generation, limit, &group_cancel).await;
        }

        let deadline = Instant::now() + Duration::from_millis(wait_ms);
        let mut wake_rx = topic_ref.wake_tx.subscribe();

        loop {
            let wake_ver = *wake_rx.borrow();

            match self.try_fetch_once(&topic_ref, topic, group, consumer_id, generation, limit, &group_cancel).await {
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

    pub async fn disconnect(&self, client_id: String) {
        info!("[StreamManager] Disconnecting client: {}", client_id);
        for (_, topic_ref) in Self::collect_topics(&self.topics) {
            let mut should_notify = false;
            {
                let mut state = topic_ref.state.lock();
                if let Some(bindings) = state.client_map.remove(&client_id) {
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
                active_path: recovered.segments.last()
                    .map(|s| s.path.clone())
                    .unwrap_or_else(|| {
                        PathBuf::from(persistence_path).join(&name).join("1.log")
                    }),
            }),
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
                                        dlt_entries: group.dlt.clone(),
                                        parked_keys: group.parked_keys.clone(),
                                    })
                                }).collect::<BTreeMap<_, _>>())
                            }
                        };

                        if let Some(groups_data) = groups_data {
                            let _ = storage_tx.send(StorageCommand::SaveState {
                                topic_name,
                                groups: groups_data,
                            });
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
                        let retention = {
                            let state = topic_ref.state.lock();
                            state.full_config.retention.clone()
                        };

                        let (reply_tx, reply_rx) = oneshot::channel();
                        if storage_tx.send(StorageCommand::ApplyRetention {
                            topic_name,
                            retention,
                            reply: reply_tx,
                        }).is_err() {
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
        topic: &str,
        group: &str,
        consumer_id: &str,
        generation: u64,
        limit: usize,
        group_cancel: &CancellationToken,
    ) -> Result<Vec<Message>, String> {
        // 1. Compute fetch plan under lock (which seqs to read)
        let (plan, was_clamped) = {
            let mut state = topic_ref.state.lock();
            let head_seq = state.head_seq;
            let Some(group_ref) = state.groups.get_mut(group) else {
                return Err("Group not found".to_string());
            };

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
        let offsets = {
            let state = topic_ref.state.lock();
            plan.iter()
                .filter_map(|seq| state.index.get(seq).map(|off| (*seq, *off)))
                .collect::<Vec<_>>()
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
            topic_name: topic.to_string(),
            offsets,
            reply: tx,
        }).is_err() {
            return Err("Storage read failed".to_string());
        }

        let messages = rx.await.map_err(|_| "Storage read failed".to_string())?;
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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::brokers::stream::options::{SeekTarget, StreamCreateOptions};
use crate::brokers::stream::config::SystemStreamConfig;
use crate::brokers::stream::domain::group::ConsumerGroup;
use crate::brokers::stream::domain::message::Message;
use crate::brokers::stream::snapshot::{ConsumerGroupSnapshot, StreamSnapshot, TopicSnapshot};
use crate::brokers::stream::domain::persistence::{recover_topic, MessageToAppend, StorageCommand, StorageManager};
use crate::brokers::stream::domain::topic::{TopicConfig, TopicState};

struct TopicShared {
    inner: Mutex<TopicInner>,
    notify: Notify,
    persisted_seq: Arc<AtomicU64>,
}

#[derive(Clone)]
struct ConsumerBinding {
    group_id: String,
    consumer_id: String,
}

struct TopicInner {
    state: TopicState,
    groups: HashMap<String, ConsumerGroup>,
    client_map: HashMap<String, Vec<ConsumerBinding>>,
    groups_dirty: bool,
    full_config: TopicConfig,
}

enum FetchAttempt {
    Ready(Vec<Message>),
    NeedColdRead { from_seq: u64 },
    Wait,
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
            config.default_flush_ms,
            config.max_segment_size,
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
            } else {
                let config_path = base_path.join("config.json");
                if let Ok(data) = serde_json::to_string_pretty(&topic_config) {
                    let _ = tokio::fs::write(&config_path, data).await;
                }
            }
        } else {
            let config_path = base_path.join("config.json");
            if !config_path.exists() {
                if let Ok(data) = serde_json::to_string_pretty(&topic_config) {
                    let _ = tokio::fs::write(&config_path, data).await;
                }
            }
        }

        let shared = Self::build_topic_shared(name.clone(), topic_config).await;

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

    pub async fn publish(&self, topic: &str, payload: Bytes) -> Result<u64, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let persisted_seq = topic_ref.persisted_seq.clone();

        let (seq, timestamp) = {
            let mut inner = Self::lock_topic(&topic_ref.inner);
            inner.state.append(payload.clone())
        };

        let _ = self.storage_tx.send(StorageCommand::Append {
            topic_name: topic.to_string(),
            messages: vec![MessageToAppend {
                seq,
                timestamp,
                payload,
            }],
            persisted_seq,
        });

        topic_ref.notify.notify_waiters();
        Ok(seq)
    }

    pub async fn read(&self, topic: &str, from_seq: u64, limit: usize) -> Vec<Message> {
        let Some(topic_ref) = self.get_topic(topic) else {
            return Vec::new();
        };

        let (effective_from_seq, messages, need_cold) = {
            let inner = Self::lock_topic(&topic_ref.inner);
            let effective_from_seq = from_seq.max(inner.state.head_seq);
            let messages = inner.state.read(effective_from_seq, limit);
            let need_cold = messages.is_empty()
                && effective_from_seq < inner.state.ram_start_seq
                && effective_from_seq < inner.state.next_seq;
            (effective_from_seq, messages, need_cold)
        };

        if !messages.is_empty() || !need_cold {
            return messages;
        }

        let (tx, rx) = oneshot::channel();
        let _ = self.storage_tx.send(StorageCommand::ColdRead {
            topic_name: topic.to_string(),
            from_seq: effective_from_seq,
            limit,
            reply: tx,
        });
        rx.await.unwrap_or_default()
    }

    pub async fn fetch(&self, group: &str, consumer_id: &str, generation: u64, limit: usize, topic: &str, wait_ms: u64) -> Result<Vec<Message>, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;

        let group_cancel = {
            let inner = Self::lock_topic(&topic_ref.inner);
            match inner.groups.get(group) {
                Some(g) => g.cancel_token(),
                None => return Err("Group not found".to_string()),
            }
        };

        if wait_ms == 0 {
            return match self.try_fetch_once(&topic_ref, group, consumer_id, generation, limit)? {
                FetchAttempt::Ready(messages) => Ok(messages),
                FetchAttempt::NeedColdRead { from_seq } => self.cold_fetch(&topic_ref, topic, group, consumer_id, generation, limit, from_seq, &group_cancel).await,
                FetchAttempt::Wait => Ok(Vec::new()),
            };
        }

        let deadline = Instant::now() + Duration::from_millis(wait_ms);

        loop {
            let notified = topic_ref.notify.notified();

            match self.try_fetch_once(&topic_ref, group, consumer_id, generation, limit) {
                Ok(FetchAttempt::Ready(messages)) => return Ok(messages),
                Ok(FetchAttempt::NeedColdRead { from_seq }) => {
                    return self.cold_fetch(&topic_ref, topic, group, consumer_id, generation, limit, from_seq, &group_cancel).await;
                }
                Ok(FetchAttempt::Wait) => {}
                Err(e) if e == "NOT_MEMBER" && !self.is_active_member(&topic_ref, group, consumer_id) => {
                    return Ok(Vec::new());
                }
                Err(e) => return Err(e),
            }

            if Instant::now() >= deadline {
                return Ok(Vec::new());
            }

            tokio::select! {
                _ = notified => {}
                _ = sleep_until(deadline) => return Ok(Vec::new()),
                _ = group_cancel.cancelled() => return Ok(Vec::new()),
            }
        }
    }

    fn is_active_member(&self, topic_ref: &Arc<TopicShared>, group: &str, consumer_id: &str) -> bool {
        let inner = Self::lock_topic(&topic_ref.inner);
        inner.groups.get(group).map_or(false, |g| g.is_member(consumer_id))
    }

    pub async fn ack(&self, group: &str, topic: &str, consumer_id: &str, generation: u64, seq: u64) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let mut inner = Self::lock_topic(&topic_ref.inner);
        let head_seq = inner.state.head_seq;
        let Some(group_ref) = inner.groups.get_mut(group) else {
            return Err("Group not found".to_string());
        };

        let was_clamped = group_ref.clamp_head(head_seq);
        group_ref.ack(consumer_id, generation, seq)?;
        if was_clamped {
            inner.groups_dirty = true;
        }
        inner.groups_dirty = true;
        Ok(())
    }

    pub async fn seek(&self, group: &str, topic: &str, target: SeekTarget) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        {
            let mut inner = Self::lock_topic(&topic_ref.inner);
            let last_seq = inner.state.next_seq.saturating_sub(1);
            let head_seq = inner.state.head_seq;
            let max_ack_pending = inner.full_config.max_ack_pending;
            let ack_wait = Duration::from_millis(inner.full_config.ack_wait_ms);
            let max_deliveries = inner.full_config.max_deliveries;
            let group_ref = inner.groups.entry(group.to_string())
                .or_insert_with(|| ConsumerGroup::new(group.to_string(), head_seq, max_ack_pending, ack_wait, max_deliveries));

            match target {
                SeekTarget::Beginning => group_ref.seek_beginning(head_seq),
                SeekTarget::End => group_ref.seek_end(last_seq),
            }
            inner.groups_dirty = true;
        }
        topic_ref.notify.notify_waiters();
        Ok(())
    }

    pub async fn leave_group(&self, group: &str, topic: &str, consumer_id: &str, generation: u64) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let should_notify = {
            let mut inner = Self::lock_topic(&topic_ref.inner);
            let Some(group_ref) = inner.groups.get_mut(group) else {
                return Err("Group not found".to_string());
            };

            if group_ref.generation() != generation {
                return Err("FENCED".to_string());
            }

            let Some(connection_client_id) = group_ref.remove_member(consumer_id) else {
                return Err("NOT_MEMBER".to_string());
            };

            let mut remove_client_key = false;
            if let Some(bindings) = inner.client_map.get_mut(&connection_client_id) {
                bindings.retain(|binding| !(binding.group_id == group && binding.consumer_id == consumer_id));
                remove_client_key = bindings.is_empty();
            }
            if remove_client_key {
                inner.client_map.remove(&connection_client_id);
            }

            inner.groups_dirty = true;
            true
        };
        if should_notify {
            topic_ref.notify.notify_waiters();
        }
        Ok(())
    }

    pub async fn join_group(&self, group: &str, topic: &str, connection_client_id: &str) -> Result<JoinGroupResult, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let mut inner = Self::lock_topic(&topic_ref.inner);
        let head_seq = inner.state.head_seq;
        let max_ack_pending = inner.full_config.max_ack_pending;
        let ack_wait = Duration::from_millis(inner.full_config.ack_wait_ms);
        let max_deliveries = inner.full_config.max_deliveries;
        let client_id = connection_client_id.to_string();
        let group_id = group.to_string();
        let group_exists = inner.groups.contains_key(&group_id);

        let (ack_floor, consumer_id, generation, was_clamped) = {
            let group_ref = inner.groups.entry(group_id.clone())
                .or_insert_with(|| ConsumerGroup::new(group_id.clone(), head_seq, max_ack_pending, ack_wait, max_deliveries));
            let was_clamped = group_ref.clamp_head(head_seq);
            let consumer_id = group_ref.add_member(client_id.clone());
            (group_ref.ack_floor, consumer_id, group_ref.generation(), was_clamped)
        };

        inner.client_map.entry(client_id).or_default().push(ConsumerBinding {
            group_id: group_id.clone(),
            consumer_id: consumer_id.clone(),
        });

        if !group_exists || was_clamped {
            inner.groups_dirty = true;
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
                let mut inner = Self::lock_topic(&topic_ref.inner);
                if let Some(bindings) = inner.client_map.remove(&client_id) {
                    for binding in bindings {
                        if let Some(group_ref) = inner.groups.get_mut(&binding.group_id) {
                            if group_ref.remove_member(&binding.consumer_id).is_some() {
                                should_notify = true;
                                inner.groups_dirty = true;
                            }
                        }
                    }
                }
            }
            if should_notify {
                topic_ref.notify.notify_waiters();
            }
        }
    }

    pub async fn get_snapshot(&self) -> StreamSnapshot {
        let mut topics = Vec::new();

        for (_, topic_ref) in Self::collect_topics(&self.topics) {
            let inner = Self::lock_topic(&topic_ref.inner);
            let groups = inner.groups.values().map(|group| ConsumerGroupSnapshot {
                id: group.id.clone(),
                ack_floor: group.ack_floor,
                pending_count: group.pending.len(),
            }).collect();

            let mut safe_config = inner.full_config.clone();
            safe_config.persistence_path = "".to_string();

            topics.push(TopicSnapshot {
                name: inner.state.name.clone(),
                last_seq: inner.state.next_seq.saturating_sub(1),
                groups,
                config: safe_config,
            });
        }

        StreamSnapshot { topics }
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

    fn lock_topic<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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
            let topic_ref = Self::build_topic_shared(name.clone(), topic_config).await;

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

    async fn build_topic_shared(name: String, config: TopicConfig) -> Arc<TopicShared> {
        let base_path = PathBuf::from(&config.persistence_path).join(&name);
        if let Err(e) = tokio::fs::create_dir_all(&base_path).await {
            tracing::error!("Failed to create topic directory at {:?}: {}", base_path, e);
        }

        let recovered = recover_topic(&name, PathBuf::from(config.persistence_path.clone())).await;
        let state = TopicState::restore(name.clone(), config.ram_soft_limit, recovered.head_seq.max(1), recovered.messages);

        let ack_wait = Duration::from_millis(config.ack_wait_ms);
        let persisted_seq = Arc::new(AtomicU64::new(state.next_seq.saturating_sub(1)));
        let mut groups = HashMap::new();
        for (group_id, ack_floor) in recovered.groups_data {
            groups.insert(
                group_id.clone(),
                ConsumerGroup::restore(group_id, ack_floor, state.head_seq, config.max_ack_pending, ack_wait, config.max_deliveries),
            );
        }

        Arc::new(TopicShared {
            inner: Mutex::new(TopicInner {
                state,
                groups,
                client_map: HashMap::new(),
                groups_dirty: false,
                full_config: config,
            }),
            notify: Notify::new(),
            persisted_seq,
        })
    }

    fn spawn_background_tasks(&self) {
        let cancel = self.cancel.clone();
        let topics = self.topics.clone();
        let eviction_interval_ms = self.config.eviction_interval_ms;
        tokio::spawn({
            let cancel = cancel.clone();
            async move {
                let mut timer = tokio::time::interval(Duration::from_millis(eviction_interval_ms));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = timer.tick() => {}
                    }
                    for (_, topic_ref) in StreamManager::collect_topics(&topics) {
                        let persisted_seq = topic_ref.persisted_seq.load(Ordering::Acquire);
                        let mut inner = StreamManager::lock_topic(&topic_ref.inner);
                        inner.state.evict(persisted_seq);
                    }
                }
            }
        });

        let topics = self.topics.clone();
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
                            let mut inner = StreamManager::lock_topic(&topic_ref.inner);
                            if !inner.groups_dirty {
                                None
                            } else {
                                inner.groups_dirty = false;
                                Some(inner.groups.iter().map(|(id, group)| (id.clone(), group.ack_floor)).collect::<HashMap<_, _>>())
                            }
                        };

                        if let Some(groups_data) = groups_data {
                            let _ = storage_tx.send(StorageCommand::SaveGroups {
                                topic_name,
                                groups_data,
                            });
                        }
                    }
                }
            }
        });

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
                        let (retention, max_segment_size) = {
                            let inner = StreamManager::lock_topic(&topic_ref.inner);
                            (inner.full_config.retention.clone(), inner.full_config.max_segment_size)
                        };

                        let (reply_tx, reply_rx) = oneshot::channel();
                        if storage_tx.send(StorageCommand::ApplyRetention {
                            topic_name,
                            retention,
                            max_segment_size,
                            reply: reply_tx,
                        }).is_err() {
                            continue;
                        }

                        let Ok(outcome) = reply_rx.await else {
                            continue;
                        };

                        let mut should_notify = false;
                        {
                            let mut inner = StreamManager::lock_topic(&topic_ref.inner);
                            let mut groups_changed = false;
                            if outcome.head_seq != inner.state.head_seq {
                                inner.state.apply_head(outcome.head_seq);
                                for group in inner.groups.values_mut() {
                                    if group.clamp_head(outcome.head_seq) {
                                        groups_changed = true;
                                    }
                                }
                                if groups_changed {
                                    inner.groups_dirty = true;
                                }
                                should_notify = true;
                            }
                        }
                        if should_notify {
                            topic_ref.notify.notify_waiters();
                        }
                    }
                }
            }
        });

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
                            let mut inner = StreamManager::lock_topic(&topic_ref.inner);
                            let mut groups_changed = false;
                            for group in inner.groups.values_mut() {
                                if group.check_redelivery() {
                                    groups_changed = true;
                                    should_notify = true;
                                }
                            }
                            if groups_changed {
                                inner.groups_dirty = true;
                            }
                        }
                        if should_notify {
                            topic_ref.notify.notify_waiters();
                        }
                    }
                }
            }
        });
    }
    fn try_fetch_once(&self, topic_ref: &Arc<TopicShared>, group: &str, consumer_id: &str, generation: u64, limit: usize) -> Result<FetchAttempt, String> {
        let mut inner = Self::lock_topic(&topic_ref.inner);
        let TopicInner {
            state,
            groups,
            groups_dirty,
            ..
        } = &mut *inner;

        let head_seq = state.head_seq;
        let next_seq = state.next_seq;
        let ram_start_seq = state.ram_start_seq;

        let (was_clamped, messages, is_fetching_cold, from_seq) = {
            let group_ref = match groups.get_mut(group) {
                Some(g) => g,
                None => return Err("Group not found".to_string()),
            };

            let was_clamped = group_ref.clamp_head(head_seq);

            if group_ref.is_backpressured() {
                return Ok(FetchAttempt::Ready(Vec::new()));
            }

            let messages = group_ref.fetch(consumer_id, generation, limit, &state.log, ram_start_seq, head_seq)?;
            let is_fetching_cold = group_ref.is_fetching_cold;
            let from_seq = group_ref.next_fetch_seq(head_seq);
            (was_clamped, messages, is_fetching_cold, from_seq)
        };

        if was_clamped {
            *groups_dirty = true;
        }

        if !messages.is_empty() {
            return Ok(FetchAttempt::Ready(messages));
        }

        if is_fetching_cold {
            return Ok(FetchAttempt::Ready(Vec::new()));
        }

        if from_seq < next_seq {
            if from_seq < ram_start_seq {
                if let Some(group_ref) = groups.get_mut(group) {
                    group_ref.is_fetching_cold = true;
                }
                return Ok(FetchAttempt::NeedColdRead { from_seq });
            }
        }

        Ok(FetchAttempt::Wait)
    }

    async fn cold_fetch(&self, topic_ref: &Arc<TopicShared>, topic: &str, group: &str, consumer_id: &str, generation: u64, limit: usize, from_seq: u64, group_cancel: &CancellationToken) -> Result<Vec<Message>, String> {
        let (tx, rx) = oneshot::channel();
        if self.storage_tx.send(StorageCommand::ColdRead {
            topic_name: topic.to_string(),
            from_seq,
            limit,
            reply: tx,
        }).is_err() {
            let mut inner = Self::lock_topic(&topic_ref.inner);
            if let Some(group_ref) = inner.groups.get_mut(group) {
                group_ref.is_fetching_cold = false;
            }
            return Err("Disk read failed".to_string());
        }

        let messages = match rx.await {
            Ok(messages) => messages,
            Err(_) => {
                let mut inner = Self::lock_topic(&topic_ref.inner);
                if let Some(group_ref) = inner.groups.get_mut(group) {
                    group_ref.is_fetching_cold = false;
                }
                return Err("Disk read failed".to_string());
            }
        };

        let mut inner = Self::lock_topic(&topic_ref.inner);
        let head_seq = inner.state.head_seq;
        if let Some(group_ref) = inner.groups.get_mut(group) {
            group_ref.is_fetching_cold = false;
            if group_cancel.is_cancelled() {
                return Ok(Vec::new());
            }
            let was_clamped = group_ref.clamp_head(head_seq);
            let registered = group_ref.register_cold_messages(consumer_id, generation, messages, head_seq)?;
            if was_clamped {
                inner.groups_dirty = true;
            }
            Ok(registered)
        } else {
            Err("Group disappeared during cold read".to_string())
        }
    }
}

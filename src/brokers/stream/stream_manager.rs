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

use crate::brokers::stream::commands::{SeekTarget, StreamCreateOptions};
use crate::brokers::stream::config::SystemStreamConfig;
use crate::brokers::stream::group::ConsumerGroup;
use crate::brokers::stream::message::Message;
use crate::brokers::stream::storage::{recover_topic, MessageToAppend, StorageCommand, StorageManager};
use crate::brokers::stream::topic::{TopicConfig, TopicState};
use crate::dashboard::stream::StreamBrokerSnapshot;

struct TopicShared {
    inner: Mutex<TopicInner>,
    notify: Notify,
    persisted_seq: Arc<AtomicU64>,
}

struct TopicInner {
    state: TopicState,
    groups: HashMap<String, ConsumerGroup>,
    client_map: HashMap<String, Vec<String>>,
    groups_dirty: bool,
    max_ack_pending: usize,
    ack_wait: Duration,
    full_config: TopicConfig,
}

enum FetchAttempt {
    Ready(Vec<Message>),
    NeedColdRead { from_seq: u64 },
    Wait,
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
    pub fn new(config: Arc<SystemStreamConfig>) -> Self {
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

        manager.spawn_background_tasks();
        manager.spawn_warm_start();
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
        let topic_config = Self::load_topic_config(&base_path, options, &self.config).await;

        info!("[StreamManager] Creating topic '{}'", name);

        if let Err(e) = tokio::fs::create_dir_all(&base_path).await {
            tracing::error!("Failed to create topic directory at {:?}: {}", base_path, e);
        } else {
            let config_path = base_path.join("config.json");
            if let Ok(data) = serde_json::to_string_pretty(&topic_config) {
                let _ = tokio::fs::write(&config_path, data).await;
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

        if self.topics.remove(&name).is_some() {
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

        let (messages, need_cold) = {
            let inner = Self::lock_topic(&topic_ref.inner);
            let messages = inner.state.read(from_seq, limit);
            let need_cold = messages.is_empty()
                && from_seq < inner.state.ram_start_seq
                && from_seq < inner.state.next_seq;
            (messages, need_cold)
        };

        if !messages.is_empty() || !need_cold {
            return messages;
        }

        let (tx, rx) = oneshot::channel();
        let _ = self.storage_tx.send(StorageCommand::ColdRead {
            topic_name: topic.to_string(),
            from_seq,
            limit,
            reply: tx,
        });
        rx.await.unwrap_or_default()
    }

    pub async fn fetch(&self, group: &str, client: &str, limit: usize, topic: &str, wait_ms: u64) -> Result<Vec<Message>, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;

        let group_cancel = {
            let inner = Self::lock_topic(&topic_ref.inner);
            match inner.groups.get(group) {
                Some(g) => g.cancel_token(),
                None => return Err("Group not found".to_string()),
            }
        };

        if wait_ms == 0 {
            return match self.try_fetch_once(&topic_ref, group, client, limit)? {
                FetchAttempt::Ready(messages) => Ok(messages),
                FetchAttempt::NeedColdRead { from_seq } => self.cold_fetch(&topic_ref, topic, group, client, limit, from_seq, &group_cancel).await,
                FetchAttempt::Wait => Ok(Vec::new()),
            };
        }

        let deadline = Instant::now() + Duration::from_millis(wait_ms);

        loop {
            let notified = topic_ref.notify.notified();

            match self.try_fetch_once(&topic_ref, group, client, limit) {
                Ok(FetchAttempt::Ready(messages)) => return Ok(messages),
                Ok(FetchAttempt::NeedColdRead { from_seq }) => {
                    return self.cold_fetch(&topic_ref, topic, group, client, limit, from_seq, &group_cancel).await;
                }
                Ok(FetchAttempt::Wait) => {}
                Err(e) if e == "NOT_MEMBER" && !self.is_active_member(&topic_ref, group, client) => {
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

    fn is_active_member(&self, topic_ref: &Arc<TopicShared>, group: &str, client: &str) -> bool {
        let inner = Self::lock_topic(&topic_ref.inner);
        inner.groups.get(group).map_or(false, |g| g.is_member(client))
    }

    pub async fn ack(&self, group: &str, topic: &str, seq: u64) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let mut inner = Self::lock_topic(&topic_ref.inner);
        if let Some(group_ref) = inner.groups.get_mut(group) {
            group_ref.ack(seq)?;
            inner.groups_dirty = true;
            Ok(())
        } else {
            Err("Group not found".to_string())
        }
    }

    pub async fn nack(&self, group: &str, topic: &str, seq: u64) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        {
            let mut inner = Self::lock_topic(&topic_ref.inner);
            if let Some(group_ref) = inner.groups.get_mut(group) {
                group_ref.nack(seq)?;
            } else {
                return Err("Group not found".to_string());
            }
        }
        topic_ref.notify.notify_waiters();
        Ok(())
    }

    pub async fn seek(&self, group: &str, topic: &str, target: SeekTarget) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        {
            let mut inner = Self::lock_topic(&topic_ref.inner);
            let last_seq = inner.state.next_seq.saturating_sub(1);
            let max_ack_pending = inner.max_ack_pending;
            let ack_wait = inner.ack_wait;
            let group_ref = inner.groups.entry(group.to_string())
                .or_insert_with(|| ConsumerGroup::new(group.to_string(), max_ack_pending, ack_wait));

            match target {
                SeekTarget::Beginning => group_ref.seek_beginning(),
                SeekTarget::End => group_ref.seek_end(last_seq),
            }
            inner.groups_dirty = true;
        }
        topic_ref.notify.notify_waiters();
        Ok(())
    }

    pub async fn leave_group(&self, group: &str, topic: &str, client: &str) -> Result<(), String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        {
            let mut inner = Self::lock_topic(&topic_ref.inner);
            if let Some(group_ref) = inner.groups.get_mut(group) {
                group_ref.remove_member(client);
            }

            if let Some(group_ids) = inner.client_map.get_mut(client) {
                group_ids.retain(|g| g != group);
            }
        }
        topic_ref.notify.notify_waiters();
        Ok(())
    }

    pub async fn join_group(&self, group: &str, topic: &str, client: &str) -> Result<u64, String> {
        let topic_ref = self.get_topic(topic).ok_or("Topic not found")?;
        let mut inner = Self::lock_topic(&topic_ref.inner);
        let max_ack_pending = inner.max_ack_pending;
        let ack_wait = inner.ack_wait;
        let client_id = client.to_string();
        let group_id = group.to_string();

        let ack_floor = {
            let group_ref = inner.groups.entry(group_id.clone())
                .or_insert_with(|| ConsumerGroup::new(group_id.clone(), max_ack_pending, ack_wait));
            group_ref.add_member(client_id.clone());
            group_ref.ack_floor
        };

        let group_ids = inner.client_map.entry(client_id).or_default();
        if !group_ids.contains(&group_id) {
            group_ids.push(group_id);
        }

        Ok(ack_floor)
    }

    pub async fn disconnect(&self, client_id: String) {
        info!("[StreamManager] Disconnecting client: {}", client_id);
        for (_, topic_ref) in Self::collect_topics(&self.topics) {
            let mut should_notify = false;
            {
                let mut inner = Self::lock_topic(&topic_ref.inner);
                if let Some(group_ids) = inner.client_map.remove(&client_id) {
                    for group_id in group_ids {
                        if let Some(group_ref) = inner.groups.get_mut(&group_id) {
                            if group_ref.remove_member(&client_id) {
                                should_notify = true;
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

    pub async fn get_snapshot(&self) -> StreamBrokerSnapshot {
        let mut topics = Vec::new();

        for (_, topic_ref) in Self::collect_topics(&self.topics) {
            let inner = Self::lock_topic(&topic_ref.inner);
            let groups = inner.groups.values().map(|group| {
                crate::dashboard::stream::ConsumerGroupSummary {
                    id: group.id.clone(),
                    ack_floor: group.ack_floor,
                    pending_count: group.pending.len(),
                }
            }).collect();

            let mut safe_config = inner.full_config.clone();
            safe_config.persistence_path = "".to_string();

            topics.push(crate::dashboard::stream::TopicSummary {
                name: inner.state.name.clone(),
                last_seq: inner.state.next_seq.saturating_sub(1),
                groups,
                config: safe_config,
            });
        }

        StreamBrokerSnapshot { topics }
    }

    pub async fn exists(&self, name: &str) -> bool {
        self.topics.contains_key(name)
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

    async fn build_topic_shared(name: String, config: TopicConfig) -> Arc<TopicShared> {
        let base_path = PathBuf::from(&config.persistence_path).join(&name);
        if let Err(e) = tokio::fs::create_dir_all(&base_path).await {
            tracing::error!("Failed to create topic directory at {:?}: {}", base_path, e);
        }

        let recovered = recover_topic(&name, PathBuf::from(config.persistence_path.clone())).await;
        let mut state = TopicState::restore(name.clone(), config.ram_soft_limit, recovered.messages);
        if let Some(first) = recovered.segments.first() {
            state.start_seq = first.start_seq;
        }

        let ack_wait = Duration::from_millis(config.ack_wait_ms);
        let persisted_seq = Arc::new(AtomicU64::new(state.next_seq.saturating_sub(1)));
        let mut groups = HashMap::new();
        for (group_id, ack_floor) in recovered.groups_data {
            groups.insert(
                group_id.clone(),
                ConsumerGroup::restore(group_id, ack_floor, config.max_ack_pending, ack_wait),
            );
        }

        Arc::new(TopicShared {
            inner: Mutex::new(TopicInner {
                state,
                groups,
                client_map: HashMap::new(),
                groups_dirty: false,
                max_ack_pending: config.max_ack_pending,
                ack_wait,
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

                        let _ = storage_tx.send(StorageCommand::ApplyRetention {
                            topic_name,
                            retention,
                            max_segment_size,
                        });
                    }
                }
            }
        });

        let topics = self.topics.clone();
        tokio::spawn({
            let cancel = cancel.clone();
            async move {
                let mut timer = tokio::time::interval(Duration::from_secs(5));
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
                            for group in inner.groups.values_mut() {
                                if group.check_redelivery() {
                                    should_notify = true;
                                }
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

    fn spawn_warm_start(&self) {
        let topics = self.topics.clone();
        let deleted_topics = self.deleted_topics.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            let persistence_path = PathBuf::from(&config.persistence_path);
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
                if deleted_topics.contains_key(&name) {
                    continue;
                }

                let topic_config = StreamManager::load_topic_config(&path, StreamCreateOptions::default(), &config).await;
                let topic_ref = StreamManager::build_topic_shared(name.clone(), topic_config).await;

                if !path.exists() || deleted_topics.contains_key(&name) {
                    continue;
                }

                use dashmap::mapref::entry::Entry;
                match topics.entry(name.clone()) {
                    Entry::Occupied(_) => {}
                    Entry::Vacant(v) => {
                        v.insert(topic_ref);
                        info!("[StreamManager] Warm start: Restored topic '{}'", name);
                    }
                }
            }
        });
    }

    fn try_fetch_once(&self, topic_ref: &Arc<TopicShared>, group: &str, client: &str, limit: usize) -> Result<FetchAttempt, String> {
        let mut inner = Self::lock_topic(&topic_ref.inner);
        let TopicInner {
            state,
            groups,
            ..
        } = &mut *inner;

        let group_ref = match groups.get_mut(group) {
            Some(g) => g,
            None => return Err("Group not found".to_string()),
        };

        if !group_ref.is_member(client) {
            return Err("NOT_MEMBER".to_string());
        }

        if group_ref.is_backpressured() {
            return Ok(FetchAttempt::Ready(Vec::new()));
        }

        let messages = group_ref.fetch(client, limit, &state.log, state.ram_start_seq);
        if !messages.is_empty() {
            return Ok(FetchAttempt::Ready(messages));
        }

        if group_ref.is_fetching_cold {
            return Ok(FetchAttempt::Ready(Vec::new()));
        }

        if group_ref.next_deliver_seq < state.next_seq {
            let from_seq = if let Some(seq) = group_ref.redeliver.front() {
                *seq
            } else {
                group_ref.next_deliver_seq
            };

            if from_seq < state.ram_start_seq {
                group_ref.is_fetching_cold = true;
                return Ok(FetchAttempt::NeedColdRead { from_seq });
            }
        }

        Ok(FetchAttempt::Wait)
    }

    async fn cold_fetch(&self, topic_ref: &Arc<TopicShared>, topic: &str, group: &str, client: &str, limit: usize, from_seq: u64, group_cancel: &CancellationToken) -> Result<Vec<Message>, String> {
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
        if let Some(group_ref) = inner.groups.get_mut(group) {
            group_ref.is_fetching_cold = false;
            if group_cancel.is_cancelled() {
                return Ok(Vec::new());
            }
            Ok(group_ref.register_cold_messages(client, messages))
        } else {
            Err("Group disappeared during cold read".to_string())
        }
    }
}

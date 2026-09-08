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

use crate::brokers::stream::config::SystemStreamConfig;
use crate::brokers::stream::domain::group::ConsumerGroup;
use crate::brokers::stream::domain::message::Message;
use crate::brokers::stream::domain::persistence::{
    record_len, recover_topic, GroupPersistentState, Segment, StorageCommand, StorageManager,
    MAX_STREAM_RECORD_BYTES,
};
use crate::brokers::stream::domain::topic::{StreamDefinition, TopicConfig};
use crate::brokers::stream::options::{SeekTarget, StreamCreateOptions};
use crate::brokers::{BrokerError, BrokerErrorKind, ProvisionOutcome, ProvisionResult};
use crate::protocol::STREAM_MAX_KEY_BYTES;

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
    lifecycle_gate: AsyncMutex<()>,
    storage_tx: mpsc::Sender<StorageCommand>,
    config: Arc<SystemStreamConfig>,
    cancel: CancellationToken,
}

impl StreamManager {
    const MAX_TOPIC_NAME_BYTES: usize = 255;

    fn validate_topic_name(name: &str) -> Result<(), BrokerError> {
        if name.is_empty() || name.len() > Self::MAX_TOPIC_NAME_BYTES {
            return Err(BrokerError::invalid_argument(format!(
                "Invalid stream name: length must be between 1 and {} bytes",
                Self::MAX_TOPIC_NAME_BYTES
            )));
        }
        if name == "."
            || name == ".."
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(BrokerError::invalid_argument(
                "Invalid stream name: only ASCII letters, digits, '.', '_' and '-' are allowed",
            ));
        }
        Ok(())
    }

    fn config_json(config: &TopicConfig) -> serde_json::Value {
        serde_json::json!({
            "retention": {
                "maxAgeMs": config.retention.max_age_ms,
                "maxBytes": config.retention.max_bytes,
            },
            "maxSegmentSize": config.max_segment_size,
            "maxAckPending": config.max_ack_pending,
            "ackWaitMs": config.ack_wait_ms,
            "maxDeliveries": config.max_deliveries,
        })
    }

    fn config_conflict(name: &str, requested: &TopicConfig, actual: &TopicConfig) -> BrokerError {
        let mut differences = Vec::with_capacity(6);
        if requested.retention.max_age_ms != actual.retention.max_age_ms {
            differences.push(serde_json::json!({
                "path": "config.retention.maxAgeMs",
                "requested": requested.retention.max_age_ms,
                "actual": actual.retention.max_age_ms,
            }));
        }
        if requested.retention.max_bytes != actual.retention.max_bytes {
            differences.push(serde_json::json!({
                "path": "config.retention.maxBytes",
                "requested": requested.retention.max_bytes,
                "actual": actual.retention.max_bytes,
            }));
        }
        if requested.max_segment_size != actual.max_segment_size {
            differences.push(serde_json::json!({
                "path": "config.maxSegmentSize",
                "requested": requested.max_segment_size,
                "actual": actual.max_segment_size,
            }));
        }
        if requested.max_ack_pending != actual.max_ack_pending {
            differences.push(serde_json::json!({
                "path": "config.maxAckPending",
                "requested": requested.max_ack_pending,
                "actual": actual.max_ack_pending,
            }));
        }
        if requested.ack_wait_ms != actual.ack_wait_ms {
            differences.push(serde_json::json!({
                "path": "config.ackWaitMs",
                "requested": requested.ack_wait_ms,
                "actual": actual.ack_wait_ms,
            }));
        }
        if requested.max_deliveries != actual.max_deliveries {
            differences.push(serde_json::json!({
                "path": "config.maxDeliveries",
                "requested": requested.max_deliveries,
                "actual": actual.max_deliveries,
            }));
        }
        BrokerError::config_conflict(
            format!(
                "Stream '{}' already exists with different configuration",
                name
            ),
            serde_json::json!({
                "resourceKind": "stream",
                "resourceName": name,
                "requested": Self::config_json(requested),
                "actual": Self::config_json(actual),
                "differences": differences,
            }),
        )
    }

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
            lifecycle_gate: AsyncMutex::new(()),
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
                state
                    .groups
                    .iter()
                    .map(|(id, group)| {
                        (
                            id.clone(),
                            GroupPersistentState {
                                ack_floor: group.ack_floor,
                                dlt_entries: group.dlt_snapshot(),
                                parked_keys: group.parked_keys_snapshot(),
                                redeliver_entries: group.redeliver_snapshot(),
                            },
                        )
                    })
                    .collect::<BTreeMap<_, _>>()
            };
            let _ = self
                .storage_tx
                .send(StorageCommand::SaveState {
                    topic_name,
                    groups: groups_data,
                })
                .await;
        }

        // Shutdown storage manager (drains remaining commands then exits)
        let (tx, rx) = oneshot::channel();
        let _ = self
            .storage_tx
            .send(StorageCommand::Shutdown { reply: tx })
            .await;
        let _ = rx.await;
    }

    pub async fn create_topic(
        &self,
        name: String,
        options: StreamCreateOptions,
    ) -> Result<ProvisionResult<StreamDefinition>, BrokerError> {
        Self::validate_topic_name(&name)?;
        let _lifecycle_guard = self.lifecycle_gate.lock().await;
        self.deleted_topics.remove(&name);
        let requested = TopicConfig::from_options(options, &self.config);

        if let Some(topic_ref) = self.get_topic(&name) {
            let actual = topic_ref.state.lock().full_config.clone();
            if actual != requested {
                return Err(Self::config_conflict(&name, &requested, &actual));
            }
            return Ok(ProvisionResult {
                outcome: ProvisionOutcome::Unchanged,
                definition: StreamDefinition {
                    name,
                    config: actual,
                },
            });
        }

        let base_path = PathBuf::from(&self.config.persistence_path).join(&name);
        let existed_on_disk = match tokio::fs::symlink_metadata(&base_path).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(BrokerError::invalid_argument(
                    "Invalid stream path: symbolic links are not allowed",
                ));
            }
            Ok(metadata) if metadata.is_dir() => true,
            Ok(_) => {
                return Err(BrokerError::invalid_argument(
                    "Invalid stream path: expected a directory",
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(BrokerError::storage(format!(
                    "Failed to inspect stream path: {}",
                    error
                )))
            }
        };

        info!("[StreamManager] Creating stream '{}'", name);

        if !existed_on_disk {
            tokio::fs::create_dir_all(&base_path)
                .await
                .map_err(|error| {
                    BrokerError::storage(format!("Failed to create stream directory: {}", error))
                })?;
        }

        let config_path = base_path.join("config.json");
        let actual = if config_path.exists() {
            let data = tokio::fs::read_to_string(&config_path)
                .await
                .map_err(|error| {
                    BrokerError::storage(format!("Failed to read stream configuration: {}", error))
                })?;
            serde_json::from_str::<TopicConfig>(&data).map_err(|error| {
                BrokerError::storage(format!("Failed to parse stream configuration: {}", error))
            })?
        } else {
            let data = serde_json::to_string_pretty(&requested).map_err(|error| {
                BrokerError::storage(format!(
                    "Failed to serialize stream configuration: {}",
                    error
                ))
            })?;
            tokio::fs::write(&config_path, data)
                .await
                .map_err(|error| {
                    BrokerError::storage(format!("Failed to write stream configuration: {}", error))
                })?;
            requested.clone()
        };

        if actual != requested {
            return Err(Self::config_conflict(&name, &requested, &actual));
        }

        let shared =
            Self::build_topic_shared(name.clone(), actual.clone(), &self.config.persistence_path)
                .await;
        self.topics.insert(name.clone(), shared);
        Ok(ProvisionResult {
            outcome: ProvisionOutcome::Created,
            definition: StreamDefinition {
                name,
                config: actual,
            },
        })
    }

    pub async fn delete_topic(&self, name: String) -> Result<(), BrokerError> {
        Self::validate_topic_name(&name)?;
        let _lifecycle_guard = self.lifecycle_gate.lock().await;
        self.deleted_topics.insert(name.clone(), ());

        let topic_path = PathBuf::from(&self.config.persistence_path).join(&name);
        let topic_ref = self.get_topic(&name);
        let _append_guard = match &topic_ref {
            Some(topic_ref) => Some(topic_ref.append_gate.clone().lock_owned().await),
            None => None,
        };
        let _retention_guard = match &topic_ref {
            Some(topic_ref) => Some(topic_ref.retention_gate.write().await),
            None => None,
        };

        if let Some(topic_ref) = &topic_ref {
            if self
                .topics
                .get(&name)
                .is_some_and(|current| Arc::ptr_eq(current.value(), topic_ref))
            {
                self.topics.remove(&name);
            }
        }

        let exists_on_disk = match tokio::fs::symlink_metadata(&topic_path).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(BrokerError::invalid_argument(
                    "Invalid stream path: symbolic links are not allowed",
                ));
            }
            Ok(metadata) => metadata.is_dir(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(BrokerError::storage(format!(
                    "Failed to inspect stream path: {}",
                    error
                )))
            }
        };

        if topic_ref.is_some() || exists_on_disk {
            let (del_tx, del_rx) = oneshot::channel();
            self.storage_tx
                .send(StorageCommand::DropTopic {
                    topic_name: name,
                    reply: del_tx,
                })
                .await
                .map_err(|_| BrokerError::storage("Storage unavailable"))?;
            del_rx
                .await
                .map_err(|_| BrokerError::storage("Storage delete failed"))?
                .map_err(|error| {
                    BrokerError::storage(format!("Storage delete failed: {}", error))
                })?;
        }
        Ok(())
    }

    pub async fn publish_batch(
        &self,
        topic: &str,
        items: Vec<(Option<Bytes>, Bytes)>,
    ) -> Result<Vec<u64>, BrokerError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        for (key, payload) in &items {
            if key.as_ref().is_some_and(Bytes::is_empty) {
                return Err(BrokerError::invalid_argument(
                    "Stream key must not be empty",
                ));
            }
            if key
                .as_ref()
                .is_some_and(|key| key.len() > STREAM_MAX_KEY_BYTES)
            {
                return Err(BrokerError::invalid_argument(format!(
                    "Stream key exceeds {} bytes",
                    STREAM_MAX_KEY_BYTES
                )));
            }
            let size = record_len(key.as_deref(), payload);
            if size > MAX_STREAM_RECORD_BYTES as u64 {
                return Err(BrokerError::invalid_argument(format!(
                    "Stream record too large: {} bytes (max: {})",
                    size, MAX_STREAM_RECORD_BYTES
                )));
            }
        }
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        let append_guard = topic_ref.append_gate.clone().lock_owned().await;
        if !self
            .get_topic(topic)
            .is_some_and(|current| Arc::ptr_eq(&current, &topic_ref))
        {
            return Err(BrokerError::not_found(format!(
                "Stream '{}' not found",
                topic
            )));
        }
        let permit = self
            .storage_tx
            .reserve()
            .await
            .map_err(|_| BrokerError::storage("Storage unavailable"))?;

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
                messages.push(Message {
                    seq,
                    timestamp,
                    key,
                    payload,
                });
            }

            let bytes_len: u64 = messages
                .iter()
                .map(|m| record_len(m.key.as_deref(), &m.payload))
                .sum();

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
                                if segments.last().map(|segment| &segment.path)
                                    != Some(&commit_path)
                                {
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
        reply_rx
            .await
            .map_err(|_| BrokerError::storage("Storage append failed"))?
            .map_err(BrokerError::storage)?;
        Ok(seqs)
    }

    pub async fn publish(
        &self,
        topic: &str,
        key: Option<Bytes>,
        payload: Bytes,
    ) -> Result<u64, BrokerError> {
        let seqs = self.publish_batch(topic, vec![(key, payload)]).await?;
        Ok(seqs.into_iter().next().unwrap_or(0))
    }

    pub async fn peek_dlt(
        &self,
        topic: &str,
        group: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(u64, String, u32, Option<Bytes>)>, BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        let state = topic_ref.state.lock();
        let group_ref = state.groups.get(group).ok_or_else(|| {
            BrokerError::not_found(format!("Consumer group '{}' not found", group))
        })?;
        let entries = group_ref.peek_dlt(limit, offset);
        Ok(entries
            .into_iter()
            .map(|(seq, e)| (seq, e.reason, e.attempts, e.key))
            .collect())
    }

    pub async fn move_to_stream(
        &self,
        topic: &str,
        group: &str,
        seq: u64,
    ) -> Result<bool, BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        let key_unblocked = {
            let mut state = topic_ref.state.lock();
            let group_ref = state.groups.get_mut(group).ok_or_else(|| {
                BrokerError::not_found(format!("Consumer group '{}' not found", group))
            })?;
            let key_unblocked = group_ref.move_to_stream(seq)?;
            state.groups_dirty = true;
            key_unblocked
        };
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(key_unblocked)
    }

    pub async fn delete_dlt(
        &self,
        topic: &str,
        group: &str,
        seq: u64,
    ) -> Result<bool, BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        let key_unblocked = {
            let mut state = topic_ref.state.lock();
            let group_ref = state.groups.get_mut(group).ok_or_else(|| {
                BrokerError::not_found(format!("Consumer group '{}' not found", group))
            })?;
            let key_unblocked = group_ref.delete_dlt(seq)?;
            state.groups_dirty = true;
            key_unblocked
        };
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(key_unblocked)
    }

    pub async fn purge_dlt(&self, topic: &str, group: &str) -> Result<usize, BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        let count = {
            let mut state = topic_ref.state.lock();
            let group_ref = state.groups.get_mut(group).ok_or_else(|| {
                BrokerError::not_found(format!("Consumer group '{}' not found", group))
            })?;
            let count = group_ref.purge_dlt();
            state.groups_dirty = true;
            count
        };
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(count)
    }

    pub async fn read(
        &self,
        topic: &str,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<Message>, BrokerError> {
        let Some(topic_ref) = self.get_topic(topic) else {
            return Ok(Vec::new());
        };
        let retention_guard = topic_ref.retention_gate.clone().read_owned().await;

        let (offsets, segments) = {
            let state = topic_ref.state.lock();
            let offsets = state
                .index
                .range(from_seq..)
                .take(limit)
                .map(|(seq, offset)| (*seq, *offset))
                .collect::<Vec<_>>();
            (offsets, state.segments.clone())
        };

        if offsets.is_empty() {
            return Ok(Vec::new());
        }

        let (tx, rx) = oneshot::channel();
        self.storage_tx
            .send(StorageCommand::ReadRange {
                segments,
                retention_guard,
                offsets,
                reply: tx,
            })
            .await
            .map_err(|_| BrokerError::storage("Storage unavailable"))?;
        rx.await
            .map_err(|_| BrokerError::storage("Storage read failed"))?
            .map_err(|error| BrokerError::storage(format!("Storage read failed: {}", error)))
    }

    pub async fn fetch(
        &self,
        group: &str,
        consumer_id: &str,
        generation: u64,
        limit: usize,
        topic: &str,
        wait_ms: u64,
    ) -> Result<Vec<Message>, BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;

        let group_cancel = {
            let state = topic_ref.state.lock();
            match state.groups.get(group) {
                Some(g) => g.cancel_token(),
                None => {
                    return Err(BrokerError::not_found(format!(
                        "Consumer group '{}' not found",
                        group
                    )))
                }
            }
        };

        if wait_ms == 0 {
            return self
                .try_fetch_once(
                    &topic_ref,
                    group,
                    consumer_id,
                    generation,
                    limit,
                    &group_cancel,
                )
                .await;
        }

        let deadline = Instant::now() + Duration::from_millis(wait_ms);
        let mut wake_rx = topic_ref.wake_tx.subscribe();

        loop {
            let wake_ver = *wake_rx.borrow();

            match self
                .try_fetch_once(
                    &topic_ref,
                    group,
                    consumer_id,
                    generation,
                    limit,
                    &group_cancel,
                )
                .await
            {
                Ok(messages) if !messages.is_empty() => return Ok(messages),
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind,
                        BrokerErrorKind::NotMember | BrokerErrorKind::Fenced
                    ) && !self.is_active_member(&topic_ref, group, consumer_id) =>
                {
                    return Ok(Vec::new());
                }
                Err(error) => return Err(error),
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

    fn is_active_member(
        &self,
        topic_ref: &Arc<TopicShared>,
        group: &str,
        consumer_id: &str,
    ) -> bool {
        let state = topic_ref.state.lock();
        state
            .groups
            .get(group)
            .map_or(false, |g| g.is_member(consumer_id))
    }

    pub async fn ack(
        &self,
        group: &str,
        topic: &str,
        consumer_id: &str,
        generation: u64,
        seq: u64,
    ) -> Result<(), BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        {
            let mut state = topic_ref.state.lock();
            let head_seq = state.head_seq;
            let Some(group_ref) = state.groups.get_mut(group) else {
                return Err(BrokerError::not_found(format!(
                    "Consumer group '{}' not found",
                    group
                )));
            };

            group_ref.clamp_head(head_seq);
            group_ref.ack(consumer_id, generation, seq)?;
            state.groups_dirty = true;
        }
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(())
    }

    pub async fn seek(
        &self,
        group: &str,
        topic: &str,
        target: SeekTarget,
    ) -> Result<(), BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        {
            let mut state = topic_ref.state.lock();
            let last_seq = state.next_seq.saturating_sub(1);
            let head_seq = state.head_seq;
            let max_ack_pending = state.full_config.max_ack_pending;
            let ack_wait = Duration::from_millis(state.full_config.ack_wait_ms);
            let max_deliveries = state.full_config.max_deliveries;
            let group_ref = state.groups.entry(group.to_string()).or_insert_with(|| {
                ConsumerGroup::new(
                    group.to_string(),
                    head_seq,
                    max_ack_pending,
                    ack_wait,
                    max_deliveries,
                )
            });

            match target {
                SeekTarget::Beginning => group_ref.seek_beginning(head_seq),
                SeekTarget::End => group_ref.seek_end(last_seq),
            }
            state.groups_dirty = true;
        }
        topic_ref.wake_tx.send_modify(|v| *v += 1);
        Ok(())
    }

    pub async fn leave_group(
        &self,
        group: &str,
        topic: &str,
        consumer_id: &str,
        generation: u64,
    ) -> Result<(), BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        let should_notify = {
            let mut state = topic_ref.state.lock();
            let Some(group_ref) = state.groups.get_mut(group) else {
                return Err(BrokerError::not_found(format!(
                    "Consumer group '{}' not found",
                    group
                )));
            };

            if group_ref.generation() != generation {
                return Err(BrokerError::fenced());
            }

            let Some(connection_client_id) = group_ref.remove_member(consumer_id) else {
                return Err(BrokerError::not_member());
            };

            let mut remove_client_key = false;
            if let Some(bindings) = state.client_map.get_mut(&connection_client_id) {
                bindings.retain(|binding| {
                    !(binding.group_id == group && binding.consumer_id == consumer_id)
                });
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

    pub async fn join_group(
        &self,
        group: &str,
        topic: &str,
        connection_client_id: &str,
    ) -> Result<JoinGroupResult, BrokerError> {
        let topic_ref = self
            .get_topic(topic)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", topic)))?;
        let mut state = topic_ref.state.lock();
        let head_seq = state.head_seq;
        let max_ack_pending = state.full_config.max_ack_pending;
        let ack_wait = Duration::from_millis(state.full_config.ack_wait_ms);
        let max_deliveries = state.full_config.max_deliveries;
        let client_id = connection_client_id.to_string();
        let group_id = group.to_string();
        let group_exists = state.groups.contains_key(&group_id);

        let (ack_floor, consumer_id, generation, was_clamped) = {
            let group_ref = state.groups.entry(group_id.clone()).or_insert_with(|| {
                ConsumerGroup::new(
                    group_id.clone(),
                    head_seq,
                    max_ack_pending,
                    ack_wait,
                    max_deliveries,
                )
            });
            let was_clamped = group_ref.clamp_head(head_seq);
            let consumer_id = group_ref.add_member(client_id.clone());
            (
                group_ref.ack_floor,
                consumer_id,
                group_ref.generation(),
                was_clamped,
            )
        };

        state
            .client_map
            .entry(client_id)
            .or_default()
            .push(ConsumerBinding {
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
        if Self::validate_topic_name(name).is_err() {
            return false;
        }
        if self.topics.contains_key(name) {
            return true;
        }

        let path = PathBuf::from(&self.config.persistence_path).join(name);
        tokio::fs::symlink_metadata(path)
            .await
            .map(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
            .unwrap_or(false)
    }

    pub async fn describe(&self, name: &str) -> Result<StreamDefinition, BrokerError> {
        Self::validate_topic_name(name)?;
        let topic_ref = self
            .get_topic(name)
            .ok_or_else(|| BrokerError::not_found(format!("Stream '{}' not found", name)))?;
        let config = topic_ref.state.lock().full_config.clone();
        Ok(StreamDefinition {
            name: name.to_string(),
            config,
        })
    }

    fn get_topic(&self, topic: &str) -> Option<Arc<TopicShared>> {
        self.topics.get(topic).map(|entry| entry.value().clone())
    }

    fn collect_topics(
        topics: &Arc<DashMap<String, Arc<TopicShared>>>,
    ) -> Vec<(String, Arc<TopicShared>)> {
        topics
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    async fn load_topic_config(
        base_path: &PathBuf,
        options: StreamCreateOptions,
        config: &SystemStreamConfig,
    ) -> TopicConfig {
        let config_path = base_path.join("config.json");
        if let Ok(data) = tokio::fs::read_to_string(&config_path).await {
            serde_json::from_str(&data)
                .unwrap_or_else(|_| TopicConfig::from_options(options, config))
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
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }

            let Some(topic_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            let name = topic_name.to_string();
            if Self::validate_topic_name(&name).is_err() || self.deleted_topics.contains_key(&name)
            {
                continue;
            }

            let topic_config =
                Self::load_topic_config(&path, StreamCreateOptions::default(), &self.config).await;

            let config_path = path.join("config.json");
            if !config_path.exists() {
                if let Ok(data) = serde_json::to_string_pretty(&topic_config) {
                    let _ = tokio::fs::write(&config_path, data).await;
                }
            }

            let topic_ref =
                Self::build_topic_shared(name.clone(), topic_config, &self.config.persistence_path)
                    .await;

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

    async fn build_topic_shared(
        name: String,
        config: TopicConfig,
        persistence_path: &str,
    ) -> Arc<TopicShared> {
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
                ConsumerGroup::restore(
                    group_id,
                    group_state.ack_floor,
                    head_seq,
                    config.max_ack_pending,
                    ack_wait,
                    config.max_deliveries,
                    group_state.dlt_entries,
                    group_state.parked_keys,
                    group_state.redeliver_entries,
                ),
            );
        }

        let active_path = recovered
            .segments
            .last()
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
                                Some(
                                    state
                                        .groups
                                        .iter()
                                        .map(|(id, group)| {
                                            (
                                                id.clone(),
                                                GroupPersistentState {
                                                    ack_floor: group.ack_floor,
                                                    dlt_entries: group.dlt_snapshot(),
                                                    parked_keys: group.parked_keys_snapshot(),
                                                    redeliver_entries: group.redeliver_snapshot(),
                                                },
                                            )
                                        })
                                        .collect::<BTreeMap<_, _>>(),
                                )
                            }
                        };

                        if let Some(groups_data) = groups_data {
                            let _ = storage_tx
                                .send(StorageCommand::SaveState {
                                    topic_name,
                                    groups: groups_data,
                                })
                                .await;
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
                        if storage_tx
                            .send(StorageCommand::ApplyRetention {
                                topic_name,
                                retention,
                                reply: reply_tx,
                            })
                            .await
                            .is_err()
                        {
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
                                let to_remove: Vec<u64> =
                                    state.index.range(..new_head_seq).map(|(k, _)| *k).collect();
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
    ) -> Result<Vec<Message>, BrokerError> {
        let retention_guard = topic_ref.retention_gate.clone().read_owned().await;

        // 1. Compute fetch plan under lock (which seqs to read)
        let (plan, was_clamped) = {
            let mut state = topic_ref.state.lock();
            let head_seq = state.head_seq;
            let Some(group_ref) = state.groups.get_mut(group) else {
                return Err(BrokerError::not_found(format!(
                    "Consumer group '{}' not found",
                    group
                )));
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
            let offsets = plan
                .iter()
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
        if self
            .storage_tx
            .send(StorageCommand::ReadRange {
                segments,
                retention_guard,
                offsets,
                reply: tx,
            })
            .await
            .is_err()
        {
            return Err(BrokerError::storage("Storage read failed"));
        }

        let messages = rx
            .await
            .map_err(|_| BrokerError::storage("Storage read failed"))?
            .map_err(|error| BrokerError::storage(format!("Storage read failed: {}", error)))?;
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        // 4. Deliver messages to the group (under lock)
        let mut state = topic_ref.state.lock();
        let head_seq = state.head_seq;
        let Some(group_ref) = state.groups.get_mut(group) else {
            return Err(BrokerError::not_found(format!(
                "Consumer group '{}' not found",
                group
            )));
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
            lifecycle_gate: AsyncMutex::new(()),
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

    #[test]
    fn topic_names_cannot_escape_the_persistence_directory() {
        for invalid in [
            "",
            ".",
            "..",
            "../outside",
            "nested/topic",
            "nested\\topic",
            "/tmp/topic",
            "topic name",
        ] {
            assert!(
                StreamManager::validate_topic_name(invalid).is_err(),
                "{invalid:?} must be rejected"
            );
        }
        assert!(StreamManager::validate_topic_name("topic_42-chat.events").is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn topic_directory_symlinks_are_rejected() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), temp_dir.path().join("linked-topic")).unwrap();

        let mut config = SystemStreamConfig::default();
        config.persistence_path = temp_dir.path().to_str().unwrap().to_string();
        let manager = StreamManager::new(Arc::new(config)).await;

        let error = manager
            .create_topic("linked-topic".to_string(), StreamCreateOptions::default())
            .await
            .unwrap_err();

        assert!(error.message.contains("symbolic links"));
        manager.shutdown().await;
    }
}

//! Queue Manager: Shared-state router and lifecycle manager for queues.
//! Each queue is an Arc<QueueShared> with a Mutex<QueueInner> for state
//! and a Notify for long-polling wakeup.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::Notify;
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;
use bytes::Bytes;
use uuid::Uuid;
use tracing::{error, info};

use crate::brokers::queue::queue::{QueueConfig, QueueState, Message, QueueMessageView, current_time_ms};
use crate::brokers::queue::commands::QueueCreateOptions;
use crate::brokers::queue::dlq::{DlqMessage, DlqState};
use crate::brokers::queue::persistence::{QueueStore, StorageOp};
use crate::brokers::queue::config::SystemQueueConfig;
use crate::dashboard::queue::QueueSummary;

// ==========================================
// SHARED STATE
// ==========================================

struct QueueShared {
    inner: Mutex<QueueInner>,
    notify: Notify,
    store: QueueStore,
}

struct QueueInner {
    name: String,
    state: QueueState,
    dlq: DlqState,
    config: QueueConfig,
}

// ==========================================
// QUEUE MANAGER
// ==========================================

#[derive(Clone)]
pub struct QueueManager {
    queues: Arc<DashMap<String, Arc<QueueShared>>>,
    config: Arc<SystemQueueConfig>,
    cancel: CancellationToken,
}

impl QueueManager {
    pub fn new(system_config: Arc<SystemQueueConfig>) -> Self {
        let queues = Arc::new(DashMap::new());
        let cancel = CancellationToken::new();

        // Ensure persistence directory exists (one-time setup)
        let persistence_path = std::path::PathBuf::from(&system_config.persistence_path);
        if let Err(e) = std::fs::create_dir_all(&persistence_path) {
            error!("Failed to create queue data directory at {:?}: {}", persistence_path, e);
        }

        let manager = Self {
            queues: queues.clone(),
            config: system_config.clone(),
            cancel: cancel.clone(),
        };

        // WARM START: Discover and restore queues from filesystem
        let persistence_path = std::path::PathBuf::from(&system_config.persistence_path);
        if persistence_path.exists() {
            if let Ok(entries) = std::fs::read_dir(&persistence_path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                            if filename.ends_with(".db") && !filename.ends_with(".db-wal") && !filename.ends_with(".db-shm") {
                                let queue_name = filename.trim_end_matches(".db").to_string();

                                let config_path = persistence_path.join(format!("{}.config.json", queue_name));
                                let config = if let Ok(data) = std::fs::read_to_string(&config_path) {
                                    serde_json::from_str(&data).unwrap_or_else(|_| QueueConfig::from_options(QueueCreateOptions::default(), &system_config))
                                } else {
                                    QueueConfig::from_options(QueueCreateOptions::default(), &system_config)
                                };

                                let shared = Self::build_queue(queue_name.clone(), config, &system_config);
                                queues.insert(queue_name.clone(), shared);
                                info!("[QueueManager] Warm start: Restored queue '{}'", queue_name);
                            }
                        }
                    }
                }
            }
        }

        manager.spawn_time_event_task();
        manager
    }

    // ==========================================
    // INTERNAL HELPERS
    // ==========================================

    fn build_queue(name: String, config: QueueConfig, system_config: &SystemQueueConfig) -> Arc<QueueShared> {
        let persistence_path = std::path::PathBuf::from(&system_config.persistence_path);
        let db_path = persistence_path.join(format!("{}.db", name));
        let store = QueueStore::new(
            db_path,
            system_config.default_flush_ms,
            system_config.writer_batch_size,
        );

        let mut main_state = QueueState::new();
        let mut dlq_state = DlqState::new();

        // Recovery
        match store.recover() {
            Ok((main_messages, dlq_messages)) => {
                let main_count = main_messages.len();
                let dlq_count = dlq_messages.len();

                for msg in main_messages {
                    main_state.push(msg);
                }
                for msg in dlq_messages {
                    dlq_state.push(msg);
                }

                if main_count > 0 || dlq_count > 0 {
                    info!("Queue '{}': Recovered {} main + {} DLQ messages from storage", name, main_count, dlq_count);
                }
            }
            Err(e) => {
                error!("Queue '{}': Persistence recovery failed: {}", name, e);
            }
        }

        Arc::new(QueueShared {
            inner: Mutex::new(QueueInner {
                name,
                state: main_state,
                dlq: dlq_state,
                config,
            }),
            notify: Notify::new(),
            store,
        })
    }

    fn spawn_time_event_task(&self) {
        let queues = self.queues.clone();
        let cancel = self.cancel.clone();

        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_millis(50));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = timer.tick() => {}
                }

                for entry in queues.iter() {
                    let shared = entry.value().clone();
                    let now = current_time_ms();

                    let (requeued, dlq_msgs) = {
                        let mut inner = Self::lock(&shared.inner);
                        
                        // Check if processing is needed
                        let should_process = inner.state.next_timeout().map(|ts| ts <= now).unwrap_or(false);
                        if !should_process {
                            continue;
                        }

                        let max_retries = inner.config.max_retries;
                        let (requeued, dlq_msgs) = inner.state.process_expired(max_retries);

                        for ref dlq_msg in &dlq_msgs {
                            inner.dlq.push((*dlq_msg).clone());
                        }

                        (requeued, dlq_msgs)
                    };

                    if requeued.is_empty() && dlq_msgs.is_empty() {
                        continue;
                    }

                    for msg in &requeued {
                        shared.store.execute(StorageOp::UpdateState {
                            id: msg.id,
                            visible_at: 0,
                            attempts: msg.attempts,
                        });
                    }

                    for dlq_msg in dlq_msgs {
                        shared.store.execute(StorageOp::MoveToDLQ {
                            id: dlq_msg.id,
                            msg: dlq_msg,
                        });
                    }

                    if !requeued.is_empty() {
                        shared.notify.notify_waiters();
                    }
                }
            }
        });
    }

    #[inline]
    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn get_queue(&self, name: &str) -> Option<Arc<QueueShared>> {
        self.queues.get(name).map(|r| r.value().clone())
    }

    fn persist_batch_state(&self, shared: &Arc<QueueShared>, msgs: &[Message]) {
        for msg in msgs {
            shared.store.execute(StorageOp::UpdateState {
                id: msg.id,
                visible_at: msg.visible_at,
                attempts: msg.attempts,
            });
        }
    }

    // ==========================================
    // PUBLIC API
    // ==========================================

    pub async fn create_queue(&self, name: String, options: QueueCreateOptions) -> Result<(), String> {
        use dashmap::mapref::entry::Entry;

        match self.queues.entry(name.clone()) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(v) => {
                let config = QueueConfig::from_options(options, &self.config);

                // Persist config
                let persistence_path = std::path::PathBuf::from(&self.config.persistence_path);
                let config_path = persistence_path.join(format!("{}.config.json", name));
                if let Ok(data) = serde_json::to_string_pretty(&config) {
                    let _ = std::fs::write(&config_path, data);
                }

                let shared = Self::build_queue(name, config, &self.config);
                v.insert(shared);
                Ok(())
            }
        }
    }

    pub async fn delete_queue(&self, name: String) -> Result<(), String> {
        if let Some((_, shared)) = self.queues.remove(&name) {
            shared.store.shutdown().await;
        }

        // Delete Persistence (safe: writer has flushed and closed)
        let base_path = std::path::PathBuf::from(&self.config.persistence_path);
        let db_path = base_path.join(format!("{}.db", name));
        let wal_path = base_path.join(format!("{}.db-wal", name));
        let shm_path = base_path.join(format!("{}.db-shm", name));
        let config_path = base_path.join(format!("{}.config.json", name));

        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(wal_path);
        let _ = std::fs::remove_file(shm_path);
        let _ = std::fs::remove_file(config_path);

        Ok(())
    }

    pub async fn push(&self, queue_name: String, payload: Bytes, priority: u8, delay_ms: Option<u64>) -> Result<(), String> {
        let shared = self.get_queue(&queue_name)
            .ok_or_else(|| format!("Queue '{}' not found. Create it first.", queue_name))?;

        let msg = Message::new(payload, priority, delay_ms);
        {
            let mut inner = Self::lock(&shared.inner);
            inner.state.push(msg.clone());
        }

        shared.store.execute(StorageOp::Insert(msg));

        if delay_ms.is_none() {
            shared.notify.notify_waiters();
        }

        Ok(())
    }

    pub async fn pop(&self, queue_name: &str) -> Option<Message> {
        let shared = self.get_queue(queue_name)?;

        let (msg_opt, _) = {
            let mut inner = Self::lock(&shared.inner);
            let vt = inner.config.visibility_timeout_ms;
            inner.state.pop(vt)
        };

        if let Some(msg) = &msg_opt {
            shared.store.execute(StorageOp::UpdateState {
                id: msg.id,
                visible_at: msg.visible_at,
                attempts: msg.attempts,
            });
        }

        msg_opt
    }

    pub async fn ack(&self, queue_name: &str, id: Uuid) -> bool {
        let shared = match self.get_queue(queue_name) {
            Some(s) => s,
            None => return false,
        };

        let result = {
            let mut inner = Self::lock(&shared.inner);
            inner.state.ack(id)
        };

        if result {
            shared.store.execute(StorageOp::Delete(id));
        }

        result
    }

    pub async fn nack(&self, queue_name: &str, id: Uuid, reason: String) -> bool {
        let shared = match self.get_queue(queue_name) {
            Some(s) => s,
            None => return false,
        };

        let (requeued, dlq_msg) = {
            let mut inner = Self::lock(&shared.inner);
            let max_retries = inner.config.max_retries;
            let (requeued, dlq_msg) = inner.state.nack(id, reason, max_retries);

            if let Some(ref dlq_message) = dlq_msg {
                inner.dlq.push(dlq_message.clone());
            }

            (requeued, dlq_msg)
        };

        if let Some(msg) = requeued {
            shared.store.execute(StorageOp::UpdateState {
                id: msg.id,
                visible_at: msg.visible_at,
                attempts: msg.attempts,
            });
            shared.notify.notify_waiters();
            return true;
        }

        if let Some(dlq_message) = dlq_msg {
            shared.store.execute(StorageOp::MoveToDLQ {
                id: dlq_message.id,
                msg: dlq_message,
            });
            return true;
        }

        false
    }

    pub async fn consume_batch(&self, queue_name: String, max: Option<usize>, wait_ms: Option<u64>) -> Result<Vec<Message>, String> {
        let shared = self.get_queue(&queue_name)
            .ok_or_else(|| format!("Queue '{}' not found. Create it first.", queue_name))?;

        let max_val = max.unwrap_or(self.config.default_batch_size);
        let wait_val = wait_ms.unwrap_or(self.config.default_wait_ms);

        // Try immediate fetch
        let msgs = {
            let mut inner = Self::lock(&shared.inner);
            let vt = inner.config.visibility_timeout_ms;
            let (msgs, _) = inner.state.take_batch(max_val, vt);
            msgs
        };
        if !msgs.is_empty() {
            self.persist_batch_state(&shared, &msgs);
            return Ok(msgs);
        }

        // No messages and no wait -> return empty
        if wait_val == 0 {
            return Ok(vec![]);
        }

        // Long polling loop (like stream fetch)
        let deadline = Instant::now() + Duration::from_millis(wait_val);

        loop {
            let notified = shared.notify.notified();

            // Try fetch under lock
            let msgs = {
                let mut inner = Self::lock(&shared.inner);
                let vt = inner.config.visibility_timeout_ms;
                let (msgs, _) = inner.state.take_batch(max_val, vt);
                msgs
            };
            if !msgs.is_empty() {
                self.persist_batch_state(&shared, &msgs);
                return Ok(msgs);
            }

            if Instant::now() >= deadline {
                return Ok(vec![]);
            }

            tokio::select! {
                _ = notified => {}
                _ = sleep_until(deadline) => return Ok(vec![]),
            }
        }
    }

    pub async fn get_snapshot(&self) -> Vec<QueueSummary> {
        let mut queues = Vec::new();

        for entry in self.queues.iter() {
            let shared = entry.value().clone();
            let inner = Self::lock(&shared.inner);
            let (pending, inflight, scheduled) = inner.state.get_counters();
            queues.push(QueueSummary {
                name: inner.name.clone(),
                pending,
                inflight,
                scheduled,
                dlq: inner.dlq.len(),
                config: inner.config.clone(),
            });
        }

        queues
    }

    pub async fn get_messages(&self, queue_name: String, state_filter: String, offset: usize, limit: usize, search: Option<String>) -> Option<(usize, Vec<QueueMessageView>)> {
        let shared = self.get_queue(&queue_name)?;
        let inner = Self::lock(&shared.inner);
        Some(inner.state.get_messages(state_filter, offset, limit, search))
    }

    pub async fn exists(&self, name: &str) -> bool {
        self.queues.contains_key(name)
    }

    // --- DLQ Operations ---

    /// Peek messages from DLQ without consuming them
    pub async fn peek_dlq(&self, queue_name: &str, limit: usize, offset: usize) -> Result<(usize, Vec<DlqMessage>), String> {
        let shared = self.get_queue(queue_name)
            .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;

        let inner = Self::lock(&shared.inner);
        Ok(inner.dlq.peek(offset, limit))
    }

    /// Move a message from DLQ back to main queue (replay/retry)
    pub async fn move_to_queue(&self, queue_name: &str, message_id: Uuid) -> Result<bool, String> {
        let shared = self.get_queue(queue_name)
            .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;

        let new_msg = {
            let mut inner = Self::lock(&shared.inner);
            if let Some(dlq_msg) = inner.dlq.remove(&message_id) {
                let new_msg = dlq_msg.clone().to_message();
                inner.state.push(new_msg.clone());
                Some(new_msg)
            } else {
                None
            }
        };

        if let Some(msg) = new_msg {
            shared.store.execute(StorageOp::MoveToMain {
                id: message_id,
                msg,
            });
            shared.notify.notify_waiters();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Delete a specific message from DLQ
    pub async fn delete_dlq(&self, queue_name: &str, message_id: Uuid) -> Result<bool, String> {
        let shared = self.get_queue(queue_name)
            .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;

        let removed = {
            let mut inner = Self::lock(&shared.inner);
            inner.dlq.remove(&message_id).is_some()
        };

        if removed {
            shared.store.execute(StorageOp::DeleteDLQ(message_id));
        }

        Ok(removed)
    }

    /// Purge all messages from DLQ
    pub async fn purge_dlq(&self, queue_name: &str) -> Result<usize, String> {
        let shared = self.get_queue(queue_name)
            .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;

        let count = {
            let mut inner = Self::lock(&shared.inner);
            let count = inner.dlq.len();
            inner.dlq.clear();
            count
        };

        shared.store.execute(StorageOp::PurgeDLQ);
        Ok(count)
    }
}


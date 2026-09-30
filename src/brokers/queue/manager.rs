//! QueueManager: the public, transport-agnostic API of the queue engine.
//!
//! Structure: submitters enqueue `QueueRequest`s on a bounded channel with a
//! byte-budget admission gate; a dedicated writer thread (`worker.rs`) owns
//! the shared SQLite database and replies over oneshot channels after commit.
//! Runtime memory holds only watches (long-poll wakeups), the closed flag and
//! the expiry timer — all durable state lives in SQLite.
//!
//! Key flow: `submit()` validates, charges admission, sends the command, and
//! returns a `PendingReply`; convenience methods (`push`, `consume_batch`,
//! ...) build requests on top of the same path. `consume_batch` additionally
//! implements the long-poll loop: an empty consume parks on the queue's watch
//! and is woken by post-commit effects.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::brokers::queue::config::SystemQueueConfig;
use crate::brokers::queue::domain::definition::{QueueConfig, QueueDefinition};
use crate::brokers::queue::domain::message::{DlqMessage, Message, PushItem};
use crate::brokers::queue::domain::ops::{Command, QueueDomain, QueueReply, QueueRequest};
use crate::brokers::queue::domain::storage::{self, SPEC};
use crate::brokers::queue::options::QueueCreateOptions;
use crate::brokers::{
    validate_resource_name, BrokerError, BrokerErrorKind, ProvisionResult,
};
use crate::durable::{now_millis, EngineHandle, Store};
use crate::protocol::QUEUE_MAX_PUSH_ITEMS;

/// Visibility-expiry sweep cadence. Deadlines are evaluated against durable
/// `visible_at` values inside the writer transaction, so this only bounds
/// redelivery latency.
const EXPIRY_SWEEP_MS: u64 = 50;

/// Charged admission weight of one pushed item: the payload plus its
/// storage-row overhead (id, counters, timestamps).
const PUSH_ITEM_OVERHEAD: usize = 64;

/// Completion handle for a submitted command.
pub type PendingReply = crate::durable::PendingReply<QueueDomain>;

pub struct QueueManager {
    engine: EngineHandle<QueueDomain>,
    config: Arc<SystemQueueConfig>,
    cancel: CancellationToken,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl QueueManager {
    /// Open the shared database (fail-closed on any layout/schema error),
    /// recover, and start the writer + expiry timer.
    pub async fn new(config: Arc<SystemQueueConfig>) -> Result<Self, BrokerError> {
        let root = PathBuf::from(&config.persistence_path);
        let (store, continuations) = tokio::task::spawn_blocking(move || {
            let mut store = Store::open(&root, SPEC)?;
            let conts = storage::recover(&mut store)?;
            Ok::<_, BrokerError>((store, conts))
        })
        .await
        .map_err(|e| BrokerError::storage(format!("Queue storage startup failed: {e}")))??;

        let (cmd_tx, rx) = mpsc::channel(config.storage_queue_capacity.max(1));
        let engine =
            EngineHandle::<QueueDomain>::new("Queue", cmd_tx, config.storage_queue_max_bytes);
        let handle =
            crate::durable::spawn("queue", QueueDomain, store, rx, continuations, &engine);

        let manager = Self {
            engine,
            config,
            cancel: CancellationToken::new(),
            worker: Mutex::new(Some(handle)),
        };
        manager.spawn_timer();
        Ok(manager)
    }

    /// Diagnostics for benchmarks: `(sql statements, writer batches, exec ns,
    /// commit ns)` for this engine. Snapshot and diff around a workload.
    #[doc(hidden)]
    pub fn sql_stats(&self) -> (u64, u64, u64, u64) {
        let stats = self.engine.stats();
        (
            stats.sql_stmts.load(Ordering::Relaxed),
            stats.batches.load(Ordering::Relaxed),
            stats.exec_ns.load(Ordering::Relaxed),
            stats.commit_ns.load(Ordering::Relaxed),
        )
    }

    fn spawn_timer(&self) {
        let tx = self.engine.sender();
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(EXPIRY_SWEEP_MS));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        // A full channel means the writer is saturated: skip
                        // this tick instead of queueing maintenance behind
                        // client work — expiry re-runs next tick.
                        let _ = tx.try_send(Command {
                            op: QueueRequest::ExpireLeases { now_ms: now_millis() },
                            bytes: 0,
                            reply: None,
                        });
                    }
                    _ = cancel.cancelled() => break,
                }
            }
        });
    }

    // ------------------------------------------------------------------
    // Submission path
    // ------------------------------------------------------------------

    /// Validate + admit + enqueue. Returns once the command is queued;
    /// the reply arrives on `PendingReply` after the commit.
    pub async fn submit(&self, request: QueueRequest) -> Result<PendingReply, BrokerError> {
        if self.engine.is_closed() {
            return Err(BrokerError::storage("Queue storage is shut down"));
        }
        Self::validate(&request)?;
        let bytes = Self::admission_bytes(&request);
        self.engine.submit(request, bytes).await
    }

    /// Pre-queue validation: cheap checks that must fail before the command
    /// consumes channel space.
    fn validate(request: &QueueRequest) -> Result<(), BrokerError> {
        let name = match request {
            QueueRequest::CreateQueue { name, .. }
            | QueueRequest::DeleteQueue { name }
            | QueueRequest::QueueExists { name }
            | QueueRequest::DescribeQueue { name }
            | QueueRequest::Consume { name, .. }
            | QueueRequest::Ack { name, .. }
            | QueueRequest::Nack { name, .. }
            | QueueRequest::PeekDlq { name, .. }
            | QueueRequest::MoveToQueue { name, .. }
            | QueueRequest::DeleteDlq { name, .. }
            | QueueRequest::PurgeDlq { name } => name,
            QueueRequest::Push { name, items } => {
                validate_resource_name("queue", name)?;
                if items.len() > QUEUE_MAX_PUSH_ITEMS {
                    return Err(BrokerError::invalid_argument(format!(
                        "Push batch too large: {} items (max: {})",
                        items.len(),
                        QUEUE_MAX_PUSH_ITEMS
                    )));
                }
                return Ok(());
            }
            _ => return Ok(()),
        };
        validate_resource_name("queue", name)
    }

    fn admission_bytes(request: &QueueRequest) -> usize {
        match request {
            QueueRequest::Push { items, .. } => items
                .iter()
                .map(|i| PUSH_ITEM_OVERHEAD + i.payload.len())
                .sum(),
            _ => 0,
        }
    }

    fn unexpected(reply: QueueReply) -> BrokerError {
        BrokerError::new(
            BrokerErrorKind::Internal,
            format!("Unexpected queue reply variant: {reply:?}"),
        )
    }

    fn queue_watcher(&self, name: &str) -> watch::Receiver<u64> {
        self.engine
            .watches()
            .entry(name.to_string())
            .or_insert_with(|| watch::channel(0u64).0)
            .subscribe()
    }

    // ------------------------------------------------------------------
    // Public API — thin wrappers over submit()
    // ------------------------------------------------------------------

    pub async fn create_queue(
        &self,
        name: String,
        options: QueueCreateOptions,
    ) -> Result<ProvisionResult<QueueDefinition>, BrokerError> {
        validate_resource_name("queue", &name)?;
        let requested = QueueConfig::from_options(options, &self.config);
        let reply = self
            .submit(QueueRequest::CreateQueue { name, requested })
            .await?
            .wait()
            .await?;
        match reply {
            QueueReply::Provision(result) => Ok(*result),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Idempotent: deleting a missing queue succeeds.
    pub async fn delete_queue(&self, name: String) -> Result<(), BrokerError> {
        match self
            .submit(QueueRequest::DeleteQueue { name })
            .await?
            .wait()
            .await?
        {
            QueueReply::Unit => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Invalid names report `false` (the resource cannot exist); storage
    /// failures propagate.
    pub async fn exists(&self, name: &str) -> Result<bool, BrokerError> {
        if validate_resource_name("queue", name).is_err() {
            return Ok(false);
        }
        match self
            .submit(QueueRequest::QueueExists {
                name: name.to_string(),
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::Bool(found) => Ok(found),
            other => Err(Self::unexpected(other)),
        }
    }

    pub async fn describe(&self, name: &str) -> Result<QueueDefinition, BrokerError> {
        match self
            .submit(QueueRequest::DescribeQueue {
                name: name.to_string(),
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::Definition(def) => Ok(*def),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Push one message.
    pub async fn push(
        &self,
        queue_name: String,
        payload: Bytes,
        priority: u8,
    ) -> Result<(), BrokerError> {
        self.push_batch(queue_name, vec![(payload, priority)]).await
    }

    /// Atomic batch push: the batch commits or fails as a unit.
    pub async fn push_batch(
        &self,
        queue_name: String,
        items: Vec<(Bytes, u8)>,
    ) -> Result<(), BrokerError> {
        if items.is_empty() {
            return Ok(());
        }
        let items = items
            .into_iter()
            .map(|(payload, priority)| PushItem { payload, priority })
            .collect();
        match self
            .submit(QueueRequest::Push {
                name: queue_name,
                items,
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::Unit => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Single non-blocking consume. Missing queues report `None` (the
    /// resource cannot hold messages), storage failures propagate.
    pub async fn pop(&self, queue_name: &str) -> Result<Option<Message>, BrokerError> {
        match self
            .consume_batch(queue_name.to_string(), Some(1), Some(0))
            .await
        {
            Ok(mut msgs) => Ok(msgs.pop()),
            Err(e) if e.kind == BrokerErrorKind::ResourceNotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Consume up to `max` messages, long-polling up to `wait_ms` when the
    /// queue is empty. An empty page parks on the queue's watch until a
    /// post-commit wakeup or the deadline elapses.
    pub async fn consume_batch(
        &self,
        queue_name: String,
        max: Option<usize>,
        wait_ms: Option<u64>,
    ) -> Result<Vec<Message>, BrokerError> {
        let max_val = max.unwrap_or(self.config.default_batch_size);
        let wait_val = wait_ms.unwrap_or(self.config.default_wait_ms);
        if max_val == 0 {
            return Err(BrokerError::invalid_argument("batch_size must be >= 1"));
        }
        let deadline = (wait_val > 0).then(|| Instant::now() + Duration::from_millis(wait_val));
        loop {
            // Subscribe BEFORE submitting so a commit landing between the two
            // is observed as a version bump instead of a missed wakeup.
            let mut watcher = self.queue_watcher(&queue_name);
            let version = *watcher.borrow();
            let reply = self
                .submit(QueueRequest::Consume {
                    name: queue_name.clone(),
                    limit: max_val,
                })
                .await?
                .wait()
                .await?;
            match reply {
                QueueReply::Consumed(messages) if !messages.is_empty() => {
                    return Ok(messages)
                }
                QueueReply::Consumed(_) => {}
                other => return Err(Self::unexpected(other)),
            }
            let Some(deadline) = deadline else {
                return Ok(Vec::new());
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(Vec::new());
            }
            if *watcher.borrow() != version {
                continue; // work arrived between subscribe and submit
            }
            tokio::select! {
                // Sender dropped (queue deleted) also resolves the wait:
                // the resubmission then fails NOT_FOUND.
                _ = watcher.changed() => continue,
                _ = tokio::time::sleep(remaining) => return Ok(Vec::new()),
            }
        }
    }

    /// Complete the delivery identified by `delivery_token`. Stale tokens and
    /// expired leases report `false`.
    pub async fn ack(
        &self,
        queue_name: &str,
        id: Uuid,
        delivery_token: u64,
    ) -> Result<bool, BrokerError> {
        match self
            .submit(QueueRequest::Ack {
                name: queue_name.to_string(),
                id,
                delivery_token,
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::Bool(done) => Ok(done),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Requeue (or dead-letter at `max_deliveries`) the leased delivery.
    pub async fn nack(
        &self,
        queue_name: &str,
        id: Uuid,
        delivery_token: u64,
        reason: String,
    ) -> Result<bool, BrokerError> {
        match self
            .submit(QueueRequest::Nack {
                name: queue_name.to_string(),
                id,
                delivery_token,
                reason,
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::Bool(done) => Ok(done),
            other => Err(Self::unexpected(other)),
        }
    }

    // --- DLQ Operations ---

    /// Peek DLQ entries (most recent failure first) without consuming them.
    pub async fn peek_dlq(
        &self,
        queue_name: &str,
        limit: usize,
        offset: usize,
    ) -> Result<(usize, Vec<DlqMessage>), BrokerError> {
        match self
            .submit(QueueRequest::PeekDlq {
                name: queue_name.to_string(),
                limit,
                offset,
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::DlqPage(total, items) => Ok((total, items)),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Replay a DLQ entry back to the live queue (fresh delivery count).
    pub async fn move_to_queue(
        &self,
        queue_name: &str,
        message_id: Uuid,
    ) -> Result<bool, BrokerError> {
        match self
            .submit(QueueRequest::MoveToQueue {
                name: queue_name.to_string(),
                id: message_id,
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::Bool(done) => Ok(done),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Delete a specific entry from the DLQ.
    pub async fn delete_dlq(
        &self,
        queue_name: &str,
        message_id: Uuid,
    ) -> Result<bool, BrokerError> {
        match self
            .submit(QueueRequest::DeleteDlq {
                name: queue_name.to_string(),
                id: message_id,
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::Bool(done) => Ok(done),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Remove every DLQ entry. Returns the number removed.
    pub async fn purge_dlq(&self, queue_name: &str) -> Result<usize, BrokerError> {
        match self
            .submit(QueueRequest::PurgeDlq {
                name: queue_name.to_string(),
            })
            .await?
            .wait()
            .await?
        {
            QueueReply::Count(n) => Ok(n),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Stop the timer, drain admitted work, checkpoint the WAL and join the
    /// writer thread. `closed` is set first so no new work is admitted while
    /// the barrier command is in flight; the shutdown itself bypasses
    /// `submit` (which rejects once closed).
    pub async fn shutdown(&self) {
        if self.engine.mark_closed() {
            return;
        }
        self.cancel.cancel();
        let (tx, rx) = oneshot::channel();
        if self
            .engine
            .sender()
            .send(Command {
                op: QueueRequest::Shutdown,
                bytes: 0,
                reply: Some(tx),
            })
            .await
            .is_ok()
        {
            let _ = rx.await;
        }
        let handle = self.worker.lock().ok().and_then(|mut h| h.take());
        if let Some(handle) = handle {
            let _ = tokio::task::spawn_blocking(move || handle.join()).await;
        }
    }
}

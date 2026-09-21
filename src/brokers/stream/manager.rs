//! StreamManager: the public, transport-agnostic API of the stream engine.
//!
//! Structure: submitters enqueue `StreamRequest`s on a bounded channel with a
//! byte-budget admission gate; a dedicated writer thread (`worker.rs`) owns
//! the shared SQLite database and replies over oneshot channels after commit.
//! Runtime memory holds only watches (long-poll wakeups), the closed flag and
//! timers — all durable state lives in SQLite.
//!
//! Key flow: `submit()` validates, charges admission, sends the command, and
//! returns a `PendingReply`; convenience methods (`publish`, `fetch`, ...)
//! build requests on top of the same path. `fetch` additionally implements
//! the long-poll loop: an empty page parks on a per-group watch and is woken
//! by post-commit effects.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::brokers::stream::config::SystemStreamConfig;
use crate::brokers::stream::domain::definition::{StreamConfig, StreamDefinition};
use crate::brokers::stream::domain::message::{
    ConsumerIdentity, Delivery, DlsEntry, Message, PubItem,
};
use crate::brokers::stream::domain::ops::{Command, StreamReply, StreamRequest};
use crate::brokers::stream::domain::storage::Store;
use crate::brokers::stream::domain::types::{event_logical_bytes, fetch_item_encoded_bytes};
use crate::brokers::stream::options::{SeekTarget, StreamCreateOptions};
use crate::brokers::stream::worker::{self, now_millis, Admission, WatchMap};
use crate::brokers::{
    validate_resource_name, BrokerError, BrokerErrorKind, ProvisionResult,
};
use crate::protocol::{STREAM_MAX_KEY_BYTES, STREAM_MAX_PUBLISH_BATCH};

/// Absolute per-record storage bound (defense in depth; the effective fetch
/// budget is usually much tighter).
const MAX_STREAM_RECORD_BYTES: u64 = 64 * 1024 * 1024;
/// Lease-expiry sweep cadence. Deadlines are evaluated against durable
/// `deadline_ms` values inside the writer transaction, so this only bounds
/// redelivery latency.
const LEASE_SWEEP_MS: u64 = 50;

#[derive(Debug)]
pub struct JoinGroupResult {
    pub ack_floor: u64,
    pub consumer_id: String,
    pub generation: u64,
}

/// Completion handle for a submitted command.
pub struct PendingReply {
    rx: oneshot::Receiver<Result<StreamReply, BrokerError>>,
}

impl PendingReply {
    /// Wrap a completion produced outside the submit path (the fetch
    /// long-poll loop resolves through the same reply channel).
    pub(crate) fn wrap(rx: oneshot::Receiver<Result<StreamReply, BrokerError>>) -> Self {
        Self { rx }
    }
}

impl PendingReply {
    pub async fn wait(self) -> Result<StreamReply, BrokerError> {
        self.rx
            .await
            .map_err(|_| BrokerError::storage("Stream storage unavailable"))?
    }
}

pub struct StreamManager {
    cmd_tx: mpsc::Sender<Command>,
    admission: Arc<Admission>,
    watches: Arc<WatchMap>,
    config: Arc<SystemStreamConfig>,
    cancel: CancellationToken,
    closed: AtomicBool,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl StreamManager {
    /// Open the shared database (fail-closed on any layout/schema error),
    /// recover, and start the writer + maintenance timers.
    pub async fn new(config: Arc<SystemStreamConfig>) -> Result<Self, BrokerError> {
        let root = PathBuf::from(&config.persistence_path);
        let (store, continuations) = tokio::task::spawn_blocking(move || Store::open(&root))
            .await
            .map_err(|e| BrokerError::storage(format!("Stream storage startup failed: {e}")))??;

        let (cmd_tx, rx) = mpsc::channel(config.storage_queue_capacity.max(1));
        let admission = Arc::new(Admission::new(config.storage_queue_max_bytes));
        let watches = Arc::new(WatchMap::new());
        let handle = worker::spawn(
            store,
            rx,
            continuations,
            Arc::clone(&watches),
            Arc::clone(&admission),
            config.fetch_response_bytes as u64,
        );

        let manager = Self {
            cmd_tx,
            admission,
            watches,
            config,
            cancel: CancellationToken::new(),
            closed: AtomicBool::new(false),
            worker: Mutex::new(Some(handle)),
        };
        manager.spawn_timers();
        Ok(manager)
    }

    pub fn config(&self) -> &SystemStreamConfig {
        &self.config
    }

    fn spawn_timers(&self) {
        let tx = self.cmd_tx.clone();
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(LEASE_SWEEP_MS));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let _ = tx.try_send(Command {
                            op: StreamRequest::ExpireLeases { now_ms: now_millis() },
                            bytes: 0,
                            reply: None,
                        });
                    }
                    _ = cancel.cancelled() => break,
                }
            }
        });
        let tx = self.cmd_tx.clone();
        let cancel = self.cancel.clone();
        let interval_ms = self.config.retention_check_interval_ms.max(100);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let _ = tx.try_send(Command {
                            op: StreamRequest::RetentionTick,
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
    pub async fn submit(&self, request: StreamRequest) -> Result<PendingReply, BrokerError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(BrokerError::storage("Stream storage is shut down"));
        }
        self.validate(&request)?;
        let bytes = Self::admission_bytes(&request);
        self.admission.acquire(bytes).await;
        let (tx, rx) = oneshot::channel();
        match self
            .cmd_tx
            .send(Command {
                op: request,
                bytes,
                reply: Some(tx),
            })
            .await
        {
            Ok(()) => Ok(PendingReply { rx }),
            Err(_) => {
                self.admission.release(bytes);
                Err(BrokerError::storage("Stream storage unavailable"))
            }
        }
    }

    /// Pre-queue validation: cheap checks that must fail before the command
    /// consumes queue space (names, publish item limits, fetch encodability).
    fn validate(&self, request: &StreamRequest) -> Result<(), BrokerError> {
        match request {
            StreamRequest::CreateStream { name, .. }
            | StreamRequest::DeleteStream { name }
            | StreamRequest::StreamExists { name }
            | StreamRequest::DescribeStream { name }
            | StreamRequest::Read { name, .. } => validate_resource_name("stream", name),
            StreamRequest::Publish { name, items } => {
                validate_resource_name("stream", name)?;
                if items.len() > STREAM_MAX_PUBLISH_BATCH {
                    return Err(BrokerError::invalid_argument(format!(
                        "Publish batch too large: {} items (max: {})",
                        items.len(),
                        STREAM_MAX_PUBLISH_BATCH
                    )));
                }
                for item in items {
                    if item.key.len() > STREAM_MAX_KEY_BYTES {
                        return Err(BrokerError::invalid_argument(format!(
                            "Stream key exceeds {} bytes",
                            STREAM_MAX_KEY_BYTES
                        )));
                    }
                    let record = event_logical_bytes(item.key.len(), item.payload.len());
                    if record > MAX_STREAM_RECORD_BYTES {
                        return Err(BrokerError::invalid_argument(format!(
                            "Stream record too large: {} bytes (max: {})",
                            record, MAX_STREAM_RECORD_BYTES
                        )));
                    }
                    let encoded = 4 + fetch_item_encoded_bytes(item.key.len(), item.payload.len());
                    if encoded > self.config.fetch_response_bytes as u64 {
                        return Err(BrokerError::invalid_argument(format!(
                            "Stream record can never be fetched: {} bytes exceeds the fetch \
                             response budget of {} bytes",
                            encoded, self.config.fetch_response_bytes
                        )));
                    }
                }
                Ok(())
            }
            StreamRequest::Join { name, .. }
            | StreamRequest::Fetch { name, .. }
            | StreamRequest::Ack { name, .. }
            | StreamRequest::Leave { name, .. }
            | StreamRequest::Seek { name, .. }
            | StreamRequest::PeekDls { name, .. }
            | StreamRequest::ReplayDls { name, .. }
            | StreamRequest::DeleteDls { name, .. }
            | StreamRequest::PurgeDls { name, .. } => validate_resource_name("stream", name),
            _ => Ok(()),
        }
    }

    fn admission_bytes(request: &StreamRequest) -> usize {
        match request {
            StreamRequest::Publish { items, .. } => items
                .iter()
                .map(|i| 38usize + i.key.len() + i.payload.len())
                .sum(),
            _ => 0,
        }
    }

    fn unexpected(reply: StreamReply) -> BrokerError {
        BrokerError::new(
            BrokerErrorKind::Internal,
            format!("Unexpected stream reply variant: {reply:?}"),
        )
    }

    fn group_watcher(&self, stream: &str, group: &str) -> watch::Receiver<u64> {
        self.watches
            .entry(stream.to_string())
            .or_default()
            .entry(group.to_string())
            .or_insert_with(|| watch::channel(0u64).0)
            .subscribe()
    }

    // ------------------------------------------------------------------
    // Public API — thin wrappers over submit()
    // ------------------------------------------------------------------

    pub async fn create_stream(
        &self,
        name: String,
        options: StreamCreateOptions,
    ) -> Result<ProvisionResult<StreamDefinition>, BrokerError> {
        let requested = StreamConfig::from_options(options, &self.config);
        let reply = self
            .submit(StreamRequest::CreateStream { name, requested })
            .await?
            .wait()
            .await?;
        match reply {
            StreamReply::Provision(result) => Ok(*result),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Idempotent: deleting a missing stream succeeds.
    pub async fn delete_stream(&self, name: String) -> Result<(), BrokerError> {
        match self
            .submit(StreamRequest::DeleteStream { name })
            .await?
            .wait()
            .await?
        {
            StreamReply::Unit => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Invalid names report `false` (the resource cannot exist); storage
    /// failures propagate.
    pub async fn exists(&self, name: &str) -> Result<bool, BrokerError> {
        if validate_resource_name("stream", name).is_err() {
            return Ok(false);
        }
        match self
            .submit(StreamRequest::StreamExists {
                name: name.to_string(),
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Bool(found) => Ok(found),
            other => Err(Self::unexpected(other)),
        }
    }

    pub async fn describe(&self, name: &str) -> Result<StreamDefinition, BrokerError> {
        match self
            .submit(StreamRequest::DescribeStream {
                name: name.to_string(),
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Definition(def) => Ok(*def),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Publish one record. `key` empty means keyless.
    pub async fn publish(
        &self,
        name: &str,
        key: Bytes,
        payload: Bytes,
    ) -> Result<u64, BrokerError> {
        let seqs = self
            .publish_batch(name, vec![PubItem { key, payload }])
            .await?;
        seqs.first()
            .copied()
            .ok_or_else(|| BrokerError::storage("Publish returned no sequence"))
    }

    /// Atomic batch publish: sequences are contiguous and the batch commits
    /// or fails as a unit.
    pub async fn publish_batch(
        &self,
        name: &str,
        items: Vec<PubItem>,
    ) -> Result<Vec<u64>, BrokerError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        match self
            .submit(StreamRequest::Publish {
                name: name.to_string(),
                items,
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Published(seqs) => Ok(seqs),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Read stored messages without delivery semantics (no leases, no
    /// receipts). `from_seq` is inclusive.
    pub async fn read(
        &self,
        name: &str,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<Message>, BrokerError> {
        match self
            .submit(StreamRequest::Read {
                name: name.to_string(),
                from_seq,
                limit,
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Read(messages) => Ok(messages),
            other => Err(Self::unexpected(other)),
        }
    }

    pub async fn join_group(
        &self,
        name: &str,
        group: &str,
        connection_id: &str,
    ) -> Result<JoinGroupResult, BrokerError> {
        match self
            .submit(StreamRequest::Join {
                name: name.to_string(),
                group: group.to_string(),
                connection_id: connection_id.to_string(),
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Join {
                ack_floor,
                consumer_id,
                generation,
            } => Ok(JoinGroupResult {
                ack_floor,
                consumer_id,
                generation,
            }),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Fetch with long-poll: an empty page parks on the group's watch until a
    /// post-commit wakeup or `wait_ms` elapses. Membership/fencing errors
    /// propagate (the SDK rejoins).
    pub async fn fetch(
        &self,
        name: &str,
        group: &str,
        identity: &ConsumerIdentity,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<Delivery>, BrokerError> {
        let deadline = (wait_ms > 0).then(|| Instant::now() + Duration::from_millis(wait_ms));
        loop {
            // Subscribe BEFORE submitting so a commit landing between the two
            // is observed as a version bump instead of a missed wakeup.
            let mut watcher = self.group_watcher(name, group);
            let version = *watcher.borrow();
            let reply = self
                .submit(StreamRequest::Fetch {
                    name: name.to_string(),
                    group: group.to_string(),
                    identity: identity.clone(),
                    limit,
                })
                .await?
                .wait()
                .await?;
            match reply {
                StreamReply::Fetch(deliveries) if !deliveries.is_empty() => {
                    return Ok(deliveries)
                }
                StreamReply::Fetch(_) => {}
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
                // Sender dropped (stream deleted) also resolves the wait:
                // the resubmission then fails NOT_FOUND.
                _ = watcher.changed() => continue,
                _ = tokio::time::sleep(remaining) => return Ok(Vec::new()),
            }
        }
    }

    /// Consume the lease identified by `receipt`. Stale receipts/epochs and
    /// unknown sequences are FENCED; unknown members are NOT_MEMBER.
    pub async fn ack(
        &self,
        name: &str,
        group: &str,
        identity: &ConsumerIdentity,
        seq: u64,
        receipt: [u8; 16],
    ) -> Result<(), BrokerError> {
        match self
            .submit(StreamRequest::Ack {
                name: name.to_string(),
                group: group.to_string(),
                identity: identity.clone(),
                seq,
                receipt,
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Unit => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    pub async fn leave_group(
        &self,
        name: &str,
        group: &str,
        identity: &ConsumerIdentity,
    ) -> Result<(), BrokerError> {
        match self
            .submit(StreamRequest::Leave {
                name: name.to_string(),
                group: group.to_string(),
                identity: identity.clone(),
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Unit => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    pub async fn seek(
        &self,
        name: &str,
        group: &str,
        target: SeekTarget,
    ) -> Result<(), BrokerError> {
        match self
            .submit(StreamRequest::Seek {
                name: name.to_string(),
                group: group.to_string(),
                target,
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Unit => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    pub async fn peek_dls(
        &self,
        name: &str,
        group: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DlsEntry>, BrokerError> {
        match self
            .submit(StreamRequest::PeekDls {
                name: name.to_string(),
                group: group.to_string(),
                limit,
                offset,
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::DlsEntries(entries) => Ok(entries),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Move a parked entry back to the live stream of deliveries for `group`.
    pub async fn move_to_stream(
        &self,
        name: &str,
        group: &str,
        seq: u64,
    ) -> Result<(), BrokerError> {
        match self
            .submit(StreamRequest::ReplayDls {
                name: name.to_string(),
                group: group.to_string(),
                seq,
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Unit => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    pub async fn delete_dls(
        &self,
        name: &str,
        group: &str,
        seq: u64,
    ) -> Result<(), BrokerError> {
        match self
            .submit(StreamRequest::DeleteDls {
                name: name.to_string(),
                group: group.to_string(),
                seq,
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Unit => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Remove every parked membership for the group. Returns the number of
    /// retained parked entries that were resolved.
    pub async fn purge_dls(&self, name: &str, group: &str) -> Result<usize, BrokerError> {
        match self
            .submit(StreamRequest::PurgeDls {
                name: name.to_string(),
                group: group.to_string(),
            })
            .await?
            .wait()
            .await?
        {
            StreamReply::Count(n) => Ok(n),
            other => Err(Self::unexpected(other)),
        }
    }

    /// Release all leases/memberships owned by a connection (teardown path).
    pub async fn disconnect(&self, connection_id: &str) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        if let Ok(pending) = self
            .submit(StreamRequest::Disconnect {
                connection_id: connection_id.to_string(),
            })
            .await
        {
            if let Err(e) = pending.wait().await {
                warn!("Stream disconnect cleanup failed for {connection_id}: {e}");
            }
        }
    }

    /// Stop timers, drain admitted work, checkpoint the WAL and join the
    /// writer thread. `closed` is set first so no new work is admitted while
    /// the barrier command is in flight; the shutdown itself bypasses
    /// `submit` (which rejects once closed).
    pub async fn shutdown(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.cancel.cancel();
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command {
                op: StreamRequest::Shutdown,
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

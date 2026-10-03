//! Command/reply contract between `StreamManager` (async submitters) and the
//! shared durable engine (`crate::durable`). Everything here is
//! transport-agnostic.

use crate::brokers::stream::domain::definition::{StreamConfig, StreamDefinition};
use crate::brokers::stream::domain::message::{
    BatchUpdate, ConsumerIdentity, DlsEntry, FetchLimits, FetchPage, Message, PubItem,
};
use crate::brokers::stream::options::SeekTarget;
use crate::brokers::ProvisionResult;

/// Engine marker type carrying the domain's runtime config (the fetch
/// response byte budget, baked into every batch `ExecCtx`). The `Domain`
/// impl lives in `worker.rs`.
pub struct StreamDomain(pub u64);

/// A submitted command plus its completion channel and admission weight.
pub type Command = crate::durable::Command<StreamDomain>;

/// One durable command executed inside the writer transaction.
#[derive(Debug)]
pub enum StreamRequest {
    CreateStream {
        name: String,
        /// Concrete effective config: the manager resolves system defaults
        /// from `StreamCreateOptions` before submitting so recipes stay free
        /// of runtime-config dependencies.
        requested: StreamConfig,
    },
    DeleteStream {
        name: String,
    },
    StreamExists {
        name: String,
    },
    DescribeStream {
        name: String,
    },
    Publish {
        name: String,
        items: Vec<PubItem>,
    },
    Read {
        name: String,
        from_seq: u64,
        limit: usize,
    },
    Join {
        name: String,
        group: String,
        connection_id: String,
    },
    Fetch {
        name: String,
        group: String,
        identity: ConsumerIdentity,
        limits: FetchLimits,
    },
    UpdateBatch {
        name: String,
        group: String,
        identity: ConsumerIdentity,
        update: BatchUpdate,
    },
    /// Wire-level bulk update (S_UPDATE_BATCHES): N cumulative batch updates
    /// from one consumer. Per-item failures come back in
    /// `StreamReply::BatchOutcome`.
    UpdateBatches {
        name: String,
        group: String,
        identity: ConsumerIdentity,
        updates: Vec<BatchUpdate>,
    },
    Leave {
        name: String,
        group: String,
        identity: ConsumerIdentity,
    },
    /// Release every lease owned by this connection (session teardown).
    Disconnect {
        connection_id: String,
    },
    Seek {
        name: String,
        group: String,
        target: SeekTarget,
    },
    PeekDls {
        name: String,
        group: String,
        limit: usize,
        offset: usize,
    },
    /// Remove a parked member and re-queue it as a delivery obligation.
    ReplayDls {
        name: String,
        group: String,
        seq: u64,
    },
    /// Drop a parked member without redelivery.
    DeleteDls {
        name: String,
        group: String,
        seq: u64,
    },
    PurgeDls {
        name: String,
        group: String,
    },
    /// Maintenance commands sent by the runtime timers (not exposed on TCP).
    ExpireLeases {
        now_ms: u64,
    },
    RetentionTick,
    /// Flush remaining work, checkpoint WAL, then terminate the writer.
    Shutdown,
}

/// Reply payload per command. Variants are intentionally sparse: the adapter
/// knows which opcode produced the call and encodes accordingly.
#[derive(Debug)]
pub enum StreamReply {
    Unit,
    Bool(bool),
    Provision(Box<ProvisionResult<StreamDefinition>>),
    Definition(Box<StreamDefinition>),
    Published(Vec<u64>),
    Read(Vec<Message>),
    Fetch(FetchPage),
    Join {
        ack_floor: u64,
        consumer_id: String,
        epoch: [u8; 16],
    },
    DlsEntries(Vec<DlsEntry>),
    Count(usize),
    /// Per-item failures of an UpdateBatches run, as `(receipt, kind)` —
    /// empty means every update was consumed.
    BatchOutcome(Vec<([u8; 16], crate::brokers::BrokerErrorKind)>),
}

/// Side effects staged inside a transaction and applied only after commit:
/// long-poll wakeups, deleted-stream notifications, and follow-up
/// continuations for paged background work.
#[derive(Debug, Default)]
pub struct Effects {
    /// `(stream, group)` pairs whose waiting fetches should re-poll.
    pub wakes: Vec<(String, String)>,
    /// Streams deleted in this commit: every group watcher must be closed.
    pub deleted_streams: Vec<String>,
    /// Continuations to enqueue after commit (init pages, GC, checkpoints).
    pub followups: Vec<Continuation>,
}

impl Effects {
    pub fn wake(&mut self, stream: &str, group: &str) {
        let pair = (stream.to_string(), group.to_string());
        if !self.wakes.contains(&pair) {
            self.wakes.push(pair);
        }
    }
}

/// Bounded background work slices. Each item is one transaction on the
/// writer; unfinished work re-enqueues itself until done.
#[derive(Debug)]
pub enum Continuation {
    /// Page the next `key_id` range of a freshly created epoch's lanes.
    InitEpoch { epoch_id: [u8; 16] },
    /// Continue the lease-expiry sweep after a batch exhausted its budget.
    ExpireLeases,
    /// Continue the retention pass after a batch exhausted its budget.
    Retention,
    /// Bounded deletion of dead rows (deleted streams, superseded epochs).
    Gc,
    PruneKeys { after: Option<([u8; 16], i64)> },
}

//! Command/reply contract between `QueueManager` (async submitters) and the
//! shared durable engine (`crate::durable`). Everything here is
//! transport-agnostic.

use uuid::Uuid;

use crate::brokers::queue::domain::definition::{QueueConfig, QueueDefinition};
use crate::brokers::queue::domain::message::{DlqMessage, Message, PushItem};
use crate::brokers::ProvisionResult;

/// Engine marker type: the `Domain` impl lives in `worker.rs`. Declared here
/// so domain types can parameterize engine generics without importing the
/// infrastructure layer.
pub struct QueueDomain;

/// A submitted command plus its completion channel and admission weight.
pub type Command = crate::durable::Command<QueueDomain>;

/// One durable command executed inside the writer transaction.
#[derive(Debug)]
pub enum QueueRequest {
    CreateQueue {
        name: String,
        /// Concrete effective config: the manager resolves system defaults
        /// before submitting so recipes stay free of runtime-config deps.
        requested: QueueConfig,
    },
    DeleteQueue {
        name: String,
    },
    QueueExists {
        name: String,
    },
    DescribeQueue {
        name: String,
    },
    Push {
        name: String,
        items: Vec<PushItem>,
    },
    /// Lease up to `limit` ready messages (assigns visible_at/token/attempts).
    Consume {
        name: String,
        limit: usize,
    },
    Ack {
        name: String,
        id: Uuid,
        delivery_token: u64,
    },
    Nack {
        name: String,
        id: Uuid,
        delivery_token: u64,
        reason: String,
    },
    PeekDlq {
        name: String,
        limit: usize,
        offset: usize,
    },
    /// Move a DLQ entry back to the live queue (replay).
    MoveToQueue {
        name: String,
        id: Uuid,
    },
    DeleteDlq {
        name: String,
        id: Uuid,
    },
    PurgeDlq {
        name: String,
    },
    /// Maintenance command sent by the runtime timer (not exposed on TCP):
    /// requeue expired leases / move exhausted deliveries to the DLQ.
    ExpireLeases {
        now_ms: u64,
    },
    /// Flush remaining work, checkpoint WAL, then terminate the writer.
    Shutdown,
}

/// Reply payload per command. Variants are intentionally sparse: the adapter
/// knows which opcode produced the call and encodes accordingly.
#[derive(Debug)]
pub enum QueueReply {
    Unit,
    Bool(bool),
    Provision(Box<ProvisionResult<QueueDefinition>>),
    Definition(Box<QueueDefinition>),
    Consumed(Vec<Message>),
    /// (total_dlq_count, page)
    DlqPage(usize, Vec<DlqMessage>),
    Count(usize),
}

/// Side effects staged inside a transaction and applied only after commit:
/// long-poll wakeups, deleted-queue invalidation, follow-up continuations.
#[derive(Debug, Default)]
pub struct Effects {
    /// Queue names whose waiting consumers should re-poll.
    pub wakes: Vec<String>,
    /// Queues deleted in this commit: their watchers must be closed.
    pub deleted_queues: Vec<String>,
    /// Continuations to enqueue after commit (expiry slices, GC).
    pub followups: Vec<Continuation>,
}

impl Effects {
    pub fn wake(&mut self, queue: &str) {
        let name = queue.to_string();
        if !self.wakes.contains(&name) {
            self.wakes.push(name);
        }
    }
}

/// Bounded background work slices. Each item is one transaction on the
/// writer; unfinished work re-enqueues itself until done.
#[derive(Debug)]
pub enum Continuation {
    /// Continue the lease-expiry sweep after a batch exhausted its budget.
    Expire,
    /// Continue the GC of deleted queues after a slice exhausted its budget.
    Gc,
}

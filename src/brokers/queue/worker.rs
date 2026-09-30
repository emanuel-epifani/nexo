//! Dedicated SQLite writer thread: the single owner of `Store`.
//!
//! Commands arrive on a bounded channel, are drained into one transaction
//! (bounded in count), each guarded by a savepoint so a domain error rolls
//! back only its own work. Replies, wakeups and follow-up continuations are
//! staged and applied only after the commit lands — a crash or commit failure
//! never leaks a staged notification.
//!
//! Ordering: submission order is preserved through the channel, so commands
//! execute in TCP read order per connection. `Consume` is a barrier command:
//! it stops the drain so its leases are not held uncommitted behind a bulk
//! push batch.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::sync::{mpsc, watch, Semaphore};
use tracing::{error, warn};
use uuid::Uuid;

use crate::brokers::queue::domain::message::PushItem;
use crate::brokers::queue::domain::ops::{
    Command, Continuation, Effects, QueueReply, QueueRequest,
};
use crate::brokers::queue::domain::recipes::{self, CmdError, ExecCtx};
use crate::brokers::queue::domain::storage::Store;
use crate::brokers::BrokerError;

/// Max commands coalesced into one commit. Bounded so a barrier command's
/// leases never wait behind an arbitrarily large write batch.
const MAX_BATCH_COMMANDS: usize = 256;

/// Max items a merged push run may carry: bounds the rows one commit
/// writes so a queued barrier never waits behind an unbounded insert.
const MAX_PUSH_RUN_ITEMS: usize = 4096;

/// Writer diagnostics for benchmarks: how much of a batch is spent executing
/// statements vs committing. Always-on relaxed counters; one add per batch.
pub static BATCHES: AtomicU64 = AtomicU64::new(0);
pub static EXEC_NS: AtomicU64 = AtomicU64::new(0);
pub static COMMIT_NS: AtomicU64 = AtomicU64::new(0);

/// Queue-level watch signals: `watches[queue]` is bumped on every commit
/// that may have made new work visible (pushes, requeues, DLQ replays).
/// Deleting a queue drops its sender, which parked consumers observe as
/// `closed` → they resubmit and get NOT_FOUND.
pub type WatchMap = DashMap<String, watch::Sender<u64>>;

/// Byte-budget admission shared between submitters (async) and the worker
/// (releases on dequeue). Permits are bytes: `acquire` takes `bytes` permits
/// (FIFO wait when the budget is full) and `release` returns them after
/// dequeue. A semaphore keeps waiter registration atomic with the
/// availability check, which a hand-rolled Notify loop cannot guarantee.
/// The channel count bound lives in `mpsc` itself.
pub struct Admission {
    max_bytes: usize,
    sem: Semaphore,
}

impl Admission {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes: max_bytes.max(1),
            sem: Semaphore::new(max_bytes.max(1)),
        }
    }

    /// Wait until `bytes` fit the in-flight budget. A single command larger
    /// than the whole budget is admitted anyway (record-size limits are
    /// enforced separately); acquire and release skip the semaphore
    /// symmetrically so the over-charge stays balanced.
    pub async fn acquire(&self, bytes: usize) {
        if bytes == 0 || bytes >= self.max_bytes {
            return;
        }
        let permits = u32::try_from(bytes).unwrap_or(u32::MAX);
        self.sem
            .acquire_many(permits)
            .await
            .expect("admission semaphore is never closed")
            .forget();
    }

    pub fn release(&self, bytes: usize) {
        if bytes == 0 || bytes >= self.max_bytes {
            return;
        }
        self.sem.add_permits(bytes);
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

enum Item {
    Cmd(Command),
    /// Consecutive pushes for the same queue, folded by `merge_runs` into one
    /// `push` recipe call (one counter bump, multi-row INSERT). Items move
    /// out of the commands — no payload copies.
    PushRun(Vec<Command>),
    /// Consecutive acks for the same queue, folded into one recipe call.
    AckRun(Vec<Command>),
    Cont(Continuation),
}

/// An item decomposed for execution: the recipe input plus the reply
/// channels it serves.
enum Prep {
    Single(QueueRequest),
    Push {
        name: String,
        items: Vec<PushItem>,
        /// Number of merged commands — one `Unit` reply each.
        commands: usize,
    },
    Ack {
        name: String,
        acks: Vec<(Uuid, u64)>,
    },
}

type ReplyTx = tokio::sync::oneshot::Sender<Result<QueueReply, BrokerError>>;

pub struct Worker {
    store: Store,
    rx: mpsc::Receiver<Command>,
    conts: VecDeque<Continuation>,
    watches: Arc<WatchMap>,
    admission: Arc<Admission>,
    shutdown_requested: bool,
    channel_closed: bool,
}

/// Spawn the writer thread. `initial` continuations resume unfinished work
/// discovered during recovery (deleted-queue GC).
pub fn spawn(
    store: Store,
    rx: mpsc::Receiver<Command>,
    initial: Vec<Continuation>,
    watches: Arc<WatchMap>,
    admission: Arc<Admission>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("queue-writer".to_string())
        .spawn(move || {
            Worker {
                store,
                rx,
                conts: initial.into(),
                watches,
                admission,
                shutdown_requested: false,
                channel_closed: false,
            }
            .run()
        })
        .expect("failed to spawn queue writer thread")
}

struct Outcome {
    reply: Option<ReplyTx>,
    result: Result<QueueReply, BrokerError>,
}

impl Worker {
    fn run(&mut self) {
        loop {
            if self.shutdown_requested {
                // Drain whatever was already admitted, then checkpoint + exit.
                while let Ok(cmd) = self.rx.try_recv() {
                    self.admission.release(cmd.bytes);
                    self.exec_batch(vec![Item::Cmd(cmd)]);
                }
                while let Some(cont) = self.conts.pop_front() {
                    self.exec_batch(vec![Item::Cont(cont)]);
                }
                if let Err(e) = self.store.checkpoint(true) {
                    error!("Queue WAL checkpoint on shutdown failed: {}", e);
                }
                return;
            }

            let mut batch: Vec<Item> = Vec::with_capacity(MAX_BATCH_COMMANDS);
            if let Some(cont) = self.conts.pop_front() {
                batch.push(Item::Cont(cont));
            }
            // When the queue was empty, sleep on the first command — then
            // still drain whatever queued up meanwhile: otherwise every
            // low-rate command commits alone.
            let mut drain = true;
            if batch.is_empty() && !self.channel_closed {
                match self.rx.blocking_recv() {
                    Some(cmd) => {
                        self.admission.release(cmd.bytes);
                        drain = !cmd.is_barrier();
                        batch.push(Item::Cmd(cmd));
                    }
                    None => self.channel_closed = true,
                }
            }
            if drain {
                loop {
                    match self.rx.try_recv() {
                        Ok(cmd) => {
                            self.admission.release(cmd.bytes);
                            let barrier = cmd.is_barrier();
                            batch.push(Item::Cmd(cmd));
                            if barrier || batch.len() >= MAX_BATCH_COMMANDS {
                                break;
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            self.channel_closed = true;
                            break;
                        }
                    }
                }
            }
            if batch.is_empty() {
                if self.conts.is_empty() && self.channel_closed {
                    // No work and no senders: idle forever would only hold the
                    // lock file; shutdown ordering is owned by the manager.
                    if let Err(e) = self.store.checkpoint(true) {
                        error!("Queue WAL checkpoint failed: {}", e);
                    }
                    return;
                }
                continue;
            }
            let saw_shutdown = batch.iter().any(|item| {
                matches!(
                    item,
                    Item::Cmd(Command {
                        op: QueueRequest::Shutdown,
                        ..
                    })
                )
            });
            self.exec_batch(batch);
            if saw_shutdown {
                self.shutdown_requested = true;
            }
        }
    }

    /// Execute one drained batch inside a single transaction, replying to
    /// every command only after commit.
    fn exec_batch(&mut self, batch: Vec<Item>) {
        let ctx = ExecCtx {
            now_ms: now_millis(),
        };
        let mut tx = match self.store.conn.transaction() {
            Ok(tx) => tx,
            Err(e) => {
                let err = BrokerError::storage(format!("Cannot start transaction: {e}"));
                for item in batch {
                    for reply in item_replies(item) {
                        if let Some(reply) = reply {
                            let _ = reply.send(Err(BrokerError::storage(err.to_string())));
                        }
                    }
                }
                return;
            }
        };

        let mut outcomes: Vec<Outcome> = Vec::with_capacity(batch.len());
        let mut effects = Effects::default();
        let mut fatal: Option<BrokerError> = None;
        let exec_started = Instant::now();

        for item in merge_runs(batch) {
            match item {
                Item::Cont(cont) => {
                    if fatal.is_some() {
                        // Keep background work ordered; it retries next batch.
                        self.conts.push_back(cont);
                        continue;
                    }
                    match tx.savepoint() {
                        Ok(mut sp) => match recipes::run_continuation(&sp, &cont, &ctx) {
                            Ok((_, fx)) => {
                                if let Err(e) = sp.commit() {
                                    fatal = Some(BrokerError::storage(format!(
                                        "Savepoint release failed: {e}"
                                    )));
                                } else {
                                    merge(&mut effects, fx);
                                }
                            }
                            Err(CmdError::Expected(e)) => {
                                warn!("Queue continuation dropped domain error: {e}");
                                let _ = sp.rollback();
                            }
                            Err(CmdError::Fatal(e)) => {
                                let _ = sp.rollback();
                                error!("Queue continuation hit storage error: {e}");
                                fatal = Some(e);
                            }
                        },
                        Err(e) => {
                            fatal =
                                Some(BrokerError::storage(format!("Cannot open savepoint: {e}")));
                        }
                    }
                }
                item => {
                    let (prep, replies) = decompose(item);
                    if let Some(e) = &fatal {
                        abort_commands(replies, e, &mut outcomes);
                        continue;
                    }
                    match tx.savepoint() {
                        Ok(mut sp) => match run_prep(&sp, &prep, &ctx) {
                            Ok((results, fx)) => {
                                if let Err(e) = sp.commit() {
                                    abort_commands(
                                        replies,
                                        &BrokerError::storage(format!(
                                            "Savepoint release failed: {e}"
                                        )),
                                        &mut outcomes,
                                    );
                                    fatal = Some(BrokerError::storage(format!(
                                        "Savepoint release failed: {e}"
                                    )));
                                } else {
                                    merge(&mut effects, fx);
                                    for (reply, result) in
                                        replies.into_iter().zip(results.into_iter())
                                    {
                                        outcomes.push(Outcome { reply, result });
                                    }
                                }
                            }
                            Err(CmdError::Expected(e)) => {
                                let _ = sp.rollback();
                                for reply in replies {
                                    outcomes.push(Outcome {
                                        reply,
                                        result: Err(e.clone()),
                                    });
                                }
                            }
                            Err(CmdError::Fatal(e)) => {
                                let _ = sp.rollback();
                                abort_commands(
                                    replies,
                                    &BrokerError::storage(format!("Command aborted: {e}")),
                                    &mut outcomes,
                                );
                                fatal = Some(e);
                            }
                        },
                        Err(e) => {
                            abort_commands(
                                replies,
                                &BrokerError::storage(format!("Cannot open savepoint: {e}")),
                                &mut outcomes,
                            );
                            fatal =
                                Some(BrokerError::storage(format!("Cannot open savepoint: {e}")));
                        }
                    }
                }
            }
        }

        EXEC_NS.fetch_add(exec_started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        BATCHES.fetch_add(1, Ordering::Relaxed);
        match fatal {
            None => {
                let commit_started = Instant::now();
                match tx.commit() {
                    Ok(()) => {
                        COMMIT_NS.fetch_add(
                            commit_started.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        for outcome in outcomes {
                            if let Some(reply) = outcome.reply {
                                let _ = reply.send(outcome.result);
                            }
                        }
                        self.apply(effects);
                    }
                    Err(e) => {
                        let err = BrokerError::storage(format!("Transaction commit failed: {e}"));
                        error!("Queue writer commit failed: {}", err);
                        for outcome in outcomes {
                            if let Some(reply) = outcome.reply {
                                let _ = reply.send(Err(BrokerError::storage(err.to_string())));
                            }
                        }
                    }
                }
            }
            Some(e) => {
                // Whole transaction rolls back; every command gets the error.
                drop(tx);
                error!("Queue writer aborted batch: {}", e);
                for outcome in outcomes {
                    if let Some(reply) = outcome.reply {
                        let _ = reply.send(match outcome.result {
                            Ok(_) => Err(BrokerError::storage(format!(
                                "Batch aborted by storage error: {e}"
                            ))),
                            Err(err) => Err(err),
                        });
                    }
                }
                // Avoid a hot spin when the database is persistently broken.
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }

    /// Post-commit side effects: consumer wakeups, deleted-queue
    /// invalidation, continuation scheduling.
    fn apply(&mut self, effects: Effects) {
        for queue in effects.wakes {
            if let Some(watch) = self.watches.get(&queue) {
                watch.send_modify(|v| *v = v.wrapping_add(1));
            }
        }
        for queue in effects.deleted_queues {
            self.watches.remove(&queue);
        }
        for cont in effects.followups {
            self.conts.push_back(cont);
        }
    }
}

/// Split an executable item into its recipe input and its reply channels.
/// Payloads move out of commands — no copies.
fn decompose(item: Item) -> (Prep, Vec<Option<ReplyTx>>) {
    match item {
        Item::Cmd(cmd) => {
            let Command { op, reply, .. } = cmd;
            (Prep::Single(op), vec![reply])
        }
        Item::PushRun(cmds) => {
            let mut name = String::new();
            let mut items = Vec::new();
            let mut replies = Vec::with_capacity(cmds.len());
            for cmd in cmds {
                let Command { op, reply, .. } = cmd;
                let QueueRequest::Push {
                    name: n,
                    items: it,
                } = op
                else {
                    unreachable!("PushRun carries only push commands")
                };
                name = n;
                items.extend(it);
                replies.push(reply);
            }
            (
                Prep::Push {
                    name,
                    items,
                    commands: replies.len(),
                },
                replies,
            )
        }
        Item::AckRun(cmds) => {
            let mut name = String::new();
            let mut acks = Vec::with_capacity(cmds.len());
            let mut replies = Vec::with_capacity(cmds.len());
            for cmd in cmds {
                let Command { op, reply, .. } = cmd;
                let QueueRequest::Ack {
                    name: n,
                    id,
                    delivery_token,
                } = op
                else {
                    unreachable!("AckRun carries only ack commands")
                };
                name = n;
                acks.push((id, delivery_token));
                replies.push(reply);
            }
            (Prep::Ack { name, acks }, replies)
        }
        Item::Cont(_) => unreachable!("continuations are handled before decompose"),
    }
}

/// Dispatch one prepared item through its recipe inside a savepoint.
/// Per-command results always line up 1:1 with `replies`.
fn run_prep(
    conn: &rusqlite::Connection,
    prep: &Prep,
    ctx: &ExecCtx,
) -> Result<(Vec<Result<QueueReply, BrokerError>>, Effects), CmdError> {
    match prep {
        Prep::Single(op) => {
            let (reply, fx) = recipes::execute(conn, op, ctx)?;
            Ok((vec![Ok(reply)], fx))
        }
        Prep::Push {
            name,
            items,
            commands,
        } => {
            let (_, fx) = recipes::push(conn, name, items, ctx)?;
            Ok((
                (0..*commands).map(|_| Ok(QueueReply::Unit)).collect(),
                fx,
            ))
        }
        Prep::Ack { name, acks } => {
            let results = recipes::ack_many(conn, name, acks, ctx)?;
            Ok((
                results
                    .into_iter()
                    .map(|b| Ok(QueueReply::Bool(b)))
                    .collect(),
                Effects::default(),
            ))
        }
    }
}

fn abort_commands(
    replies: Vec<Option<ReplyTx>>,
    err: &BrokerError,
    outcomes: &mut Vec<Outcome>,
) {
    for reply in replies {
        outcomes.push(Outcome {
            reply,
            result: Err(BrokerError::storage(format!(
                "Batch aborted by storage error: {}",
                err.message
            ))),
        });
    }
}

fn item_replies(item: Item) -> Vec<Option<ReplyTx>> {
    match item {
        Item::Cmd(cmd) => vec![cmd.reply],
        Item::PushRun(cmds) | Item::AckRun(cmds) => {
            cmds.into_iter().map(|c| c.reply).collect()
        }
        Item::Cont(_) => Vec::new(),
    }
}

/// Fold runs of consecutive mergeable commands into `Item::AckRun` (acks for
/// the same queue) or `Item::PushRun` (pushes for the same queue, capped by
/// item count). Order is preserved: any other item ends the run, so a mixed
/// batch degrades to per-command execution. Each merged command keeps its
/// own reply slot.
fn merge_runs(batch: Vec<Item>) -> Vec<Item> {
    fn push_key(cmd: &Command) -> Option<&String> {
        match &cmd.op {
            QueueRequest::Push { name, .. } => Some(name),
            _ => None,
        }
    }
    fn ack_key(cmd: &Command) -> Option<&String> {
        match &cmd.op {
            QueueRequest::Ack { name, .. } => Some(name),
            _ => None,
        }
    }
    let mut out: Vec<Item> = Vec::with_capacity(batch.len());
    let mut push_run_items = 0usize;
    for item in batch {
        if matches!(&item, Item::Cmd(cmd) if push_key(cmd).is_some()) {
            let Item::Cmd(cmd) = item else { unreachable!() };
            let QueueRequest::Push { name, items } = &cmd.op else {
                unreachable!()
            };
            if let Some(Item::PushRun(run)) = out.last_mut() {
                if push_key(&run[0]) == Some(name)
                    && push_run_items + items.len() <= MAX_PUSH_RUN_ITEMS
                {
                    push_run_items += items.len();
                    run.push(cmd);
                    continue;
                }
            }
            push_run_items = items.len();
            out.push(Item::PushRun(vec![cmd]));
            continue;
        }
        if matches!(&item, Item::Cmd(cmd) if ack_key(cmd).is_some()) {
            let Item::Cmd(cmd) = item else { unreachable!() };
            if let Some(Item::AckRun(run)) = out.last_mut() {
                if ack_key(&run[0]) == ack_key(&cmd) {
                    run.push(cmd);
                    continue;
                }
            }
            out.push(Item::AckRun(vec![cmd]));
            continue;
        }
        out.push(item);
    }
    out
}

fn merge(into: &mut Effects, from: Effects) {
    for name in from.wakes {
        if !into.wakes.contains(&name) {
            into.wakes.push(name);
        }
    }
    for name in from.deleted_queues {
        if !into.deleted_queues.contains(&name) {
            into.deleted_queues.push(name);
        }
    }
    into.followups.extend(from.followups);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: a submitter parked on a saturated budget must wake when
    /// the worker releases bytes. The semaphore makes waiter registration
    /// atomic with the availability check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admission_waiter_wakes_on_release() {
        let admission = Arc::new(Admission::new(8));
        admission.acquire(4).await;
        admission.acquire(4).await;
        assert_eq!(admission.sem.available_permits(), 0);

        let waiter = {
            let admission = Arc::clone(&admission);
            tokio::spawn(async move { admission.acquire(4).await })
        };
        tokio::task::yield_now().await;
        admission.release(4);
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("acquire lost a wakeup and stalled")
            .unwrap();
        assert_eq!(admission.sem.available_permits(), 0);
    }

    /// Commands at or over the whole budget bypass the gate; acquire and
    /// release must skip the semaphore symmetrically or permits leak.
    #[tokio::test]
    async fn admission_oversized_bypasses_budget() {
        let admission = Admission::new(8);
        admission.acquire(16).await;
        admission.release(16);
        assert_eq!(admission.sem.available_permits(), 8);
    }
}

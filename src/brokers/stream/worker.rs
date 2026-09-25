//! Dedicated SQLite writer thread: the single owner of `Store`.
//!
//! Commands arrive on a bounded channel, are drained into one transaction
//! (bounded in count), each guarded by a savepoint so a domain error rolls
//! back only its own work. Replies, wakeups and follow-up continuations are
//! staged and applied only after the commit lands — a crash or commit failure
//! never leaks a staged notification.
//!
//! Ordering: submission order is preserved through the channel, so commands
//! execute in TCP read order per connection. Barrier commands (fetch/seek/
//! leave/disconnect/shutdown) stop the drain so their leases and epoch
//! changes are not held uncommitted behind a bulk mutation batch.

use std::collections::VecDeque;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::sync::{mpsc, watch, Semaphore};
use tracing::{error, warn};

use crate::brokers::stream::domain::message::ConsumerIdentity;
use crate::brokers::stream::domain::ops::{
    Command, Continuation, Effects, StreamReply, StreamRequest,
};
use crate::brokers::stream::domain::recipes::{self, CmdError, ExecCtx};
use crate::brokers::stream::domain::storage::Store;
use crate::brokers::BrokerError;

/// Max commands coalesced into one commit. Bounded so a barrier command's
/// leases never wait behind an arbitrarily large write batch.
const MAX_BATCH_COMMANDS: usize = 256;

/// Group-level watch signals: `watches[stream][group]` is bumped on every
/// commit that may have made new work visible (publishes, acks, lease
/// releases, epoch init pages). Removing the stream entry drops every sender,
/// which parked fetch waiters observe as `closed` → they resubmit and get
/// NOT_FOUND.
pub type WatchMap = DashMap<String, DashMap<String, watch::Sender<u64>>>;

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
    /// Consecutive acks for the same (stream, group, consumer), folded by
    /// `merge_ack_runs`: the worker executes them as one op so the batch
    /// pays member resolution and the epoch write once per run.
    AckRun(Vec<Command>),
    Cont(Continuation),
}

pub struct Worker {
    store: Store,
    rx: mpsc::Receiver<Command>,
    conts: VecDeque<Continuation>,
    watches: Arc<WatchMap>,
    admission: Arc<Admission>,
    fetch_response_bytes: u64,
    shutdown_requested: bool,
    channel_closed: bool,
}

/// Spawn the writer thread. `initial` continuations resume unfinished work
/// discovered during recovery (paged epoch init, deleted-stream GC).
pub fn spawn(
    store: Store,
    rx: mpsc::Receiver<Command>,
    initial: Vec<Continuation>,
    watches: Arc<WatchMap>,
    admission: Arc<Admission>,
    fetch_response_bytes: u64,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("stream-writer".to_string())
        .spawn(move || {
            Worker {
                store,
                rx,
                conts: initial.into(),
                watches,
                admission,
                fetch_response_bytes,
                shutdown_requested: false,
                channel_closed: false,
            }
            .run()
        })
        .expect("failed to spawn stream writer thread")
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
                    error!("Stream WAL checkpoint on shutdown failed: {}", e);
                }
                return;
            }

            let mut batch: Vec<Item> = Vec::with_capacity(MAX_BATCH_COMMANDS);
            if let Some(cont) = self.conts.pop_front() {
                batch.push(Item::Cont(cont));
            }
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
            if batch.is_empty() && !self.channel_closed {
                match self.rx.blocking_recv() {
                    Some(cmd) => {
                        self.admission.release(cmd.bytes);
                        batch.push(Item::Cmd(cmd));
                    }
                    None => self.channel_closed = true,
                }
            }
            if batch.is_empty() {
                if self.conts.is_empty() && self.channel_closed {
                    // No work and no senders: idle forever would only hold the
                    // lock file; shutdown ordering is owned by the manager.
                    if let Err(e) = self.store.checkpoint(true) {
                        error!("Stream WAL checkpoint failed: {}", e);
                    }
                    return;
                }
                continue;
            }
            let saw_shutdown = batch.iter().any(|item| {
                matches!(
                    item,
                    Item::Cmd(Command {
                        op: crate::brokers::stream::domain::ops::StreamRequest::Shutdown,
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
            fetch_response_bytes: self.fetch_response_bytes,
        };
        let mut tx = match self.store.conn.transaction() {
            Ok(tx) => tx,
            Err(e) => {
                let err = BrokerError::storage(format!("Cannot start transaction: {e}"));
                for item in batch {
                    if let Item::Cmd(cmd) = item {
                        fail_command(cmd, &err);
                    }
                }
                return;
            }
        };

        struct Outcome {
            reply: Option<tokio::sync::oneshot::Sender<Result<StreamReply, BrokerError>>>,
            result: Result<StreamReply, BrokerError>,
        }

        let mut outcomes: Vec<Outcome> = Vec::with_capacity(batch.len());
        let mut effects = Effects::default();
        let mut fatal: Option<BrokerError> = None;

        for item in merge_ack_runs(batch) {
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
                                warn!("Stream continuation dropped domain error: {e}");
                                let _ = sp.rollback();
                            }
                            Err(CmdError::Fatal(e)) => {
                                let _ = sp.rollback();
                                error!("Stream continuation hit storage error: {e}");
                                fatal = Some(e);
                            }
                        },
                        Err(e) => {
                            fatal =
                                Some(BrokerError::storage(format!("Cannot open savepoint: {e}")));
                        }
                    }
                }
                Item::Cmd(cmd) => {
                    if let Some(e) = &fatal {
                        outcomes.push(Outcome {
                            reply: cmd.reply,
                            result: Err(BrokerError::storage(format!(
                                "Batch aborted by storage error: {e}"
                            ))),
                        });
                        continue;
                    }
                    match tx.savepoint() {
                        Ok(mut sp) => match recipes::execute(&sp, &cmd.op, &ctx) {
                            Ok((reply, fx)) => {
                                if let Err(e) = sp.commit() {
                                    outcomes.push(Outcome {
                                        reply: cmd.reply,
                                        result: Err(BrokerError::storage(format!(
                                            "Savepoint release failed: {e}"
                                        ))),
                                    });
                                    fatal = Some(BrokerError::storage(format!(
                                        "Savepoint release failed: {e}"
                                    )));
                                } else {
                                    merge(&mut effects, fx);
                                    outcomes.push(Outcome {
                                        reply: cmd.reply,
                                        result: Ok(reply),
                                    });
                                }
                            }
                            Err(CmdError::Expected(e)) => {
                                let _ = sp.rollback();
                                outcomes.push(Outcome {
                                    reply: cmd.reply,
                                    result: Err(e),
                                });
                            }
                            Err(CmdError::Fatal(e)) => {
                                let _ = sp.rollback();
                                outcomes.push(Outcome {
                                    reply: cmd.reply,
                                    result: Err(BrokerError::storage(format!(
                                        "Command aborted: {e}"
                                    ))),
                                });
                                fatal = Some(e);
                            }
                        },
                        Err(e) => {
                            outcomes.push(Outcome {
                                reply: cmd.reply,
                                result: Err(BrokerError::storage(format!(
                                    "Cannot open savepoint: {e}"
                                ))),
                            });
                            fatal =
                                Some(BrokerError::storage(format!("Cannot open savepoint: {e}")));
                        }
                    }
                }
                Item::AckRun(cmds) => {
                    if let Some(e) = &fatal {
                        for cmd in cmds {
                            outcomes.push(Outcome {
                                reply: cmd.reply,
                                result: Err(BrokerError::storage(format!(
                                    "Batch aborted by storage error: {e}"
                                ))),
                            });
                        }
                        continue;
                    }
                    let (name, group, identity) = match &cmds[0].op {
                        StreamRequest::Ack {
                            name,
                            group,
                            identity,
                            ..
                        }
                        | StreamRequest::AckMany {
                            name,
                            group,
                            identity,
                            ..
                        } => (name, group, identity),
                        _ => unreachable!("AckRun carries only ack commands"),
                    };
                    // Flatten every command's (seq, receipt) pairs into one
                    // ack_many call; `counts` remembers how many pairs each
                    // command owns so replies can be sliced back per command.
                    let mut acks: Vec<(u64, [u8; 16])> = Vec::new();
                    let mut counts: Vec<usize> = Vec::with_capacity(cmds.len());
                    for cmd in &cmds {
                        match &cmd.op {
                            StreamRequest::Ack { seq, receipt, .. } => {
                                acks.push((*seq, *receipt));
                                counts.push(1);
                            }
                            StreamRequest::AckMany { acks: more, .. } => {
                                acks.extend_from_slice(more);
                                counts.push(more.len());
                            }
                            _ => unreachable!("AckRun carries only ack commands"),
                        }
                    }
                    match tx.savepoint() {
                        Ok(mut sp) => match recipes::ack_many(&sp, name, group, identity, &acks) {
                            Ok((results, fx)) => {
                                if let Err(e) = sp.commit() {
                                    for cmd in cmds {
                                        outcomes.push(Outcome {
                                            reply: cmd.reply,
                                            result: Err(BrokerError::storage(format!(
                                                "Savepoint release failed: {e}"
                                            ))),
                                        });
                                    }
                                    fatal = Some(BrokerError::storage(format!(
                                        "Savepoint release failed: {e}"
                                    )));
                                } else {
                                    merge(&mut effects, fx);
                                    let mut results = results.into_iter();
                                    for (cmd, n) in cmds.into_iter().zip(counts) {
                                        let slice: Vec<Result<StreamReply, BrokerError>> =
                                            results.by_ref().take(n).collect();
                                        let result = match &cmd.op {
                                            StreamRequest::Ack { .. } => {
                                                slice.into_iter().next().unwrap_or_else(|| {
                                                    Err(BrokerError::new(
                                                        crate::brokers::BrokerErrorKind::Internal,
                                                        "Ack run result missing",
                                                    ))
                                                })
                                            }
                                            StreamRequest::AckMany { acks: mine, .. } => {
                                                let failed = mine
                                                    .iter()
                                                    .zip(&slice)
                                                    .filter(|(_, r)| r.is_err())
                                                    .map(|((seq, _), _)| *seq)
                                                    .collect();
                                                Ok(StreamReply::AckOutcome(failed))
                                            }
                                            _ => unreachable!(),
                                        };
                                        outcomes.push(Outcome {
                                            reply: cmd.reply,
                                            result,
                                        });
                                    }
                                }
                            }
                            Err(CmdError::Expected(e)) => {
                                let _ = sp.rollback();
                                for cmd in cmds {
                                    outcomes.push(Outcome {
                                        reply: cmd.reply,
                                        result: Err(BrokerError::new(e.kind, e.message.clone())),
                                    });
                                }
                            }
                            Err(CmdError::Fatal(e)) => {
                                let _ = sp.rollback();
                                for cmd in cmds {
                                    outcomes.push(Outcome {
                                        reply: cmd.reply,
                                        result: Err(BrokerError::storage(format!(
                                            "Command aborted: {e}"
                                        ))),
                                    });
                                }
                                fatal = Some(e);
                            }
                        },
                        Err(e) => {
                            for cmd in cmds {
                                outcomes.push(Outcome {
                                    reply: cmd.reply,
                                    result: Err(BrokerError::storage(format!(
                                        "Cannot open savepoint: {e}"
                                    ))),
                                });
                            }
                            fatal =
                                Some(BrokerError::storage(format!("Cannot open savepoint: {e}")));
                        }
                    }
                }
            }
        }

        match fatal {
            None => match tx.commit() {
                Ok(()) => {
                    for outcome in outcomes {
                        if let Some(reply) = outcome.reply {
                            let _ = reply.send(outcome.result);
                        }
                    }
                    self.apply(effects);
                }
                Err(e) => {
                    let err = BrokerError::storage(format!("Transaction commit failed: {e}"));
                    error!("Stream writer commit failed: {}", err);
                    for outcome in outcomes {
                        if let Some(reply) = outcome.reply {
                            let _ = reply.send(Err(BrokerError::storage(err.to_string())));
                        }
                    }
                }
            },
            Some(e) => {
                // Whole transaction rolls back; every command gets the error.
                drop(tx);
                error!("Stream writer aborted batch: {}", e);
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

    /// Post-commit side effects: group wakeups, deleted-stream invalidation,
    /// continuation scheduling.
    fn apply(&mut self, effects: Effects) {
        for (stream, group) in effects.wakes {
            if let Some(groups) = self.watches.get(&stream) {
                if let Some(watch) = groups.get(&group) {
                    watch.send_modify(|v| *v = v.wrapping_add(1));
                }
            }
        }
        for stream in effects.deleted_streams {
            self.watches.remove(&stream);
        }
        for cont in effects.followups {
            self.conts.push_back(cont);
        }
    }
}

fn fail_command(cmd: Command, err: &BrokerError) {
    if let Some(reply) = cmd.reply {
        let _ = reply.send(Err(BrokerError::storage(err.to_string())));
    }
}

/// Fold runs of consecutive `Ack`/`AckMany` commands for the same (stream,
/// group, consumer) into `Item::AckRun`. Order is preserved: any non-ack item
/// ends the run, so a mixed batch degrades to per-command execution. Each
/// merged command keeps its own reply slot.
fn merge_ack_runs(batch: Vec<Item>) -> Vec<Item> {
    fn ack_key(cmd: &Command) -> Option<(&String, &String, &ConsumerIdentity)> {
        match &cmd.op {
            StreamRequest::Ack {
                name,
                group,
                identity,
                ..
            }
            | StreamRequest::AckMany {
                name,
                group,
                identity,
                ..
            } => Some((name, group, identity)),
            _ => None,
        }
    }
    let mut out: Vec<Item> = Vec::with_capacity(batch.len());
    for item in batch {
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
    for pair in from.wakes {
        if !into.wakes.contains(&pair) {
            into.wakes.push(pair);
        }
    }
    for s in from.deleted_streams {
        if !into.deleted_streams.contains(&s) {
            into.deleted_streams.push(s);
        }
    }
    into.followups.extend(from.followups);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: a submitter parked on a saturated budget must wake when
    /// the worker releases bytes. The former AtomicUsize+Notify loop could
    /// drop a wakeup landing between the failed check and waiter
    /// registration, stalling the connection read loop; the semaphore makes
    /// registration atomic with the check.
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

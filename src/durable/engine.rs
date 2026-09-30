//! The writer pipeline shared by every durable domain.
//!
//! A dedicated thread owns `Store` — the only task touching SQLite.
//! Commands arrive on a bounded channel, are drained into one transaction
//! (bounded in count), each guarded by a savepoint so a domain error rolls
//! back only its own work. Replies, wakeups and follow-up continuations are
//! staged and applied only after the commit lands — a crash or commit
//! failure never leaks a staged notification.
//!
//! Ordering: submission order is preserved through the channel, so commands
//! execute in TCP read order per connection. Domain-declared barrier
//! commands stop the drain so their effects are not held uncommitted behind
//! a bulk mutation batch.
//!
//! `Domain` carries everything broker-specific: request/reply types, the
//! batch execution context, run merging, post-commit effects and the watch
//! registry shape. The engine owns the machinery: drain, transactions,
//! savepoints, aborts, effects ordering and shutdown.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::brokers::BrokerError;
use crate::durable::handle::EngineHandle;
use crate::durable::storage::Store;
use crate::durable::types::{install_stats, now_millis, Admission, CmdError, ReplyTx, Stats};

/// Max commands coalesced into one commit. Bounded so a barrier command's
/// effects never wait behind an arbitrarily large write batch.
const MAX_BATCH_COMMANDS: usize = 256;

/// One durable command executed inside the writer transaction.
pub struct Command<D: Domain> {
    pub op: D::Request,
    /// Bytes charged against the in-flight byte budget (0 for lightweight
    /// ops).
    pub bytes: usize,
    /// `None` for internal/maintenance commands that do not need a reply.
    pub reply: Option<ReplyTx<D::Reply>>,
}

/// A drained item: one command, one merged run of commands, or one
/// continuation slice.
pub enum Item<D: Domain> {
    Cmd(Command<D>),
    Run(D::Run),
    Cont(D::Continuation),
}

/// Everything broker-specific the engine needs: the command/reply types,
/// the batch context passed to recipes, and the merge/effect policy.
///
/// The domain impl lives next to the recipes it dispatches to (the broker's
/// `worker.rs`); the engine only orchestrates.
pub trait Domain: Send + Sized + 'static {
    type Request: Send;
    type Reply: Send;
    /// Bounded background work slices. Each item is one transaction on the
    /// writer; unfinished work re-enqueues itself until done.
    type Continuation: Send;
    /// A group of commands folded by `merge_runs`. Domains without merging
    /// use `()` and leave the default `merge_runs`/`take_replies`/
    /// `execute_run` impls.
    type Run: Send;
    /// Side effects staged inside a transaction, merged across batch items
    /// and applied only after commit.
    type Effects: Default;
    /// Batch execution context handed to every recipe (wall clock plus any
    /// domain config the domain impl captures).
    type Ctx;
    /// Long-poll watch registry (e.g. `DashMap<key, watch::Sender>`).
    type Watches: Default + Send + Sync;

    fn make_ctx(&self, now_ms: u64) -> Self::Ctx;

    /// Merge the per-item effects of a finished savepoint into the batch's.
    fn merge_effects(into: &mut Self::Effects, from: Self::Effects);

    /// Requests whose effects must not wait behind a bulk batch stop the
    /// drain (e.g. lease-granting reads).
    fn is_barrier(req: &Self::Request) -> bool;

    /// Terminal request: the writer drains admitted work and exits.
    fn is_shutdown(req: &Self::Request) -> bool;

    /// Execute one request inside `conn`'s savepoint.
    fn execute(
        &self,
        conn: &Connection,
        req: &Self::Request,
        ctx: &Self::Ctx,
    ) -> Result<(Self::Reply, Self::Effects), CmdError>;

    /// Execute one continuation slice inside `conn`'s savepoint.
    fn run_continuation(
        &self,
        conn: &Connection,
        cont: &Self::Continuation,
        ctx: &Self::Ctx,
    ) -> Result<Self::Effects, CmdError>;

    /// Fold runs of consecutive mergeable commands into `Item::Run`. Order
    /// is preserved: any other item ends a run, so a mixed batch degrades to
    /// per-command execution. Default: no merging.
    fn merge_runs(&self, batch: Vec<Item<Self>>) -> Vec<Item<Self>> {
        batch
    }

    /// Peel the reply channels out of a run before it enters the savepoint,
    /// in the same order `execute_run` produces results.
    fn take_replies(run: &mut Self::Run) -> Vec<Option<ReplyTx<Self::Reply>>> {
        let _ = run;
        Vec::new()
    }

    /// Execute one merged run; must produce exactly one result per reply
    /// channel peeled by `take_replies`.
    fn execute_run(
        &self,
        _conn: &Connection,
        run: Self::Run,
        _ctx: &Self::Ctx,
    ) -> Result<(Vec<Result<Self::Reply, BrokerError>>, Self::Effects), CmdError> {
        let _ = run;
        unreachable!("domain does not emit merged runs")
    }

    /// Post-commit side effects: deliver long-poll wakeups, drop watchers of
    /// deleted resources, enqueue follow-up continuations.
    fn apply_effects(
        &self,
        watches: &Self::Watches,
        effects: Self::Effects,
        followups: &mut VecDeque<Self::Continuation>,
    );
}

pub struct Worker<D: Domain> {
    label: &'static str,
    domain: D,
    store: Store,
    rx: mpsc::Receiver<Command<D>>,
    conts: VecDeque<D::Continuation>,
    watches: Arc<D::Watches>,
    admission: Arc<Admission>,
    stats: Arc<Stats>,
    shutdown_requested: bool,
    channel_closed: bool,
}

/// Spawn the writer thread. `initial` continuations resume unfinished work
/// discovered during recovery (paged inits, deleted-resource GC).
pub fn spawn<D: Domain>(
    label: &'static str,
    domain: D,
    store: Store,
    rx: mpsc::Receiver<Command<D>>,
    initial: Vec<D::Continuation>,
    engine: &EngineHandle<D>,
) -> JoinHandle<()> {
    let watches = Arc::clone(engine.watches());
    let admission = Arc::clone(engine.admission());
    let stats = Arc::clone(engine.stats());
    std::thread::Builder::new()
        .name(format!("{label}-writer"))
        .spawn(move || {
            install_stats(Arc::clone(&stats));
            Worker {
                label,
                domain,
                store,
                rx,
                conts: initial.into(),
                watches,
                admission,
                stats,
                shutdown_requested: false,
                channel_closed: false,
            }
            .run()
        })
        .expect("failed to spawn durable writer thread")
}

struct Outcome<D: Domain> {
    reply: Option<ReplyTx<D::Reply>>,
    result: Result<D::Reply, BrokerError>,
}

/// A savepoint unit: a single command or a merged run.
enum Work<D: Domain> {
    Single(D::Request),
    Run(D::Run),
}

impl<D: Domain> Worker<D> {
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
                    error!("{} WAL checkpoint on shutdown failed: {}", self.label, e);
                }
                return;
            }

            let mut batch: Vec<Item<D>> = Vec::with_capacity(MAX_BATCH_COMMANDS);
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
                        drain = !D::is_barrier(&cmd.op);
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
                            let barrier = D::is_barrier(&cmd.op);
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
                    // No work and no senders: idle forever would only hold
                    // the lock file; shutdown ordering is owned by the
                    // manager.
                    if let Err(e) = self.store.checkpoint(true) {
                        error!("{} WAL checkpoint failed: {}", self.label, e);
                    }
                    return;
                }
                continue;
            }
            let saw_shutdown = batch.iter().any(
                |item| matches!(item, Item::Cmd(cmd) if D::is_shutdown(&cmd.op)),
            );
            self.exec_batch(batch);
            if saw_shutdown {
                self.shutdown_requested = true;
            }
        }
    }

    /// Execute one drained batch inside a single transaction, replying to
    /// every command only after commit.
    fn exec_batch(&mut self, batch: Vec<Item<D>>) {
        let ctx = self.domain.make_ctx(now_millis());
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

        let mut outcomes: Vec<Outcome<D>> = Vec::with_capacity(batch.len());
        let mut effects = D::Effects::default();
        let mut fatal: Option<BrokerError> = None;
        let exec_started = Instant::now();

        for item in self.domain.merge_runs(batch) {
            match item {
                Item::Cont(cont) => {
                    if fatal.is_some() {
                        // Keep background work ordered; it retries next batch.
                        self.conts.push_back(cont);
                        continue;
                    }
                    match tx.savepoint() {
                        Ok(mut sp) => {
                            match self.domain.run_continuation(&sp, &cont, &ctx) {
                                Ok(fx) => {
                                    if let Err(e) = sp.commit() {
                                        fatal = Some(BrokerError::storage(format!(
                                            "Savepoint release failed: {e}"
                                        )));
                                    } else {
                                        D::merge_effects(&mut effects, fx);
                                    }
                                }
                                Err(CmdError::Expected(e)) => {
                                    warn!("{} continuation dropped domain error: {e}", self.label);
                                    let _ = sp.rollback();
                                }
                                Err(CmdError::Fatal(e)) => {
                                    let _ = sp.rollback();
                                    error!("{} continuation hit storage error: {e}", self.label);
                                    fatal = Some(e);
                                }
                            }
                        }
                        Err(e) => {
                            fatal =
                                Some(BrokerError::storage(format!("Cannot open savepoint: {e}")));
                        }
                    }
                }
                item => {
                    let (work, replies) = match item {
                        Item::Cmd(cmd) => (Work::<D>::Single(cmd.op), vec![cmd.reply]),
                        Item::Run(mut run) => {
                            let replies = D::take_replies(&mut run);
                            (Work::Run(run), replies)
                        }
                        Item::Cont(_) => unreachable!("continuations handled above"),
                    };
                    if let Some(e) = &fatal {
                        abort_commands(replies, e, &mut outcomes);
                        continue;
                    }
                    match tx.savepoint() {
                        Ok(mut sp) => {
                            let executed = match work {
                                Work::Single(op) => match self.domain.execute(&sp, &op, &ctx) {
                                    Ok((reply, fx)) => Ok((vec![Ok(reply)], fx)),
                                    Err(e) => Err(e),
                                },
                                Work::Run(run) => self.domain.execute_run(&sp, run, &ctx),
                            };
                            match executed {
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
                                        D::merge_effects(&mut effects, fx);
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
                            }
                        }
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

        self.stats
            .exec_ns
            .fetch_add(exec_started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats.batches.fetch_add(1, Ordering::Relaxed);
        match fatal {
            None => {
                let commit_started = Instant::now();
                match tx.commit() {
                    Ok(()) => {
                        self.stats
                            .commit_ns
                            .fetch_add(commit_started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        for outcome in outcomes {
                            if let Some(reply) = outcome.reply {
                                let _ = reply.send(outcome.result);
                            }
                        }
                        self.domain
                            .apply_effects(&self.watches, effects, &mut self.conts);
                    }
                    Err(e) => {
                        let err = BrokerError::storage(format!("Transaction commit failed: {e}"));
                        error!("{} writer commit failed: {}", self.label, err);
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
                error!("{} writer aborted batch: {}", self.label, e);
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
}

/// Replies waiting on one batch item — used when the transaction cannot
/// even open (every queued command fails fast).
fn item_replies<D: Domain>(item: Item<D>) -> Vec<Option<ReplyTx<D::Reply>>> {
    match item {
        Item::Cmd(cmd) => vec![cmd.reply],
        Item::Run(mut run) => D::take_replies(&mut run),
        Item::Cont(_) => Vec::new(),
    }
}

fn abort_commands<D: Domain>(
    replies: Vec<Option<ReplyTx<D::Reply>>>,
    err: &BrokerError,
    outcomes: &mut Vec<Outcome<D>>,
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

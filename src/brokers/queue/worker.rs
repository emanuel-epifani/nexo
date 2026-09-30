//! Queue `Domain` impl: everything broker-specific the durable engine
//! needs. The machinery (drain, transaction, savepoints, commit, effects,
//! shutdown) lives in `crate::durable::engine`.
//!
//! `Consume` is a barrier request: it stops the drain so its leases are not
//! held uncommitted behind a bulk push batch. Consecutive `Push`/`Ack`
//! commands for the same queue fold into merged runs so a batch pays their
//! fixed costs once.

use std::collections::VecDeque;

use dashmap::DashMap;
use rusqlite::Connection;
use tokio::sync::watch;
use uuid::Uuid;

use crate::brokers::queue::domain::message::PushItem;
use crate::brokers::queue::domain::ops::{
    Command, Continuation, Effects, QueueDomain, QueueReply, QueueRequest,
};
use crate::brokers::queue::domain::recipes;
use crate::brokers::BrokerError;
use crate::durable::{CmdError, Domain, ExecCtx, Item, ReplyTx};

/// Max items a merged push run may carry: bounds the rows one commit writes
/// so a queued barrier never waits behind an unbounded insert.
const MAX_PUSH_RUN_ITEMS: usize = 4096;

/// Queue-level watch signals: `watches[queue]` is bumped on every commit
/// that may have made new work visible (pushes, requeues, DLQ replays).
/// Deleting a queue drops its sender, which parked consumers observe as
/// `closed` → they resubmit and get NOT_FOUND.
pub type WatchMap = DashMap<String, watch::Sender<u64>>;

/// A merged run of same-queue commands.
pub enum Run {
    /// Consecutive pushes: one `push` recipe call (one counter bump,
    /// multi-row INSERT). Items move out of the commands — no payload copies.
    Push(Vec<Command>),
    /// Consecutive acks for the same queue, folded into one `ack_many` call.
    Ack(Vec<Command>),
}

impl Domain for QueueDomain {
    type Request = QueueRequest;
    type Reply = QueueReply;
    type Continuation = Continuation;
    type Run = Run;
    type Effects = Effects;
    type Ctx = ExecCtx;
    type Watches = WatchMap;

    fn make_ctx(&self, now_ms: u64) -> ExecCtx {
        ExecCtx { now_ms }
    }

    fn merge_effects(into: &mut Effects, from: Effects) {
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

    /// Requests whose replies order leases against later work stop the drain
    /// so their commits are not delayed behind bulk write batches.
    fn is_barrier(req: &QueueRequest) -> bool {
        matches!(
            req,
            QueueRequest::Consume { .. } | QueueRequest::Shutdown
        )
    }

    fn is_shutdown(req: &QueueRequest) -> bool {
        matches!(req, QueueRequest::Shutdown)
    }

    fn execute(
        &self,
        conn: &Connection,
        req: &QueueRequest,
        ctx: &ExecCtx,
    ) -> Result<(QueueReply, Effects), CmdError> {
        recipes::execute(conn, req, ctx)
    }

    fn run_continuation(
        &self,
        conn: &Connection,
        cont: &Continuation,
        ctx: &ExecCtx,
    ) -> Result<Effects, CmdError> {
        recipes::run_continuation(conn, cont, ctx).map(|(_, fx)| fx)
    }

    /// Fold runs of consecutive mergeable commands. Each merged command
    /// keeps its own reply slot.
    fn merge_runs(&self, batch: Vec<Item<QueueDomain>>) -> Vec<Item<QueueDomain>> {
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
        let mut out: Vec<Item<QueueDomain>> = Vec::with_capacity(batch.len());
        let mut push_run_items = 0usize;
        for item in batch {
            if matches!(&item, Item::Cmd(cmd) if push_key(cmd).is_some()) {
                let Item::Cmd(cmd) = item else { unreachable!() };
                let QueueRequest::Push { name, items } = &cmd.op else {
                    unreachable!()
                };
                if let Some(Item::Run(Run::Push(run))) = out.last_mut() {
                    if push_key(&run[0]) == Some(name)
                        && push_run_items + items.len() <= MAX_PUSH_RUN_ITEMS
                    {
                        push_run_items += items.len();
                        run.push(cmd);
                        continue;
                    }
                }
                push_run_items = items.len();
                out.push(Item::Run(Run::Push(vec![cmd])));
                continue;
            }
            if matches!(&item, Item::Cmd(cmd) if ack_key(cmd).is_some()) {
                let Item::Cmd(cmd) = item else { unreachable!() };
                if let Some(Item::Run(Run::Ack(run))) = out.last_mut() {
                    if ack_key(&run[0]) == ack_key(&cmd) {
                        run.push(cmd);
                        continue;
                    }
                }
                out.push(Item::Run(Run::Ack(vec![cmd])));
                continue;
            }
            out.push(item);
        }
        out
    }

    /// Reply channels in result order: one per merged command.
    fn take_replies(run: &mut Run) -> Vec<Option<ReplyTx<QueueReply>>> {
        match run {
            Run::Push(cmds) | Run::Ack(cmds) => {
                cmds.iter_mut().map(|c| c.reply.take()).collect()
            }
        }
    }

    /// Results line up 1:1 with the peeled reply channels.
    fn execute_run(
        &self,
        conn: &Connection,
        run: Run,
        ctx: &ExecCtx,
    ) -> Result<(Vec<Result<QueueReply, BrokerError>>, Effects), CmdError> {
        match run {
            Run::Push(cmds) => {
                let commands = cmds.len();
                let mut name = String::new();
                let mut items: Vec<PushItem> = Vec::new();
                for cmd in cmds {
                    let QueueRequest::Push { name: n, items: it } = cmd.op else {
                        unreachable!("push run carries only push commands")
                    };
                    name = n;
                    items.extend(it);
                }
                let (_, fx) = recipes::push(conn, &name, &items, ctx)?;
                Ok((
                    (0..commands).map(|_| Ok(QueueReply::Unit)).collect(),
                    fx,
                ))
            }
            Run::Ack(cmds) => {
                let mut name = String::new();
                let mut acks: Vec<(Uuid, u64)> = Vec::with_capacity(cmds.len());
                for cmd in cmds {
                    let QueueRequest::Ack {
                        name: n,
                        id,
                        delivery_token,
                    } = cmd.op
                    else {
                        unreachable!("ack run carries only ack commands")
                    };
                    name = n;
                    acks.push((id, delivery_token));
                }
                let results = recipes::ack_many(conn, &name, &acks, ctx)?;
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

    /// Post-commit side effects: consumer wakeups, deleted-queue
    /// invalidation, continuation scheduling.
    fn apply_effects(
        &self,
        watches: &WatchMap,
        effects: Effects,
        followups: &mut VecDeque<Continuation>,
    ) {
        for queue in effects.wakes {
            if let Some(watch) = watches.get(&queue) {
                watch.send_modify(|v| *v = v.wrapping_add(1));
            }
        }
        for queue in effects.deleted_queues {
            watches.remove(&queue);
        }
        followups.extend(effects.followups);
    }
}

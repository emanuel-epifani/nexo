//! Stream `Domain` impl: everything broker-specific the durable engine
//! needs. The machinery (drain, transaction, savepoints, commit, effects,
//! shutdown) lives in `crate::durable::engine`.
//!
//! `Fetch`/`Seek`/`Leave`/`Disconnect` are barrier requests: they stop the
//! drain so leases and epoch changes are not held uncommitted behind a bulk
//! publish batch. Consecutive publishes for the same stream fold into a
//! `PubRun` (one recipe call, seqs sliced back per command); consecutive
//! acks for the same (stream, group, consumer) fold into an `AckRun` (one
//! `ack_many` call, outcomes mapped back per command).

use std::collections::VecDeque;

use dashmap::DashMap;
use rusqlite::Connection;
use tokio::sync::watch;

use crate::brokers::stream::domain::message::{ConsumerIdentity, PubItem};
use crate::brokers::stream::domain::ops::{
    Command, Continuation, Effects, StreamDomain, StreamReply, StreamRequest,
};
use crate::brokers::stream::domain::recipes::{self, ExecCtx};
use crate::brokers::BrokerError;
use crate::durable::{CmdError, Domain, Item, ReplyTx};

/// Max items a merged publish run may carry: bounds the rows one commit
/// writes so a queued barrier never waits behind an unbounded insert.
const MAX_PUB_RUN_ITEMS: usize = 4096;

/// Group-level watch signals: `watches[stream][group]` is bumped on every
/// commit that may have made new work visible (publishes, acks, lease
/// releases, epoch init pages). Removing the stream entry drops every sender,
/// which parked fetch waiters observe as `closed` → they resubmit and get
/// NOT_FOUND.
pub type WatchMap = DashMap<String, DashMap<String, watch::Sender<u64>>>;

/// A merged run of same-key commands. Each carries its folded `Command`s:
/// ops keep their payloads (no copies), reply slots are peeled by
/// `take_replies`.
pub enum Run {
    Publish(Vec<Command>),
    Ack(Vec<Command>),
}

impl Domain for StreamDomain {
    type Request = StreamRequest;
    type Reply = StreamReply;
    type Continuation = Continuation;
    type Run = Run;
    type Effects = Effects;
    type Ctx = ExecCtx;
    type Watches = WatchMap;

    fn make_ctx(&self, now_ms: u64) -> ExecCtx {
        ExecCtx {
            now_ms,
            fetch_response_bytes: self.0,
        }
    }

    fn merge_effects(into: &mut Effects, from: Effects) {
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

    /// Commands whose replies order leases/epoch changes against later work
    /// stop the drain so their commits are not delayed behind bulk writes.
    fn is_barrier(req: &StreamRequest) -> bool {
        matches!(
            req,
            StreamRequest::Fetch { .. }
                | StreamRequest::Seek { .. }
                | StreamRequest::Leave { .. }
                | StreamRequest::Disconnect { .. }
                | StreamRequest::Shutdown
        )
    }

    fn is_shutdown(req: &StreamRequest) -> bool {
        matches!(req, StreamRequest::Shutdown)
    }

    fn execute(
        &self,
        conn: &Connection,
        req: &StreamRequest,
        ctx: &ExecCtx,
    ) -> Result<(StreamReply, Effects), CmdError> {
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

    /// Fold runs of consecutive mergeable commands into `Run::Ack` (acks for
    /// the same stream/group/consumer) or `Run::Publish` (publishes for the
    /// same stream, capped by item count).
    fn merge_runs(&self, batch: Vec<Item<StreamDomain>>) -> Vec<Item<StreamDomain>> {
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
        let mut out: Vec<Item<StreamDomain>> = Vec::with_capacity(batch.len());
        let mut pub_run_items = 0usize;
        for item in batch {
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
            if matches!(&item, Item::Cmd(cmd) if matches!(cmd.op, StreamRequest::Publish { .. }))
            {
                let Item::Cmd(cmd) = item else { unreachable!() };
                let StreamRequest::Publish { name, items } = &cmd.op else {
                    unreachable!()
                };
                if let Some(Item::Run(Run::Publish(run))) = out.last_mut() {
                    let StreamRequest::Publish {
                        name: run_name, ..
                    } = &run[0].op
                    else {
                        unreachable!()
                    };
                    if *run_name == *name && pub_run_items + items.len() <= MAX_PUB_RUN_ITEMS {
                        pub_run_items += items.len();
                        run.push(cmd);
                        continue;
                    }
                }
                pub_run_items = items.len();
                out.push(Item::Run(Run::Publish(vec![cmd])));
                continue;
            }
            out.push(item);
        }
        out
    }

    fn take_replies(run: &mut Run) -> Vec<Option<ReplyTx<StreamReply>>> {
        match run {
            Run::Publish(cmds) | Run::Ack(cmds) => {
                cmds.iter_mut().map(|c| c.reply.take()).collect()
            }
        }
    }

    /// Execute one merged run; the returned vec maps 1:1 to the peeled
    /// reply slots (publish: seq slices per command; ack: one outcome per
    /// command shaped by its variant).
    fn execute_run(
        &self,
        conn: &Connection,
        run: Run,
        ctx: &ExecCtx,
    ) -> Result<(Vec<Result<StreamReply, BrokerError>>, Effects), CmdError> {
        match run {
            Run::Publish(cmds) => {
                // Items move out of the commands (no payload copies);
                // `counts` maps each seq range back to its own reply.
                let mut name = String::new();
                let mut items: Vec<PubItem> = Vec::new();
                let mut counts: Vec<usize> = Vec::with_capacity(cmds.len());
                for mut cmd in cmds {
                    if let StreamRequest::Publish { name: n, items: it } = &mut cmd.op {
                        name = std::mem::take(n);
                        counts.push(it.len());
                        items.append(it);
                    } else {
                        unreachable!("publish run carries only publish commands")
                    }
                }
                let (reply, fx) = recipes::publish(conn, &name, &items, ctx)?;
                let mut seqs = match reply {
                    StreamReply::Published(seqs) => seqs.into_iter(),
                    _ => unreachable!("publish returns Published"),
                };
                let results = counts
                    .iter()
                    .map(|n| {
                        Ok(StreamReply::Published(
                            seqs.by_ref().take(*n).collect::<Vec<u64>>(),
                        ))
                    })
                    .collect();
                Ok((results, fx))
            }
            Run::Ack(cmds) => {
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
                    _ => unreachable!("ack run carries only ack commands"),
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
                        _ => unreachable!("ack run carries only ack commands"),
                    }
                }
                let (results, fx) = recipes::ack_many(conn, name, group, identity, &acks)?;
                let mut results = results.into_iter();
                let outcomes = cmds
                    .iter()
                    .zip(&counts)
                    .map(|(cmd, n)| {
                        let slice: Vec<Result<StreamReply, BrokerError>> =
                            results.by_ref().take(*n).collect();
                        match &cmd.op {
                            StreamRequest::Ack { .. } => slice
                                .into_iter()
                                .next()
                                .unwrap_or_else(|| {
                                    Err(BrokerError::new(
                                        crate::brokers::BrokerErrorKind::Internal,
                                        "Ack run result missing",
                                    ))
                                }),
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
                        }
                    })
                    .collect();
                Ok((outcomes, fx))
            }
        }
    }

    /// Post-commit side effects: group wakeups, deleted-stream invalidation,
    /// continuation scheduling.
    fn apply_effects(
        &self,
        watches: &WatchMap,
        effects: Effects,
        followups: &mut VecDeque<Continuation>,
    ) {
        for (stream, group) in effects.wakes {
            if let Some(groups) = watches.get(&stream) {
                if let Some(watch) = groups.get(&group) {
                    watch.send_modify(|v| *v = v.wrapping_add(1));
                }
            }
        }
        for stream in effects.deleted_streams {
            watches.remove(&stream);
        }
        followups.extend(effects.followups);
    }
}

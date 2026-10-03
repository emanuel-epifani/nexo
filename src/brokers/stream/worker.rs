//! Stream `Domain` impl: everything broker-specific the durable engine
//! needs. The machinery (drain, transaction, savepoints, commit, effects,
//! shutdown) lives in `crate::durable::engine`.
//!
//! `Fetch`/`Seek`/`Leave`/`Disconnect` are barrier requests: they stop the
//! drain so leases and epoch changes are not held uncommitted behind a bulk
//! publish batch. Consecutive publishes for the same stream fold into a
//! `PubRun` (one recipe call, seqs sliced back per command); consecutive
//! batch updates for the same (stream, group, consumer) fold into an
//! `UpdateRun` (one `update_batches` call, outcomes mapped back per command).

use std::collections::VecDeque;

use dashmap::DashMap;
use rusqlite::Connection;
use tokio::sync::watch;

use crate::brokers::stream::domain::message::{BatchUpdate, ConsumerIdentity, PubItem};
use crate::brokers::stream::domain::ops::{
    Command, Continuation, Effects, StreamDomain, StreamReply, StreamRequest,
};
use crate::brokers::stream::domain::recipes::{self, ExecCtx};
use crate::brokers::BrokerError;
use crate::durable::{CmdError, Domain, Item, ReplyTx};

/// Max items a merged publish run may carry: bounds the rows one commit
/// writes so a queued barrier never waits behind an unbounded insert.
const MAX_PUB_RUN_ITEMS: usize = 4096;

const MAX_UPDATE_RUN_ITEMS: usize = 4096;

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
    Update(Vec<Command>),
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

    /// Fold runs of consecutive mergeable commands into `Run::Update`
    /// (updates for the same stream/group/consumer) or `Run::Publish`
    /// (publishes for the same stream, capped by item count).
    fn merge_runs(&self, batch: Vec<Item<StreamDomain>>) -> Vec<Item<StreamDomain>> {
        fn update_key(cmd: &Command) -> Option<(&String, &String, &ConsumerIdentity)> {
            match &cmd.op {
                StreamRequest::UpdateBatch {
                    name,
                    group,
                    identity,
                    ..
                }
                | StreamRequest::UpdateBatches {
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
        let mut update_run_items = 0usize;
        for item in batch {
            if matches!(&item, Item::Cmd(cmd) if update_key(cmd).is_some()) {
                let Item::Cmd(cmd) = item else { unreachable!() };
                let n = match &cmd.op {
                    StreamRequest::UpdateBatch { .. } => 1,
                    StreamRequest::UpdateBatches { updates, .. } => updates.len(),
                    _ => unreachable!(),
                };
                if let Some(Item::Run(Run::Update(run))) = out.last_mut() {
                    if update_key(&run[0]) == update_key(&cmd)
                        && update_run_items + n <= MAX_UPDATE_RUN_ITEMS
                    {
                        update_run_items += n;
                        run.push(cmd);
                        continue;
                    }
                }
                update_run_items = n;
                out.push(Item::Run(Run::Update(vec![cmd])));
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
            Run::Publish(cmds) | Run::Update(cmds) => {
                cmds.iter_mut().map(|c| c.reply.take()).collect()
            }
        }
    }

    /// Execute one merged run; the returned vec maps 1:1 to the peeled
    /// reply slots (publish: seq slices per command; update: one outcome per
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
            Run::Update(cmds) => {
                let (name, group, identity) = match &cmds[0].op {
                    StreamRequest::UpdateBatch {
                        name,
                        group,
                        identity,
                        ..
                    }
                    | StreamRequest::UpdateBatches {
                        name,
                        group,
                        identity,
                        ..
                    } => (name, group, identity),
                    _ => unreachable!("update run carries only update commands"),
                };
                // Flatten every command's updates into one update_batches
                // call; `counts` remembers how many items each command owns
                // so replies can be sliced back per command.
                let mut updates: Vec<BatchUpdate> = Vec::new();
                let mut counts: Vec<usize> = Vec::with_capacity(cmds.len());
                for cmd in &cmds {
                    match &cmd.op {
                        StreamRequest::UpdateBatch { update, .. } => {
                            updates.push(*update);
                            counts.push(1);
                        }
                        StreamRequest::UpdateBatches { updates: more, .. } => {
                            updates.extend_from_slice(more);
                            counts.push(more.len());
                        }
                        _ => unreachable!("update run carries only update commands"),
                    }
                }
                let (outcomes, fx) =
                    recipes::update_batches(conn, name, group, identity, &updates, ctx)?;
                let mut outcomes = outcomes.into_iter();
                let results = cmds
                    .iter()
                    .zip(&counts)
                    .map(|(cmd, n)| {
                        let slice: Vec<Option<crate::brokers::BrokerErrorKind>> =
                            outcomes.by_ref().take(*n).collect();
                        match &cmd.op {
                            StreamRequest::UpdateBatch { .. } => match slice.into_iter().next() {
                                Some(Some(kind)) => Err(BrokerError::new(kind, "Batch update rejected")),
                                Some(None) => Ok(StreamReply::Unit),
                                None => Err(BrokerError::new(
                                    crate::brokers::BrokerErrorKind::Internal,
                                    "Batch update produced no outcome",
                                )),
                            },
                            StreamRequest::UpdateBatches { updates: mine, .. } => {
                                let failures = mine
                                    .iter()
                                    .zip(slice)
                                    .filter_map(|(u, o)| o.map(|k| (u.receipt, k)))
                                    .collect();
                                Ok(StreamReply::BatchOutcome(failures))
                            }
                            _ => unreachable!(),
                        }
                    })
                    .collect();
                Ok((results, fx))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::brokers::stream::domain::message::BatchDisposition;

    fn update_cmd(n: usize) -> Command {
        Command {
            op: StreamRequest::UpdateBatches {
                name: "s".into(),
                group: "g".into(),
                identity: ConsumerIdentity {
                    connection_id: "conn".into(),
                    consumer_id: "c".into(),
                    epoch: [0u8; 16],
                },
                updates: vec![
                    BatchUpdate {
                        receipt: [0u8; 16],
                        completed: 0,
                        disposition: BatchDisposition::Checkpoint,
                    };
                    n
                ],
            },
            bytes: 0,
            reply: None,
        }
    }

    #[test]
    fn update_runs_split_at_item_cap() {
        let domain = StreamDomain(1 << 20);
        let runs = domain.merge_runs(vec![
            Item::Cmd(update_cmd(3000)),
            Item::Cmd(update_cmd(3000)),
        ]);
        assert_eq!(runs.len(), 2);
        let runs = domain.merge_runs(vec![
            Item::Cmd(update_cmd(2000)),
            Item::Cmd(update_cmd(2000)),
        ]);
        assert_eq!(runs.len(), 1, "requests within the cap still fold");
        let runs = domain.merge_runs(vec![Item::Cmd(update_cmd(5000))]);
        assert_eq!(runs.len(), 1, "an oversized single request runs alone");
    }
}

//! Durable engine: the shared SQLite-primary pipeline used by the stateful
//! brokers (queue, stream — and anything else that wants commit-confirmed
//! durability).
//!
//! Layout:
//! - `storage`: `Store` + `Spec` — one WAL database per domain, process
//!   lock file, fail-closed layout/schema validation, checkpoints.
//! - `engine`: `Domain` trait + the writer thread — bounded drain, one
//!   transaction per batch, per-command savepoints, run merging, staged
//!   effects applied only after commit, continuations, shutdown.
//! - `handle`: `EngineHandle`/`PendingReply` — the async submit side
//!   (admission, channel, closed flag) held by the broker's manager.
//! - `types`: error taxonomy (`CmdError`), `Admission`, `Stats`,
//!   `ReplyTx`, and the `exec`/`query_*`/`sql` helpers all recipes share.
//!
//! A domain provides: `Request`/`Reply`/`Continuation`/`Run`/`Effects`/
//! `Ctx`/`Watches` types plus the dispatch methods (`execute`,
//! `run_continuation`, `merge_runs`, `apply_effects`). The engine owns
//! ordering, transactions, aborts and reply-after-commit — every fix and
//! optimization in that machinery lands on all domains at once.

mod engine;
mod handle;
mod storage;
mod types;

pub use engine::{spawn, Command, Domain, Item};
pub use handle::{EngineHandle, PendingReply};
pub use storage::{Spec, Store};
pub use types::{
    exec, expected, fatal, now_millis, query_one, query_vec, sql, Admission, CmdError, ExecCtx,
    ReplyTx, Stats, R,
};

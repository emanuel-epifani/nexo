//! Engine primitives shared by every durable domain: error taxonomy, batch
//! context, byte-budget admission, diagnostics and the SQL statement helpers
//! all recipes run through.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, Row};
use tokio::sync::{oneshot, Semaphore};

use crate::brokers::BrokerError;

/// Channel a command's reply is sent on after its transaction commits.
pub type ReplyTx<R> = oneshot::Sender<Result<R, BrokerError>>;

/// `Expected` is a per-command rejection — the command's savepoint rolls
/// back and the error becomes its reply. `Fatal` is a storage-level failure
/// that aborts the whole batch transaction.
#[derive(Debug)]
pub enum CmdError {
    Expected(BrokerError),
    Fatal(BrokerError),
}

impl CmdError {
    pub fn into_error(self) -> BrokerError {
        match self {
            Self::Expected(e) | Self::Fatal(e) => e,
        }
    }
}

/// Any unexpected SQLite error aborts the whole batch transaction.
impl From<rusqlite::Error> for CmdError {
    fn from(e: rusqlite::Error) -> Self {
        CmdError::Fatal(BrokerError::storage(format!("SQLite error: {e}")))
    }
}

/// Shorthand for recipe result types.
pub type R<T> = Result<T, CmdError>;

pub fn expected(e: BrokerError) -> CmdError {
    CmdError::Expected(e)
}

pub fn fatal(context: &str, e: impl std::fmt::Display) -> CmdError {
    CmdError::Fatal(BrokerError::storage(format!("{context}: {e}")))
}

/// Map a bare SQLite result into a `Fatal` error with context.
pub fn sql<T>(result: rusqlite::Result<T>, context: &str) -> R<T> {
    result.map_err(|e| fatal(context, e))
}

/// Minimal batch context handed to recipes. Domains needing extra runtime
/// config define their own `Ctx` type and capture it in their `Domain`
/// impl (e.g. the stream fetch byte budget).
#[derive(Clone, Copy)]
pub struct ExecCtx {
    pub now_ms: u64,
}

/// Per-engine writer diagnostics. Shared between the worker thread (which
/// increments them) and the manager (`sql_stats` reads).
#[derive(Default)]
pub struct Stats {
    pub sql_stmts: AtomicU64,
    pub batches: AtomicU64,
    pub exec_ns: AtomicU64,
    pub commit_ns: AtomicU64,
}

thread_local! {
    /// The writer thread's stats. Recipe helpers run on that thread, so a
    /// thread-local handle counts statements per engine instance instead of
    /// a process-global counter that parallel tests/managers would pollute.
    static ENGINE_STATS: RefCell<Option<Arc<Stats>>> = const { RefCell::new(None) };
}

pub(crate) fn install_stats(stats: Arc<Stats>) {
    ENGINE_STATS.with(|s| *s.borrow_mut() = Some(stats));
}

fn bump_sql() {
    ENGINE_STATS.with(|s| {
        if let Some(stats) = s.borrow().as_ref() {
            stats.sql_stmts.fetch_add(1, Ordering::Relaxed);
        }
    });
}

/// All statement access goes through `prepare_cached`: every recipe runs on
/// the writer's single connection, so cached plans amortize parse/plan cost
/// across commands and transactions.
pub fn exec<P: rusqlite::Params>(
    conn: &Connection,
    text: &str,
    params: P,
) -> rusqlite::Result<usize> {
    bump_sql();
    conn.prepare_cached(text)?.execute(params)
}

pub fn query_one<P: rusqlite::Params, T>(
    conn: &Connection,
    text: &str,
    params: P,
    map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    bump_sql();
    conn.prepare_cached(text)?.query_row(params, map)
}

/// Prepare + map + collect in one step. The collect result is bound to a
/// local first so the `MappedRows` temporary dies before the `Statement`
/// it borrows (block-tail temporaries would otherwise outlive `stmt`).
pub fn query_vec<P: rusqlite::Params, T>(
    conn: &Connection,
    text: &str,
    params: P,
    map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ctx: &'static str,
) -> R<Vec<T>> {
    bump_sql();
    let mut stmt = sql(conn.prepare_cached(text), ctx)?;
    let rows = stmt.query_map(params, map)?.collect::<Result<Vec<_>, _>>();
    sql(rows, ctx)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Regression: a submitter parked on a saturated budget must wake when
    /// the worker releases bytes. A hand-rolled Notify loop could drop a
    /// wakeup landing between the failed check and waiter registration,
    /// stalling the connection read loop; the semaphore makes registration
    /// atomic with the check.
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

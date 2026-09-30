//! Shared SQLite store: lifecycle, schema, process ownership and recovery.
//!
//! One `queues.sqlite3` (WAL) inside the persistence root holds every queue's
//! messages and DLQ entries, so a commit is the only unit of durability.
//! `synchronous = NORMAL` is deliberate: a successful commit promises
//! crash-consistency of the database file, not survival of a power loss, and
//! producer retries may duplicate messages — exactly-once external effects
//! are not claimed.
//!
//! Startup fails closed: an unrecognized directory layout (including the
//! legacy per-queue `{name}.db` / `{name}.config.json` files), a schema that
//! is not version `SCHEMA_VERSION`, an unsafe symlink, or another live
//! process holding the lock are all startup errors. Nothing is ever
//! recreated over existing bytes.

use std::fs::{self, File, OpenOptions};
use std::path::Path;

use rusqlite::Connection;

use crate::brokers::queue::domain::definition::QueueConfig;
use crate::brokers::queue::domain::ops::Continuation;
use crate::brokers::queue::domain::types::{
    DB_FILE_NAME, LOCK_FILE_NAME, MSG_INFLIGHT, MSG_READY, SCHEMA_VERSION,
};
use crate::brokers::queue::worker::now_millis;
use crate::brokers::BrokerError;

/// Bound on a single recovery slice so startup stays responsive on stores
/// with many in-flight leases; the sweep loops until clean.
const RECOVERY_SLICE: usize = 1024;

pub(crate) const SCHEMA: &str = r#"
CREATE TABLE schema_meta (
    schema_version INTEGER NOT NULL
);

-- next_ready_seq / next_token / next_dlq_seq hold the LAST ISSUED value
-- (0 = none yet); consumers of a counter bump it inside their transaction.
CREATE TABLE queues (
    id             BLOB(16) PRIMARY KEY,
    name           TEXT NOT NULL,
    deleted        INTEGER NOT NULL DEFAULT 0,
    config_json    TEXT NOT NULL,
    next_ready_seq INTEGER NOT NULL,
    next_token     INTEGER NOT NULL,
    next_dlq_seq   INTEGER NOT NULL
);
-- Deleted queues keep their row until GC finishes, but must not block reuse
-- of the name, so uniqueness applies only to live rows.
CREATE UNIQUE INDEX queues_name_live ON queues(name) WHERE deleted = 0;

CREATE TABLE messages (
    queue_id       BLOB(16) NOT NULL REFERENCES queues(id),
    id             BLOB(16) NOT NULL,
    payload        BLOB NOT NULL,
    priority       INTEGER NOT NULL,
    state          INTEGER NOT NULL CHECK (state IN (0, 1)),
    ready_seq      INTEGER NOT NULL,
    delivery_token INTEGER NOT NULL,
    visible_at     INTEGER NOT NULL,
    attempts       INTEGER NOT NULL CHECK (attempts >= 0),
    failure_reason TEXT,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (queue_id, id),
    -- INFLIGHT iff the lease pair is set: the illegal intermediate states
    -- are not representable.
    CHECK ((state = 1) = (delivery_token <> 0 AND visible_at <> 0))
);
-- The "ready" heap of the old in-memory design: highest priority first,
-- FIFO (ready_seq) within a priority.
CREATE INDEX ready_idx ON messages(queue_id, priority DESC, ready_seq) WHERE state = 0;
-- The "in_flight" heap: earliest lease expiry first, scanned by the sweeper.
CREATE INDEX inflight_deadlines ON messages(visible_at, queue_id) WHERE state = 1;

CREATE TABLE dlq (
    queue_id       BLOB(16) NOT NULL REFERENCES queues(id),
    dlq_seq        INTEGER NOT NULL,
    id             BLOB(16) NOT NULL,
    payload        BLOB NOT NULL,
    priority       INTEGER NOT NULL,
    attempts       INTEGER NOT NULL,
    created_at     INTEGER NOT NULL,
    failed_at      INTEGER NOT NULL,
    failure_reason TEXT NOT NULL,
    PRIMARY KEY (queue_id, dlq_seq)
);
-- Point lookups for replay/delete by message id.
CREATE INDEX dlq_id ON dlq(queue_id, id);
"#;

/// Owns the SQLite connection and the process lock file.
///
/// The connection lives on the dedicated writer thread: `Store` is `Send`
/// but must never be touched from async code.
pub struct Store {
    pub(crate) conn: Connection,
    _lock_file: File,
}

impl Store {
    /// Open (or create) the shared database under `root`, acquire the
    /// ownership lock, validate/create the schema and run crash recovery.
    ///
    /// Returns the list of continuations the worker must resume
    /// (deleted-queue GC).
    pub fn open(root: &Path) -> Result<(Self, Vec<Continuation>), BrokerError> {
        check_layout(root)?;
        fs::create_dir_all(root).map_err(|e| {
            BrokerError::storage(format!("Cannot create queue persistence dir: {e}"))
        })?;

        let db_path = root.join(DB_FILE_NAME);
        reject_symlink(&db_path)?;
        let lock_path = root.join(LOCK_FILE_NAME);
        reject_symlink(&lock_path)?;

        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| BrokerError::storage(format!("Cannot open queue lock file: {e}")))?;
        lock_file.try_lock().map_err(|_| {
            BrokerError::storage(format!(
                "Another process holds the queue storage lock at {}",
                lock_path.display()
            ))
        })?;

        let conn = Connection::open(&db_path)
            .map_err(|e| BrokerError::storage(format!("Cannot open queue database: {e}")))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA cache_size = -64000;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 268435456;
             PRAGMA wal_autocheckpoint = 4000;",
        )
        .map_err(|e| BrokerError::storage(format!("Cannot configure queue database: {e}")))?;
        conn.set_prepared_statement_cache_capacity(256);

        let fresh = !table_exists(&conn, "schema_meta")?;
        if fresh {
            conn.execute_batch(SCHEMA)
                .map_err(|e| BrokerError::storage(format!("Cannot create queue schema: {e}")))?;
            conn.execute(
                "INSERT INTO schema_meta(schema_version) VALUES (?1)",
                [SCHEMA_VERSION],
            )
            .map_err(|e| BrokerError::storage(format!("Cannot stamp schema version: {e}")))?;
        } else {
            let version: i64 = conn
                .query_row(
                    "SELECT schema_version FROM schema_meta LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .map_err(|e| {
                    BrokerError::storage(format!("Cannot read queue schema version: {e}"))
                })?;
            if version != SCHEMA_VERSION {
                return Err(BrokerError::storage(format!(
                    "Unsupported queue schema version {version} (expected {SCHEMA_VERSION})"
                )));
            }
            validate_schema(&conn)?;
        }

        let mut store = Store {
            conn,
            _lock_file: lock_file,
        };
        let continuations = store.recover()?;
        Ok((store, continuations))
    }

    /// Crash recovery: every in-flight lease dies with the process, so each
    /// leased message is either returned to READY (fresh ready_seq, reason
    /// kept) or moved to the DLQ when `attempts` already reached the queue's
    /// `max_deliveries`. Rows of deleted queues are skipped — GC owns them.
    fn recover(&mut self) -> Result<Vec<Continuation>, BrokerError> {
        let now = now_millis() as i64;
        let tx = self
            .conn
            .transaction()
            .map_err(|e| BrokerError::storage(format!("Recovery transaction failed: {e}")))?;
        loop {
            let leased: Vec<(Vec<u8>, Vec<u8>, i64, String)> = tx
                .prepare_cached(
                    "SELECT m.queue_id, m.id, m.attempts, q.config_json
                     FROM messages m JOIN queues q ON q.id = m.queue_id
                     WHERE m.state = ?1 AND q.deleted = 0 LIMIT ?2",
                )
                .and_then(|mut s| {
                    s.query_map(rusqlite::params![MSG_INFLIGHT, RECOVERY_SLICE as i64], |r| {
                        Ok((
                            r.get::<_, Vec<u8>>(0)?,
                            r.get::<_, Vec<u8>>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, String>(3)?,
                        ))
                    })?
                    .collect()
                })
                .map_err(|e| BrokerError::storage(format!("Recovery lease scan failed: {e}")))?;
            if leased.is_empty() {
                break;
            }
            for (queue_id, id, attempts, config_json) in leased {
                let max_deliveries: i64 = serde_json::from_str::<QueueConfig>(&config_json)
                    .map(|c| c.max_deliveries as i64)
                    .map_err(|e| {
                        BrokerError::storage(format!(
                            "Corrupt storage: persisted queue config is invalid: {e}"
                        ))
                    })?;
                if attempts >= max_deliveries {
                    // Exhausted deliveries: park into the DLQ at the tail.
                    tx.execute(
                        "UPDATE queues SET next_dlq_seq = next_dlq_seq + 1 WHERE id = ?1",
                        [&queue_id],
                    )
                    .map_err(|e| BrokerError::storage(format!("Recovery dlq_seq bump: {e}")))?;
                    tx.execute(
                        "INSERT INTO dlq(queue_id, dlq_seq, id, payload, priority, attempts,
                                         created_at, failed_at, failure_reason)
                         SELECT queue_id, (SELECT next_dlq_seq FROM queues WHERE id = ?1),
                                id, payload, priority, attempts, created_at, ?2,
                                COALESCE(failure_reason, 'Timeout')
                         FROM messages WHERE queue_id = ?1 AND id = ?3",
                        rusqlite::params![queue_id, now, id],
                    )
                    .map_err(|e| BrokerError::storage(format!("Recovery DLQ move: {e}")))?;
                    tx.execute(
                        "DELETE FROM messages WHERE queue_id = ?1 AND id = ?2",
                        rusqlite::params![queue_id, id],
                    )
                    .map_err(|e| BrokerError::storage(format!("Recovery DLQ delete: {e}")))?;
                } else {
                    // Back to READY at the tail of the queue, lease cleared.
                    tx.execute(
                        "UPDATE queues SET next_ready_seq = next_ready_seq + 1 WHERE id = ?1",
                        [&queue_id],
                    )
                    .map_err(|e| {
                        BrokerError::storage(format!("Recovery ready_seq bump: {e}"))
                    })?;
                    tx.execute(
                        "UPDATE messages SET state = ?1, visible_at = 0, delivery_token = 0,
                                ready_seq = (SELECT next_ready_seq FROM queues WHERE id = ?2)
                         WHERE queue_id = ?2 AND id = ?3",
                        rusqlite::params![MSG_READY, queue_id, id],
                    )
                    .map_err(|e| BrokerError::storage(format!("Recovery requeue: {e}")))?;
                }
            }
        }
        tx.commit()
            .map_err(|e| BrokerError::storage(format!("Recovery commit failed: {e}")))?;

        let deleted: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM queues WHERE deleted = 1",
                [],
                |r| r.get(0),
            )
            .map_err(|e| BrokerError::storage(format!("Recovery GC scan failed: {e}")))?;
        let mut continuations = Vec::new();
        if deleted > 0 {
            continuations.push(Continuation::Gc);
        }
        Ok(continuations)
    }

    /// Passive checkpoint, run by the worker when idle or on shutdown.
    pub fn checkpoint(&self, truncate: bool) -> Result<(), BrokerError> {
        let sql = if truncate {
            "PRAGMA wal_checkpoint(TRUNCATE)"
        } else {
            "PRAGMA wal_checkpoint(PASSIVE)"
        };
        self.conn
            .query_row(sql, [], |r| r.get::<_, i64>(0))
            .map_err(|e| BrokerError::storage(format!("WAL checkpoint failed: {e}")))?;
        Ok(())
    }
}

/// Whitelisted files of the SQLite layout. Anything else in the root —
/// per-queue `{name}.db` files, `{name}.config.json` sidecars — is the
/// legacy layout or foreign data and must stop startup instead of being
/// silently overwritten.
fn check_layout(root: &Path) -> Result<(), BrokerError> {
    if !root.exists() {
        return Ok(());
    }
    if !root.is_dir() {
        return Err(BrokerError::storage(format!(
            "Queue persistence path {} is not a directory",
            root.display()
        )));
    }
    for entry in fs::read_dir(root)
        .map_err(|e| BrokerError::storage(format!("Cannot inspect queue persistence dir: {e}")))?
    {
        let entry =
            entry.map_err(|e| BrokerError::storage(format!("Cannot list queue dir: {e}")))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let known = name == DB_FILE_NAME
            || name == LOCK_FILE_NAME
            || name == format!("{DB_FILE_NAME}-wal")
            || name == format!("{DB_FILE_NAME}-shm")
            || name == format!("{DB_FILE_NAME}-journal");
        if !known {
            return Err(BrokerError::storage(format!(
                "Unrecognized entry '{}' in queue persistence dir {} — \
                 refusing to start over a legacy or foreign layout",
                name,
                root.display()
            )));
        }
    }
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), BrokerError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(BrokerError::storage(format!(
            "Queue storage file {} is a symlink; refusing to use it",
            path.display()
        ))),
        Ok(_) | Err(_) => Ok(()),
    }
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool, BrokerError> {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .map_err(|e| BrokerError::storage(format!("Cannot inspect queue schema: {e}")))
}

/// Fail closed when a version-stamped database is missing tables or indexes
/// (partial schema creation, manual tampering).
fn validate_schema(conn: &Connection) -> Result<(), BrokerError> {
    const OBJECTS: &[&str] = &[
        "schema_meta",
        "queues",
        "queues_name_live",
        "messages",
        "ready_idx",
        "inflight_deadlines",
        "dlq",
        "dlq_id",
    ];
    for name in OBJECTS {
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                [name],
                |r| r.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(|e| BrokerError::storage(format!("Cannot inspect queue schema: {e}")))?;
        if !exists {
            return Err(BrokerError::storage(format!(
                "Queue database schema is missing '{name}'"
            )));
        }
    }
    Ok(())
}

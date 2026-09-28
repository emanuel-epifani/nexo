//! Shared SQLite store: lifecycle, schema, process ownership and recovery.
//!
//! One `streams.sqlite3` (WAL) inside the persistence root holds every
//! stream's events and every group's delivery state, so a commit is the only
//! unit of durability. `synchronous = NORMAL` is deliberate: a successful
//! commit promises crash-consistency of the database file, not survival of
//! a power loss, and producer retries may duplicate events — exactly-once
//! external effects are not claimed.
//!
//! Startup fails closed: an unrecognized directory layout, a schema that is
//! not version `SCHEMA_VERSION`, an unsafe symlink, or another live process
//! holding the lock are all startup errors. Nothing is ever recreated over
//! existing bytes.

use std::fs::{self, File, OpenOptions};
use std::path::Path;

use rusqlite::Connection;

use crate::brokers::stream::domain::ops::Continuation;
use crate::brokers::stream::domain::types::{DB_FILE_NAME, LOCK_FILE_NAME, SCHEMA_VERSION};
use crate::brokers::BrokerError;

/// Bound on a single recovery slice so startup stays responsive on stores
/// with many in-flight leases; the sweep loops until clean.
const RECOVERY_SLICE: usize = 1024;

pub(crate) const SCHEMA: &str = r#"
CREATE TABLE schema_meta (
    schema_version INTEGER NOT NULL
);

CREATE TABLE streams (
    id                 BLOB(16) PRIMARY KEY,
    name               TEXT NOT NULL,
    deleted            INTEGER NOT NULL DEFAULT 0,
    config_json        TEXT NOT NULL,
    last_seq           BLOB(8) NOT NULL,
    retained_after_seq BLOB(8) NOT NULL,
    last_key_id        INTEGER NOT NULL,
    logical_bytes      INTEGER NOT NULL CHECK (logical_bytes >= 0)
);
-- Deleted streams keep their row until GC finishes, but must not block reuse
-- of the name, so uniqueness applies only to live rows.
CREATE UNIQUE INDEX streams_name_live ON streams(name) WHERE deleted = 0;

CREATE TABLE stream_keys (
    stream_id           BLOB(16) NOT NULL REFERENCES streams(id),
    key_id              INTEGER NOT NULL CHECK (key_id >= 0),
    key                 BLOB NOT NULL,
    last_pos            BLOB(8) NOT NULL,
    retained_after_pos  BLOB(8) NOT NULL,
    PRIMARY KEY (stream_id, key_id),
    CHECK (key_id > 0 OR length(key) = 0)
);
CREATE UNIQUE INDEX stream_keys_key ON stream_keys(stream_id, key);

CREATE TABLE events (
    stream_id     BLOB(16) NOT NULL,
    seq           BLOB(8) NOT NULL,
    key_id        INTEGER NOT NULL,
    key_pos       BLOB(8) NOT NULL,
    timestamp_ms  INTEGER NOT NULL,
    payload       BLOB NOT NULL,
    payload_bytes INTEGER NOT NULL CHECK (payload_bytes >= 0),
    logical_bytes INTEGER NOT NULL CHECK (logical_bytes > 0),
    PRIMARY KEY (stream_id, seq),
    FOREIGN KEY (stream_id, key_id) REFERENCES stream_keys(stream_id, key_id)
);
CREATE UNIQUE INDEX events_key_pos ON events(stream_id, key_id, key_pos);
CREATE INDEX events_by_key_seq ON events(stream_id, key_id, seq);

CREATE TABLE groups (
    id           BLOB(16) PRIMARY KEY,
    stream_id    BLOB(16) NOT NULL REFERENCES streams(id),
    name         TEXT NOT NULL,
    generation   BLOB(8) NOT NULL,
    active_epoch BLOB(16),
    UNIQUE (stream_id, name),
    UNIQUE (id, stream_id),
    -- Group and epoch are inserted inside one transaction in either order.
    FOREIGN KEY (active_epoch, stream_id)
        REFERENCES group_epochs(id, stream_id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE group_epochs (
    id                 BLOB(16) NOT NULL,
    group_id           BLOB(16) NOT NULL,
    stream_id          BLOB(16) NOT NULL,
    start_after_seq    BLOB(8) NOT NULL,
    initialized        INTEGER NOT NULL,
    init_key_cursor    INTEGER NOT NULL,
    init_key_target    INTEGER NOT NULL,
    -- Keyless source state lives on the epoch row: it is a computed
    -- admission watermark, not a deliverable row.
    keyless_cursor_pos BLOB(8) NOT NULL,
    keyless_ticket     INTEGER NOT NULL,
    next_ready_ticket  INTEGER NOT NULL,
    pending_count      INTEGER NOT NULL CHECK (pending_count >= 0),
    max_pending        INTEGER NOT NULL CHECK (max_pending > 0),
    PRIMARY KEY (id),
    UNIQUE (id, stream_id),
    FOREIGN KEY (group_id, stream_id) REFERENCES groups(id, stream_id)
);
CREATE INDEX epochs_of_stream ON group_epochs(stream_id);
CREATE INDEX group_active_epoch ON groups(stream_id, active_epoch);

CREATE TABLE key_lanes (
    epoch_id       BLOB(16) NOT NULL,
    stream_id      BLOB(16) NOT NULL,
    key_id         INTEGER NOT NULL CHECK (key_id > 0),
    cursor_pos     BLOB(8) NOT NULL,
    normal_seq     BLOB(8),
    head_seq       BLOB(8),
    head_pos       BLOB(8),
    head_origin    INTEGER,
    state          INTEGER NOT NULL CHECK (state BETWEEN 0 AND 3),
    attempts       INTEGER NOT NULL CHECK (attempts >= 0),
    ready_ticket   INTEGER NOT NULL,
    receipt        BLOB(16),
    owner          TEXT,
    connection_id  TEXT,
    deadline_ms    INTEGER,
    PRIMARY KEY (epoch_id, key_id),
    -- LEASED iff the lease quadruple is set.
    CHECK ((state = 2) = (receipt IS NOT NULL AND owner IS NOT NULL
                          AND connection_id IS NOT NULL AND deadline_ms IS NOT NULL)),
    -- READY/LEASED iff a head exists.
    CHECK ((state = 1 OR state = 2) =
           (head_seq IS NOT NULL AND head_pos IS NOT NULL AND head_origin IS NOT NULL)),
    -- PARKED lanes carry no frontier or head: every original is resolved or parked.
    CHECK (state != 3 OR (normal_seq IS NULL AND head_seq IS NULL AND receipt IS NULL)),
    FOREIGN KEY (epoch_id, stream_id) REFERENCES group_epochs(id, stream_id),
    FOREIGN KEY (stream_id, key_id) REFERENCES stream_keys(stream_id, key_id)
);
CREATE INDEX ready_lanes ON key_lanes(epoch_id, ready_ticket, key_id) WHERE state = 1;
CREATE INDEX lane_deadlines ON key_lanes(deadline_ms, epoch_id, key_id) WHERE state = 2;
CREATE INDEX lane_owners ON key_lanes(connection_id, owner, epoch_id) WHERE state = 2;
CREATE INDEX lane_normal_frontier ON key_lanes(epoch_id, normal_seq)
    WHERE normal_seq IS NOT NULL AND state <> 3;
CREATE INDEX lane_head_expiry ON key_lanes(stream_id, head_seq) WHERE head_seq IS NOT NULL;
-- Per-key sweep used by retention (any state) and by lane fan-out checks.
CREATE INDEX lanes_by_key ON key_lanes(stream_id, key_id, epoch_id);

CREATE TABLE keyless_deliveries (
    epoch_id      BLOB(16) NOT NULL,
    stream_id     BLOB(16) NOT NULL,
    seq           BLOB(8) NOT NULL,
    key_pos       BLOB(8) NOT NULL,
    origin        INTEGER NOT NULL CHECK (origin IN (0, 1)),
    state         INTEGER NOT NULL CHECK (state IN (1, 2)),
    attempts      INTEGER NOT NULL CHECK (attempts >= 0),
    ready_ticket  INTEGER NOT NULL,
    receipt       BLOB(16),
    owner         TEXT,
    connection_id TEXT,
    deadline_ms   INTEGER,
    PRIMARY KEY (epoch_id, seq),
    CHECK ((state = 2) = (receipt IS NOT NULL AND owner IS NOT NULL
                          AND connection_id IS NOT NULL AND deadline_ms IS NOT NULL)),
    FOREIGN KEY (epoch_id, stream_id) REFERENCES group_epochs(id, stream_id),
    FOREIGN KEY (stream_id, seq) REFERENCES events(stream_id, seq)
);
CREATE INDEX keyless_ready ON keyless_deliveries(epoch_id, ready_ticket, seq) WHERE state = 1;
CREATE INDEX keyless_deadlines ON keyless_deliveries(deadline_ms, epoch_id, seq) WHERE state = 2;
CREATE INDEX keyless_owners ON keyless_deliveries(connection_id, owner, epoch_id) WHERE state = 2;
CREATE INDEX keyless_original_frontier ON keyless_deliveries(epoch_id, seq) WHERE origin = 0;
CREATE INDEX keyless_expiry ON keyless_deliveries(stream_id, seq);

CREATE TABLE replay_intents (
    epoch_id  BLOB(16) NOT NULL,
    stream_id BLOB(16) NOT NULL,
    key_id    INTEGER NOT NULL,
    key_pos   BLOB(8) NOT NULL,
    seq       BLOB(8) NOT NULL,
    PRIMARY KEY (epoch_id, key_id, key_pos),
    UNIQUE (epoch_id, seq),
    FOREIGN KEY (epoch_id, stream_id) REFERENCES group_epochs(id, stream_id),
    FOREIGN KEY (stream_id, seq) REFERENCES events(stream_id, seq)
);
CREATE INDEX replay_expiry ON replay_intents(stream_id, seq);

CREATE TABLE members (
    epoch_id      BLOB(16) NOT NULL,
    stream_id     BLOB(16) NOT NULL,
    group_id      BLOB(16) NOT NULL,
    consumer_id   TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    PRIMARY KEY (epoch_id, consumer_id),
    FOREIGN KEY (epoch_id, stream_id) REFERENCES group_epochs(id, stream_id),
    FOREIGN KEY (group_id, stream_id) REFERENCES groups(id, stream_id)
);
CREATE INDEX members_connection ON members(connection_id);
CREATE INDEX members_stream ON members(stream_id);

CREATE TABLE dls_ranges (
    epoch_id   BLOB(16) NOT NULL,
    stream_id  BLOB(16) NOT NULL,
    key_id     INTEGER NOT NULL CHECK (key_id >= 0),
    first_pos  BLOB(8) NOT NULL,
    last_pos   BLOB(8),
    first_seq  BLOB(8),
    reason     INTEGER NOT NULL CHECK (reason IN (0, 1)),
    attempts   INTEGER NOT NULL CHECK (attempts >= 0),
    PRIMARY KEY (epoch_id, key_id, first_pos),
    CHECK (last_pos IS NULL OR last_pos >= first_pos),
    FOREIGN KEY (epoch_id, stream_id) REFERENCES group_epochs(id, stream_id),
    FOREIGN KEY (stream_id, key_id) REFERENCES stream_keys(stream_id, key_id)
);
CREATE INDEX dls_order ON dls_ranges(epoch_id, first_seq, key_id, first_pos)
    WHERE first_seq IS NOT NULL;
CREATE INDEX dls_by_key ON dls_ranges(stream_id, key_id, epoch_id, first_pos);
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
    /// (unfinished epoch inits, deleted-stream GC).
    pub fn open(root: &Path) -> Result<(Self, Vec<Continuation>), BrokerError> {
        check_layout(root)?;
        fs::create_dir_all(root).map_err(|e| {
            BrokerError::storage(format!("Cannot create stream persistence dir: {e}"))
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
            .map_err(|e| BrokerError::storage(format!("Cannot open stream lock file: {e}")))?;
        lock_file.try_lock().map_err(|_| {
            BrokerError::storage(format!(
                "Another process holds the stream storage lock at {}",
                lock_path.display()
            ))
        })?;

        let conn = Connection::open(&db_path).map_err(|e| {
            BrokerError::storage(format!("Cannot open stream database: {e}"))
        })?;
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
        .map_err(|e| BrokerError::storage(format!("Cannot configure stream database: {e}")))?;
        // Recipes use ~60 distinct statements through `prepare_cached`; the
        // default cache of 16 would thrash on any mixed workload.
        conn.set_prepared_statement_cache_capacity(256);

        let fresh = !table_exists(&conn, "schema_meta")?;
        if fresh {
            conn.execute_batch(SCHEMA).map_err(|e| {
                BrokerError::storage(format!("Cannot create stream schema: {e}"))
            })?;
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
                    BrokerError::storage(format!("Cannot read stream schema version: {e}"))
                })?;
            if version != SCHEMA_VERSION {
                return Err(BrokerError::storage(format!(
                    "Unsupported stream schema version {version} (expected {SCHEMA_VERSION})"
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

    /// Crash recovery: rebuild the durable-obligation set without touching
    /// payloads. Membership rows are runtime state and are wiped; every
    /// leased delivery is either returned to READY or parked by
    /// `max_deliveries`, then pending counters are reconciled.
    fn recover(&mut self) -> Result<Vec<Continuation>, BrokerError> {
        let tx = self.conn.transaction().map_err(|e| {
            BrokerError::storage(format!("Recovery transaction failed to start: {e}"))
        })?;
        tx.execute("DELETE FROM members", [])
            .map_err(|e| BrokerError::storage(format!("Recovery failed to clear members: {e}")))?;

        // Reclaim leased keyed lanes in slices until none remain.
        loop {
            let leased: Vec<(Vec<u8>, i64, i64)> = tx
                .prepare_cached(
                    "SELECT epoch_id, key_id, attempts FROM key_lanes
                     WHERE state = 2 LIMIT ?1",
                )
                .and_then(|mut s| {
                    s.query_map([RECOVERY_SLICE as i64], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })?
                    .collect()
                })
                .map_err(|e| BrokerError::storage(format!("Recovery lane scan failed: {e}")))?;
            if leased.is_empty() {
                break;
            }
            for (epoch_id, key_id, attempts) in leased {
                super::recipes::recover_lane_lease(&tx, &epoch_id, key_id, attempts)?;
            }
        }

        // Reclaim leased keyless deliveries.
        loop {
            let leased: Vec<(Vec<u8>, Vec<u8>, i64)> = tx
                .prepare_cached(
                    "SELECT epoch_id, seq, attempts FROM keyless_deliveries
                     WHERE state = 2 LIMIT ?1",
                )
                .and_then(|mut s| {
                    s.query_map([RECOVERY_SLICE as i64], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })?
                    .collect()
                })
                .map_err(|e| {
                    BrokerError::storage(format!("Recovery keyless scan failed: {e}"))
                })?;
            if leased.is_empty() {
                break;
            }
            for (epoch_id, seq, attempts) in leased {
                super::recipes::recover_keyless_lease(&tx, &epoch_id, &seq, attempts)?;
            }
        }

        // All leases are gone by construction: reconcile counters wholesale.
        tx.execute("UPDATE group_epochs SET pending_count = 0", [])
            .map_err(|e| {
                BrokerError::storage(format!("Recovery failed to reconcile counters: {e}"))
            })?;

        tx.commit()
            .map_err(|e| BrokerError::storage(format!("Recovery commit failed: {e}")))?;

        let mut continuations = Vec::new();
        {
            let mut stmt = self
                .conn
                .prepare_cached("SELECT id FROM group_epochs WHERE initialized = 0")
                .map_err(|e| {
                    BrokerError::storage(format!("Recovery resume scan failed: {e}"))
                })?;
            let epochs = stmt
                .query_map([], |r| r.get::<_, Vec<u8>>(0))
                .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
                .map_err(|e| {
                    BrokerError::storage(format!("Recovery resume scan failed: {e}"))
                })?;
            for epoch_id in epochs {
                let id: [u8; 16] = epoch_id.as_slice().try_into().map_err(|_| {
                    BrokerError::storage("Corrupt storage: epoch id is not 16 bytes")
                })?;
                continuations.push(Continuation::InitEpoch { epoch_id: id });
            }
        }
        let deleted: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM streams WHERE deleted = 1",
                [],
                |r| r.get(0),
            )
            .map_err(|e| BrokerError::storage(format!("Recovery GC scan failed: {e}")))?;
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
/// per-stream directories, `.log` segments, `state.log`/`groups.log`
/// snapshots, `config.json` files — is the legacy layout or foreign data and
/// must stop startup instead of being silently overwritten.
fn check_layout(root: &Path) -> Result<(), BrokerError> {
    if !root.exists() {
        return Ok(());
    }
    if !root.is_dir() {
        return Err(BrokerError::storage(format!(
            "Stream persistence path {} is not a directory",
            root.display()
        )));
    }
    for entry in fs::read_dir(root).map_err(|e| {
        BrokerError::storage(format!("Cannot inspect stream persistence dir: {e}"))
    })? {
        let entry = entry
            .map_err(|e| BrokerError::storage(format!("Cannot list stream storage: {e}")))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let known = name == DB_FILE_NAME
            || name == LOCK_FILE_NAME
            || name == format!("{DB_FILE_NAME}-wal")
            || name == format!("{DB_FILE_NAME}-shm")
            || name == format!("{DB_FILE_NAME}-journal");
        if !known {
            return Err(BrokerError::storage(format!(
                "Unrecognized entry '{}' in stream persistence dir {} — \
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
            "Stream storage file {} is a symlink; refusing to use it",
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
    .map_err(|e| BrokerError::storage(format!("Cannot inspect stream schema: {e}")))
}

/// Fail closed when a version-stamped database is missing tables or indexes
/// (partial schema creation, manual tampering).
fn validate_schema(conn: &Connection) -> Result<(), BrokerError> {
    const OBJECTS: &[&str] = &[
        "streams",
        "stream_keys",
        "events",
        "groups",
        "group_epochs",
        "key_lanes",
        "keyless_deliveries",
        "replay_intents",
        "members",
        "dls_ranges",
        "ready_lanes",
        "lane_deadlines",
        "lane_owners",
        "lane_normal_frontier",
        "lane_head_expiry",
        "lanes_by_key",
        "keyless_ready",
        "keyless_deadlines",
        "keyless_owners",
        "keyless_original_frontier",
        "keyless_expiry",
        "replay_expiry",
        "members_connection",
        "members_stream",
        "dls_order",
        "dls_by_key",
    ];
    for name in OBJECTS {
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                [name],
                |r| r.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(|e| BrokerError::storage(format!("Cannot inspect stream schema: {e}")))?;
        if !exists {
            return Err(BrokerError::storage(format!(
                "Stream database schema is missing '{name}'"
            )));
        }
    }
    Ok(())
}



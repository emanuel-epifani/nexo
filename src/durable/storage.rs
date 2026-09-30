//! Shared SQLite store: lifecycle, schema, process ownership.
//!
//! One database file (WAL) inside the persistence root holds every resource
//! of the domain, so a commit is the only unit of durability. `Spec` carries
//! the per-domain constants — file names, schema, version, validation — and
//! the `synchronous` mode that defines the commit's crash contract.
//!
//! Startup fails closed: an unrecognized directory layout, a schema that is
//! not `spec.schema_version`, an unsafe symlink, or another live process
//! holding the lock are all startup errors. Nothing is ever recreated over
//! existing bytes.

use std::fs::{self, File, OpenOptions};
use std::path::Path;

use rusqlite::Connection;

use crate::brokers::BrokerError;

/// Per-domain constants describing one durable store.
#[derive(Clone, Copy)]
pub struct Spec {
    /// Broker kind used in error messages and the writer thread name
    /// ("queue", "stream").
    pub kind: &'static str,
    pub db_file: &'static str,
    pub lock_file: &'static str,
    pub schema: &'static str,
    pub schema_version: i64,
    /// `synchronous` pragma: "NORMAL" promises crash-consistency of the
    /// database file, not survival of a power loss; "FULL" fsyncs every
    /// commit batch.
    pub synchronous: &'static str,
    /// Fail closed when a version-stamped database misses tables or indexes
    /// (partial schema creation, manual tampering).
    pub validate: fn(&Connection) -> Result<(), BrokerError>,
}

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
    /// ownership lock and validate/create the schema. Domain recovery runs
    /// separately on the returned store.
    pub fn open(root: &Path, spec: Spec) -> Result<Self, BrokerError> {
        check_layout(root, spec)?;
        fs::create_dir_all(root).map_err(|e| {
            BrokerError::storage(format!(
                "Cannot create {} persistence dir: {e}",
                spec.kind
            ))
        })?;

        let db_path = root.join(spec.db_file);
        reject_symlink(&db_path, spec)?;
        let lock_path = root.join(spec.lock_file);
        reject_symlink(&lock_path, spec)?;

        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                BrokerError::storage(format!("Cannot open {} lock file: {e}", spec.kind))
            })?;
        lock_file.try_lock().map_err(|_| {
            BrokerError::storage(format!(
                "Another process holds the {} storage lock at {}",
                spec.kind,
                lock_path.display()
            ))
        })?;

        let conn = Connection::open(&db_path).map_err(|e| {
            BrokerError::storage(format!("Cannot open {} database: {e}", spec.kind))
        })?;
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = {};
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA cache_size = -64000;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 268435456;
             PRAGMA wal_autocheckpoint = 4000;",
            spec.synchronous
        ))
        .map_err(|e| {
            BrokerError::storage(format!("Cannot configure {} database: {e}", spec.kind))
        })?;
        // Recipes use dozens of distinct statements through `prepare_cached`;
        // the default cache of 16 would thrash on any mixed workload.
        conn.set_prepared_statement_cache_capacity(256);

        let fresh = !table_exists(&conn, spec)?;
        if fresh {
            conn.execute_batch(spec.schema).map_err(|e| {
                BrokerError::storage(format!("Cannot create {} schema: {e}", spec.kind))
            })?;
            conn.execute(
                "INSERT INTO schema_meta(schema_version) VALUES (?1)",
                [spec.schema_version],
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
                    BrokerError::storage(format!(
                        "Cannot read {} schema version: {e}",
                        spec.kind
                    ))
                })?;
            if version != spec.schema_version {
                return Err(BrokerError::storage(format!(
                    "Unsupported {} schema version {version} (expected {})",
                    spec.kind, spec.schema_version
                )));
            }
            (spec.validate)(&conn)?;
        }

        Ok(Store {
            conn,
            _lock_file: lock_file,
        })
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

/// Whitelisted files of the SQLite layout. Anything else in the root is a
/// legacy layout or foreign data and must stop startup instead of being
/// silently overwritten.
fn check_layout(root: &Path, spec: Spec) -> Result<(), BrokerError> {
    if !root.exists() {
        return Ok(());
    }
    if !root.is_dir() {
        return Err(BrokerError::storage(format!(
            "{} persistence path {} is not a directory",
            spec.kind,
            root.display()
        )));
    }
    for entry in fs::read_dir(root).map_err(|e| {
        BrokerError::storage(format!("Cannot inspect {} persistence dir: {e}", spec.kind))
    })? {
        let entry = entry
            .map_err(|e| BrokerError::storage(format!("Cannot list {} dir: {e}", spec.kind)))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let known = name == spec.db_file
            || name == spec.lock_file
            || name == format!("{}-wal", spec.db_file)
            || name == format!("{}-shm", spec.db_file)
            || name == format!("{}-journal", spec.db_file);
        if !known {
            return Err(BrokerError::storage(format!(
                "Unrecognized entry '{}' in {} persistence dir {} — \
                 refusing to start over a legacy or foreign layout",
                name,
                spec.kind,
                root.display()
            )));
        }
    }
    Ok(())
}

fn reject_symlink(path: &Path, spec: Spec) -> Result<(), BrokerError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(BrokerError::storage(format!(
            "{} storage file {} is a symlink; refusing to use it",
            spec.kind,
            path.display()
        ))),
        Ok(_) | Err(_) => Ok(()),
    }
}

fn table_exists(conn: &Connection, spec: Spec) -> Result<bool, BrokerError> {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_meta'",
        [],
        |r| r.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .map_err(|e| BrokerError::storage(format!("Cannot inspect {} schema: {e}", spec.kind)))
}

//! Queue domain storage: schema, spec and crash recovery for the shared
//! `queues.sqlite3` (lifecycle machinery lives in `crate::durable::storage`).
//!
//! `synchronous = NORMAL` is deliberate: a successful commit promises
//! crash-consistency of the database file, not survival of a power loss, and
//! producer retries may duplicate messages — exactly-once external effects
//! are not claimed.

use rusqlite::Connection;

use crate::brokers::queue::domain::definition::QueueConfig;
use crate::brokers::queue::domain::ops::Continuation;
use crate::brokers::queue::domain::types::{
    DB_FILE_NAME, LOCK_FILE_NAME, MSG_INFLIGHT, MSG_READY, SCHEMA_VERSION,
};
use crate::brokers::BrokerError;
use crate::durable::{now_millis, Spec, Store};

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

pub const SPEC: Spec = Spec {
    kind: "queue",
    db_file: DB_FILE_NAME,
    lock_file: LOCK_FILE_NAME,
    schema: SCHEMA,
    schema_version: SCHEMA_VERSION,
    synchronous: "NORMAL",
    validate: validate_schema,
};

/// Crash recovery: every in-flight lease dies with the process, so each
/// leased message is either returned to READY (fresh ready_seq, reason
/// kept) or moved to the DLQ when `attempts` already reached the queue's
/// `max_deliveries`. Rows of deleted queues are skipped — GC owns them.
pub fn recover(store: &mut Store) -> Result<Vec<Continuation>, BrokerError> {
    let now = now_millis() as i64;
    let tx = store
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
                .map_err(|e| BrokerError::storage(format!("Recovery ready_seq bump: {e}")))?;
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

    let deleted: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM queues WHERE deleted = 1", [], |r| {
            r.get(0)
        })
        .map_err(|e| BrokerError::storage(format!("Recovery GC scan failed: {e}")))?;
    let mut continuations = Vec::new();
    if deleted > 0 {
        continuations.push(Continuation::Gc);
    }
    Ok(continuations)
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

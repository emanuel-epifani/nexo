//! Transaction recipes: every queue command is a deterministic function over
//! `&Connection` executed inside the writer's transaction. Recipes never touch
//! async, channels or timers — they stage `Effects` that the worker applies
//! after commit.
//!
//! Conventions:
//! - `Expected` errors roll back only this command's savepoint and become the
//!   command reply; `Fatal` errors abort the whole batch transaction.
//! - Delivery fencing is enforced in SQL: ack/nack match `delivery_token`
//!   and a live lease (`state=1 AND visible_at > now`) in the WHERE clause;
//!   zero affected rows means the delivery is stale — no separate read path.
//! - Message ids are `BLOB(16)` UUIDs; counters live on the `queues` row and
//!   are bumped inside the same transaction that consumes them.

use rusqlite::{params, Connection, OptionalExtension, Row};
use uuid::Uuid;

use crate::brokers::queue::domain::definition::{QueueConfig, QueueDefinition};
use crate::brokers::queue::domain::message::{DlqMessage, Message, PushItem};
use crate::brokers::queue::domain::ops::{Continuation, Effects, QueueReply, QueueRequest};
use crate::brokers::queue::domain::types::{MSG_INFLIGHT, MSG_READY};
use crate::brokers::{config_conflict_error, BrokerError, ProvisionOutcome, ProvisionResult};
use crate::durable::{
    exec, expected, fatal, query_one, query_vec, sql, CmdError, ExecCtx, R,
};

/// Rows processed per expiry sweep / GC continuation slice.
pub const BATCH_LIMIT: usize = 1024;
/// Rows per multi-row INSERT chunk: 11 columns × 200 stays far under the
/// 32 766 bind-parameter limit.
const INSERT_CHUNK: usize = 200;

// ---------------------------------------------------------------------------
// Row types / lookups
// ---------------------------------------------------------------------------

pub struct QueueRow {
    pub id: Vec<u8>,
    pub name: String,
    pub config: QueueConfig,
    pub max_deliveries: i64,
    pub visibility_timeout_ms: i64,
}

fn blob16(bytes: &[u8], what: &str) -> R<[u8; 16]> {
    bytes.try_into().map_err(|_| {
        CmdError::Fatal(BrokerError::storage(format!(
            "Corrupt storage: {what} is not 16 bytes"
        )))
    })
}

fn uuid_blob(row: &Row<'_>, idx: usize, what: &str) -> rusqlite::Result<Uuid> {
    let bytes = row.get::<_, Vec<u8>>(idx)?;
    Uuid::from_slice(&bytes).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            bytes.len(),
            rusqlite::types::Type::Blob,
            format!("{what} is not a uuid").into(),
        )
    })
}

fn parse_config(json: &str) -> R<QueueConfig> {
    serde_json::from_str(json).map_err(|e| {
        CmdError::Fatal(BrokerError::storage(format!(
            "Corrupt storage: persisted queue config is invalid: {e}"
        )))
    })
}

pub fn config_json(config: &QueueConfig) -> serde_json::Value {
    serde_json::json!({
        "visibilityTimeoutMs": config.visibility_timeout_ms,
        "maxDeliveries": config.max_deliveries,
    })
}

fn map_queue_row(row: &Row<'_>) -> rusqlite::Result<(Vec<u8>, String, String)> {
    Ok((
        row.get::<_, Vec<u8>>(0)?,
        row.get::<_, String>(1)?,
        row.get::<_, String>(2)?,
    ))
}

fn load_queue(conn: &Connection, name: &str) -> R<Option<QueueRow>> {
    let row = query_one(
        conn,
        "SELECT id, name, config_json FROM queues WHERE name = ?1 AND deleted = 0",
        [name],
        map_queue_row,
    )
    .optional()
    .map_err(|e| fatal("Cannot load queue", e))?;
    let Some((id, name, json)) = row else {
        return Ok(None);
    };
    let config = parse_config(&json)?;
    Ok(Some(QueueRow {
        id,
        name,
        max_deliveries: config.max_deliveries as i64,
        visibility_timeout_ms: config.visibility_timeout_ms as i64,
        config,
    }))
}

fn require_queue(conn: &Connection, name: &str) -> R<QueueRow> {
    load_queue(conn, name)?.ok_or_else(|| {
        expected(BrokerError::not_found(format!("Queue '{name}' not found")))
    })
}

/// Bump a `queues` counter by `n` inside the tx; returns the first value the
/// caller may consume (i.e. `new_value - n + 1`).
fn bump_counter(conn: &Connection, queue_id: &[u8], column: &str, n: i64) -> R<i64> {
    let text = format!(
        "UPDATE queues SET {column} = {column} + ?2 WHERE id = ?1 RETURNING {column}"
    );
    let new_val = sql(
        query_one(conn, &text, params![queue_id, n], |r| r.get::<_, i64>(0)),
        "Cannot bump queue counter",
    )?;
    Ok(new_val - n + 1)
}

fn map_message_row(row: &Row<'_>) -> rusqlite::Result<Message> {
    Ok(Message {
        id: uuid_blob(row, 0, "message id")?,
        payload: bytes::Bytes::from(row.get::<_, Vec<u8>>(1)?),
        priority: row.get::<_, u8>(2)?,
        attempts: row.get::<_, u32>(3)?,
        created_at: row.get::<_, i64>(4)? as u64,
        visible_at: row.get::<_, i64>(5)? as u64,
        ready_seq: row.get::<_, i64>(6)? as u64,
        delivery_token: row.get::<_, i64>(7)? as u64,
        failure_reason: row.get::<_, Option<String>>(8)?,
    })
}

fn map_dlq_row(row: &Row<'_>) -> rusqlite::Result<DlqMessage> {
    Ok(DlqMessage {
        id: uuid_blob(row, 0, "dlq id")?,
        payload: bytes::Bytes::from(row.get::<_, Vec<u8>>(1)?),
        priority: row.get::<_, u8>(2)?,
        attempts: row.get::<_, u32>(3)?,
        created_at: row.get::<_, i64>(4)? as u64,
        failed_at: row.get::<_, i64>(5)? as u64,
        dlq_seq: row.get::<_, i64>(6)? as u64,
        failure_reason: row.get::<_, String>(7)?,
    })
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn execute(
    conn: &Connection,
    op: &QueueRequest,
    ctx: &ExecCtx,
) -> Result<(QueueReply, Effects), CmdError> {
    match op {
        QueueRequest::CreateQueue { name, requested } => create_queue(conn, name, requested),
        QueueRequest::DeleteQueue { name } => delete_queue(conn, name),
        QueueRequest::QueueExists { name } => {
            let exists = load_queue(conn, name)?.is_some();
            Ok((QueueReply::Bool(exists), Effects::default()))
        }
        QueueRequest::DescribeQueue { name } => {
            let queue = require_queue(conn, name)?;
            Ok((
                QueueReply::Definition(Box::new(QueueDefinition {
                    name: queue.name.clone(),
                    config: queue.config,
                })),
                Effects::default(),
            ))
        }
        QueueRequest::Push { name, items } => push(conn, name, items, ctx),
        QueueRequest::Consume { name, limit } => consume(conn, name, *limit, ctx),
        QueueRequest::Ack {
            name,
            id,
            delivery_token,
        } => {
            let (done, effects) = ack_one(conn, name, id, *delivery_token, ctx)?;
            Ok((QueueReply::Bool(done), effects))
        }
        QueueRequest::Nack {
            name,
            id,
            delivery_token,
            reason,
        } => nack(conn, name, id, *delivery_token, reason, ctx),
        QueueRequest::PeekDlq { name, limit, offset } => peek_dlq(conn, name, *limit, *offset),
        QueueRequest::MoveToQueue { name, id } => move_to_queue(conn, name, id),
        QueueRequest::DeleteDlq { name, id } => delete_dlq(conn, name, id),
        QueueRequest::PurgeDlq { name } => purge_dlq(conn, name),
        QueueRequest::ExpireLeases { now_ms } => expire_leases(conn, *now_ms),
        QueueRequest::Shutdown => Ok((QueueReply::Unit, Effects::default())),
    }
}

pub fn run_continuation(
    conn: &Connection,
    cont: &Continuation,
    ctx: &ExecCtx,
) -> Result<(QueueReply, Effects), CmdError> {
    match cont {
        Continuation::Expire => expire_leases(conn, ctx.now_ms),
        Continuation::Gc => gc_slice(conn),
    }
}

// ---------------------------------------------------------------------------
// Provisioning
// ---------------------------------------------------------------------------

fn create_queue(
    conn: &Connection,
    name: &str,
    requested: &QueueConfig,
) -> Result<(QueueReply, Effects), CmdError> {
    if let Some(existing) = load_queue(conn, name)? {
        if existing.config != *requested {
            return Err(expected(config_conflict_error(
                "queue",
                name,
                config_json(requested),
                config_json(&existing.config),
            )));
        }
        return Ok((
            QueueReply::Provision(Box::new(ProvisionResult {
                outcome: ProvisionOutcome::Unchanged,
                definition: QueueDefinition {
                    name: name.to_string(),
                    config: existing.config,
                },
            })),
            Effects::default(),
        ));
    }
    let queue_id = Uuid::new_v4().into_bytes();
    let json =
        serde_json::to_string(requested).map_err(|e| fatal("Cannot serialize queue config", e))?;
    sql(
        exec(
            conn,
            "INSERT INTO queues(id, name, deleted, config_json, next_ready_seq, next_token,
                               next_dlq_seq)
             VALUES (?1, ?2, 0, ?3, 0, 0, 0)",
            params![queue_id.as_slice(), name, json],
        ),
        "Cannot create queue",
    )?;
    Ok((
        QueueReply::Provision(Box::new(ProvisionResult {
            outcome: ProvisionOutcome::Created,
            definition: QueueDefinition {
                name: name.to_string(),
                config: requested.clone(),
            },
        })),
        Effects::default(),
    ))
}

/// Logical delete is O(1): the name frees immediately (partial unique index)
/// while row cleanup rides a bounded `Gc` continuation.
fn delete_queue(conn: &Connection, name: &str) -> Result<(QueueReply, Effects), CmdError> {
    let Some(queue) = load_queue(conn, name)? else {
        return Ok((QueueReply::Unit, Effects::default()));
    };
    sql(
        exec(
            conn,
            "UPDATE queues SET deleted = 1 WHERE id = ?1",
            [queue.id.as_slice()],
        ),
        "Cannot mark queue deleted",
    )?;
    Ok((
        QueueReply::Unit,
        Effects {
            wakes: vec![queue.name.clone()],
            deleted_queues: vec![queue.name.clone()],
            followups: vec![Continuation::Gc],
        },
    ))
}

// ---------------------------------------------------------------------------
// Push / consume
// ---------------------------------------------------------------------------

/// Insert a batch of messages: ready_seq values come from the queue's
/// durable counter bumped once per batch inside this transaction.
/// Public for the worker's `PushRun` merging.
pub fn push(
    conn: &Connection,
    name: &str,
    items: &[PushItem],
    ctx: &ExecCtx,
) -> Result<(QueueReply, Effects), CmdError> {
    let queue = require_queue(conn, name)?;
    if items.is_empty() {
        return Ok((QueueReply::Unit, Effects::default()));
    }
    let base = bump_counter(conn, &queue.id, "next_ready_seq", items.len() as i64)?;
    let now = ctx.now_ms as i64;

    for (chunk_idx, chunk) in items.chunks(INSERT_CHUNK).enumerate() {
        let mut text = String::with_capacity(160 + chunk.len() * 12);
        text.push_str(
            "INSERT INTO messages(queue_id, id, payload, priority, state, ready_seq,
                                 delivery_token, visible_at, attempts, failure_reason, created_at)
             VALUES ",
        );
        for i in 0..chunk.len() {
            if i > 0 {
                text.push(',');
            }
            text.push_str("(?1,?,?,?,?,?,?,?,?,?,?)");
        }
        // Values must be materialized before `params` borrows them.
        let ids: Vec<Vec<u8>> = chunk
            .iter()
            .map(|_| Uuid::new_v4().into_bytes().to_vec())
            .collect();
        let seqs: Vec<i64> = (0..chunk.len())
            .map(|i| base + (chunk_idx * INSERT_CHUNK + i) as i64)
            .collect();
        let payloads: Vec<&[u8]> = chunk.iter().map(|it| it.payload.as_ref()).collect();
        let priorities: Vec<u8> = chunk.iter().map(|it| it.priority).collect();
        let zero: i64 = 0;
        let none_str: Option<String> = None;
        let mut params: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(1 + chunk.len() * 10);
        params.push(&queue.id);
        for i in 0..chunk.len() {
            params.push(&ids[i]);
            params.push(&payloads[i]);
            params.push(&priorities[i]);
            params.push(&MSG_READY);
            params.push(&seqs[i]);
            params.push(&zero);
            params.push(&zero);
            params.push(&zero);
            params.push(&none_str);
            params.push(&now);
        }
        sql(exec(conn, &text, rusqlite::params_from_iter(params)), "Cannot push")?;
    }

    let mut effects = Effects::default();
    effects.wake(&queue.name);
    Ok((QueueReply::Unit, effects))
}

/// Lease up to `limit` ready messages: the pick is the `ready_idx` range scan
/// (priority DESC, ready_seq ASC), and each row is leased with a fresh
/// delivery_token drawn from the queue counter — all inside one tx.
fn consume(
    conn: &Connection,
    name: &str,
    limit: usize,
    ctx: &ExecCtx,
) -> Result<(QueueReply, Effects), CmdError> {
    let queue = require_queue(conn, name)?;
    let candidates: Vec<Vec<u8>> = query_vec(
        conn,
        "SELECT id FROM messages
         WHERE queue_id = ?1 AND state = ?2
         ORDER BY priority DESC, ready_seq ASC LIMIT ?3",
        params![queue.id, MSG_READY, limit as i64],
        |r| r.get::<_, Vec<u8>>(0),
        "Cannot scan ready messages",
    )?;
    if candidates.is_empty() {
        return Ok((QueueReply::Consumed(Vec::new()), Effects::default()));
    }

    let n = candidates.len() as i64;
    let first_token = bump_counter(conn, &queue.id, "next_token", n)?;
    let visible_until = ctx.now_ms as i64 + queue.visibility_timeout_ms;

    // Lease + RETURNING materializes the consumed rows in one statement.
    const LEASE_SQL: &str =
        "UPDATE messages SET state = ?1, visible_at = ?2, delivery_token = ?3,
                attempts = attempts + 1
         WHERE queue_id = ?4 AND id = ?5 RETURNING id, payload, priority,
                attempts, created_at, visible_at, ready_seq, delivery_token,
                failure_reason";
    let mut messages = Vec::with_capacity(candidates.len());
    for (i, id) in candidates.iter().enumerate() {
        let msg = sql(
            query_one(
                conn,
                LEASE_SQL,
                params![MSG_INFLIGHT, visible_until, first_token + i as i64, queue.id, id],
                map_message_row,
            ),
            "Cannot lease message",
        )?;
        messages.push(msg);
    }
    Ok((QueueReply::Consumed(messages), Effects::default()))
}

// ---------------------------------------------------------------------------
// Ack / nack
// ---------------------------------------------------------------------------

/// `None` when the delivery is stale (unknown id, wrong token, expired or
/// missing lease). Public for the worker's `AckRun` merging.
pub fn ack_one(
    conn: &Connection,
    name: &str,
    id: &Uuid,
    delivery_token: u64,
    ctx: &ExecCtx,
) -> Result<(bool, Effects), CmdError> {
    let Some(queue) = load_queue(conn, name)? else {
        return Ok((false, Effects::default()));
    };
    let deleted = sql(
        exec(
            conn,
            "DELETE FROM messages
             WHERE queue_id = ?1 AND id = ?2 AND state = ?3
                   AND delivery_token = ?4 AND visible_at > ?5",
            params![
                queue.id,
                id.as_bytes().as_slice(),
                MSG_INFLIGHT,
                delivery_token as i64,
                ctx.now_ms as i64
            ],
        ),
        "Cannot ack message",
    )?;
    Ok((deleted > 0, Effects::default()))
}

fn nack(
    conn: &Connection,
    name: &str,
    id: &Uuid,
    delivery_token: u64,
    reason: &str,
    ctx: &ExecCtx,
) -> Result<(QueueReply, Effects), CmdError> {
    let Some(queue) = load_queue(conn, name)? else {
        return Ok((QueueReply::Bool(false), Effects::default()));
    };
    let row = query_one(
        conn,
        "SELECT state, delivery_token, visible_at, attempts FROM messages
         WHERE queue_id = ?1 AND id = ?2",
        params![queue.id, id.as_bytes().as_slice()],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot load message for nack", e))?;
    let Some((state, token, visible_at, attempts)) = row else {
        return Ok((QueueReply::Bool(false), Effects::default()));
    };
    // Only a live lease may be nacked: stale token, ready message or an
    // already-expired lease are all rejected.
    if state != MSG_INFLIGHT || token != delivery_token as i64 || visible_at <= ctx.now_ms as i64
    {
        return Ok((QueueReply::Bool(false), Effects::default()));
    }

    let mut effects = Effects::default();
    if attempts >= queue.max_deliveries {
        move_to_dlq(conn, &queue, id, reason, ctx.now_ms as i64)?;
    } else {
        let seq = bump_counter(conn, &queue.id, "next_ready_seq", 1)?;
        sql(
            exec(
                conn,
                "UPDATE messages SET state = ?1, visible_at = 0, delivery_token = 0,
                        ready_seq = ?2, failure_reason = ?3
                 WHERE queue_id = ?4 AND id = ?5",
                params![MSG_READY, seq, reason, queue.id, id.as_bytes().as_slice()],
            ),
            "Cannot requeue nacked message",
        )?;
        effects.wake(&queue.name);
    }
    Ok((QueueReply::Bool(true), effects))
}

/// Atomic move from `messages` to `dlq` inside the caller's savepoint.
fn move_to_dlq(
    conn: &Connection,
    queue: &QueueRow,
    id: &Uuid,
    reason: &str,
    now_ms: i64,
) -> R<()> {
    let dlq_seq = bump_counter(conn, &queue.id, "next_dlq_seq", 1)?;
    sql(
        exec(
            conn,
            "INSERT INTO dlq(queue_id, dlq_seq, id, payload, priority, attempts,
                             created_at, failed_at, failure_reason)
             SELECT queue_id, ?1, id, payload, priority, attempts, created_at, ?2, ?3
             FROM messages WHERE queue_id = ?4 AND id = ?5",
            params![dlq_seq, now_ms, reason, queue.id, id.as_bytes().as_slice()],
        ),
        "Cannot move message to DLQ",
    )?;
    sql(
        exec(
            conn,
            "DELETE FROM messages WHERE queue_id = ?1 AND id = ?2",
            params![queue.id, id.as_bytes().as_slice()],
        ),
        "Cannot delete dead-lettered message",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Lease expiry
// ---------------------------------------------------------------------------

/// One bounded slice of expired leases. Rows over `max_deliveries` park into
/// the DLQ, the rest are requeued at the ready tail. Re-enqueues itself while
/// the slice stays saturated.
fn expire_leases(conn: &Connection, now_ms: u64) -> Result<(QueueReply, Effects), CmdError> {
    let due: Vec<(Vec<u8>, Vec<u8>, i64, String, String)> = query_vec(
        conn,
        "SELECT m.queue_id, m.id, m.attempts, q.name, q.config_json
         FROM messages m JOIN queues q ON q.id = m.queue_id
         WHERE m.state = ?1 AND m.visible_at <= ?2 AND q.deleted = 0
         ORDER BY m.visible_at LIMIT ?3",
        params![MSG_INFLIGHT, now_ms as i64, BATCH_LIMIT as i64],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        },
        "Cannot scan expired leases",
    )?;
    let more = due.len() == BATCH_LIMIT;

    let mut effects = Effects::default();
    let mut config_cache: std::collections::HashMap<Vec<u8>, QueueConfig> =
        std::collections::HashMap::new();
    for (queue_id, id, attempts, qname, json) in &due {
        let config = match config_cache.get(queue_id) {
            Some(c) => c,
            None => {
                config_cache.insert(queue_id.clone(), parse_config(json)?);
                config_cache.get(queue_id).unwrap()
            }
        };
        let queue = QueueRow {
            id: queue_id.clone(),
            name: qname.clone(),
            max_deliveries: config.max_deliveries as i64,
            visibility_timeout_ms: config.visibility_timeout_ms as i64,
            config: config.clone(),
        };
        let msg_id = Uuid::from_bytes(blob16(id, "message id")?);
        if *attempts >= queue.max_deliveries {
            move_to_dlq(conn, &queue, &msg_id, "Timeout", now_ms as i64)?;
        } else {
            let seq = bump_counter(conn, &queue.id, "next_ready_seq", 1)?;
            sql(
                exec(
                    conn,
                    "UPDATE messages SET state = ?1, visible_at = 0, delivery_token = 0,
                            ready_seq = ?2
                     WHERE queue_id = ?3 AND id = ?4",
                    params![MSG_READY, seq, queue_id, id],
                ),
                "Cannot requeue expired lease",
            )?;
            effects.wake(qname);
        }
    }
    if more {
        effects.followups.push(Continuation::Expire);
    }
    Ok((QueueReply::Unit, effects))
}

// ---------------------------------------------------------------------------
// DLQ operations
// ---------------------------------------------------------------------------

fn peek_dlq(
    conn: &Connection,
    name: &str,
    limit: usize,
    offset: usize,
) -> Result<(QueueReply, Effects), CmdError> {
    let queue = require_queue(conn, name)?;
    let total = sql(
        query_one(
            conn,
            "SELECT COUNT(*) FROM dlq WHERE queue_id = ?1",
            [queue.id.as_slice()],
            |r| r.get::<_, i64>(0),
        ),
        "Cannot count DLQ",
    )? as usize;
    // Most recent failure first: dlq_seq is monotonic per queue.
    const PEEK_SQL: &str =
        "SELECT id, payload, priority, attempts, created_at, failed_at, dlq_seq,
                failure_reason FROM dlq WHERE queue_id = ?1
         ORDER BY dlq_seq DESC LIMIT ?2 OFFSET ?3";
    let items = query_vec(
        conn,
        PEEK_SQL,
        params![queue.id, limit as i64, offset as i64],
        map_dlq_row,
        "Cannot peek DLQ",
    )?;
    Ok((QueueReply::DlqPage(total, items), Effects::default()))
}

/// Replay: the DLQ row becomes a fresh ready message (attempts reset, reason
/// cleared, new ready_seq at the tail) inside one savepoint.
fn move_to_queue(conn: &Connection, name: &str, id: &Uuid) -> Result<(QueueReply, Effects), CmdError> {
    let Some(queue) = load_queue(conn, name)? else {
        return Ok((QueueReply::Bool(false), Effects::default()));
    };
    let row = query_one(
        conn,
        "SELECT payload, priority, created_at FROM dlq WHERE queue_id = ?1 AND id = ?2",
        params![queue.id, id.as_bytes().as_slice()],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, u8>(1)?,
                r.get::<_, i64>(2)?,
            ))
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot load DLQ entry", e))?;
    let Some((payload, priority, created_at)) = row else {
        return Ok((QueueReply::Bool(false), Effects::default()));
    };
    let seq = bump_counter(conn, &queue.id, "next_ready_seq", 1)?;
    sql(
        exec(
            conn,
            "INSERT INTO messages(queue_id, id, payload, priority, state, ready_seq,
                                 delivery_token, visible_at, attempts, failure_reason, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, 0, 0, NULL, ?7)",
            params![
                queue.id,
                id.as_bytes().as_slice(),
                payload,
                priority,
                MSG_READY,
                seq,
                created_at
            ],
        ),
        "Cannot replay DLQ entry",
    )?;
    sql(
        exec(
            conn,
            "DELETE FROM dlq WHERE queue_id = ?1 AND id = ?2",
            params![queue.id, id.as_bytes().as_slice()],
        ),
        "Cannot delete replayed DLQ entry",
    )?;
    let mut effects = Effects::default();
    effects.wake(&queue.name);
    Ok((QueueReply::Bool(true), effects))
}

fn delete_dlq(conn: &Connection, name: &str, id: &Uuid) -> Result<(QueueReply, Effects), CmdError> {
    let Some(queue) = load_queue(conn, name)? else {
        return Ok((QueueReply::Bool(false), Effects::default()));
    };
    let deleted = sql(
        exec(
            conn,
            "DELETE FROM dlq WHERE queue_id = ?1 AND id = ?2",
            params![queue.id, id.as_bytes().as_slice()],
        ),
        "Cannot delete DLQ entry",
    )?;
    Ok((QueueReply::Bool(deleted > 0), Effects::default()))
}

fn purge_dlq(conn: &Connection, name: &str) -> Result<(QueueReply, Effects), CmdError> {
    let queue = require_queue(conn, name)?;
    let purged = sql(
        exec(
            conn,
            "DELETE FROM dlq WHERE queue_id = ?1",
            [queue.id.as_slice()],
        ),
        "Cannot purge DLQ",
    )?;
    Ok((QueueReply::Count(purged), Effects::default()))
}

// ---------------------------------------------------------------------------
// GC
// ---------------------------------------------------------------------------

/// One bounded slice of deleted-queue cleanup: child rows drain before the
/// queue row itself, so a huge queue never stalls the writer behind one
/// giant delete. Re-enqueues itself while dead queues remain.
fn gc_slice(conn: &Connection) -> Result<(QueueReply, Effects), CmdError> {
    let dead: Vec<Vec<u8>> = query_vec(
        conn,
        "SELECT id FROM queues WHERE deleted = 1 LIMIT 16",
        [],
        |r| r.get::<_, Vec<u8>>(0),
        "Cannot list deleted queues",
    )?;
    for queue_id in &dead {
        sql(
            exec(
                conn,
                "DELETE FROM messages WHERE queue_id = ?1 AND rowid IN
                     (SELECT rowid FROM messages WHERE queue_id = ?1 LIMIT ?2)",
                params![queue_id, BATCH_LIMIT as i64],
            ),
            "Cannot GC queue messages",
        )?;
        sql(
            exec(
                conn,
                "DELETE FROM dlq WHERE queue_id = ?1 AND rowid IN
                     (SELECT rowid FROM dlq WHERE queue_id = ?1 LIMIT ?2)",
                params![queue_id, BATCH_LIMIT as i64],
            ),
            "Cannot GC queue DLQ",
        )?;
        // The queue row goes only once both child tables are drained;
        // otherwise the FK would fail and the next slice continues the job.
        sql(
            exec(
                conn,
                "DELETE FROM queues WHERE id = ?1
                     AND NOT EXISTS (SELECT 1 FROM messages WHERE queue_id = ?1 LIMIT 1)
                     AND NOT EXISTS (SELECT 1 FROM dlq WHERE queue_id = ?1 LIMIT 1)",
                [queue_id.as_slice()],
            ),
            "Cannot GC queue row",
        )?;
    }
    let remaining: i64 = sql(
        query_one(conn, "SELECT COUNT(*) FROM queues WHERE deleted = 1", [], |r| {
            r.get(0)
        }),
        "Cannot count deleted queues",
    )?;
    let mut effects = Effects::default();
    if remaining > 0 {
        effects.followups.push(Continuation::Gc);
    }
    Ok((QueueReply::Unit, effects))
}

// ---------------------------------------------------------------------------
// Merged runs (worker entry points)
// ---------------------------------------------------------------------------

/// Ack commands for the same queue folded into one savepoint: per-ack
/// outcomes (delivery found & fenced) map back to each command's reply slot.
pub fn ack_many(
    conn: &Connection,
    name: &str,
    acks: &[(Uuid, u64)],
    ctx: &ExecCtx,
) -> Result<Vec<bool>, CmdError> {
    if load_queue(conn, name)?.is_none() {
        return Ok(acks.iter().map(|_| false).collect());
    }
    let mut results = Vec::with_capacity(acks.len());
    for (id, token) in acks {
        let (done, _) = ack_one(conn, name, id, *token, ctx)?;
        results.push(done);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::brokers::queue::domain::storage::SCHEMA;
    use rusqlite::Connection;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
        conn
    }

    fn ctx() -> ExecCtx {
        ExecCtx { now_ms: 1_000 }
    }

    fn config(max_deliveries: u32, vt_ms: u64) -> QueueConfig {
        QueueConfig {
            visibility_timeout_ms: vt_ms,
            max_deliveries,
        }
    }

    fn make_queue(conn: &Connection, name: &str, cfg: QueueConfig) {
        create_queue(conn, name, &cfg).unwrap();
    }

    fn push_one(conn: &Connection, q: &str, payload: &str, priority: u8) {
        let items = vec![PushItem {
            payload: bytes::Bytes::from(payload.to_string()),
            priority,
        }];
        let (reply, _) = push(conn, q, &items, &ctx()).unwrap();
        assert!(matches!(reply, QueueReply::Unit));
    }

    fn consume_one(conn: &Connection, q: &str, now_ms: u64) -> Message {
        let (reply, _) = consume(conn, q, 1, &ExecCtx { now_ms }).unwrap();
        match reply {
            QueueReply::Consumed(mut msgs) => msgs.pop().expect("expected one message"),
            _ => panic!("unexpected reply"),
        }
    }

    #[test]
    fn consume_leases_in_priority_then_fifo_order() {
        let conn = test_conn();
        make_queue(&conn, "q", config(5, 30_000));
        push_one(&conn, "q", "low", 1);
        push_one(&conn, "q", "high", 10);
        push_one(&conn, "q", "first5", 5);
        push_one(&conn, "q", "second5", 5);

        assert_eq!(consume_one(&conn, "q", 100).payload, bytes::Bytes::from("high"));
        assert_eq!(consume_one(&conn, "q", 100).payload, bytes::Bytes::from("first5"));
        assert_eq!(consume_one(&conn, "q", 100).payload, bytes::Bytes::from("second5"));
        assert_eq!(consume_one(&conn, "q", 100).payload, bytes::Bytes::from("low"));
    }

    #[test]
    fn ack_fences_by_token_and_live_lease() {
        let conn = test_conn();
        make_queue(&conn, "q", config(5, 60_000));
        push_one(&conn, "q", "m", 0);
        let msg = consume_one(&conn, "q", 1_000);
        assert_eq!(msg.delivery_token, 1);

        let (ok, _) = ack_one(&conn, "q", &msg.id, 999, &ExecCtx { now_ms: 1_000 }).unwrap();
        assert!(!ok, "wrong token must be rejected");
        // Lease expired at ack time.
        let (ok, _) = ack_one(&conn, "q", &msg.id, 1, &ExecCtx { now_ms: 100_000 }).unwrap();
        assert!(!ok, "expired lease must be rejected");
        let (ok, _) = ack_one(&conn, "q", &msg.id, 1, &ExecCtx { now_ms: 1_000 }).unwrap();
        assert!(ok, "live lease + current token must be accepted");
        let (ok, _) = ack_one(&conn, "q", &msg.id, 1, &ExecCtx { now_ms: 1_000 }).unwrap();
        assert!(!ok, "double ack must be rejected");
    }

    #[test]
    fn nack_requeues_or_parks_at_max_deliveries() {
        let conn = test_conn();
        make_queue(&conn, "q", config(2, 60_000));
        push_one(&conn, "q", "m", 0);

        let m1 = consume_one(&conn, "q", 1_000);
        let (reply, _) = nack(&conn, "q", &m1.id, m1.delivery_token, "f", &ctx()).unwrap();
        assert!(matches!(reply, QueueReply::Bool(true)));
        // Requeued with a fresh ready_seq and cleared lease.
        let m2 = consume_one(&conn, "q", 1_000);
        assert_eq!(m2.delivery_token, 2);
        assert_eq!(m2.attempts, 2);

        let (reply, _) = nack(&conn, "q", &m2.id, m2.delivery_token, "f", &ctx()).unwrap();
        assert!(matches!(reply, QueueReply::Bool(true)));
        // attempts(2) >= max_deliveries(2) → DLQ, nothing consumable.
        let (reply, _) = consume(&conn, "q", 1, &ctx()).unwrap();
        assert!(matches!(reply, QueueReply::Consumed(ref v) if v.is_empty()));
        let (reply, _) = peek_dlq(&conn, "q", 10, 0).unwrap();
        assert!(matches!(reply, QueueReply::DlqPage(1, _)));
    }

    #[test]
    fn expire_requeues_live_and_parks_exhausted() {
        let conn = test_conn();
        make_queue(&conn, "q", config(2, 10));
        push_one(&conn, "q", "a", 0);
        push_one(&conn, "q", "b", 0);
        let (reply, _) = consume(&conn, "q", 2, &ExecCtx { now_ms: 1_000 }).unwrap();
        let msgs = match reply {
            QueueReply::Consumed(v) => v,
            _ => panic!(),
        };
        assert_eq!(msgs.len(), 2);

        // First expiry: both requeued (attempts 1 < 2).
        let (_, effects) = expire_leases(&conn, 2_000).unwrap();
        assert_eq!(effects.wakes, vec!["q".to_string()]);
        let m = consume_one(&conn, "q", 2_000);
        assert_eq!(m.attempts, 2);

        // Second expiry: attempts 2 >= 2 → DLQ; the other requeues.
        let (_, _) = expire_leases(&conn, 3_000).unwrap();
        let (reply, _) = peek_dlq(&conn, "q", 10, 0).unwrap();
        match reply {
            QueueReply::DlqPage(total, items) => {
                assert_eq!(total, 1);
                assert_eq!(items[0].failure_reason, "Timeout");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn delete_queue_is_idempotent_and_frees_name() {
        let conn = test_conn();
        make_queue(&conn, "q", config(5, 30_000));
        delete_queue(&conn, "q").unwrap();
        // Recreate under the same name while the old row awaits GC.
        let (reply, _) = create_queue(&conn, "q", &config(9, 1_000)).unwrap();
        match reply {
            QueueReply::Provision(p) => assert_eq!(p.outcome, ProvisionOutcome::Created),
            _ => panic!(),
        }
        delete_queue(&conn, "missing").unwrap();
    }

    #[test]
    fn dlq_replay_restores_ready_message() {
        let conn = test_conn();
        make_queue(&conn, "q", config(0, 10)); // first expiry parks to DLQ
        push_one(&conn, "q", "m", 0);
        let m = consume_one(&conn, "q", 1_000);
        let (_, _) = expire_leases(&conn, 5_000).unwrap();

        let (reply, _) = move_to_queue(&conn, "q", &m.id).unwrap();
        assert!(matches!(reply, QueueReply::Bool(true)));
        let back = consume_one(&conn, "q", 10_000);
        assert_eq!(back.id, m.id);
        assert_eq!(back.attempts, 1, "replayed message starts a fresh attempt count");
        assert_eq!(back.failure_reason, None);
    }

    #[test]
    fn gc_drains_children_then_row() {
        let conn = test_conn();
        make_queue(&conn, "q", config(0, 10));
        push_one(&conn, "q", "m", 0);
        let m = consume_one(&conn, "q", 1_000);
        let (_, _) = expire_leases(&conn, 5_000).unwrap();
        delete_queue(&conn, "q").unwrap();

        let (_, fx) = gc_slice(&conn).unwrap();
        // Both child tables drained in one slice → queue row gone, no followup.
        assert!(fx.followups.is_empty());
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM queues", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        let msgs: i64 = conn
            .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(msgs, 0);
        let dlq: i64 = conn
            .query_row("SELECT COUNT(*) FROM dlq", [], |r| r.get(0))
            .unwrap();
        assert_eq!(dlq, 0);
        let _ = m;
    }
}

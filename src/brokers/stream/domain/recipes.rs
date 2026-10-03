//! Transaction recipes: every stream command is a deterministic function over
//! `&Connection` executed inside the writer's transaction. Recipes never touch
//! async, channels or timers — they stage `Effects` that the worker applies
//! after commit.
//!
//! Conventions:
//! - `Expected` errors roll back only this command's savepoint and become the
//!   command reply; `Fatal` errors abort the whole batch transaction.
//! - u64 orderable columns are `BLOB(8)` big-endian (`types::encode_u64`).
//! - A lane's `cursor_pos` is the last resolved ORIGINAL position; intents
//!   (replays) live strictly below it once the lane is unparked, because the
//!   unpark jump sets the cursor to the key tail covered by parking.

use rusqlite::{params, Connection, OptionalExtension};

use crate::brokers::stream::domain::definition::{StreamConfig, StreamDefinition};
use crate::brokers::stream::domain::message::{
    BatchDisposition, BatchUpdate, DeliveryBatch, DlsEntry, FetchLimits, FetchPage, Message,
};
use crate::brokers::stream::domain::ops::{Continuation, Effects, StreamReply, StreamRequest};
use crate::brokers::stream::domain::types::{
    decode_u64, encode_u64, event_logical_bytes, fetch_batch_header_bytes,
    fetch_event_encoded_bytes, DlsReason, LaneState, Origin, FETCH_PAGE_HEADER_BYTES,
};
use crate::brokers::stream::options::SeekTarget;
use crate::brokers::{config_conflict_error, BrokerError, ProvisionOutcome, ProvisionResult};
use crate::durable::{exec, expected, fatal, query_one, query_vec, sql, CmdError, R};
use bytes::Bytes;
use uuid::Uuid;

/// Rows processed per continuation slice / maintenance batch.
pub const BATCH_LIMIT: usize = 1024;

const KEY_GC_PAGE: usize = 64;
/// Groups reclaimed per GC slice; each group deletes its epoch rows + group
/// row atomically (deferred `active_epoch` FK).
const GROUP_GC_BATCH: usize = 64;
/// Keys initialized per `InitEpoch` slice.
pub const INIT_PAGE: i64 = 512;

/// Batch context carrying the fetch response byte budget — the one knob
/// recipes need that the shared `durable::ExecCtx` does not model.
#[derive(Clone, Copy)]
pub struct ExecCtx {
    pub now_ms: u64,
    /// Max encoded size of one FETCH response including the 4-byte count.
    pub fetch_response_bytes: u64,
}

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

pub struct StreamRow {
    pub id: [u8; 16],
    pub name: String,
    pub config: StreamConfig,
    pub last_seq: u64,
    pub retained_after: u64,
    pub last_key_id: i64,
    pub logical_bytes: u64,
}

#[derive(Clone)]
pub struct EpochRow {
    pub id: [u8; 16],
    pub group_id: [u8; 16],
    pub stream_id: [u8; 16],
    pub start_after: u64,
    pub initialized: bool,
    pub init_cursor: i64,
    pub init_target: i64,
    pub keyless_cursor: u64,
    pub keyless_ticket: i64,
    pub next_ticket: i64,
    pub pending: i64,
    pub pending_bytes: i64,
    pub max_pending: i64,
    pub max_pending_bytes: i64,
}

#[derive(Clone)]
pub struct LaneRow {
    pub key_id: i64,
    pub cursor_pos: u64,
    pub normal_seq: Option<u64>,
    pub head_seq: Option<u64>,
    pub head_pos: Option<u64>,
    pub head_origin: Option<i64>,
    pub state: i64,
    pub attempts: i64,
    pub receipt: Option<Vec<u8>>,
    pub owner: Option<String>,
    pub connection_id: Option<String>,
    pub deadline_ms: Option<i64>,
    pub lease_first_pos: Option<u64>,
    pub lease_count: i64,
    pub lease_acked: i64,
    pub lease_bytes: i64,
}

// ---------------------------------------------------------------------------
// Small decoders / lookups
// ---------------------------------------------------------------------------

fn blob16(bytes: &[u8], what: &str) -> R<[u8; 16]> {
    bytes.try_into().map_err(|_| {
        CmdError::Fatal(BrokerError::storage(format!(
            "Corrupt storage: {what} is not 16 bytes"
        )))
    })
}

fn blob8(row: &rusqlite::Row<'_>, idx: usize, what: &str) -> rusqlite::Result<u64> {
    let bytes = row.get::<_, Vec<u8>>(idx)?;
    decode_u64(&bytes).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            bytes.len(),
            rusqlite::types::Type::Blob,
            format!("{what} is not 8 bytes").into(),
        )
    })
}

fn opt_blob8(row: &rusqlite::Row<'_>, idx: usize, what: &str) -> rusqlite::Result<Option<u64>> {
    match row.get::<_, Option<Vec<u8>>>(idx)? {
        Some(bytes) => decode_u64(&bytes).map(Some).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                bytes.len(),
                rusqlite::types::Type::Blob,
                format!("{what} is not 8 bytes").into(),
            )
        }),
        None => Ok(None),
    }
}

fn parse_config(json: &str) -> R<StreamConfig> {
    serde_json::from_str(json).map_err(|e| {
        CmdError::Fatal(BrokerError::storage(format!(
            "Corrupt storage: persisted stream config is invalid: {e}"
        )))
    })
}

pub fn config_json(config: &StreamConfig) -> serde_json::Value {
    serde_json::json!({
        "retention": {
            "maxAgeMs": config.retention.max_age_ms,
            "maxBytes": config.retention.max_bytes,
        },
        "maxAckPending": config.max_ack_pending,
        "maxAckPendingBytes": config.max_ack_pending_bytes,
        "ackWaitMs": config.ack_wait_ms,
        "maxDeliveries": config.max_deliveries,
    })
}

fn load_stream(conn: &Connection, name: &str) -> R<Option<StreamRow>> {
    let row = query_one(
        conn,
        "SELECT id, config_json, last_seq, retained_after_seq, last_key_id, logical_bytes
         FROM streams WHERE name = ?1 AND deleted = 0",
        [name],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, String>(1)?,
                blob8(r, 2, "last_seq")?,
                blob8(r, 3, "retained_after_seq")?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
            ))
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot load stream", e))?;
    let Some((id, config_json, last_seq, retained_after, last_key_id, logical)) = row else {
        return Ok(None);
    };
    if logical < 0 {
        return Err(CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: negative logical_bytes",
        )));
    }
    Ok(Some(StreamRow {
        id: blob16(&id, "stream id")?,
        name: name.to_string(),
        config: parse_config(&config_json)?,
        last_seq,
        retained_after,
        last_key_id,
        logical_bytes: logical as u64,
    }))
}

fn require_stream(conn: &Connection, name: &str) -> R<StreamRow> {
    load_stream(conn, name)?
        .ok_or_else(|| expected(BrokerError::not_found(format!("Stream '{name}' not found"))))
}

struct GroupRow {
    id: [u8; 16],
    active_epoch: Option<[u8; 16]>,
}

fn load_group(conn: &Connection, stream_id: &[u8; 16], name: &str) -> R<Option<GroupRow>> {
    query_one(
        conn,
        "SELECT id, active_epoch FROM groups WHERE stream_id = ?1 AND name = ?2",
        params![stream_id.as_slice(), name],
        |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<Vec<u8>>>(1)?)),
    )
    .optional()
    .map_err(|e| fatal("Cannot load group", e))?
    .map(|(id, epoch)| {
        Ok(GroupRow {
            id: blob16(&id, "group id")?,
            active_epoch: epoch.map(|e| blob16(&e, "active epoch")).transpose()?,
        })
    })
    .transpose()
}

fn require_group(conn: &Connection, stream_id: &[u8; 16], name: &str) -> R<GroupRow> {
    load_group(conn, stream_id, name)?
        .ok_or_else(|| expected(BrokerError::not_found(format!("Group '{name}' not found"))))
}

fn load_epoch(conn: &Connection, epoch_id: &[u8; 16]) -> R<Option<EpochRow>> {
    query_one(
        conn,
        "SELECT id, group_id, stream_id, start_after_seq, initialized, init_key_cursor,
                init_key_target, keyless_cursor_pos, keyless_ticket, next_ready_ticket,
                pending_count, pending_bytes, max_pending, max_pending_bytes
         FROM group_epochs WHERE id = ?1",
        [epoch_id.as_slice()],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, Vec<u8>>(2)?,
                blob8(r, 3, "start_after_seq")?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                blob8(r, 7, "keyless_cursor_pos")?,
                r.get::<_, i64>(8)?,
                r.get::<_, i64>(9)?,
                r.get::<_, i64>(10)?,
                r.get::<_, i64>(11)?,
                r.get::<_, i64>(12)?,
                r.get::<_, i64>(13)?,
            ))
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot load epoch", e))?
    .map(
        |(
            id,
            group_id,
            stream_id,
            start_after,
            initialized,
            init_cursor,
            init_target,
            keyless_cursor,
            keyless_ticket,
            next_ticket,
            pending,
            pending_bytes,
            max_pending,
            max_pending_bytes,
        )| {
            Ok(EpochRow {
                id: blob16(&id, "epoch id")?,
                group_id: blob16(&group_id, "group id")?,
                stream_id: blob16(&stream_id, "stream id")?,
                start_after,
                initialized: initialized != 0,
                init_cursor,
                init_target,
                keyless_cursor,
                keyless_ticket,
                next_ticket,
                pending,
                pending_bytes,
                max_pending,
                max_pending_bytes,
            })
        },
    )
    .transpose()
}

fn require_epoch(conn: &Connection, group: &GroupRow) -> R<EpochRow> {
    let epoch_id = group.active_epoch.ok_or_else(|| {
        CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: group without active epoch",
        ))
    })?;
    load_epoch(conn, &epoch_id)?.ok_or_else(|| {
        CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: active epoch row missing",
        ))
    })
}

fn save_epoch(conn: &Connection, epoch: &EpochRow) -> R<()> {
    sql(
        exec(
            conn,
            "UPDATE group_epochs SET initialized=?2, init_key_cursor=?3, init_key_target=?4,
                keyless_cursor_pos=?5, keyless_ticket=?6, next_ready_ticket=?7,
                pending_count=?8, pending_bytes=?9
             WHERE id=?1",
            params![
                epoch.id.as_slice(),
                epoch.initialized as i64,
                epoch.init_cursor,
                epoch.init_target,
                encode_u64(epoch.keyless_cursor).as_slice(),
                epoch.keyless_ticket,
                epoch.next_ticket,
                epoch.pending,
                epoch.pending_bytes,
            ],
        ),
        "Cannot persist epoch",
    )?;
    Ok(())
}

fn take_ticket(epoch: &mut EpochRow) -> R<i64> {
    let ticket = epoch.next_ticket;
    epoch.next_ticket = epoch
        .next_ticket
        .checked_add(1)
        .ok_or_else(|| fatal("Ready ticket counter exhausted", ""))?;
    Ok(ticket)
}

/// Greatest `key_pos` of `key_id` whose event seq is `<= seq_boundary`.
/// Position and seq are correlated within a key, so this is the last position
/// already "consumed" for a lane starting after `seq_boundary`.
fn key_boundary_pos(
    conn: &Connection,
    stream_id: &[u8; 16],
    key_id: i64,
    seq_boundary: u64,
) -> R<u64> {
    query_one(
        conn,
        "SELECT key_pos FROM events WHERE stream_id=?1 AND key_id=?2 AND seq<=?3
         ORDER BY seq DESC LIMIT 1",
        params![
            stream_id.as_slice(),
            key_id,
            encode_u64(seq_boundary).as_slice()
        ],
        |r| blob8(r, 0, "key_pos"),
    )
    .optional()
    .map_err(|e| fatal("Cannot resolve key boundary", e))
    .map(|o| o.unwrap_or(0))
}

/// First retained event position strictly above `pos` for a key.
fn next_event_pos(
    conn: &Connection,
    stream_id: &[u8; 16],
    key_id: i64,
    pos: u64,
) -> R<Option<(u64, u64)>> {
    query_one(
        conn,
        "SELECT key_pos, seq FROM events WHERE stream_id=?1 AND key_id=?2 AND key_pos>?3
         ORDER BY key_pos LIMIT 1",
        params![stream_id.as_slice(), key_id, encode_u64(pos).as_slice()],
        |r| Ok((blob8(r, 0, "key_pos")?, blob8(r, 1, "seq")?)),
    )
    .optional()
    .map_err(|e| fatal("Cannot load next event position", e))
}

/// Smallest retained event seq inside `[first_pos, last_pos]` for a key.
fn first_retained_seq_in_range(
    conn: &Connection,
    stream_id: &[u8; 16],
    key_id: i64,
    first_pos: u64,
    last_pos: Option<u64>,
) -> R<Option<u64>> {
    query_one(
        conn,
        "SELECT seq FROM events WHERE stream_id=?1 AND key_id=?2 AND key_pos>=?3
           AND (?4 IS NULL OR key_pos<=?4) ORDER BY key_pos LIMIT 1",
        params![
            stream_id.as_slice(),
            key_id,
            encode_u64(first_pos).as_slice(),
            last_pos.map(|p| encode_u64(p).to_vec()),
        ],
        |r| blob8(r, 0, "seq"),
    )
    .optional()
    .map_err(|e| fatal("Cannot resolve DLS range member", e))
}

fn key_tail_pos(conn: &Connection, stream_id: &[u8; 16], key_id: i64) -> R<u64> {
    query_one(
        conn,
        "SELECT last_pos FROM stream_keys WHERE stream_id=?1 AND key_id=?2",
        params![stream_id.as_slice(), key_id],
        |r| blob8(r, 0, "last_pos"),
    )
    .optional()
    .map_err(|e| fatal("Cannot load key tail", e))
    .map(|o| o.unwrap_or(0))
}

fn insert_range(
    conn: &Connection,
    epoch: &EpochRow,
    key_id: i64,
    first_pos: u64,
    last_pos: Option<u64>,
    reason: DlsReason,
    attempts: i64,
) -> R<()> {
    let first_seq =
        first_retained_seq_in_range(conn, &epoch.stream_id, key_id, first_pos, last_pos)?;
    sql(
        exec(
            conn,
            "INSERT INTO dls_ranges(epoch_id, stream_id, key_id, first_pos, last_pos, first_seq,
                                    reason, attempts)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                epoch.id.as_slice(),
                epoch.stream_id.as_slice(),
                key_id,
                encode_u64(first_pos).as_slice(),
                last_pos.map(|p| encode_u64(p).to_vec()),
                first_seq.map(|s| encode_u64(s).to_vec()),
                reason as i64,
                attempts,
            ],
        ),
        "Cannot insert DLS range",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Lane transitions
// ---------------------------------------------------------------------------

/// Recompute `normal_seq` (first unresolved original seq) and the lane head
/// (min of next original and oldest replay intent, by position), then persist
/// the EMPTY/READY state. LEASED lanes keep their in-flight lease; PARKED
/// lanes must not call this. A WAITING caller whose head changed must drop
/// the head first (retention path).
/// Returns true when the lane transitioned into READY.
fn recompute_lane(conn: &Connection, epoch: &mut EpochRow, lane: &LaneRow) -> R<bool> {
    let next_original = next_event_pos(conn, &epoch.stream_id, lane.key_id, lane.cursor_pos)?;
    let normal_seq = next_original.map(|(_, seq)| seq);

    let intent: Option<(u64, u64)> = query_one(
        conn,
        "SELECT key_pos, seq FROM replay_intents WHERE epoch_id=?1 AND key_id=?2
         ORDER BY key_pos LIMIT 1",
        params![epoch.id.as_slice(), lane.key_id],
        |r| Ok((blob8(r, 0, "key_pos")?, blob8(r, 1, "seq")?)),
    )
    .optional()
    .map_err(|e| fatal("Cannot load replay intents", e))?;

    let head = match (next_original, intent) {
        (Some((pos, seq)), Some((ipos, iseq))) => {
            if ipos <= pos {
                Some((iseq, ipos, Origin::Replay))
            } else {
                Some((seq, pos, Origin::Original))
            }
        }
        (Some((pos, seq)), None) => Some((seq, pos, Origin::Original)),
        (None, Some((ipos, iseq))) => Some((iseq, ipos, Origin::Replay)),
        (None, None) => None,
    };

    let (state, ticket, hseq, hpos, horigin, attempts) = match head {
        Some((seq, pos, origin)) => (
            LaneState::Ready as i64,
            take_ticket(epoch)?,
            Some(seq),
            Some(pos),
            Some(origin as i64),
            0i64,
        ),
        None => (LaneState::Empty as i64, 0, None, None, None, 0),
    };
    // cursor_pos is written from the in-memory row: callers that advance the
    // cursor (checkpoint) mutate the copy first, fusing the advance into this
    // write.
    sql(
        exec(
            conn,
            &format!(
                "UPDATE key_lanes SET cursor_pos=?3, normal_seq=?4, head_seq=?5, head_pos=?6,
                    head_origin=?7, state=?8, attempts=?9, ready_ticket=?10, deadline_ms=NULL,
                    {CLEAR_LEASE}
                 WHERE epoch_id=?1 AND key_id=?2"
            ),
            params![
                epoch.id.as_slice(),
                lane.key_id,
                encode_u64(lane.cursor_pos).as_slice(),
                normal_seq.map(encode_u64).map(|b| b.to_vec()),
                hseq.map(encode_u64).map(|b| b.to_vec()),
                hpos.map(encode_u64).map(|b| b.to_vec()),
                horigin,
                state,
                attempts,
                ticket,
            ],
        ),
        "Cannot update lane",
    )?;
    Ok(head.is_some())
}

/// Insert a lane that does not exist yet for `(epoch, key_id)`, resolving its
/// original cursor against the epoch start boundary. Returns the row when the
/// lane was created, `None` when nothing is pending for the key (no lane is
/// worth materializing: lazy creation keeps the table proportional to work).
fn ensure_lane(
    conn: &Connection,
    epoch: &mut EpochRow,
    key_id: i64,
    last_pos: u64,
) -> R<Option<LaneRow>> {
    let boundary = key_boundary_pos(conn, &epoch.stream_id, key_id, epoch.start_after)?;
    // `last_pos` already includes the event being inserted (publish bumps it
    // before lane fan-out), so boundary >= last_pos means nothing is pending.
    if boundary >= last_pos {
        return Ok(None);
    }
    let cursor = boundary;
    sql(
        exec(
            conn,
            "INSERT INTO key_lanes(epoch_id, stream_id, key_id, cursor_pos, state, attempts,
                                   ready_ticket)
             VALUES (?1,?2,?3,?4,0,0,0)",
            params![
                epoch.id.as_slice(),
                epoch.stream_id.as_slice(),
                key_id,
                encode_u64(cursor).as_slice(),
            ],
        ),
        "Cannot create lane",
    )?;
    Ok(Some(LaneRow {
        key_id,
        cursor_pos: cursor,
        normal_seq: None,
        head_seq: None,
        head_pos: None,
        head_origin: None,
        state: LaneState::Empty as i64,
        attempts: 0,
        receipt: None,
        owner: None,
        connection_id: None,
        deadline_ms: None,
        lease_first_pos: None,
        lease_count: 0,
        lease_acked: 0,
        lease_bytes: 0,
    }))
}

/// Lane row mapping shared by every `key_lanes` select: the columns of
/// `LANE_COLS` in canonical order starting at `base`, so point lookups and
/// IN-scans map the same shape.
fn lane_row(r: &rusqlite::Row<'_>, base: usize) -> rusqlite::Result<LaneRow> {
    Ok(LaneRow {
        key_id: r.get(base)?,
        cursor_pos: blob8(r, base + 1, "cursor_pos")?,
        normal_seq: opt_blob8(r, base + 2, "normal_seq")?,
        head_seq: opt_blob8(r, base + 3, "head_seq")?,
        head_pos: opt_blob8(r, base + 4, "head_pos")?,
        head_origin: r.get(base + 5)?,
        state: r.get(base + 6)?,
        attempts: r.get(base + 7)?,
        receipt: r.get(base + 8)?,
        owner: r.get(base + 9)?,
        connection_id: r.get(base + 10)?,
        deadline_ms: r.get(base + 11)?,
        lease_first_pos: opt_blob8(r, base + 12, "lease_first_pos")?,
        lease_count: r.get(base + 13)?,
        lease_acked: r.get(base + 14)?,
        lease_bytes: r.get(base + 15)?,
    })
}

/// Lane columns in `lane_row` order — every `key_lanes` select uses this
/// exact list so the mapping stays single-sourced by convention.
const LANE_COLS: &str = "key_id, cursor_pos, normal_seq, head_seq, head_pos, head_origin, state,
    attempts, receipt, owner, connection_id, deadline_ms, lease_first_pos, lease_count,
    lease_acked, lease_bytes";

fn load_lane(conn: &Connection, epoch_id: &[u8; 16], key_id: i64) -> R<Option<LaneRow>> {
    query_one(
        conn,
        &format!("SELECT {LANE_COLS} FROM key_lanes WHERE epoch_id=?1 AND key_id=?2"),
        params![epoch_id.as_slice(), key_id],
        |r| lane_row(r, 0),
    )
    .optional()
    .map_err(|e| fatal("Cannot load lane", e))
}

fn load_lane_by_receipt(
    conn: &Connection,
    epoch_id: &[u8; 16],
    receipt: &[u8; 16],
) -> R<Option<LaneRow>> {
    query_one(
        conn,
        &format!(
            "SELECT {LANE_COLS} FROM key_lanes
             WHERE epoch_id=?1 AND state=2 AND receipt=?2"
        ),
        params![epoch_id.as_slice(), receipt.as_slice()],
        |r| lane_row(r, 0),
    )
    .optional()
    .map_err(|e| fatal("Cannot load lane by receipt", e))
}

const CLEAR_LEASE: &str = "receipt=NULL, owner=NULL, connection_id=NULL,
    lease_first_pos=NULL, lease_count=0, lease_acked=0, lease_bytes=0";

fn set_lane_ready(conn: &Connection, epoch: &mut EpochRow, lane: &LaneRow, attempts: i64) -> R<()> {
    let ticket = take_ticket(epoch)?;
    sql(
        exec(
            conn,
            &format!(
                "UPDATE key_lanes SET state=1, attempts=?3, ready_ticket=?4, deadline_ms=NULL,
                    {CLEAR_LEASE}
                 WHERE epoch_id=?1 AND key_id=?2"
            ),
            params![epoch.id.as_slice(), lane.key_id, attempts, ticket],
        ),
        "Cannot ready lane",
    )?;
    Ok(())
}

fn set_lane_waiting(
    conn: &Connection,
    epoch: &EpochRow,
    lane: &LaneRow,
    head: (u64, u64, i64),
    attempts: i64,
    retry_at_ms: u64,
) -> R<()> {
    sql(
        exec(
            conn,
            &format!(
                "UPDATE key_lanes SET state=4, attempts=?3, ready_ticket=0, head_seq=?4,
                    head_pos=?5, head_origin=?6, deadline_ms=?7, cursor_pos=?8,
                    normal_seq=?9, {CLEAR_LEASE}
                 WHERE epoch_id=?1 AND key_id=?2"
            ),
            params![
                epoch.id.as_slice(),
                lane.key_id,
                attempts,
                encode_u64(head.0).as_slice(),
                encode_u64(head.1).as_slice(),
                head.2,
                retry_at_ms as i64,
                encode_u64(lane.cursor_pos).as_slice(),
                lane.normal_seq.map(encode_u64).map(|b| b.to_vec()),
            ],
        ),
        "Cannot wait lane",
    )?;
    Ok(())
}

/// Park a lane: the failed head becomes the first member of (or extends) the
/// DLS range set, the lane loses its head/frontier, and `attempts` of the
/// failed head are preserved on the range.
fn park_lane(conn: &Connection, epoch: &mut EpochRow, lane: &LaneRow, reason: DlsReason) -> R<()> {
    let head_pos = lane
        .head_pos
        .ok_or_else(|| CmdError::Fatal(BrokerError::storage("Cannot park lane without a head")))?;
    match lane.head_origin {
        Some(o) if o == Origin::Replay as i64 => {
            // A re-failed replay parks as a singleton interval.
            insert_range(
                conn,
                epoch,
                lane.key_id,
                head_pos,
                Some(head_pos),
                reason,
                lane.attempts,
            )?;
            sql(
                exec(
                    conn,
                    "DELETE FROM replay_intents WHERE epoch_id=?1 AND key_id=?2 AND key_pos=?3",
                    params![
                        epoch.id.as_slice(),
                        lane.key_id,
                        encode_u64(head_pos).as_slice()
                    ],
                ),
                "Cannot consume replay intent",
            )?;
        }
        _ => {
            // Original head exhausted: the open interval covers this position
            // and every later original until the park is resolved.
            insert_range(
                conn,
                epoch,
                lane.key_id,
                head_pos,
                None,
                reason,
                lane.attempts,
            )?;
        }
    }
    sql(
        exec(
            conn,
            &format!(
                "UPDATE key_lanes SET state=3, normal_seq=NULL, head_seq=NULL, head_pos=NULL,
                    head_origin=NULL, attempts=0, ready_ticket=0, deadline_ms=NULL,
                    cursor_pos=?3, {CLEAR_LEASE}
                 WHERE epoch_id=?1 AND key_id=?2"
            ),
            params![
                epoch.id.as_slice(),
                lane.key_id,
                encode_u64(lane.cursor_pos).as_slice()
            ],
        ),
        "Cannot park lane",
    )?;
    Ok(())
}

/// Reclaim one leased keyed lane after its lease is invalidated (expiry,
/// member exit, recovery): refund the unconfirmed suffix, then READY the
/// first unconfirmed head while attempts remain or PARK it once
/// `max_deliveries` is exhausted. `charge_pending` decrements the epoch
/// credit (recovery sweeps reconcile counters wholesale instead).
fn reclaim_lane_lease(
    conn: &Connection,
    epoch: &mut EpochRow,
    lane: &LaneRow,
    max_deliveries: u32,
    charge_pending: bool,
) -> R<()> {
    if charge_pending {
        epoch.pending -= lane.lease_count - lane.lease_acked;
        epoch.pending_bytes -= lane.lease_bytes;
    }
    if lane.attempts >= max_deliveries as i64 {
        park_lane(conn, epoch, lane, DlsReason::MaxDeliveries)?;
    } else {
        set_lane_ready(conn, epoch, lane, lane.attempts)?;
    }
    Ok(())
}

fn reclaim_keyless_lease(
    conn: &Connection,
    epoch: &mut EpochRow,
    seq: u64,
    key_pos: u64,
    attempts: i64,
    lease_bytes: i64,
    max_deliveries: u32,
    charge_pending: bool,
) -> R<()> {
    if charge_pending {
        epoch.pending -= 1;
        epoch.pending_bytes -= lease_bytes;
    }
    if attempts >= max_deliveries as i64 {
        sql(
            exec(
                conn,
                "DELETE FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2",
                params![epoch.id.as_slice(), encode_u64(seq).as_slice()],
            ),
            "Cannot delete keyless delivery",
        )?;
        insert_range(
            conn,
            epoch,
            0,
            key_pos,
            Some(key_pos),
            DlsReason::MaxDeliveries,
            attempts,
        )?;
    } else {
        let ticket = take_ticket(epoch)?;
        sql(
            exec(
                conn,
                "UPDATE keyless_deliveries SET state=1, ready_ticket=?3, receipt=NULL, owner=NULL,
                    connection_id=NULL, deadline_ms=NULL, lease_bytes=0
                 WHERE epoch_id=?1 AND seq=?2",
                params![epoch.id.as_slice(), encode_u64(seq).as_slice(), ticket],
            ),
            "Cannot requeue keyless delivery",
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Recovery entry points (called by Store::recover)
// ---------------------------------------------------------------------------

pub fn recover_lane_lease(
    conn: &Connection,
    epoch_id: &[u8],
    key_id: i64,
    _attempts: i64,
) -> Result<(), BrokerError> {
    let epoch_id: [u8; 16] = match epoch_id.try_into() {
        Ok(id) => id,
        Err(_) => {
            return Err(BrokerError::storage(
                "Corrupt storage: epoch id is not 16 bytes",
            ))
        }
    };
    let mut epoch = load_epoch(conn, &epoch_id)
        .map_err(|e| e.into_error())?
        .ok_or_else(|| BrokerError::storage("Corrupt storage: leased lane without epoch"))?;
    let lane = load_lane(conn, &epoch_id, key_id)
        .map_err(|e| e.into_error())?
        .ok_or_else(|| BrokerError::storage("Corrupt storage: leased lane missing"))?;
    let config = load_config_for_stream(conn, &epoch.stream_id)?;
    reclaim_lane_lease(conn, &mut epoch, &lane, config.max_deliveries, false)
        .map_err(|e| e.into_error())?;
    save_epoch(conn, &epoch).map_err(|e| e.into_error())
}

pub fn recover_keyless_lease(
    conn: &Connection,
    epoch_id: &[u8],
    seq: &[u8],
    attempts: i64,
) -> Result<(), BrokerError> {
    let epoch_id: [u8; 16] = epoch_id
        .try_into()
        .map_err(|_| BrokerError::storage("Corrupt storage: epoch id is not 16 bytes"))?;
    let seq = decode_u64(seq)?;
    let mut epoch = load_epoch(conn, &epoch_id)
        .map_err(|e| e.into_error())?
        .ok_or_else(|| BrokerError::storage("Corrupt storage: leased delivery without epoch"))?;
    let row = query_one(
        conn,
        "SELECT key_pos, lease_bytes FROM keyless_deliveries
         WHERE epoch_id=?1 AND seq=?2",
        params![epoch_id.as_slice(), encode_u64(seq).as_slice()],
        |r| Ok((blob8(r, 0, "key_pos")?, r.get::<_, i64>(1)?)),
    )
    .optional()
    .map_err(|e| BrokerError::storage(format!("Corrupt keyless delivery: {e}")))?
    .ok_or_else(|| BrokerError::storage("Corrupt storage: leased delivery missing"))?;
    let (key_pos, lease_bytes) = row;
    let config = load_config_for_stream(conn, &epoch.stream_id)?;
    reclaim_keyless_lease(
        conn,
        &mut epoch,
        seq,
        key_pos,
        attempts,
        lease_bytes,
        config.max_deliveries,
        false,
    )
    .map_err(|e| e.into_error())?;
    save_epoch(conn, &epoch).map_err(|e| e.into_error())
}

fn load_config_for_stream(
    conn: &Connection,
    stream_id: &[u8; 16],
) -> Result<StreamConfig, BrokerError> {
    let json: Option<String> = conn
        .prepare_cached("SELECT config_json FROM streams WHERE id=?1")
        .and_then(|mut s| s.query_row([stream_id.as_slice()], |r| r.get(0)).optional())
        .map_err(|e| BrokerError::storage(format!("Cannot load stream config: {e}")))?;
    let json = json
        .ok_or_else(|| BrokerError::storage("Corrupt storage: epoch references missing stream"))?;
    serde_json::from_str(&json)
        .map_err(|e| BrokerError::storage(format!("Corrupt stream config: {e}")))
}

// ---------------------------------------------------------------------------
// Membership validation
// ---------------------------------------------------------------------------

/// Resolve stream/group/epoch and validate a durable membership in a single
/// statement. The member join is keyed on the ACTIVE epoch (`members` PK is
/// (epoch_id, consumer_id)), so a stale row on a dead epoch never matches —
/// those identities are already fenced by the epoch check above.
/// `g/e/m` columns are NULL-able: their presence encodes which level failed
/// (stream missing → no row; group missing → g NULL; epoch missing → e NULL;
/// member missing → m NULL).
fn resolve_member(
    conn: &Connection,
    name: &str,
    group: &str,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
) -> R<(StreamRow, EpochRow)> {
    let row = query_one(
        conn,
        "SELECT s.id, s.config_json, s.last_seq, s.retained_after_seq, s.last_key_id,
                s.logical_bytes,
                g.id,
                e.id, e.group_id, e.stream_id, e.start_after_seq, e.initialized,
                e.init_key_cursor, e.init_key_target, e.keyless_cursor_pos,
                e.keyless_ticket, e.next_ready_ticket, e.pending_count, e.pending_bytes,
                e.max_pending, e.max_pending_bytes,
                m.connection_id
         FROM streams s
         LEFT JOIN groups g ON g.stream_id = s.id AND g.name = ?2
         LEFT JOIN group_epochs e ON e.id = g.active_epoch
         LEFT JOIN members m ON m.epoch_id = e.id AND m.consumer_id = ?3
         WHERE s.name = ?1 AND s.deleted = 0",
        params![name, group, identity.consumer_id],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, String>(1)?,
                blob8(r, 2, "last_seq")?,
                blob8(r, 3, "retained_after_seq")?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, Option<Vec<u8>>>(6)?,
                r.get::<_, Option<Vec<u8>>>(7)?,
                r.get::<_, Option<Vec<u8>>>(8)?,
                r.get::<_, Option<Vec<u8>>>(9)?,
                opt_blob8(r, 10, "start_after_seq")?,
                r.get::<_, Option<i64>>(11)?,
                r.get::<_, Option<i64>>(12)?,
                r.get::<_, Option<i64>>(13)?,
                opt_blob8(r, 14, "keyless_cursor_pos")?,
                r.get::<_, Option<i64>>(15)?,
                r.get::<_, Option<i64>>(16)?,
                r.get::<_, Option<i64>>(17)?,
                r.get::<_, Option<i64>>(18)?,
                r.get::<_, Option<i64>>(19)?,
                r.get::<_, Option<i64>>(20)?,
                r.get::<_, Option<String>>(21)?,
            ))
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot resolve member", e))?;
    let Some((
        stream_id,
        config_json,
        last_seq,
        retained_after,
        last_key_id,
        logical,
        group_id,
        epoch_id,
        epoch_group,
        epoch_stream,
        start_after,
        initialized,
        init_cursor,
        init_target,
        keyless_cursor,
        keyless_ticket,
        next_ticket,
        pending,
        pending_bytes,
        max_pending,
        max_pending_bytes,
        member_conn,
    )) = row
    else {
        return Err(expected(BrokerError::not_found(format!(
            "Stream '{name}' not found"
        ))));
    };
    if logical < 0 {
        return Err(CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: negative logical_bytes",
        )));
    }
    if group_id.is_none() {
        return Err(expected(BrokerError::not_found(format!(
            "Group '{group}' not found"
        ))));
    }
    let (Some(epoch_id), Some(epoch_group), Some(epoch_stream)) =
        (epoch_id, epoch_group, epoch_stream)
    else {
        return Err(CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: group without active epoch",
        )));
    };
    let epoch_id = blob16(&epoch_id, "epoch id")?;
    if identity.epoch != epoch_id {
        return Err(expected(BrokerError::fenced()));
    }
    let epoch = EpochRow {
        id: epoch_id,
        group_id: blob16(&epoch_group, "group id")?,
        stream_id: blob16(&epoch_stream, "stream id")?,
        start_after: start_after.ok_or_else(|| {
            CmdError::Fatal(BrokerError::storage("Corrupt storage: epoch row"))
        })?,
        initialized: initialized.unwrap_or(0) != 0,
        init_cursor: init_cursor.unwrap_or(0),
        init_target: init_target.unwrap_or(0),
        keyless_cursor: keyless_cursor.ok_or_else(|| {
            CmdError::Fatal(BrokerError::storage("Corrupt storage: epoch row"))
        })?,
        keyless_ticket: keyless_ticket.unwrap_or(0),
        next_ticket: next_ticket.unwrap_or(0),
        pending: pending.unwrap_or(0),
        pending_bytes: pending_bytes.unwrap_or(0),
        max_pending: max_pending.unwrap_or(0),
        max_pending_bytes: max_pending_bytes.unwrap_or(0),
    };
    if member_conn.as_deref() != Some(identity.connection_id.as_str()) {
        return Err(expected(BrokerError::not_member()));
    }
    Ok((
        StreamRow {
            id: blob16(&stream_id, "stream id")?,
            name: name.to_string(),
            config: parse_config(&config_json)?,
            last_seq,
            retained_after,
            last_key_id,
            logical_bytes: logical as u64,
        },
        epoch,
    ))
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

pub fn execute(
    conn: &Connection,
    op: &StreamRequest,
    ctx: &ExecCtx,
) -> Result<(StreamReply, Effects), CmdError> {
    match op {
        StreamRequest::CreateStream { name, requested } => {
            create_stream_with_config(conn, name, requested)
        }
        StreamRequest::DeleteStream { name } => delete_stream(conn, name),
        StreamRequest::StreamExists { name } => {
            let exists = load_stream(conn, name)?.is_some();
            Ok((StreamReply::Bool(exists), Effects::default()))
        }
        StreamRequest::DescribeStream { name } => {
            let stream = require_stream(conn, name)?;
            Ok((
                StreamReply::Definition(Box::new(StreamDefinition {
                    name: stream.name.clone(),
                    config: stream.config,
                })),
                Effects::default(),
            ))
        }
        StreamRequest::Publish { name, items } => publish(conn, name, items, ctx),
        StreamRequest::Read {
            name,
            from_seq,
            limit,
        } => read(conn, name, *from_seq, *limit),
        StreamRequest::Join {
            name,
            group,
            connection_id,
        } => join(conn, name, group, connection_id, ctx),
        StreamRequest::Fetch {
            name,
            group,
            identity,
            limits,
        } => fetch(conn, name, group, identity, *limits, ctx),
        StreamRequest::UpdateBatch {
            name,
            group,
            identity,
            update,
        } => {
            let (mut outcomes, effects) =
                update_batches(conn, name, group, identity, &[*update], ctx)?;
            match outcomes.pop() {
                Some(Some(kind)) => {
                    Err(expected(BrokerError::new(kind, "Batch update rejected")))
                }
                _ => Ok((StreamReply::Unit, effects)),
            }
        }
        StreamRequest::UpdateBatches {
            name,
            group,
            identity,
            updates,
        } => {
            let (outcomes, effects) =
                update_batches(conn, name, group, identity, updates, ctx)?;
            let failures = updates
                .iter()
                .zip(outcomes)
                .filter_map(|(u, o)| o.map(|k| (u.receipt, k)))
                .collect();
            Ok((StreamReply::BatchOutcome(failures), effects))
        }
        StreamRequest::Leave {
            name,
            group,
            identity,
        } => leave(conn, name, group, identity),
        StreamRequest::Disconnect { connection_id } => disconnect(conn, connection_id),
        StreamRequest::Seek {
            name,
            group,
            target,
        } => seek(conn, name, group, target, ctx),
        StreamRequest::PeekDls {
            name,
            group,
            limit,
            offset,
        } => peek_dls(conn, name, group, *limit, *offset),
        StreamRequest::ReplayDls { name, group, seq } => {
            dls_point_op(conn, name, group, *seq, true)
        }
        StreamRequest::DeleteDls { name, group, seq } => {
            dls_point_op(conn, name, group, *seq, false)
        }
        StreamRequest::PurgeDls { name, group } => purge_dls(conn, name, group),
        StreamRequest::ExpireLeases { now_ms } => expire_leases(conn, *now_ms),
        StreamRequest::RetentionTick => retention_tick(conn, ctx),
        StreamRequest::Shutdown => Ok((StreamReply::Unit, Effects::default())),
    }
}

// ---------------------------------------------------------------------------
// Provisioning
// ---------------------------------------------------------------------------

/// Provisioning: the manager resolves system defaults and hands the concrete
/// config in, keeping recipes free of runtime-config dependencies.
pub fn create_stream_with_config(
    conn: &Connection,
    name: &str,
    requested: &StreamConfig,
) -> Result<(StreamReply, Effects), CmdError> {
    if requested.max_ack_pending_bytes == 0
        || requested.max_ack_pending_bytes > i64::MAX as u64
    {
        return Err(expected(BrokerError::invalid_argument(format!(
            "maxAckPendingBytes must be in 1..={}",
            i64::MAX
        ))));
    }
    if let Some(existing) = load_stream(conn, name)? {
        if existing.config != *requested {
            return Err(expected(config_conflict_error(
                "stream",
                name,
                config_json(requested),
                config_json(&existing.config),
            )));
        }
        return Ok((
            StreamReply::Provision(Box::new(ProvisionResult {
                outcome: ProvisionOutcome::Unchanged,
                definition: StreamDefinition {
                    name: name.to_string(),
                    config: existing.config,
                },
            })),
            Effects::default(),
        ));
    }
    let stream_id = Uuid::new_v4().into_bytes();
    let config_json =
        serde_json::to_string(requested).map_err(|e| fatal("Cannot serialize stream config", e))?;
    sql(
        exec(
            conn,
            "INSERT INTO streams(id, name, deleted, config_json, last_seq, retained_after_seq,
                                 last_key_id, logical_bytes)
             VALUES (?1,?2,0,?3,?4,?5,0,0)",
            params![
                stream_id.as_slice(),
                name,
                config_json,
                encode_u64(0).as_slice(),
                encode_u64(0).as_slice(),
            ],
        ),
        "Cannot create stream",
    )?;
    // Keyless events live under the reserved key_id 0.
    sql(
        exec(
            conn,
            "INSERT INTO stream_keys(stream_id, key_id, key, last_pos, retained_after_pos)
             VALUES (?1,0,?2,?3,?4)",
            params![
                stream_id.as_slice(),
                Vec::<u8>::new(),
                encode_u64(0).as_slice(),
                encode_u64(0).as_slice(),
            ],
        ),
        "Cannot create keyless key row",
    )?;
    Ok((
        StreamReply::Provision(Box::new(ProvisionResult {
            outcome: ProvisionOutcome::Created,
            definition: StreamDefinition {
                name: name.to_string(),
                config: requested.clone(),
            },
        })),
        Effects::default(),
    ))
}

fn delete_stream(conn: &Connection, name: &str) -> Result<(StreamReply, Effects), CmdError> {
    let Some(stream) = load_stream(conn, name)? else {
        return Ok((StreamReply::Unit, Effects::default()));
    };
    sql(
        exec(
            conn,
            "UPDATE streams SET deleted=1 WHERE id=?1",
            [stream.id.as_slice()],
        ),
        "Cannot mark stream deleted",
    )?;
    let mut effects = Effects {
        deleted_streams: vec![stream.name.clone()],
        followups: vec![Continuation::Gc],
        ..Default::default()
    };
    // Wake every live group of the stream so pending fetches fail fast.
    let groups = query_vec(
        conn,
        "SELECT name FROM groups WHERE stream_id=?1",
        [stream.id.as_slice()],
        |r| r.get::<_, String>(0),
        "Cannot list groups",
    )?;
    for group in groups {
        effects.wake(&stream.name, &group);
    }
    Ok((StreamReply::Unit, effects))
}

// ---------------------------------------------------------------------------
// Publish / read
// ---------------------------------------------------------------------------

/// Insert one event batch in a single transaction: seqs are assigned in input
/// order, each distinct key is interned once, key positions advance in memory
/// and flush once per key, and events land via chunked multi-row inserts.
/// Public for the worker's `PubRun` merging; `execute` dispatches single
/// publishes through the same path.
/// Lane fan-out runs once per (epoch, key) rather than per event — every lane
/// transition is deterministic on the final event table, so collapsing the
/// batch is equivalent. Keyless items only move the source watermark lazily
/// at fetch time — no per-group fan-out rows are created.
pub fn publish(
    conn: &Connection,
    name: &str,
    items: &[crate::brokers::stream::domain::message::PubItem],
    ctx: &ExecCtx,
) -> Result<(StreamReply, Effects), CmdError> {
    let mut stream = require_stream(conn, name)?;
    let mut effects = Effects::default();
    if items.is_empty() {
        return Ok((StreamReply::Published(Vec::new()), effects));
    }

    // Live epochs for lane fan-out and wakes, loaded once per batch.
    let mut live_epochs: Vec<EpochRow> = Vec::new();
    {
        let ids = query_vec(
            conn,
            "SELECT e.id FROM group_epochs e
             JOIN groups g ON g.active_epoch = e.id AND g.stream_id = e.stream_id
             WHERE e.stream_id = ?1",
            [stream.id.as_slice()],
            |r| r.get::<_, Vec<u8>>(0),
            "Cannot list live epochs",
        )?;
        for id in ids {
            let id = blob16(&id, "epoch id")?;
            if let Some(epoch) = load_epoch(conn, &id)? {
                live_epochs.push(epoch);
            }
        }
    }
    let epoch_group: std::collections::HashMap<[u8; 16], String> = {
        let mut names: std::collections::HashMap<[u8; 16], String> =
            std::collections::HashMap::new();
        if !live_epochs.is_empty() {
            let rows = query_vec(
                conn,
                "SELECT id, name FROM groups WHERE stream_id=?1",
                [stream.id.as_slice()],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)),
                "Cannot list groups",
            )?;
            for (id, name) in rows {
                names.insert(blob16(&id, "group id")?, name);
            }
        }
        live_epochs
            .iter()
            .filter_map(|e| names.get(&e.group_id).map(|n| (e.id, n.clone())))
            .collect()
    };

    // Intern each distinct key once, in first-appearance order.
    let mut key_ids: std::collections::HashMap<&[u8], i64> =
        std::collections::HashMap::new();
    let mut keyed_order: Vec<i64> = Vec::new();
    for item in items {
        if item.key.is_empty() || key_ids.contains_key(item.key.as_ref()) {
            continue;
        }
        let key_id = intern_key(conn, &mut stream, &item.key)?;
        key_ids.insert(item.key.as_ref(), key_id);
        keyed_order.push(key_id);
    }

    // Assign seqs in input order and key positions in per-key order. Positions
    // advance in memory; `stream_keys.last_pos` flushes once per used key.
    let mut seqs = Vec::with_capacity(items.len());
    let mut rows: Vec<(u64, i64, u64, &crate::brokers::stream::domain::message::PubItem)> =
        Vec::with_capacity(items.len());
    let mut pos_map: std::collections::HashMap<i64, u64> = std::collections::HashMap::new();
    let mut key_events: std::collections::HashMap<i64, Vec<(u64, u64)>> =
        std::collections::HashMap::new();
    let mut logical_added: u64 = 0;
    for item in items {
        let seq = stream
            .last_seq
            .checked_add(1)
            .ok_or_else(|| CmdError::Fatal(BrokerError::storage("Stream sequence exhausted")))?;
        stream.last_seq = seq;
        seqs.push(seq);
        let key_id = if item.key.is_empty() {
            0
        } else {
            key_ids[item.key.as_ref()]
        };
        let pos = match pos_map.entry(key_id) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                *e.get_mut() += 1;
                *e.get()
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                let last = key_tail_pos(conn, &stream.id, key_id)?;
                e.insert(last + 1);
                last + 1
            }
        };
        let logical = event_logical_bytes(item.key.len(), item.payload.len());
        if logical > stream.config.max_ack_pending_bytes {
            return Err(expected(BrokerError::invalid_argument(format!(
                "Stream event of {logical} logical bytes exceeds maxAckPendingBytes {}",
                stream.config.max_ack_pending_bytes
            ))));
        }
        logical_added += logical;
        if key_id != 0 {
            key_events.entry(key_id).or_default().push((pos, seq));
        }
        rows.push((seq, key_id, pos, item));
    }

    insert_events(conn, &stream, ctx.now_ms, &rows)?;

    // Lane fan-out: one IN-scan loads every keyed lane of an epoch, then one
    // transition per (epoch, key) — a lane settles on its final state after
    // the last event of its key, so intermediate transitions are redundant.
    let mut dirty_epochs: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for (idx, epoch) in live_epochs.iter_mut().enumerate() {
        if keyed_order.is_empty() {
            break;
        }
        let lanes: std::collections::HashMap<i64, LaneRow> = {
            let (text, values) = id_in_list(
                &format!("SELECT {LANE_COLS} FROM key_lanes
                 WHERE epoch_id=?1 AND key_id IN ("),
                &epoch.id,
                &keyed_order,
            );
            query_vec(
                conn,
                &text,
                rusqlite::params_from_iter(values),
                |r| Ok((r.get::<_, i64>(0)?, lane_row(r, 0)?)),
                "Cannot load keyed lanes",
            )?
            .into_iter()
            .collect()
        };
        for &key_id in &keyed_order {
            let positions = &key_events[&key_id];
            let first_pos = positions[0].0;
            let first_seq = positions[0].1;
            let lane = match lanes.get(&key_id) {
                Some(lane) => Some(lane.clone()),
                None => ensure_lane(conn, epoch, key_id, first_pos)?,
            };
            let Some(lane) = lane else { continue };
            match LaneState::from_i64(lane.state) {
                Some(LaneState::Parked) => {
                    // Ranges may start at any position inside the batch, so
                    // first_seq must be visited per event, not once per key.
                    for &(pos, seq) in positions {
                        sql(
                            exec(
                                conn,
                                "UPDATE dls_ranges SET first_seq=?4
                                 WHERE epoch_id=?1 AND key_id=?2 AND first_seq IS NULL
                                   AND first_pos<=?3 AND (last_pos IS NULL OR ?3<=last_pos)",
                                params![
                                    epoch.id.as_slice(),
                                    key_id,
                                    encode_u64(pos).as_slice(),
                                    encode_u64(seq).as_slice(),
                                ],
                            ),
                            "Cannot populate DLS first_seq",
                        )?;
                    }
                }
                Some(LaneState::Empty) => {
                    if recompute_lane(conn, epoch, &lane)? {
                        dirty_epochs.insert(idx);
                        if let Some(g) = epoch_group.get(&epoch.id) {
                            effects.wake(name, g);
                        }
                    }
                }
                Some(LaneState::Ready) | Some(LaneState::Leased) | Some(LaneState::Waiting) => {
                    if lane.normal_seq.is_none() {
                        sql(
                            exec(
                                conn,
                                "UPDATE key_lanes SET normal_seq=?3
                                 WHERE epoch_id=?1 AND key_id=?2",
                                params![
                                    epoch.id.as_slice(),
                                    key_id,
                                    encode_u64(first_seq).as_slice()
                                ],
                            ),
                            "Cannot fill lane frontier",
                        )?;
                    }
                }
                None => {
                    return Err(CmdError::Fatal(BrokerError::storage(
                        "Corrupt storage: invalid lane state",
                    )))
                }
            }
        }
    }

    let keyless_published = items.iter().any(|i| i.key.is_empty());
    if keyless_published {
        // The keyless source is computed lazily at fetch time; every live
        // group of the stream may now have fresh work.
        for gname in epoch_group.values() {
            effects.wake(name, gname);
        }
    }

    for (key_id, pos) in pos_map {
        sql(
            exec(
                conn,
                "UPDATE stream_keys SET last_pos=?3 WHERE stream_id=?1 AND key_id=?2",
                params![stream.id.as_slice(), key_id, encode_u64(pos).as_slice()],
            ),
            "Cannot flush key position",
        )?;
    }
    sql(
        exec(
            conn,
            "UPDATE streams SET last_seq=?2, last_key_id=?3, logical_bytes=logical_bytes+?4
             WHERE id=?1",
            params![
                stream.id.as_slice(),
                encode_u64(stream.last_seq).as_slice(),
                stream.last_key_id,
                logical_added as i64,
            ],
        ),
        "Cannot update stream counters",
    )?;
    for idx in dirty_epochs {
        save_epoch(conn, &live_epochs[idx])?;
    }
    Ok((StreamReply::Published(seqs), effects))
}

/// Max events per INSERT statement: 8 bind params per row against the
/// 32766-parameter SQLite limit leaves generous headroom.
const EVENT_INSERT_CHUNK: usize = 200;

/// Insert pre-numbered events as chunked multi-row statements: one statement
/// per event dominated batch publish cost. Params borrow from the caller —
/// payloads are referenced, never copied.
fn insert_events(
    conn: &Connection,
    stream: &StreamRow,
    now_ms: u64,
    rows: &[(u64, i64, u64, &crate::brokers::stream::domain::message::PubItem)],
) -> R<()> {
    for chunk in rows.chunks(EVENT_INSERT_CHUNK) {
        let mut text = String::with_capacity(160 + chunk.len() * 18);
        text.push_str(
            "INSERT INTO events(stream_id, seq, key_id, key_pos, timestamp_ms, payload,
                                payload_bytes, logical_bytes) VALUES ",
        );
        for i in 0..chunk.len() {
            if i > 0 {
                text.push(',');
            }
            text.push_str("(?,?,?,?,?,?,?,?)");
        }
        // Encoded seq/pos and scalar params need owned storage that outlives
        // the statement; buffers are flat so the param list is just indexes.
        let mut enc: Vec<Vec<u8>> = Vec::with_capacity(chunk.len() * 2);
        let mut ints: Vec<i64> = Vec::with_capacity(chunk.len() * 3);
        let mut payloads: Vec<&[u8]> = Vec::with_capacity(chunk.len());
        for (seq, key_id, pos, item) in chunk {
            enc.push(encode_u64(*seq).to_vec());
            enc.push(encode_u64(*pos).to_vec());
            ints.push(*key_id);
            ints.push(item.payload.len() as i64);
            ints.push(event_logical_bytes(item.key.len(), item.payload.len()) as i64);
            payloads.push(item.payload.as_ref());
        }
        let sid: &[u8] = stream.id.as_slice();
        let now = now_ms as i64;
        let mut params: Vec<&dyn rusqlite::types::ToSql> = Vec::with_capacity(chunk.len() * 8);
        for i in 0..chunk.len() {
            params.push(&sid);
            params.push(&enc[2 * i]);
            params.push(&ints[3 * i]);
            params.push(&enc[2 * i + 1]);
            params.push(&now);
            params.push(&payloads[i]);
            params.push(&ints[3 * i + 1]);
            params.push(&ints[3 * i + 2]);
        }
        sql(
            exec(conn, &text, rusqlite::params_from_iter(params)),
            "Cannot insert events",
        )?;
    }
    Ok(())
}

/// Read-or-insert the key row; the keyless row is never interned.
fn intern_key(conn: &Connection, stream: &mut StreamRow, key: &Bytes) -> R<i64> {
    let found = query_one(
        conn,
        "SELECT key_id FROM stream_keys WHERE stream_id=?1 AND key=?2",
        params![stream.id.as_slice(), key.as_ref()],
        |r| r.get::<_, i64>(0),
    )
    .optional()
    .map_err(|e| fatal("Cannot intern key", e))?;
    if let Some(id) = found {
        return Ok(id);
    }
    let id = stream
        .last_key_id
        .checked_add(1)
        .ok_or_else(|| fatal("Key id space exhausted", ""))?;
    stream.last_key_id = id;
    sql(
        exec(
            conn,
            "INSERT INTO stream_keys(stream_id, key_id, key, last_pos, retained_after_pos)
             VALUES (?1,?2,?3,?4,?5)",
            params![
                stream.id.as_slice(),
                id,
                key.as_ref(),
                encode_u64(0).as_slice(),
                encode_u64(0).as_slice(),
            ],
        ),
        "Cannot insert key",
    )?;
    Ok(id)
}

fn read(
    conn: &Connection,
    name: &str,
    from_seq: u64,
    limit: usize,
) -> Result<(StreamReply, Effects), CmdError> {
    let stream = require_stream(conn, name)?;
    let rows = query_vec(
        conn,
        "SELECT e.seq, e.timestamp_ms, k.key, e.payload
         FROM events e JOIN stream_keys k ON k.stream_id=e.stream_id AND k.key_id=e.key_id
         WHERE e.stream_id=?1 AND e.seq>=?2 ORDER BY e.seq LIMIT ?3",
        params![
            stream.id.as_slice(),
            encode_u64(from_seq).as_slice(),
            limit as i64
        ],
        |r| {
            Ok(Message {
                seq: blob8(r, 0, "seq")?,
                timestamp: r.get::<_, i64>(1)? as u64,
                key: Bytes::from(r.get::<_, Vec<u8>>(2)?),
                payload: Bytes::from(r.get::<_, Vec<u8>>(3)?),
            })
        },
        "Cannot read events",
    )?;
    Ok((StreamReply::Read(rows), Effects::default()))
}

// ---------------------------------------------------------------------------
// Group lifecycle: join / fetch / ack / leave / seek / disconnect
// ---------------------------------------------------------------------------

fn epoch_keyless_boundary(conn: &Connection, stream_id: &[u8; 16], start_after: u64) -> R<u64> {
    key_boundary_pos(conn, stream_id, 0, start_after)
}

fn create_group_and_epoch(
    conn: &Connection,
    stream: &StreamRow,
    group_name: &str,
    start_after: u64,
) -> R<(GroupRow, EpochRow)> {
    let group_id = Uuid::new_v4().into_bytes();
    let epoch_id = Uuid::new_v4().into_bytes();
    let keyless_cursor = epoch_keyless_boundary(conn, &stream.id, start_after)?;
    sql(
        exec(
            conn,
            "INSERT INTO groups(id, stream_id, name, active_epoch)
             VALUES (?1,?2,?3,?4)",
            params![
                group_id.as_slice(),
                stream.id.as_slice(),
                group_name,
                epoch_id.as_slice(),
            ],
        ),
        "Cannot create group",
    )?;
    sql(
        exec(
            conn,
            "INSERT INTO group_epochs(id, group_id, stream_id, start_after_seq, initialized,
                init_key_cursor, init_key_target, keyless_cursor_pos, keyless_ticket,
                next_ready_ticket, pending_count, max_pending, max_pending_bytes)
             VALUES (?1,?2,?3,?4,0,0,?5,?6,1,2,0,?7,?8)",
            params![
                epoch_id.as_slice(),
                group_id.as_slice(),
                stream.id.as_slice(),
                encode_u64(start_after).as_slice(),
                stream.last_key_id,
                encode_u64(keyless_cursor).as_slice(),
                stream.config.max_ack_pending as i64,
                stream.config.max_ack_pending_bytes as i64,
            ],
        ),
        "Cannot create epoch",
    )?;
    let group = GroupRow {
        id: group_id,
        active_epoch: Some(epoch_id),
    };
    let epoch = load_epoch(conn, &epoch_id)?.ok_or_else(|| {
        CmdError::Fatal(BrokerError::storage("Corrupt storage: new epoch missing"))
    })?;
    Ok((group, epoch))
}

/// ack_floor = the last seq every live group member has durably resolved:
/// min over per-key unresolved original frontiers and the keyless source.
/// While init pages run, the conservative floor is `start_after_seq`.
fn ack_floor(conn: &Connection, epoch: &EpochRow, stream: &StreamRow) -> R<u64> {
    if !epoch.initialized {
        return Ok(epoch.start_after.max(stream.retained_after));
    }
    let min_lane: Option<u64> = query_one(
        conn,
        "SELECT MIN(normal_seq) FROM key_lanes WHERE epoch_id=?1 AND normal_seq IS NOT NULL
         AND state<>3",
        [epoch.id.as_slice()],
        |r| r.get::<_, Option<Vec<u8>>>(0),
    )
    .map_err(|e| fatal("Cannot compute ack floor", e))?
    .map(|b| decode_u64(&b))
    .transpose()
    .map_err(CmdError::Fatal)?;
    let min_keyless_delivery: Option<u64> = query_one(
        conn,
        "SELECT MIN(seq) FROM keyless_deliveries WHERE epoch_id=?1 AND origin=0",
        [epoch.id.as_slice()],
        |r| r.get::<_, Option<Vec<u8>>>(0),
    )
    .map_err(|e| fatal("Cannot compute ack floor", e))?
    .map(|b| decode_u64(&b))
    .transpose()
    .map_err(CmdError::Fatal)?;
    let next_keyless_fresh: Option<u64> = query_one(
        conn,
        "SELECT seq FROM events WHERE stream_id=?1 AND key_id=0 AND key_pos>?2
         ORDER BY key_pos LIMIT 1",
        params![
            epoch.stream_id.as_slice(),
            encode_u64(epoch.keyless_cursor).as_slice()
        ],
        |r| blob8(r, 0, "seq"),
    )
    .optional()
    .map_err(|e| fatal("Cannot compute ack floor", e))?;

    let floor = [min_lane, min_keyless_delivery, next_keyless_fresh]
        .into_iter()
        .flatten()
        .min()
        .map(|first_unresolved| first_unresolved.saturating_sub(1))
        .unwrap_or(stream.last_seq);
    Ok(floor.max(epoch.start_after).max(stream.retained_after))
}

fn join(
    conn: &Connection,
    name: &str,
    group_name: &str,
    connection_id: &str,
    _ctx: &ExecCtx,
) -> Result<(StreamReply, Effects), CmdError> {
    let stream = require_stream(conn, name)?;
    let mut effects = Effects::default();
    let (group, epoch) = match load_group(conn, &stream.id, group_name)? {
        Some(group) => {
            let epoch = require_epoch(conn, &group)?;
            (group, epoch)
        }
        None => {
            let (group, epoch) =
                create_group_and_epoch(conn, &stream, group_name, stream.retained_after)?;
            effects
                .followups
                .push(Continuation::InitEpoch { epoch_id: epoch.id });
            (group, epoch)
        }
    };
    let consumer_id = Uuid::new_v4().to_string();
    sql(
        exec(
            conn,
            "INSERT INTO members(epoch_id, stream_id, group_id, consumer_id, connection_id)
             VALUES (?1,?2,?3,?4,?5)",
            params![
                epoch.id.as_slice(),
                stream.id.as_slice(),
                group.id.as_slice(),
                consumer_id,
                connection_id,
            ],
        ),
        "Cannot add member",
    )?;
    let floor = ack_floor(conn, &epoch, &stream)?;
    Ok((
        StreamReply::Join {
            ack_floor: floor,
            consumer_id,
            epoch: epoch.id,
        },
        effects,
    ))
}

struct EvMeta {
    seq: u64,
    key_pos: u64,
    timestamp_ms: i64,
    payload_bytes: u64,
    logical: u64,
}

struct KeyedSel {
    key_id: i64,
    receipt: [u8; 16],
    first_pos: u64,
    upper_pos: u64,
    metas: Vec<EvMeta>,
    buf: std::collections::VecDeque<EvMeta>,
    exhausted: bool,
    logical: u64,
    encoded: u64,
}

struct KeylessSel {
    seq: Option<u64>,
    receipt: [u8; 16],
    meta: EvMeta,
}

/// One fetch attempt: validate the lease identity, then claim READY lane
/// heads, ready keyless retries and the keyless fresh source in ticket order.
/// Each keyed claim is a leased batch covering the head plus a contiguous
/// original suffix grown under the per-key and response budgets.
fn fetch(
    conn: &Connection,
    name: &str,
    group_name: &str,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    limits: FetchLimits,
    ctx: &ExecCtx,
) -> Result<(StreamReply, Effects), CmdError> {
    let (stream, mut epoch) = resolve_member(conn, name, group_name, identity)?;
    let lease_ms = stream.config.ack_wait_ms;
    let empty_page = || -> (StreamReply, Effects) {
        (
            StreamReply::Fetch(FetchPage {
                lease_ms,
                batches: Vec::new(),
            }),
            Effects::default(),
        )
    };

    let max_events = limits
        .max_messages
        .min(crate::protocol::STREAM_MAX_FETCH_BATCH_SIZE);
    let response_cap = (limits.max_bytes as u64).min(ctx.fetch_response_bytes);
    let mut resp_left = response_cap.saturating_sub(FETCH_PAGE_HEADER_BYTES);
    let mut events_left = max_events.min((epoch.max_pending - epoch.pending).max(0) as usize);
    let mut pending_bytes_left = (epoch.max_pending_bytes - epoch.pending_bytes).max(0) as u64;
    if events_left == 0 || pending_bytes_left == 0 {
        return Ok(empty_page());
    }
    // Per-key caps keep one hot key from monopolizing the group budget; a
    // single oversized event may still go out alone inside the group/response
    // budgets so legal large events are never stranded.
    let key_grant = limits
        .max_key_messages
        .min(crate::protocol::STREAM_MAX_KEY_BATCH_SIZE)
        .min((epoch.max_pending.max(0) as usize / 2).max(1));
    let key_byte_cap = epoch.max_pending_bytes.max(0) as u64 / 2;
    let keyed_cap = limits.max_key_batches.min(events_left);

    struct LaneCand {
        ticket: i64,
        key_id: i64,
        key_len: usize,
        head_pos: u64,
        head_origin: i64,
        last_pos: u64,
        ev: EvMeta,
    }
    struct RetryCand {
        ticket: i64,
        seq: u64,
        ev: EvMeta,
    }
    let mut lanes: std::collections::VecDeque<LaneCand> = if keyed_cap == 0 {
        std::collections::VecDeque::new()
    } else {
        query_vec(
            conn,
            "SELECT l.ready_ticket, l.key_id, l.head_pos, l.head_origin,
                    e.seq, e.key_pos, e.timestamp_ms, e.payload_bytes, e.logical_bytes,
                    LENGTH(k.key), k.last_pos
             FROM key_lanes l
             JOIN events e ON e.stream_id=l.stream_id AND e.key_id=l.key_id
                 AND e.key_pos=l.head_pos
             JOIN stream_keys k ON k.stream_id=l.stream_id AND k.key_id=l.key_id
             WHERE l.epoch_id=?1 AND l.state=1
             ORDER BY l.ready_ticket, l.key_id LIMIT ?2",
            params![epoch.id.as_slice(), keyed_cap as i64],
            |r| {
                Ok(LaneCand {
                    ticket: r.get(0)?,
                    key_id: r.get(1)?,
                    head_pos: blob8(r, 2, "head_pos")?,
                    head_origin: r.get(3)?,
                    last_pos: blob8(r, 10, "last_pos")?,
                    ev: EvMeta {
                        seq: blob8(r, 4, "seq")?,
                        key_pos: blob8(r, 5, "key_pos")?,
                        timestamp_ms: r.get(6)?,
                        payload_bytes: r.get::<_, i64>(7)?.max(0) as u64,
                        logical: r.get::<_, i64>(8)?.max(0) as u64,
                    },
                    key_len: r.get::<_, i64>(9)?.max(0) as usize,
                })
            },
            "Cannot scan ready lanes",
        )?
        .into()
    };
    let mut retries: std::collections::VecDeque<RetryCand> = query_vec(
        conn,
        "SELECT d.ready_ticket, d.seq, d.key_pos, e.timestamp_ms, e.payload_bytes,
                e.logical_bytes
         FROM keyless_deliveries d
         JOIN events e ON e.stream_id=d.stream_id AND e.seq=d.seq
         WHERE d.epoch_id=?1 AND d.state=1 ORDER BY d.ready_ticket, d.seq LIMIT ?2",
        params![epoch.id.as_slice(), (events_left + 1) as i64],
        |r| {
            let seq = blob8(r, 1, "seq")?;
            Ok(RetryCand {
                ticket: r.get(0)?,
                seq,
                ev: EvMeta {
                    seq,
                    key_pos: blob8(r, 2, "key_pos")?,
                    timestamp_ms: r.get(3)?,
                    payload_bytes: r.get::<_, i64>(4)?.max(0) as u64,
                    logical: r.get::<_, i64>(5)?.max(0) as u64,
                },
            })
        },
        "Cannot scan keyless retries",
    )?
    .into();

    enum Selected {
        Keyed(Box<KeyedSel>),
        Keyless(KeylessSel),
    }
    let mut selected: Vec<Selected> = Vec::new();
    let mut keyed_taken = 0usize;
    let mut sources: std::collections::VecDeque<EvMeta> = std::collections::VecDeque::new();
    let mut fresh_exhausted = false;
    // Sources consume ready tickets at selection time; the commit pass calls
    // take_ticket in the same order, so a virtual ticket keeps comparisons
    // honest before any row is written. The scan cursor likewise advances
    // only at commit — page queries use a local copy.
    let mut next_source_ticket = epoch.keyless_ticket;
    let mut scan_cursor = epoch.keyless_cursor;

    while events_left > 0 {
        if sources.is_empty() && !fresh_exhausted {
            let page = events_left.min(256) as i64;
            let found: Vec<EvMeta> = query_vec(
                conn,
                "SELECT key_pos, seq, timestamp_ms, payload_bytes, logical_bytes
                 FROM events
                 WHERE stream_id=?1 AND key_id=0 AND key_pos>?2
                 ORDER BY key_pos LIMIT ?3",
                params![
                    epoch.stream_id.as_slice(),
                    encode_u64(scan_cursor).as_slice(),
                    page
                ],
                |r| {
                    Ok(EvMeta {
                        key_pos: blob8(r, 0, "key_pos")?,
                        seq: blob8(r, 1, "seq")?,
                        timestamp_ms: r.get(2)?,
                        payload_bytes: r.get::<_, i64>(3)?.max(0) as u64,
                        logical: r.get::<_, i64>(4)?.max(0) as u64,
                    })
                },
                "Cannot scan fresh keyless events",
            )?;
            fresh_exhausted = (found.len() as i64) < page;
            sources = found.into();
        }
        let source_ticket = if sources.is_empty() {
            i64::MAX
        } else {
            next_source_ticket
        };
        let lane_ticket = lanes.front().map(|c| c.ticket).unwrap_or(i64::MAX);
        // Retries sit below the keyless cursor, so their seq always precedes
        // the next fresh event: the earlier of the two keyless tickets claims
        // the slot, and pending retries win over the source on ties.
        let retry_ticket = retries.front().map(|c| c.ticket).unwrap_or(i64::MAX);
        let keyless_ticket = retry_ticket.min(source_ticket);
        if lane_ticket == i64::MAX && keyless_ticket == i64::MAX {
            break;
        }

        if lane_ticket != i64::MAX && lane_ticket <= keyless_ticket {
            let Some(c) = lanes.pop_front() else { break };
            if keyed_taken >= limits.max_key_batches {
                continue;
            }
            let encoded = fetch_batch_header_bytes(c.key_len)
                + fetch_event_encoded_bytes(c.ev.payload_bytes as usize);
            if encoded > resp_left || c.ev.logical > pending_bytes_left {
                continue;
            }
            let mut upper_pos = c.head_pos;
            if c.head_origin == Origin::Original as i64
                && key_grant > 1
                && c.head_pos < c.last_pos
            {
                upper_pos = c.last_pos;
                let bound: Option<u64> = query_one(
                    conn,
                    "SELECT MIN(key_pos) FROM replay_intents
                     WHERE epoch_id=?1 AND key_id=?2",
                    params![epoch.id.as_slice(), c.key_id],
                    |r| r.get::<_, Option<Vec<u8>>>(0),
                )
                .map_err(|e| fatal("Cannot bound batch suffix", e))?
                .map(|b| decode_u64(&b))
                .transpose()
                .map_err(CmdError::Fatal)?;
                if let Some(p) = bound {
                    upper_pos = upper_pos.min(p.saturating_sub(1));
                }
            }
            if upper_pos < c.head_pos {
                continue;
            }
            resp_left -= encoded;
            pending_bytes_left -= c.ev.logical;
            events_left -= 1;
            keyed_taken += 1;
            selected.push(Selected::Keyed(Box::new(KeyedSel {
                key_id: c.key_id,
                receipt: Uuid::new_v4().into_bytes(),
                first_pos: c.head_pos,
                upper_pos,
                logical: c.ev.logical,
                encoded,
                buf: std::collections::VecDeque::new(),
                exhausted: c.head_origin == Origin::Replay as i64
                    || upper_pos <= c.head_pos
                    || key_grant <= 1,
                metas: vec![c.ev],
            })));
        } else {
            let (seq, ev) = if let Some(c) = retries.pop_front() {
                (Some(c.seq), c.ev)
            } else {
                let Some(ev) = sources.front() else { break };
                let encoded = fetch_batch_header_bytes(0)
                    + fetch_event_encoded_bytes(ev.payload_bytes as usize);
                if encoded > resp_left || ev.logical > pending_bytes_left {
                    fresh_exhausted = true;
                    sources.clear();
                    continue;
                }
                let ev = sources.pop_front().expect("front checked");
                next_source_ticket += 1;
                scan_cursor = ev.key_pos;
                (None, ev)
            };
            let encoded = fetch_batch_header_bytes(0)
                + fetch_event_encoded_bytes(ev.payload_bytes as usize);
            if encoded > resp_left || ev.logical > pending_bytes_left {
                continue;
            }
            resp_left -= encoded;
            pending_bytes_left -= ev.logical;
            events_left -= 1;
            selected.push(Selected::Keyless(KeylessSel {
                seq,
                receipt: Uuid::new_v4().into_bytes(),
                meta: ev,
            }));
        }
    }

    // Growth pass: round-robin over growable keyed batches, appending one
    // contiguous original position at a time until per-key grants, group
    // budgets or the response cap stop them.
    {
        let mut growable: std::collections::VecDeque<usize> = selected
            .iter()
            .enumerate()
            .filter_map(|(i, s)| match s {
                Selected::Keyed(b) if !b.exhausted && b.metas.len() < key_grant => Some(i),
                _ => None,
            })
            .collect();
        while events_left > 0 {
            let Some(i) = growable.pop_front() else {
                break;
            };
            let Selected::Keyed(batch) = &mut selected[i] else {
                continue;
            };
            if batch.buf.is_empty() {
                let Some(next_pos) = batch
                    .first_pos
                    .checked_add(batch.metas.len() as u64)
                else {
                    batch.exhausted = true;
                    continue;
                };
                if next_pos > batch.upper_pos {
                    batch.exhausted = true;
                    continue;
                }
                let share = events_left.div_ceil(growable.len() + 1);
                let want = (key_grant - batch.metas.len()).min(share).max(1) as i64;
                let found: Vec<EvMeta> = query_vec(
                    conn,
                    "SELECT seq, key_pos, timestamp_ms, payload_bytes, logical_bytes
                     FROM events
                     WHERE stream_id=?1 AND key_id=?2 AND key_pos>=?3 AND key_pos<=?4
                     ORDER BY key_pos LIMIT ?5",
                    params![
                        epoch.stream_id.as_slice(),
                        batch.key_id,
                        encode_u64(next_pos).as_slice(),
                        encode_u64(batch.upper_pos).as_slice(),
                        want
                    ],
                    |r| {
                        Ok(EvMeta {
                            seq: blob8(r, 0, "seq")?,
                            key_pos: blob8(r, 1, "key_pos")?,
                            timestamp_ms: r.get(2)?,
                            payload_bytes: r.get::<_, i64>(3)?.max(0) as u64,
                            logical: r.get::<_, i64>(4)?.max(0) as u64,
                        })
                    },
                    "Cannot load batch suffix",
                )?;
                if found.is_empty() {
                    batch.exhausted = true;
                    continue;
                }
                batch.buf = found.into();
            }
            let ev = batch.buf.front().expect("batch buffer non-empty");
            let enc = fetch_event_encoded_bytes(ev.payload_bytes as usize);
            let fits = enc <= resp_left
                && ev.logical <= pending_bytes_left
                && batch.logical + ev.logical <= key_byte_cap;
            if !fits {
                batch.exhausted = true;
                continue;
            }
            let ev = batch.buf.pop_front().expect("batch buffer non-empty");
            batch.logical += ev.logical;
            batch.encoded += enc;
            resp_left -= enc;
            pending_bytes_left -= ev.logical;
            events_left -= 1;
            batch.metas.push(ev);
            if batch.metas.len() < key_grant {
                growable.push_back(i);
            } else {
                batch.exhausted = true;
            }
        }
    }

    if selected.is_empty() {
        return Ok(empty_page());
    }

    // Commit pass: write the leases for the chosen batches in selection
    // order, loading payloads and key bytes only for the claimed positions.
    let deadline = ctx.now_ms.saturating_add(lease_ms) as i64;
    let mut batches: Vec<DeliveryBatch> = Vec::with_capacity(selected.len());
    let mut fresh_claims: Vec<(u64, u64, [u8; 16], u64)> = Vec::new();
    let total_events = selected
        .iter()
        .map(|s| match s {
            Selected::Keyed(b) => b.metas.len(),
            Selected::Keyless(_) => 1,
        })
        .sum::<usize>();
    let mut payloads: std::collections::HashMap<u64, Vec<u8>> =
        std::collections::HashMap::with_capacity(total_events);
    {
        let mut seqs = Vec::with_capacity(total_events);
        for item in &selected {
            match item {
                Selected::Keyed(batch) => seqs.extend(batch.metas.iter().map(|m| m.seq)),
                Selected::Keyless(batch) => seqs.push(batch.meta.seq),
            }
        }
        for chunk in seqs.chunks(400) {
            let (text, values) = seq_in_list(
                "SELECT seq, payload FROM events WHERE stream_id=?1 AND seq IN (",
                &epoch.stream_id,
                chunk,
            );
            for (seq, payload) in query_vec(
                conn,
                &text,
                rusqlite::params_from_iter(values),
                |r| Ok((blob8(r, 0, "seq")?, r.get::<_, Vec<u8>>(1)?)),
                "Cannot load fetch payloads",
            )? {
                payloads.insert(seq, payload);
            }
        }
    }
    let mut key_bytes: std::collections::HashMap<i64, Bytes> =
        std::collections::HashMap::new();
    {
        let ids: Vec<i64> = selected
            .iter()
            .filter_map(|s| match s {
                Selected::Keyed(b) => Some(b.key_id),
                _ => None,
            })
            .collect();
        for chunk in ids.chunks(400) {
            let (text, values) = id_in_list(
                "SELECT key_id, key FROM stream_keys
                 WHERE stream_id=?1 AND key_id IN (",
                &epoch.stream_id,
                chunk,
            );
            for (key_id, key) in query_vec(
                conn,
                &text,
                rusqlite::params_from_iter(values),
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
                "Cannot load batch keys",
            )? {
                key_bytes.insert(key_id, Bytes::from(key));
            }
        }
    }
    for sel in selected {
        match sel {
            Selected::Keyed(batch) => {
                let changed = sql(
                    exec(
                        conn,
                        "UPDATE key_lanes SET state=2, receipt=?3, owner=?4, connection_id=?5,
                            deadline_ms=?6, attempts=attempts+1, lease_first_pos=?7,
                            lease_count=?8, lease_acked=0, lease_bytes=?9
                         WHERE epoch_id=?1 AND key_id=?2 AND state=1",
                        params![
                            epoch.id.as_slice(),
                            batch.key_id,
                            batch.receipt.as_slice(),
                            identity.consumer_id,
                            identity.connection_id,
                            deadline,
                            encode_u64(batch.first_pos).as_slice(),
                            batch.metas.len() as i64,
                            batch.logical as i64,
                        ],
                    ),
                    "Cannot lease lane",
                )?;
                if changed == 0 {
                    continue;
                }
                epoch.pending += batch.metas.len() as i64;
                epoch.pending_bytes += batch.logical as i64;
                let key = key_bytes
                    .get(&batch.key_id)
                    .cloned()
                    .ok_or_else(|| {
                        CmdError::Fatal(BrokerError::storage(
                            "Corrupt storage: claimed lane has no key row",
                        ))
                    })?;
                let messages = batch
                    .metas
                    .into_iter()
                    .map(|m| {
                        let payload = payloads.remove(&m.seq).ok_or_else(|| {
                            CmdError::Fatal(BrokerError::storage(
                                "Corrupt storage: claimed batch position has no event",
                            ))
                        })?;
                        Ok(Message {
                            seq: m.seq,
                            timestamp: m.timestamp_ms as u64,
                            key: key.clone(),
                            payload: Bytes::from(payload),
                        })
                    })
                    .collect::<Result<Vec<Message>, CmdError>>()?;
                batches.push(DeliveryBatch {
                    receipt: batch.receipt,
                    messages,
                });
            }
            Selected::Keyless(kl) => {
                let encoded_seq = encode_u64(kl.meta.seq);
                match kl.seq {
                    Some(_seq) => {
                        let changed = sql(
                            exec(
                                conn,
                                "UPDATE keyless_deliveries SET state=2, receipt=?3, owner=?4,
                                    connection_id=?5, deadline_ms=?6, attempts=attempts+1,
                                    lease_bytes=?7
                                 WHERE epoch_id=?1 AND seq=?2 AND state=1",
                                params![
                                    epoch.id.as_slice(),
                                    encoded_seq.as_slice(),
                                    kl.receipt.as_slice(),
                                    identity.consumer_id,
                                    identity.connection_id,
                                    deadline,
                                    kl.meta.logical as i64,
                                ],
                            ),
                            "Cannot lease keyless delivery",
                        )?;
                        if changed == 0 {
                            continue;
                        }
                    }
                    None => {
                        fresh_claims.push((
                            kl.meta.seq,
                            kl.meta.key_pos,
                            kl.receipt,
                            kl.meta.logical,
                        ));
                        epoch.keyless_cursor = kl.meta.key_pos;
                        epoch.keyless_ticket = take_ticket(&mut epoch)?;
                    }
                }
                epoch.pending += 1;
                epoch.pending_bytes += kl.meta.logical as i64;
                let payload = payloads.remove(&kl.meta.seq).ok_or_else(|| {
                    CmdError::Fatal(BrokerError::storage(
                        "Corrupt storage: claimed keyless event has no payload",
                    ))
                })?;
                batches.push(DeliveryBatch {
                    receipt: kl.receipt,
                    messages: vec![Message {
                        seq: kl.meta.seq,
                        timestamp: kl.meta.timestamp_ms as u64,
                        key: Bytes::new(),
                        payload: Bytes::from(payload),
                    }],
                });
            }
        }
    }

    admit_fresh_keyless(conn, &epoch, &fresh_claims, identity, deadline)?;
    save_epoch(conn, &epoch)?;
    Ok((StreamReply::Fetch(FetchPage { lease_ms, batches }), Effects::default()))
}

/// Admission rows for fresh keyless claims, written as chunked multi-row
/// VALUES inserts — identical rows to the per-message INSERT, at ~1/Nth the
/// statement count.
fn admit_fresh_keyless(
    conn: &Connection,
    epoch: &EpochRow,
    claims: &[(u64, u64, [u8; 16], u64)],
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    deadline: i64,
) -> R<()> {
    // 9 bound params per row; chunks stay under the 999-variable limit.
    for chunk in claims.chunks(110) {
        let mut text = String::from(
            "INSERT INTO keyless_deliveries(epoch_id, stream_id, seq, key_pos, origin,
                state, attempts, ready_ticket, receipt, owner, connection_id, deadline_ms,
                lease_bytes)
             VALUES ",
        );
        let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(chunk.len() * 9);
        for (row, (seq, key_pos, receipt, logical)) in chunk.iter().enumerate() {
            if row > 0 {
                text.push(',');
            }
            text.push_str("(?,?,?,?,0,2,1,0,?,?,?,?,?)");
            values.extend([
                rusqlite::types::Value::Blob(epoch.id.to_vec()),
                rusqlite::types::Value::Blob(epoch.stream_id.to_vec()),
                rusqlite::types::Value::Blob(encode_u64(*seq).to_vec()),
                rusqlite::types::Value::Blob(encode_u64(*key_pos).to_vec()),
                rusqlite::types::Value::Blob(receipt.to_vec()),
                rusqlite::types::Value::Text(identity.consumer_id.clone()),
                rusqlite::types::Value::Text(identity.connection_id.clone()),
                rusqlite::types::Value::Integer(deadline),
                rusqlite::types::Value::Integer(*logical as i64),
            ]);
        }
        sql(
            exec(conn, &text, rusqlite::params_from_iter(values)),
            "Cannot admit keyless deliveries",
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Batch updates / leave / disconnect / seek
// ---------------------------------------------------------------------------

/// `(seq, pos, origin)` of a lane's first unconfirmed event.
fn fatal_pos_overflow() -> CmdError {
    CmdError::Fatal(BrokerError::storage("Batch position overflow"))
}

fn lane_head(lane: &LaneRow) -> R<(u64, u64, i64)> {
    match (lane.head_seq, lane.head_pos, lane.head_origin) {
        (Some(s), Some(p), Some(o)) => Ok((s, p, o)),
        _ => Err(CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: lease without head",
        ))),
    }
}

/// Close a fully confirmed batch: drop the replay obligation or keep the
/// advanced original cursor, then rebuild the next head. Returns the
/// recompute result (true when a new head became READY).
fn close_lane_lease(
    conn: &Connection,
    epoch: &mut EpochRow,
    lane: &LaneRow,
    replay: bool,
    first_pos: u64,
) -> R<bool> {
    if replay {
        // Only the assigned intent is consumed; later intents stay pending.
        sql(
            exec(
                conn,
                "DELETE FROM replay_intents WHERE epoch_id=?1 AND key_id=?2 AND key_pos=?3",
                params![
                    epoch.id.as_slice(),
                    lane.key_id,
                    encode_u64(first_pos).as_slice()
                ],
            ),
            "Cannot consume replay intent",
        )?;
    }
    recompute_lane(conn, epoch, lane)
}

fn lease_valid(
    owner: Option<&str>,
    connection_id: Option<&str>,
    deadline_ms: Option<i64>,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    ctx: &ExecCtx,
) -> bool {
    owner == Some(identity.consumer_id.as_str())
        && connection_id == Some(identity.connection_id.as_str())
        && deadline_ms.is_some_and(|d| d > ctx.now_ms as i64)
}

fn sum_logical_range(
    conn: &Connection,
    stream_id: &[u8; 16],
    key_id: i64,
    first_pos: u64,
    last_pos: u64,
) -> R<u64> {
    query_one(
        conn,
        "SELECT COALESCE(SUM(logical_bytes),0) FROM events
         WHERE stream_id=?1 AND key_id=?2 AND key_pos>=?3 AND key_pos<=?4",
        params![
            stream_id.as_slice(),
            key_id,
            encode_u64(first_pos).as_slice(),
            encode_u64(last_pos).as_slice()
        ],
        |r| r.get::<_, i64>(0),
    )
    .map(|v| v.max(0) as u64)
    .map_err(|e| fatal("Cannot sum batch bytes", e))
}

fn event_seq_at(conn: &Connection, stream_id: &[u8; 16], key_id: i64, pos: u64) -> R<u64> {
    query_one(
        conn,
        "SELECT seq FROM events WHERE stream_id=?1 AND key_id=?2 AND key_pos=?3",
        params![stream_id.as_slice(), key_id, encode_u64(pos).as_slice()],
        |r| blob8(r, 0, "seq"),
    )
    .optional()
    .map_err(|e| fatal("Cannot load batch head", e))?
    .ok_or_else(|| {
        CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: unconfirmed batch position has no event",
        ))
    })
}

fn update_lane(
    conn: &Connection,
    epoch: &mut EpochRow,
    stream: &StreamRow,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    lane: &LaneRow,
    update: &BatchUpdate,
    ctx: &ExecCtx,
) -> Result<Option<crate::brokers::BrokerErrorKind>, CmdError> {
    use crate::brokers::BrokerErrorKind;
    if !lease_valid(
        lane.owner.as_deref(),
        lane.connection_id.as_deref(),
        lane.deadline_ms,
        identity,
        ctx,
    ) {
        return Ok(Some(BrokerErrorKind::Fenced));
    }
    let first_pos = lane.lease_first_pos.ok_or_else(|| {
        CmdError::Fatal(BrokerError::storage("Corrupt storage: lease without first_pos"))
    })?;
    let count = lane.lease_count.max(0) as u64;
    let acked = lane.lease_acked.max(0) as u64;
    let completed = update.completed as u64;
    if completed > count || completed < acked {
        return Ok(Some(BrokerErrorKind::InvalidArgument));
    }
    if update.disposition == BatchDisposition::Retry && completed == count {
        return Ok(Some(BrokerErrorKind::InvalidArgument));
    }
    let replay = lane.head_origin == Some(Origin::Replay as i64);
    let mut lane = lane.clone();
    let mut lease_bytes = lane.lease_bytes.max(0) as u64;

    let delta = completed - acked;
    if delta > 0 {
        let confirmed_bytes = if completed < count {
            sum_logical_range(
                conn,
                &epoch.stream_id,
                lane.key_id,
                first_pos
                    .checked_add(acked)
                    .ok_or_else(|| fatal_pos_overflow())?,
                first_pos
                    .checked_add(completed - 1)
                    .ok_or_else(|| fatal_pos_overflow())?,
            )?
        } else {
            lease_bytes
        };
        epoch.pending -= delta as i64;
        epoch.pending_bytes -= confirmed_bytes as i64;
        lease_bytes -= confirmed_bytes;
        if !replay {
            lane.cursor_pos = first_pos
                .checked_add(completed - 1)
                .ok_or_else(|| fatal_pos_overflow())?;
        }
        if completed < count {
            let head_pos = first_pos
                .checked_add(completed)
                .ok_or_else(|| fatal_pos_overflow())?;
            lane.head_pos = Some(head_pos);
            lane.head_seq = Some(event_seq_at(conn, &epoch.stream_id, lane.key_id, head_pos)?);
            lane.head_origin = Some(Origin::Original as i64);
            lane.attempts = 1;
            lane.normal_seq = lane.head_seq;
        }
    }
    let remaining = count - completed;
    let renew_deadline = ctx.now_ms.saturating_add(stream.config.ack_wait_ms) as i64;

    match update.disposition {
        BatchDisposition::Checkpoint => {
            if remaining == 0 {
                close_lane_lease(conn, epoch, &lane, replay, first_pos)?;
                return Ok(None);
            }
            sql(
                exec(
                    conn,
                    "UPDATE key_lanes SET lease_acked=?3, lease_bytes=?4, head_seq=?5,
                        head_pos=?6, head_origin=?7, attempts=?8, deadline_ms=?9,
                        cursor_pos=?10, normal_seq=?11
                     WHERE epoch_id=?1 AND key_id=?2 AND state=2",
                    params![
                        epoch.id.as_slice(),
                        lane.key_id,
                        completed as i64,
                        lease_bytes as i64,
                        lane.head_seq.map(encode_u64).map(|b| b.to_vec()),
                        lane.head_pos.map(encode_u64).map(|b| b.to_vec()),
                        lane.head_origin,
                        lane.attempts,
                        renew_deadline,
                        encode_u64(lane.cursor_pos).as_slice(),
                        lane.normal_seq.map(encode_u64).map(|b| b.to_vec()),
                    ],
                ),
                "Cannot checkpoint lane",
            )?;
            Ok(None)
        }
        BatchDisposition::Retry => {
            epoch.pending -= remaining as i64;
            epoch.pending_bytes -= lease_bytes as i64;
            if lane.attempts >= stream.config.max_deliveries as i64 {
                park_lane(conn, epoch, &lane, DlsReason::MaxDeliveries)?;
            } else {
                let head = lane_head(&lane)?;
                set_lane_waiting(
                    conn,
                    epoch,
                    &lane,
                    head,
                    lane.attempts,
                    ctx.now_ms.saturating_add(stream.config.ack_wait_ms),
                )?;
            }
            Ok(None)
        }
        BatchDisposition::Release => {
            if remaining == 0 {
                close_lane_lease(conn, epoch, &lane, replay, first_pos)?;
                return Ok(None);
            }
            epoch.pending -= remaining as i64;
            epoch.pending_bytes -= lease_bytes as i64;
            lane.attempts = lane.attempts.saturating_sub(1);
            let ticket = take_ticket(epoch)?;
            let head = lane_head(&lane)?;
            sql(
                exec(
                    conn,
                    &format!(
                        "UPDATE key_lanes SET state=1, ready_ticket=?3, head_seq=?4,
                            head_pos=?5, head_origin=?6, attempts=?7, deadline_ms=NULL,
                            cursor_pos=?8, normal_seq=?9, {CLEAR_LEASE}
                         WHERE epoch_id=?1 AND key_id=?2"
                    ),
                    params![
                        epoch.id.as_slice(),
                        lane.key_id,
                        ticket,
                        encode_u64(head.0).as_slice(),
                        encode_u64(head.1).as_slice(),
                        head.2,
                        lane.attempts,
                        encode_u64(lane.cursor_pos).as_slice(),
                        lane.normal_seq.map(encode_u64).map(|b| b.to_vec()),
                    ],
                ),
                "Cannot release lane",
            )?;
            Ok(None)
        }
    }
}

struct KeylessRow {
    seq: u64,
    key_pos: u64,
    attempts: i64,
    receipt: Option<Vec<u8>>,
    owner: Option<String>,
    connection_id: Option<String>,
    deadline_ms: Option<i64>,
    lease_bytes: i64,
}

const KEYLESS_COLS: &str =
    "seq, key_pos, attempts, receipt, owner, connection_id, deadline_ms, lease_bytes";

fn keyless_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<KeylessRow> {
    Ok(KeylessRow {
        seq: blob8(r, 0, "seq")?,
        key_pos: blob8(r, 1, "key_pos")?,
        attempts: r.get(2)?,
        receipt: r.get(3)?,
        owner: r.get(4)?,
        connection_id: r.get(5)?,
        deadline_ms: r.get(6)?,
        lease_bytes: r.get(7)?,
    })
}

fn load_keyless_by_receipt(
    conn: &Connection,
    epoch_id: &[u8; 16],
    receipt: &[u8; 16],
) -> R<Option<KeylessRow>> {
    query_one(
        conn,
        &format!(
            "SELECT {KEYLESS_COLS} FROM keyless_deliveries
             WHERE epoch_id=?1 AND state=2 AND receipt=?2"
        ),
        params![epoch_id.as_slice(), receipt.as_slice()],
        keyless_row,
    )
    .optional()
    .map_err(|e| fatal("Cannot load keyless delivery by receipt", e))
}

fn update_keyless(
    conn: &Connection,
    epoch: &mut EpochRow,
    stream: &StreamRow,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    kl: &KeylessRow,
    update: &BatchUpdate,
    ctx: &ExecCtx,
    delete_seqs: &mut Vec<u64>,
) -> Result<Option<crate::brokers::BrokerErrorKind>, CmdError> {
    use crate::brokers::BrokerErrorKind;
    if !lease_valid(
        kl.owner.as_deref(),
        kl.connection_id.as_deref(),
        kl.deadline_ms,
        identity,
        ctx,
    ) {
        return Ok(Some(BrokerErrorKind::Fenced));
    }
    if update.completed > 1 {
        return Ok(Some(BrokerErrorKind::InvalidArgument));
    }
    if update.disposition == BatchDisposition::Retry && update.completed == 1 {
        return Ok(Some(BrokerErrorKind::InvalidArgument));
    }
    let retry_at = ctx.now_ms.saturating_add(stream.config.ack_wait_ms) as i64;
    match (update.disposition, update.completed) {
        (BatchDisposition::Checkpoint, 0) => {
            sql(
                exec(
                    conn,
                    "UPDATE keyless_deliveries SET deadline_ms=?3
                     WHERE epoch_id=?1 AND seq=?2 AND state=2",
                    params![epoch.id.as_slice(), encode_u64(kl.seq).as_slice(), retry_at],
                ),
                "Cannot renew keyless lease",
            )?;
        }
        (BatchDisposition::Checkpoint, 1) | (BatchDisposition::Release, 1) => {
            delete_seqs.push(kl.seq);
            epoch.pending -= 1;
            epoch.pending_bytes -= kl.lease_bytes;
        }
        (BatchDisposition::Retry, 0) => {
            epoch.pending -= 1;
            epoch.pending_bytes -= kl.lease_bytes;
            if kl.attempts >= stream.config.max_deliveries as i64 {
                delete_seqs.push(kl.seq);
                insert_range(
                    conn,
                    epoch,
                    0,
                    kl.key_pos,
                    Some(kl.key_pos),
                    DlsReason::MaxDeliveries,
                    kl.attempts,
                )?;
            } else {
                sql(
                    exec(
                        conn,
                        "UPDATE keyless_deliveries SET state=4, receipt=NULL, owner=NULL,
                            connection_id=NULL, deadline_ms=?3, lease_bytes=0
                         WHERE epoch_id=?1 AND seq=?2",
                        params![epoch.id.as_slice(), encode_u64(kl.seq).as_slice(), retry_at],
                    ),
                    "Cannot wait keyless delivery",
                )?;
            }
        }
        (BatchDisposition::Release, 0) => {
            epoch.pending -= 1;
            epoch.pending_bytes -= kl.lease_bytes;
            let ticket = take_ticket(epoch)?;
            sql(
                exec(
                    conn,
                    "UPDATE keyless_deliveries SET state=1, ready_ticket=?3, receipt=NULL,
                        owner=NULL, connection_id=NULL, deadline_ms=NULL, lease_bytes=0,
                        attempts=?4
                     WHERE epoch_id=?1 AND seq=?2",
                    params![
                        epoch.id.as_slice(),
                        encode_u64(kl.seq).as_slice(),
                        ticket,
                        kl.attempts.saturating_sub(1),
                    ],
                ),
                "Cannot release keyless delivery",
            )?;
        }
        _ => unreachable!("keyless completed validated to 0..=1"),
    }
    Ok(None)
}

/// `(text, params)` for `... WHERE <scope>=?1 AND <col> IN (?2..)`: values
/// bind as numbered placeholders past the scope blob. Bounded by the
/// caller's chunk size, well under the SQLite variable limit.
fn in_list(
    base: &str,
    scope: &[u8; 16],
    mut values: Vec<rusqlite::types::Value>,
) -> (String, Vec<rusqlite::types::Value>) {
    let mut text = String::from(base);
    let mut bound = Vec::with_capacity(values.len() + 1);
    bound.push(rusqlite::types::Value::Blob(scope.to_vec()));
    for i in 0..values.len() {
        if i > 0 {
            text.push(',');
        }
        text.push_str(&format!("?{}", i + 2));
    }
    text.push(')');
    bound.append(&mut values);
    (text, bound)
}

fn seq_in_list(
    base: &str,
    stream_id: &[u8; 16],
    seqs: &[u64],
) -> (String, Vec<rusqlite::types::Value>) {
    in_list(
        base,
        stream_id,
        seqs.iter()
            .map(|s| rusqlite::types::Value::Blob(encode_u64(*s).to_vec()))
            .collect(),
    )
}

fn id_in_list(
    base: &str,
    epoch_id: &[u8; 16],
    ids: &[i64],
) -> (String, Vec<rusqlite::types::Value>) {
    in_list(
        base,
        epoch_id,
        ids.iter()
            .map(|i| rusqlite::types::Value::Integer(*i))
            .collect(),
    )
}

/// Receipt rows load through the partial unique receipt indexes in chunks
/// this size, keeping each IN-list statement bounded.
const RECEIPT_LOOKUP_CHUNK: usize = 512;

pub fn update_batches(
    conn: &Connection,
    name: &str,
    group_name: &str,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    updates: &[BatchUpdate],
    ctx: &ExecCtx,
) -> Result<(Vec<Option<crate::brokers::BrokerErrorKind>>, Effects), CmdError> {
    use crate::brokers::BrokerErrorKind;
    let (stream, mut epoch) = resolve_member(conn, name, group_name, identity)?;
    let mut effects = Effects::default();
    if updates.is_empty() {
        return Ok((Vec::new(), effects));
    }
    if updates.len() > crate::protocol::STREAM_MAX_FETCH_BATCH_SIZE {
        return Err(expected(BrokerError::invalid_argument(format!(
            "Update batch too large: {} items (max: {})",
            updates.len(),
            crate::protocol::STREAM_MAX_FETCH_BATCH_SIZE
        ))));
    }

    // Bulk-load the leased rows behind the presented receipts. The partial
    // unique receipt indexes guarantee at most one live lease per receipt.
    let mut lane_of: std::collections::HashMap<[u8; 16], LaneRow> =
        std::collections::HashMap::new();
    let mut keyless_of: std::collections::HashMap<[u8; 16], KeylessRow> =
        std::collections::HashMap::new();
    for chunk in updates.chunks(RECEIPT_LOOKUP_CHUNK) {
        let receipts: Vec<rusqlite::types::Value> = chunk
            .iter()
            .map(|u| rusqlite::types::Value::Blob(u.receipt.to_vec()))
            .collect();
        let (lane_text, lane_values) = in_list(
            &format!(
                "SELECT {LANE_COLS} FROM key_lanes
                 WHERE epoch_id=?1 AND state=2 AND receipt IN ("
            ),
            &epoch.id,
            receipts.clone(),
        );
        for lane in query_vec(
            conn,
            &lane_text,
            rusqlite::params_from_iter(lane_values),
            |r| lane_row(r, 0),
            "Cannot locate lane leases",
        )? {
            if let Some(key) = lane
                .receipt
                .as_deref()
                .and_then(|r| <[u8; 16]>::try_from(r).ok())
            {
                lane_of.insert(key, lane);
            }
        }
        let (kl_text, kl_values) = in_list(
            &format!(
                "SELECT {KEYLESS_COLS} FROM keyless_deliveries
                 WHERE epoch_id=?1 AND state=2 AND receipt IN ("
            ),
            &epoch.id,
            receipts,
        );
        for row in query_vec(
            conn,
            &kl_text,
            rusqlite::params_from_iter(kl_values),
            keyless_row,
            "Cannot locate keyless leases",
        )? {
            if let Some(key) = row
                .receipt
                .as_deref()
                .and_then(|r| <[u8; 16]>::try_from(r).ok())
            {
                keyless_of.insert(key, row);
            }
        }
    }

    let mut remaining: std::collections::HashMap<[u8; 16], usize> =
        std::collections::HashMap::new();
    for u in updates {
        *remaining.entry(u.receipt).or_default() += 1;
    }

    enum Post {
        Keep,
        Remove,
        Reload,
    }
    let mut outcomes: Vec<Option<BrokerErrorKind>> = Vec::with_capacity(updates.len());
    let mut any_applied = false;
    let mut keyless_done: Vec<u64> = Vec::new();
    for update in updates {
        if let Some(n) = remaining.get_mut(&update.receipt) {
            *n -= 1;
        }
        let again = remaining.get(&update.receipt).copied().unwrap_or(0) > 0;
        let (item, post) = if let Some(lane) = lane_of.get(&update.receipt) {
            let survives = update.disposition == BatchDisposition::Checkpoint
                && (update.completed as u64) < lane.lease_count.max(0) as u64;
            match update_lane(conn, &mut epoch, &stream, identity, lane, update, ctx)? {
                None if survives && again => (None, Post::Reload),
                None if survives => (None, Post::Keep),
                None => (None, Post::Remove),
                Some(kind) => (Some(kind), Post::Keep),
            }
        } else if let Some(kl) = keyless_of.get(&update.receipt) {
            let survives =
                update.disposition == BatchDisposition::Checkpoint && update.completed == 0;
            match update_keyless(
                conn,
                &mut epoch,
                &stream,
                identity,
                kl,
                update,
                ctx,
                &mut keyless_done,
            )? {
                None if survives && again => (None, Post::Reload),
                None if survives => (None, Post::Keep),
                None => (None, Post::Remove),
                Some(kind) => (Some(kind), Post::Keep),
            }
        } else {
            (Some(BrokerErrorKind::Fenced), Post::Keep)
        };
        if item.is_none() {
            any_applied = true;
        }
        match post {
            Post::Keep => {}
            Post::Remove => {
                lane_of.remove(&update.receipt);
                keyless_of.remove(&update.receipt);
            }
            Post::Reload => {
                if lane_of.contains_key(&update.receipt) {
                    match load_lane_by_receipt(conn, &epoch.id, &update.receipt)? {
                        Some(lane) => {
                            lane_of.insert(update.receipt, lane);
                        }
                        None => {
                            lane_of.remove(&update.receipt);
                        }
                    }
                } else if keyless_of.contains_key(&update.receipt) {
                    match load_keyless_by_receipt(conn, &epoch.id, &update.receipt)? {
                        Some(row) => {
                            keyless_of.insert(update.receipt, row);
                        }
                        None => {
                            keyless_of.remove(&update.receipt);
                        }
                    }
                }
            }
        }
        outcomes.push(item);
    }
    for chunk in keyless_done.chunks(400) {
        let (text, values) = seq_in_list(
            "DELETE FROM keyless_deliveries WHERE epoch_id=?1 AND seq IN (",
            &epoch.id,
            chunk,
        );
        sql(
            exec(conn, &text, rusqlite::params_from_iter(values)),
            "Cannot consume keyless deliveries",
        )?;
    }
    if any_applied {
        save_epoch(conn, &epoch)?;
        // Freed credit / re-armed lanes let pending fetches make progress.
        effects.wake(name, group_name);
    }
    Ok((outcomes, effects))
}

/// Member exit: drop the membership row and release every lease it holds so
/// deliveries return to the group (or park on exhausted attempts).
fn leave(
    conn: &Connection,
    name: &str,
    group_name: &str,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
) -> Result<(StreamReply, Effects), CmdError> {
    let (stream, mut epoch) = resolve_member(conn, name, group_name, identity)?;
    let mut effects = Effects::default();
    release_member_leases(
        conn,
        &mut epoch,
        &stream,
        &identity.consumer_id,
        &identity.connection_id,
        &mut effects,
        name,
        group_name,
    )?;
    sql(
        exec(
            conn,
            "DELETE FROM members WHERE epoch_id=?1 AND consumer_id=?2",
            params![epoch.id.as_slice(), identity.consumer_id],
        ),
        "Cannot remove member",
    )?;
    save_epoch(conn, &epoch)?;
    Ok((StreamReply::Unit, effects))
}

/// Release every leased lane/delivery owned by `consumer_id` on `epoch`.
fn release_member_leases(
    conn: &Connection,
    epoch: &mut EpochRow,
    stream: &StreamRow,
    consumer_id: &str,
    connection_id: &str,
    effects: &mut Effects,
    stream_name: &str,
    group_name: &str,
) -> R<()> {
    let lane_keys: Vec<i64> = query_vec(
        conn,
        "SELECT key_id FROM key_lanes
         WHERE epoch_id=?1 AND owner=?2 AND connection_id=?3 AND state=2",
        params![epoch.id.as_slice(), consumer_id, connection_id],
        |r| r.get::<_, i64>(0),
        "Cannot scan member leases",
    )?;
    for key_id in lane_keys {
        let Some(lane) = load_lane(conn, &epoch.id, key_id)? else {
            continue;
        };
        reclaim_lane_lease(conn, epoch, &lane, stream.config.max_deliveries, true)?;
    }
    let keyless_rows: Vec<(u64, u64, i64, i64)> = query_vec(
        conn,
        "SELECT seq, key_pos, attempts, lease_bytes FROM keyless_deliveries
         WHERE epoch_id=?1 AND owner=?2 AND connection_id=?3 AND state=2",
        params![epoch.id.as_slice(), consumer_id, connection_id],
        |r| {
            Ok((
                blob8(r, 0, "seq")?,
                blob8(r, 1, "key_pos")?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        },
        "Cannot scan member keyless leases",
    )?;
    for (seq, key_pos, attempts, lease_bytes) in keyless_rows {
        reclaim_keyless_lease(
            conn,
            epoch,
            seq,
            key_pos,
            attempts,
            lease_bytes,
            stream.config.max_deliveries,
            true,
        )?;
    }
    effects.wake(stream_name, group_name);
    Ok(())
}

/// Session teardown: release leases and memberships of the connection.
fn disconnect(conn: &Connection, connection_id: &str) -> Result<(StreamReply, Effects), CmdError> {
    let mut effects = Effects::default();
    let memberships: Vec<(Vec<u8>, Vec<u8>, String)> = query_vec(
        conn,
        "SELECT epoch_id, stream_id, consumer_id FROM members WHERE connection_id=?1",
        [connection_id],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
            ))
        },
        "Cannot scan members",
    )?;
    for (epoch_id, stream_id, consumer_id) in memberships {
        let epoch_id = blob16(&epoch_id, "epoch id")?;
        let stream_id = blob16(&stream_id, "stream id")?;
        let Some(mut epoch) = load_epoch(conn, &epoch_id)? else {
            continue;
        };
        let Some(stream) = load_stream_by_id(conn, &stream_id)? else {
            continue;
        };
        let names = group_stream_names(conn, &epoch)?;
        release_member_leases(
            conn,
            &mut epoch,
            &stream,
            &consumer_id,
            connection_id,
            &mut effects,
            &names.0,
            &names.1,
        )?;
        sql(
            exec(
                conn,
                "DELETE FROM members WHERE epoch_id=?1 AND consumer_id=?2",
                params![epoch.id.as_slice(), consumer_id],
            ),
            "Cannot remove member",
        )?;
        save_epoch(conn, &epoch)?;
    }
    Ok((StreamReply::Unit, effects))
}

fn load_stream_by_id(conn: &Connection, id: &[u8; 16]) -> R<Option<StreamRow>> {
    let row = query_one(
        conn,
        "SELECT name, config_json, last_seq, retained_after_seq, last_key_id, logical_bytes
         FROM streams WHERE id=?1",
        [id.as_slice()],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                blob8(r, 2, "last_seq")?,
                blob8(r, 3, "retained_after_seq")?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
            ))
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot load stream", e))?;
    match row {
        Some((name, config_json, last_seq, retained_after, last_key_id, logical)) => {
            if logical < 0 {
                return Err(CmdError::Fatal(BrokerError::storage(
                    "Corrupt storage: negative logical_bytes",
                )));
            }
            Ok(Some(StreamRow {
                id: *id,
                name,
                config: parse_config(&config_json)?,
                last_seq,
                retained_after,
                last_key_id,
                logical_bytes: logical as u64,
            }))
        }
        None => Ok(None),
    }
}

/// `(stream_name, group_name)` for wakeups; group may outlive its stream
/// row during GC only by FK ordering, so a missing row is corruption.
fn group_stream_names(conn: &Connection, epoch: &EpochRow) -> R<(String, String)> {
    query_one(
        conn,
        "SELECT s.name, g.name FROM groups g JOIN streams s ON s.id=g.stream_id
         WHERE g.id=?1",
        [epoch.group_id.as_slice()],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )
    .optional()
    .map_err(|e| fatal("Cannot load names", e))?
    .ok_or_else(|| fatal("Corrupt storage: epoch without group", ""))
}

/// Seek: swap the group onto a fresh epoch. Members bound to the old epoch are
/// fenced on their next call; the old epoch is garbage-collected lazily.
fn seek(
    conn: &Connection,
    name: &str,
    group_name: &str,
    target: &SeekTarget,
    _ctx: &ExecCtx,
) -> Result<(StreamReply, Effects), CmdError> {
    let stream = require_stream(conn, name)?;
    let start_after = match target {
        SeekTarget::Beginning => stream.retained_after,
        SeekTarget::End => stream.last_seq,
    };
    let mut effects: Effects = Effects::default();
    match load_group(conn, &stream.id, group_name)? {
        Some(group) => {
            let epoch_id = Uuid::new_v4().into_bytes();
            let keyless_cursor = epoch_keyless_boundary(conn, &stream.id, start_after)?;
            sql(
                exec(conn,
                    "INSERT INTO group_epochs(id, group_id, stream_id, start_after_seq, initialized,
                        init_key_cursor, init_key_target, keyless_cursor_pos, keyless_ticket,
                        next_ready_ticket, pending_count, max_pending, max_pending_bytes)
                     VALUES (?1,?2,?3,?4,0,0,?5,?6,1,2,0,?7,?8)",
                    params![
                        epoch_id.as_slice(),
                        group.id.as_slice(),
                        stream.id.as_slice(),
                        encode_u64(start_after).as_slice(),
                        stream.last_key_id,
                        encode_u64(keyless_cursor).as_slice(),
                        stream.config.max_ack_pending as i64,
                        stream.config.max_ack_pending_bytes as i64,
                    ],
                ),
                "Cannot create epoch",
            )?;
            sql(
                exec(
                    conn,
                    "UPDATE groups SET active_epoch=?2 WHERE id=?1",
                    params![group.id.as_slice(), epoch_id.as_slice()],
                ),
                "Cannot swap epoch",
            )?;
            effects.followups.push(Continuation::InitEpoch { epoch_id });
            effects.followups.push(Continuation::Gc);
            effects.wake(name, group_name);
        }
        None => {
            let (_group, epoch) = create_group_and_epoch(conn, &stream, group_name, start_after)?;
            effects
                .followups
                .push(Continuation::InitEpoch { epoch_id: epoch.id });
        }
    }
    Ok((StreamReply::Unit, effects))
}

// ---------------------------------------------------------------------------
// DLS
// ---------------------------------------------------------------------------

/// Flatten retained range members in seq order for `peek_dls`. The join
/// itself filters memberless ranges, so `first_seq` is only an index aid.
fn peek_dls(
    conn: &Connection,
    name: &str,
    group_name: &str,
    limit: usize,
    offset: usize,
) -> Result<(StreamReply, Effects), CmdError> {
    let stream = require_stream(conn, name)?;
    let group = require_group(conn, &stream.id, group_name)?;
    let epoch = require_epoch(conn, &group)?;
    let rows = query_vec(
        conn,
        "SELECT e.seq, r.reason, r.attempts, k.key
         FROM dls_ranges r
         JOIN events e ON e.stream_id=r.stream_id AND e.key_id=r.key_id
             AND e.key_pos >= r.first_pos AND (r.last_pos IS NULL OR e.key_pos <= r.last_pos)
         JOIN stream_keys k ON k.stream_id=e.stream_id AND k.key_id=e.key_id
         WHERE r.epoch_id=?1
         ORDER BY e.seq LIMIT ?2 OFFSET ?3",
        params![epoch.id.as_slice(), limit as i64, offset as i64],
        |r| {
            let reason_kind = r.get::<_, i64>(1)?;
            Ok((
                blob8(r, 0, "seq")?,
                reason_kind,
                r.get::<_, i64>(2)?,
                r.get::<_, Vec<u8>>(3)?,
            ))
        },
        "Cannot peek DLS",
    )?;
    let entries = rows
        .into_iter()
        .map(|(seq, reason, attempts, key)| DlsEntry {
            seq,
            reason: DlsReason::from_i64(reason)
                .unwrap_or(DlsReason::MaxDeliveries)
                .describe(stream.config.max_deliveries),
            attempts: attempts.max(0) as u32,
            key: Bytes::from(key),
        })
        .collect();
    Ok((StreamReply::DlsEntries(entries), Effects::default()))
}

struct RangeRow {
    first_pos: u64,
    last_pos: Option<u64>,
    reason: i64,
    attempts: i64,
}

/// Locate the range containing `(key_id, pos)` for `epoch`.
fn find_range(
    conn: &Connection,
    epoch_id: &[u8; 16],
    key_id: i64,
    pos: u64,
) -> R<Option<RangeRow>> {
    let result = query_one(
        conn,
        "SELECT first_pos, last_pos, reason, attempts FROM dls_ranges
         WHERE epoch_id=?1 AND key_id=?2 AND first_pos<=?3
           AND (last_pos IS NULL OR ?3<=last_pos)
         ORDER BY first_pos DESC LIMIT 1",
        params![epoch_id.as_slice(), key_id, encode_u64(pos).as_slice()],
        |r| {
            Ok((
                blob8(r, 0, "first_pos")?,
                opt_blob8(r, 1, "last_pos")?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot locate DLS range", e))?
    .map(|(first_pos, last_pos, reason, attempts)| RangeRow {
        first_pos,
        last_pos,
        reason,
        attempts,
    });
    Ok(result)
}

/// Remove `pos` from its containing range, splitting into left/right pieces.
/// Returns false when the position is not a member.
fn remove_dls_point(conn: &Connection, epoch: &EpochRow, key_id: i64, pos: u64) -> R<bool> {
    let Some(range) = find_range(conn, &epoch.id, key_id, pos)? else {
        return Ok(false);
    };
    let reason = DlsReason::from_i64(range.reason).unwrap_or(DlsReason::MaxDeliveries);
    sql(
        exec(
            conn,
            "DELETE FROM dls_ranges WHERE epoch_id=?1 AND key_id=?2 AND first_pos=?3",
            params![
                epoch.id.as_slice(),
                key_id,
                encode_u64(range.first_pos).as_slice()
            ],
        ),
        "Cannot remove DLS range",
    )?;
    if range.first_pos < pos {
        insert_range(
            conn,
            epoch,
            key_id,
            range.first_pos,
            Some(pos - 1),
            reason,
            range.attempts,
        )?;
    }
    let right_exists = match range.last_pos {
        None => true,
        Some(last) => pos < last,
    };
    if right_exists {
        insert_range(
            conn,
            epoch,
            key_id,
            pos + 1,
            range.last_pos,
            reason,
            range.attempts,
        )?;
    }
    Ok(true)
}

/// A parked lane resumes when no retained DLS member remains for the key:
/// the original cursor jumps to the key tail covered by parking and the head
/// is rebuilt from remaining replay intents. Returns true if it became READY.
fn try_unpark_lane(conn: &Connection, epoch: &mut EpochRow, key_id: i64) -> R<bool> {
    let Some(lane) = load_lane(conn, &epoch.id, key_id)? else {
        return Ok(false);
    };
    if lane.state != LaneState::Parked as i64 {
        return Ok(false);
    }
    let has_members = query_one(
        conn,
        "SELECT 1 FROM dls_ranges WHERE epoch_id=?1 AND key_id=?2
         AND first_seq IS NOT NULL LIMIT 1",
        params![epoch.id.as_slice(), key_id],
        |r| r.get::<_, i64>(0),
    )
    .optional()
    .map_err(|e| fatal("Cannot check DLS membership", e))?
    .is_some();
    if has_members {
        return Ok(false);
    }
    // All ranges of this key are memberless: drop them and resume originals.
    sql(
        exec(
            conn,
            "DELETE FROM dls_ranges WHERE epoch_id=?1 AND key_id=?2",
            params![epoch.id.as_slice(), key_id],
        ),
        "Cannot clear memberless ranges",
    )?;
    let tail = key_tail_pos(conn, &epoch.stream_id, key_id)?;
    // The cursor jump rides on the recompute write — no separate UPDATE + reload.
    let mut lane = lane;
    lane.cursor_pos = tail;
    recompute_lane(conn, epoch, &lane)
}

fn dls_point_op(
    conn: &Connection,
    name: &str,
    group_name: &str,
    seq: u64,
    replay: bool,
) -> Result<(StreamReply, Effects), CmdError> {
    let stream = require_stream(conn, name)?;
    let group = require_group(conn, &stream.id, group_name)?;
    let mut epoch = require_epoch(conn, &group)?;
    let mut effects = Effects::default();

    // The target event must still be retained.
    let event = query_one(
        conn,
        "SELECT key_id, key_pos FROM events WHERE stream_id=?1 AND seq=?2",
        params![stream.id.as_slice(), encode_u64(seq).as_slice()],
        |r| Ok((r.get::<_, i64>(0)?, blob8(r, 1, "key_pos")?)),
    )
    .optional()
    .map_err(|e| fatal("Cannot load event for DLS operation", e))?;
    let Some((key_id, key_pos)) = event else {
        return Err(expected(BrokerError::not_found(format!(
            "Sequence {seq} is not in the dead-letter store"
        ))));
    };

    if !remove_dls_point(conn, &epoch, key_id, key_pos)? {
        return Err(expected(BrokerError::not_found(format!(
            "Sequence {seq} is not in the dead-letter store"
        ))));
    }

    if replay {
        if key_id == 0 {
            // Keyless replays materialize as READY delivery rows.
            let ticket = take_ticket(&mut epoch)?;
            sql(
                exec(
                    conn,
                    "INSERT INTO keyless_deliveries(epoch_id, stream_id, seq, key_pos, origin,
                        state, attempts, ready_ticket)
                     VALUES (?1,?2,?3,?4,1,1,0,?5)",
                    params![
                        epoch.id.as_slice(),
                        epoch.stream_id.as_slice(),
                        encode_u64(seq).as_slice(),
                        encode_u64(key_pos).as_slice(),
                        ticket,
                    ],
                ),
                "Cannot queue keyless replay",
            )?;
        } else {
            sql(
                exec(
                    conn,
                    "INSERT INTO replay_intents(epoch_id, stream_id, key_id, key_pos, seq)
                     VALUES (?1,?2,?3,?4,?5)",
                    params![
                        epoch.id.as_slice(),
                        epoch.stream_id.as_slice(),
                        key_id,
                        encode_u64(key_pos).as_slice(),
                        encode_u64(seq).as_slice(),
                    ],
                ),
                "Cannot record replay intent",
            )?;
            // If the removal emptied the DLS membership, the lane resumes and
            // the intent becomes deliverable immediately.
            if try_unpark_lane(conn, &mut epoch, key_id)? {
                effects.wake(name, group_name);
            } else {
                // Still parked, or already live: the intent may already be the
                // head candidate on an EMPTY lane.
                if let Some(lane) = load_lane(conn, &epoch.id, key_id)? {
                    if lane.state == LaneState::Empty as i64
                        && recompute_lane(conn, &mut epoch, &lane)?
                    {
                        effects.wake(name, group_name);
                    }
                }
            }
        }
        effects.wake(name, group_name);
    } else if key_id != 0 && try_unpark_lane(conn, &mut epoch, key_id)? {
        effects.wake(name, group_name);
    }

    save_epoch(conn, &epoch)?;
    Ok((StreamReply::Unit, effects))
}

/// Purge resolves every parked membership of the group; parked lanes resume
/// at the key tail and keep outstanding replay intents.
fn purge_dls(
    conn: &Connection,
    name: &str,
    group_name: &str,
) -> Result<(StreamReply, Effects), CmdError> {
    let stream = require_stream(conn, name)?;
    let group = require_group(conn, &stream.id, group_name)?;
    let mut epoch = require_epoch(conn, &group)?;
    let mut effects = Effects::default();

    let count: i64 = query_one(
        conn,
        "SELECT COUNT(*) FROM dls_ranges r
         JOIN events e ON e.stream_id=r.stream_id AND e.key_id=r.key_id
             AND e.key_pos >= r.first_pos AND (r.last_pos IS NULL OR e.key_pos <= r.last_pos)
         WHERE r.epoch_id=?1",
        [epoch.id.as_slice()],
        |r| r.get(0),
    )
    .map_err(|e| fatal("Cannot count DLS members", e))?;

    sql(
        exec(
            conn,
            "DELETE FROM dls_ranges WHERE epoch_id=?1",
            [epoch.id.as_slice()],
        ),
        "Cannot purge DLS",
    )?;

    let parked: Vec<LaneRow> = query_vec(
        conn,
        &format!(
            "SELECT {LANE_COLS} FROM key_lanes WHERE epoch_id=?1 AND state=3"
        ),
        [epoch.id.as_slice()],
        |r| lane_row(r, 0),
        "Cannot scan parked lanes",
    )?;
    // Key tails for all parked lanes in one IN-scan on stream_keys.
    let tails: std::collections::HashMap<i64, u64> = if parked.is_empty() {
        std::collections::HashMap::new()
    } else {
        let mut text = String::from(
            "SELECT key_id, last_pos FROM stream_keys WHERE stream_id=?1 AND key_id IN (",
        );
        let mut values: Vec<rusqlite::types::Value> =
            Vec::with_capacity(parked.len() + 1);
        values.push(rusqlite::types::Value::Blob(epoch.stream_id.to_vec()));
        for (i, lane) in parked.iter().enumerate() {
            if i > 0 {
                text.push(',');
            }
            text.push_str(&format!("?{}", i + 2));
            values.push(rusqlite::types::Value::Integer(lane.key_id));
        }
        text.push(')');
        query_vec(
            conn,
            &text,
            rusqlite::params_from_iter(values),
            |r| Ok((r.get::<_, i64>(0)?, blob8(r, 1, "last_pos")?)),
            "Cannot load key tails",
        )?
        .into_iter()
        .collect()
    };
    let mut resumed = false;
    for mut lane in parked {
        let tail = tails.get(&lane.key_id).copied().unwrap_or(0);
        // The cursor jump rides on the recompute write — no UPDATE + reload.
        lane.cursor_pos = tail;
        if recompute_lane(conn, &mut epoch, &lane)? {
            resumed = true;
        }
    }
    if resumed || count > 0 {
        effects.wake(name, group_name);
    }
    save_epoch(conn, &epoch)?;
    Ok((StreamReply::Count(count.max(0) as usize), effects))
}

// ---------------------------------------------------------------------------
// Lease expiry
// ---------------------------------------------------------------------------

/// Bounded sweep of due leases. Returns a continuation while more remain.
fn expire_leases(conn: &Connection, now_ms: u64) -> Result<(StreamReply, Effects), CmdError> {
    let mut effects = Effects::default();
    let mut epoch_cache: std::collections::HashMap<[u8; 16], EpochRow> =
        std::collections::HashMap::new();
    let mut config_cache: std::collections::HashMap<[u8; 16], StreamConfig> =
        std::collections::HashMap::new();
    let mut name_cache: std::collections::HashMap<[u8; 16], (String, String)> =
        std::collections::HashMap::new();

    macro_rules! epoch_for {
        ($id:expr) => {{
            if !epoch_cache.contains_key($id) {
                if let Some(e) = load_epoch(conn, $id)? {
                    epoch_cache.insert(*$id, e);
                }
            }
            match epoch_cache.get_mut($id) {
                Some(e) => e,
                None => continue,
            }
        }};
    }

    // Full lane rows ride in the scan — same-tx state cannot drift between
    // the scan and the release, so no per-row reload is needed.
    let due_lanes: Vec<(Vec<u8>, LaneRow)> = query_vec(
        conn,
        &format!(
            "SELECT epoch_id, {LANE_COLS} FROM key_lanes
             WHERE state IN (2, 4) AND deadline_ms<=?1 ORDER BY deadline_ms LIMIT ?2"
        ),
        params![now_ms as i64, BATCH_LIMIT as i64],
        |r| Ok((r.get::<_, Vec<u8>>(0)?, lane_row(r, 1)?)),
        "Cannot scan due lanes",
    )?;
    let more_lanes = due_lanes.len() == BATCH_LIMIT;
    for (epoch_id, lane) in due_lanes {
        let Ok(epoch_id) = <[u8; 16]>::try_from(epoch_id.as_slice()) else {
            continue;
        };
        let stream_id = match epoch_cache.entry(epoch_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.get().stream_id,
            std::collections::hash_map::Entry::Vacant(v) => {
                let Some(loaded) = load_epoch(conn, &epoch_id)? else {
                    continue;
                };
                let stream_id = loaded.stream_id;
                v.insert(loaded);
                stream_id
            }
        };
        let max_deliveries = match config_cache.entry(stream_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.get().max_deliveries,
            std::collections::hash_map::Entry::Vacant(v) => {
                let Ok(cfg) = load_config_for_stream(conn, &stream_id) else {
                    continue;
                };
                let max_deliveries = cfg.max_deliveries;
                v.insert(cfg);
                max_deliveries
            }
        };
        if let std::collections::hash_map::Entry::Vacant(e) = name_cache.entry(epoch_id) {
            if let Ok(n) = group_stream_names(conn, &epoch_cache[&epoch_id]) {
                e.insert(n);
            }
        }
        let epoch = epoch_for!(&epoch_id);
        if LaneState::from_i64(lane.state) == Some(LaneState::Waiting) {
            set_lane_ready(conn, epoch, &lane, lane.attempts)?;
        } else {
            reclaim_lane_lease(conn, epoch, &lane, max_deliveries, true)?;
        }
        if let Some((sname, gname)) = name_cache.get(&epoch_id) {
            effects.wake(&sname.clone(), &gname.clone());
        }
    }

    let due_keyless: Vec<(Vec<u8>, u64, u64, i64, i64, i64)> = query_vec(
        conn,
        "SELECT epoch_id, seq, key_pos, attempts, state, lease_bytes FROM keyless_deliveries
         WHERE state IN (2, 4) AND deadline_ms<=?1 ORDER BY deadline_ms LIMIT ?2",
        params![now_ms as i64, BATCH_LIMIT as i64],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                blob8(r, 1, "seq")?,
                blob8(r, 2, "key_pos")?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
            ))
        },
        "Cannot scan due keyless leases",
    )?;
    let more_keyless = due_keyless.len() == BATCH_LIMIT;
    for (epoch_id, seq, key_pos, attempts, state, lease_bytes) in due_keyless {
        let Ok(epoch_id) = <[u8; 16]>::try_from(epoch_id.as_slice()) else {
            continue;
        };
        let stream_id = match epoch_cache.entry(epoch_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.get().stream_id,
            std::collections::hash_map::Entry::Vacant(v) => {
                let Some(loaded) = load_epoch(conn, &epoch_id)? else {
                    continue;
                };
                let stream_id = loaded.stream_id;
                v.insert(loaded);
                stream_id
            }
        };
        let max_deliveries = match config_cache.entry(stream_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.get().max_deliveries,
            std::collections::hash_map::Entry::Vacant(v) => {
                let Ok(cfg) = load_config_for_stream(conn, &stream_id) else {
                    continue;
                };
                let max_deliveries = cfg.max_deliveries;
                v.insert(cfg);
                max_deliveries
            }
        };
        if let std::collections::hash_map::Entry::Vacant(e) = name_cache.entry(epoch_id) {
            if let Ok(n) = group_stream_names(conn, &epoch_cache[&epoch_id]) {
                e.insert(n);
            }
        }
        // The scan ran inside this same transaction and this loop is the
        // only mutator of these rows, so state/key_pos/attempts are current.
        let epoch = epoch_for!(&epoch_id);
        if state == LaneState::Waiting as i64 {
            let ticket = take_ticket(epoch)?;
            sql(
                exec(
                    conn,
                    "UPDATE keyless_deliveries SET state=1, ready_ticket=?3, deadline_ms=NULL,
                        lease_bytes=0
                     WHERE epoch_id=?1 AND seq=?2",
                    params![epoch_id.as_slice(), encode_u64(seq).as_slice(), ticket],
                ),
                "Cannot requeue keyless delivery",
            )?;
        } else {
            reclaim_keyless_lease(
                conn,
                epoch,
                seq,
                key_pos,
                attempts,
                lease_bytes,
                max_deliveries,
                true,
            )?;
        }
        if let Some((sname, gname)) = name_cache.get(&epoch_id) {
            effects.wake(&sname.clone(), &gname.clone());
        }
    }

    for epoch in epoch_cache.values() {
        save_epoch(conn, epoch)?;
    }
    if more_lanes || more_keyless {
        effects.followups.push(Continuation::ExpireLeases);
    }
    Ok((StreamReply::Unit, effects))
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

/// One pass over every live stream, one bounded prefix deletion each.
/// Re-enqueues itself via `Continuation::Retention` while work remains.
fn retention_tick(conn: &Connection, ctx: &ExecCtx) -> Result<(StreamReply, Effects), CmdError> {
    let stream_ids: Vec<Vec<u8>> = query_vec(
        conn,
        "SELECT id FROM streams WHERE deleted=0",
        [],
        |r| r.get::<_, Vec<u8>>(0),
        "Cannot list streams",
    )?;
    let mut effects = Effects::default();
    let mut more = false;
    for id in stream_ids {
        let id = blob16(&id, "stream id")?;
        let Some(stream) = load_stream_by_id(conn, &id)? else {
            continue;
        };
        if retain_stream(conn, &stream, ctx.now_ms, &mut effects)? {
            more = true;
        }
    }
    if more {
        effects.followups.push(Continuation::Retention);
    } else {
        effects.followups.push(Continuation::PruneKeys { after: None });
    }
    Ok((StreamReply::Unit, effects))
}

/// Delete a bounded expired prefix of `stream`'s events. The walk stops at
/// the first non-expired event so `retained_after_seq` stays a contiguous
/// marker.
/// Returns true when the budget was exhausted (more work likely remains).
///
/// Ordering inside the transaction:
///   A) collect the expired prefix,
///   B) remove rows with FK references into it (deliveries, intents) and
///      refund pending credits for leases that lose their head,
///   C) delete the event prefix and charge the stream,
///   D) recompute derived state (lane cursors/heads/frontiers, range clips,
///      unpark checks) against the post-delete view.
fn retain_stream(
    conn: &Connection,
    stream: &StreamRow,
    now_ms: u64,
    effects: &mut Effects,
) -> R<bool> {
    let cfg = &stream.config.retention;
    let age_cutoff = cfg.max_age_ms.map(|a| now_ms.saturating_sub(a));
    let mut excess = stream
        .logical_bytes
        .saturating_sub(cfg.max_bytes.unwrap_or(u64::MAX));
    if age_cutoff.is_none() && excess == 0 {
        return Ok(false);
    }

    // Phase A: collect the expired prefix.
    let mut deleted: Vec<(u64, i64, u64, u64)> = Vec::new();
    let mut scanned_all = false;
    {
        let mut stmt = sql(
            conn.prepare_cached(
                "SELECT seq, key_id, key_pos, timestamp_ms, logical_bytes FROM events
                 WHERE stream_id=?1 ORDER BY seq LIMIT ?2",
            ),
            "Cannot scan events for retention",
        )?;
        let mut rows = sql(
            stmt.query(params![stream.id.as_slice(), (BATCH_LIMIT + 1) as i64]),
            "Cannot scan events for retention",
        )?;
        loop {
            let Some(row) = sql(rows.next(), "Cannot scan events for retention")? else {
                scanned_all = true;
                break;
            };
            let seq = blob8(row, 0, "seq")?;
            let key_id = row.get::<_, i64>(1)?;
            let key_pos = blob8(row, 2, "key_pos")?;
            let ts = row.get::<_, i64>(3)? as u64;
            let logical = row.get::<_, i64>(4)?.max(0) as u64;
            let expired = age_cutoff.is_some_and(|c| ts < c) || excess > 0;
            if !expired || deleted.len() == BATCH_LIMIT {
                break;
            }
            excess = excess.saturating_sub(logical);
            deleted.push((seq, key_id, key_pos, logical));
        }
    }
    if deleted.is_empty() {
        return Ok(false);
    }
    // If we stopped because of the budget and the scan had not ended, another
    // batch may remain. If we stopped on the first non-expired event, only a
    // byte-overflow tail could remain — but excess>0 implies the prefix walk
    // itself continued, so budget-exhaustion is the only "more" signal needed.
    let more = !scanned_all && deleted.len() == BATCH_LIMIT;
    let Some((boundary_seq, ..)) = deleted.last().copied() else {
        return Ok(false);
    };
    let mut key_deleted_pos: std::collections::HashMap<i64, u64> = std::collections::HashMap::new();
    let mut deleted_bytes: u64 = 0;
    for (_, key_id, key_pos, logical) in &deleted {
        *key_deleted_pos.entry(*key_id).or_insert(0) =
            (*key_deleted_pos.get(key_id).unwrap_or(&0)).max(*key_pos);
        deleted_bytes += logical;
    }

    let mut dirty_epochs: std::collections::HashMap<[u8; 16], EpochRow> =
        std::collections::HashMap::new();
    let epoch_of = |conn: &Connection,
                    dirty: &mut std::collections::HashMap<[u8; 16], EpochRow>,
                    epoch_id: [u8; 16]| {
        if let std::collections::hash_map::Entry::Vacant(e) = dirty.entry(epoch_id) {
            if let Some(v) = load_epoch(conn, &epoch_id)? {
                e.insert(v);
            }
        }
        Ok::<bool, CmdError>(dirty.contains_key(&epoch_id))
    };

    // Phase B: drop FK references into the deleted prefix and refund credits.
    let expired_keyless: Vec<(Vec<u8>, Vec<u8>, i64, i64)> = query_vec(
        conn,
        "SELECT epoch_id, seq, state, lease_bytes FROM keyless_deliveries
         WHERE stream_id=?1 AND seq<=?2",
        params![stream.id.as_slice(), encode_u64(boundary_seq).as_slice()],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        },
        "Cannot scan keyless deliveries for retention",
    )?;
    for (epoch_blob, seq_blob, state, lease_bytes) in expired_keyless {
        let epoch_id = blob16(&epoch_blob, "epoch id")?;
        if epoch_of(conn, &mut dirty_epochs, epoch_id)? && state == LaneState::Leased as i64 {
            if let Some(e) = dirty_epochs.get_mut(&epoch_id) {
                e.pending -= 1;
                e.pending_bytes -= lease_bytes;
            }
        }
        sql(
            exec(
                conn,
                "DELETE FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2",
                params![epoch_id.as_slice(), seq_blob.as_slice()],
            ),
            "Cannot expire keyless delivery",
        )?;
    }
    sql(
        exec(
            conn,
            "DELETE FROM replay_intents WHERE stream_id=?1 AND seq<=?2",
            params![stream.id.as_slice(), encode_u64(boundary_seq).as_slice()],
        ),
        "Cannot expire replay intents",
    )?;

    // Phase C: delete the event prefix and charge the stream.
    sql(
        exec(
            conn,
            "DELETE FROM events WHERE stream_id=?1 AND seq<=?2",
            params![stream.id.as_slice(), encode_u64(boundary_seq).as_slice()],
        ),
        "Cannot delete expired events",
    )?;
    sql(
        exec(
            conn,
            "UPDATE streams SET retained_after_seq=?2, logical_bytes=logical_bytes-?3 WHERE id=?1",
            params![
                stream.id.as_slice(),
                encode_u64(boundary_seq).as_slice(),
                deleted_bytes as i64,
            ],
        ),
        "Cannot update stream retention state",
    )?;
    for (key_id, dpos) in &key_deleted_pos {
        sql(
            exec(
                conn,
                "UPDATE stream_keys SET retained_after_pos=?3
                 WHERE stream_id=?1 AND key_id=?2 AND retained_after_pos<?3",
                params![stream.id.as_slice(), key_id, encode_u64(*dpos).as_slice()],
            ),
            "Cannot advance retained position",
        )?;
    }

    // Phase D: derived state against the post-delete view.
    // Keyless source watermark moves past deleted positions.
    if let Some(dpos0) = key_deleted_pos.get(&0).copied() {
        sql(
            exec(
                conn,
                "UPDATE group_epochs SET keyless_cursor_pos=?2
                 WHERE stream_id=?1 AND keyless_cursor_pos<?2",
                params![stream.id.as_slice(), encode_u64(dpos0).as_slice()],
            ),
            "Cannot advance keyless watermark",
        )?;
    }
    retain_stream_ranges(conn, stream, &key_deleted_pos, boundary_seq)?;
    retain_stream_lanes(
        conn,
        stream,
        &key_deleted_pos,
        boundary_seq,
        &mut dirty_epochs,
        effects,
    )?;

    for epoch in dirty_epochs.values() {
        save_epoch(conn, epoch)?;
    }
    Ok(more)
}

/// Clip or drop `dls_ranges` members that expired for the deleted keys, then
/// refresh `first_seq` (first retained member) on the survivors.
fn retain_stream_ranges(
    conn: &Connection,
    stream: &StreamRow,
    key_deleted_pos: &std::collections::HashMap<i64, u64>,
    boundary_seq: u64,
) -> R<()> {
    for (key_id, dpos) in key_deleted_pos {
        let ranges: Vec<(Vec<u8>, u64, Option<u64>)> = query_vec(
            conn,
            "SELECT epoch_id, first_pos, last_pos FROM dls_ranges
             WHERE stream_id=?1 AND key_id=?2",
            params![stream.id.as_slice(), key_id],
            |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    blob8(r, 1, "first_pos")?,
                    opt_blob8(r, 2, "last_pos")?,
                ))
            },
            "Cannot scan DLS ranges",
        )?;
        for (epoch_blob, first_pos, last_pos) in ranges {
            let epoch_id = blob16(&epoch_blob, "epoch id")?;
            let fully = last_pos.is_some_and(|l| l <= *dpos);
            let clipped_front = *dpos >= first_pos;
            if fully {
                sql(
                    exec(
                        conn,
                        "DELETE FROM dls_ranges WHERE epoch_id=?1 AND key_id=?2 AND first_pos=?3",
                        params![
                            epoch_id.as_slice(),
                            key_id,
                            encode_u64(first_pos).as_slice()
                        ],
                    ),
                    "Cannot drop expired range",
                )?;
                continue;
            }
            if clipped_front {
                // Reinsert shifted; first_seq is recomputed below.
                sql(
                    exec(
                        conn,
                        "UPDATE dls_ranges SET first_pos=?4, first_seq=NULL
                         WHERE epoch_id=?1 AND key_id=?2 AND first_pos=?3",
                        params![
                            epoch_id.as_slice(),
                            key_id,
                            encode_u64(first_pos).as_slice(),
                            encode_u64(dpos + 1).as_slice(),
                        ],
                    ),
                    "Cannot clip range front",
                )?;
                // PK changed → the row now sits under (epoch, key, dpos+1).
            }
            // Refresh first_seq where the former member seq was deleted.
            let needs_refresh: bool = query_one(
                conn,
                "SELECT first_seq FROM dls_ranges WHERE epoch_id=?1 AND key_id=?2 AND first_pos=?3",
                params![
                    epoch_id.as_slice(),
                    key_id,
                    encode_u64(if clipped_front { dpos + 1 } else { first_pos }).as_slice()
                ],
                |r| opt_blob8(r, 0, "first_seq"),
            )
            .optional()
            .map_err(|e| fatal("Cannot inspect range", e))?
            .flatten()
            .is_none_or(|s| s <= boundary_seq);
            if needs_refresh {
                let new_first = first_retained_seq_in_range(
                    conn,
                    &stream.id,
                    *key_id,
                    if clipped_front { dpos + 1 } else { first_pos },
                    last_pos,
                )?;
                sql(
                    exec(
                        conn,
                        "UPDATE dls_ranges SET first_seq=?4
                         WHERE epoch_id=?1 AND key_id=?2 AND first_pos=?3",
                        params![
                            epoch_id.as_slice(),
                            key_id,
                            encode_u64(if clipped_front { dpos + 1 } else { first_pos }).as_slice(),
                            new_first.map(encode_u64).map(|b| b.to_vec()),
                        ],
                    ),
                    "Cannot refresh range member",
                )?;
            }
        }
    }
    Ok(())
}

/// Recompute lanes of the deleted keys: cursor advance, expired-head lease
/// retirement, head/frontier rebuild, and unpark checks for parked lanes.
fn retain_stream_lanes(
    conn: &Connection,
    stream: &StreamRow,
    key_deleted_pos: &std::collections::HashMap<i64, u64>,
    boundary_seq: u64,
    dirty_epochs: &mut std::collections::HashMap<[u8; 16], EpochRow>,
    effects: &mut Effects,
) -> R<()> {
    for (key_id, dpos) in key_deleted_pos {
        let lane_epochs: Vec<Vec<u8>> = query_vec(
            conn,
            "SELECT epoch_id FROM key_lanes WHERE stream_id=?1 AND key_id=?2",
            params![stream.id.as_slice(), key_id],
            |r| r.get::<_, Vec<u8>>(0),
            "Cannot scan lanes for retention",
        )?;
        for epoch_blob in lane_epochs {
            let epoch_id = blob16(&epoch_blob, "epoch id")?;
            let epoch = match dirty_epochs.entry(epoch_id) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => {
                    let Some(loaded) = load_epoch(conn, &epoch_id)? else {
                        continue;
                    };
                    v.insert(loaded)
                }
            };
            let Some(mut lane) = load_lane(conn, &epoch_id, *key_id)? else {
                continue;
            };
            if lane.cursor_pos < *dpos {
                sql(
                    exec(
                        conn,
                        "UPDATE key_lanes SET cursor_pos=?3 WHERE epoch_id=?1 AND key_id=?2",
                        params![epoch_id.as_slice(), key_id, encode_u64(*dpos).as_slice()],
                    ),
                    "Cannot advance lane cursor",
                )?;
                lane.cursor_pos = *dpos;
            }
            match LaneState::from_i64(lane.state) {
                Some(LaneState::Parked) => {
                    if try_unpark_lane(conn, epoch, *key_id)? {
                        let names = group_stream_names(conn, epoch)?;
                        effects.wake(&names.0, &names.1);
                    }
                }
                Some(LaneState::Leased) | Some(LaneState::Waiting) => {
                    if lane.head_seq.is_some_and(|s| s <= boundary_seq) {
                        if lane.state == LaneState::Leased as i64 {
                            epoch.pending -= lane.lease_count - lane.lease_acked;
                            epoch.pending_bytes -= lane.lease_bytes;
                        }
                        sql(
                            exec(
                                conn,
                                &format!(
                                    "UPDATE key_lanes SET state=0, head_seq=NULL, head_pos=NULL,
                                        head_origin=NULL, deadline_ms=NULL, {CLEAR_LEASE}
                                     WHERE epoch_id=?1 AND key_id=?2"
                                ),
                                params![epoch_id.as_slice(), key_id],
                            ),
                            "Cannot retire expired lease",
                        )?;
                        let Some(lane) = load_lane(conn, &epoch_id, *key_id)? else {
                            continue;
                        };
                        if recompute_lane(conn, epoch, &lane)? {
                            let names = group_stream_names(conn, epoch)?;
                            effects.wake(&names.0, &names.1);
                        }
                    }
                }
                Some(LaneState::Ready) | Some(LaneState::Empty) => {
                    let stale = lane.head_seq.is_some_and(|s| s <= boundary_seq)
                        || lane.normal_seq.is_some_and(|s| s <= boundary_seq);
                    if stale && recompute_lane(conn, epoch, &lane)? {
                        let names = group_stream_names(conn, epoch)?;
                        effects.wake(&names.0, &names.1);
                    }
                }
                None => {
                    return Err(CmdError::Fatal(BrokerError::storage(
                        "Corrupt storage: invalid lane state",
                    )))
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Continuations: epoch init, garbage collection
// ---------------------------------------------------------------------------

/// Page one slice of key lanes for a fresh epoch; re-enqueues until the
/// target key id is reached.
pub fn init_epoch(
    conn: &Connection,
    epoch_id: &[u8; 16],
) -> Result<(StreamReply, Effects), CmdError> {
    let mut effects = Effects::default();
    let Some(mut epoch) = load_epoch(conn, epoch_id)? else {
        return Ok((StreamReply::Unit, effects));
    };
    let active: bool = sql(
        query_one(
            conn,
            "SELECT EXISTS(SELECT 1 FROM groups g JOIN streams s ON s.id = g.stream_id
             WHERE g.id = ?1 AND g.active_epoch = ?2 AND s.deleted = 0)",
            params![epoch.group_id.as_slice(), epoch.id.as_slice()],
            |r| r.get(0),
        ),
        "Cannot check active epoch",
    )?;
    if !active {
        return Ok((StreamReply::Unit, effects));
    }
    if epoch.initialized {
        return Ok((StreamReply::Unit, effects));
    }
    let keys: Vec<(i64, u64)> = query_vec(
        conn,
        "SELECT key_id, last_pos FROM stream_keys
         WHERE stream_id=?1 AND key_id>?2 AND key_id<=?3
         ORDER BY key_id LIMIT ?4",
        params![
            epoch.stream_id.as_slice(),
            epoch.init_cursor,
            epoch.init_target,
            INIT_PAGE + 1
        ],
        |r| Ok((r.get::<_, i64>(0)?, blob8(r, 1, "last_pos")?)),
        "Cannot scan keys for epoch init",
    )?;
    let mut last_key = epoch.init_cursor;
    for (processed, (key_id, last_pos)) in keys.iter().enumerate() {
        if processed == INIT_PAGE as usize {
            break;
        }
        last_key = *key_id;
        let boundary = key_boundary_pos(conn, &epoch.stream_id, *key_id, epoch.start_after)?;
        // Materialize only lanes with pending originals; a key with nothing to
        // deliver lazily gets its lane on the next publish.
        if boundary < *last_pos
            && next_event_pos(conn, &epoch.stream_id, *key_id, boundary)?.is_some()
        {
            sql(
                exec(
                    conn,
                    "INSERT INTO key_lanes(epoch_id, stream_id, key_id, cursor_pos, state,
                                           attempts, ready_ticket)
                     VALUES (?1,?2,?3,?4,0,0,0)
                     ON CONFLICT(epoch_id, key_id) DO NOTHING",
                    params![
                        epoch.id.as_slice(),
                        epoch.stream_id.as_slice(),
                        key_id,
                        encode_u64(boundary).as_slice(),
                    ],
                ),
                "Cannot create lane",
            )?;
            if let Some(lane) = load_lane(conn, &epoch.id, *key_id)? {
                if lane.state == LaneState::Empty as i64 && recompute_lane(conn, &mut epoch, &lane)?
                {
                    let names = group_stream_names(conn, &epoch)?;
                    effects.wake(&names.0, &names.1);
                }
            }
        }
    }
    epoch.init_cursor = last_key;
    // The page is done either when it filled (more keys may follow) or when
    // the scan exhausted the target range (fewer rows than the page size).
    if keys.len() <= INIT_PAGE as usize {
        epoch.init_cursor = epoch.init_target;
        epoch.initialized = true;
    }
    if !epoch.initialized {
        effects
            .followups
            .push(Continuation::InitEpoch { epoch_id: epoch.id });
    }
    save_epoch(conn, &epoch)?;
    // Wake the group once per page so long-polling fetches observe progress.
    if let Ok(names) = group_stream_names(conn, &epoch) {
        effects.wake(&names.0, &names.1);
    }
    Ok((StreamReply::Unit, effects))
}

/// Bounded GC: deleted streams lose their child rows then their root row;
/// epochs superseded by seek lose child rows then the epoch row.
pub fn gc_slice(conn: &Connection) -> Result<(StreamReply, Effects), CmdError> {
    let mut effects = Effects::default();
    let mut progress = false;

    // One deleted stream per slice. Child tables drain fully before their
    // parents are touched (FK order); the cyclic groups↔group_epochs pair is
    // deleted per-group in one step so the deferred active_epoch check is
    // never violated at commit.
    let deleted_stream: Option<Vec<u8>> = query_one(
        conn,
        "SELECT id FROM streams WHERE deleted=1 LIMIT 1",
        [],
        |r| r.get(0),
    )
    .optional()
    .map_err(|e| fatal("Cannot scan deleted streams", e))?;
    if let Some(id_blob) = deleted_stream {
        let stream_id = blob16(&id_blob, "stream id")?;
        let mut budget = BATCH_LIMIT as i64;
        let mut children_done = true;
        for table in [
            "key_lanes",
            "keyless_deliveries",
            "replay_intents",
            "members",
            "dls_ranges",
            "events",
            "stream_keys",
        ] {
            while budget > 0 {
                let n = sql(
                    exec(
                        conn,
                        &format!(
                            "DELETE FROM {table} WHERE rowid IN
                             (SELECT rowid FROM {table} WHERE stream_id=?1 LIMIT ?2)"
                        ),
                        params![stream_id.as_slice(), budget],
                    ),
                    "Cannot GC stream children",
                )?;
                budget -= n as i64;
                if n == 0 {
                    break;
                }
            }
            if budget <= 0 {
                children_done = false;
                break;
            }
        }
        if children_done {
            // Children are empty: reclaim group epochs + group rows one group
            // at a time so `groups.active_epoch` (deferred) is consistent at
            // commit. Group count is user-facing and small; each group is one
            // slice iteration.
            let group_ids: Vec<Vec<u8>> = query_vec(
                conn,
                "SELECT id FROM groups WHERE stream_id=?1 LIMIT ?2",
                params![stream_id.as_slice(), GROUP_GC_BATCH as i64],
                |r| r.get(0),
                "Cannot scan deleted groups",
            )?;
            let remaining_groups = group_ids.len() == GROUP_GC_BATCH;
            for gid in &group_ids {
                sql(
                    exec(
                        conn,
                        "DELETE FROM group_epochs WHERE stream_id=?1 AND group_id=?2",
                        params![stream_id.as_slice(), gid.as_slice()],
                    ),
                    "Cannot GC group epochs",
                )?;
                sql(
                    exec(
                        conn,
                        "DELETE FROM groups WHERE stream_id=?1 AND id=?2",
                        params![stream_id.as_slice(), gid.as_slice()],
                    ),
                    "Cannot GC group row",
                )?;
            }
            if !remaining_groups {
                sql(
                    exec(
                        conn,
                        "DELETE FROM streams WHERE id=?1",
                        params![stream_id.as_slice()],
                    ),
                    "Cannot delete stream row",
                )?;
            }
        }
        progress = true;
    }

    // One orphan epoch per slice: children drain fully before the epoch row
    // is removed.
    let orphan: Option<Vec<u8>> = query_one(
        conn,
        "SELECT e.id FROM group_epochs e
         WHERE NOT EXISTS (
             SELECT 1 FROM groups g
             WHERE g.stream_id=e.stream_id AND g.active_epoch=e.id)
         AND NOT EXISTS (
             SELECT 1 FROM streams s
             WHERE s.id=e.stream_id AND s.deleted=1)
         LIMIT 1",
        [],
        |r| r.get(0),
    )
    .optional()
    .map_err(|e| fatal("Cannot scan orphan epochs", e))?;
    if let Some(id_blob) = orphan {
        let epoch_id = blob16(&id_blob, "epoch id")?;
        let mut budget = BATCH_LIMIT as i64;
        let mut drained = true;
        for table in [
            "key_lanes",
            "keyless_deliveries",
            "replay_intents",
            "members",
            "dls_ranges",
        ] {
            while budget > 0 {
                let n = sql(
                    exec(
                        conn,
                        &format!(
                            "DELETE FROM {table} WHERE rowid IN
                             (SELECT rowid FROM {table} WHERE epoch_id=?1 LIMIT ?2)"
                        ),
                        params![epoch_id.as_slice(), budget],
                    ),
                    "Cannot GC epoch children",
                )?;
                budget -= n as i64;
                if n == 0 {
                    break;
                }
            }
            if budget <= 0 {
                drained = false;
                break;
            }
        }
        if drained {
            sql(
                exec(
                    conn,
                    "DELETE FROM group_epochs WHERE id=?1",
                    params![epoch_id.as_slice()],
                ),
                "Cannot delete epoch row",
            )?;
        }
        progress = true;
    }

    let pending_deleted: i64 = query_one(
        conn,
        "SELECT COUNT(*) FROM streams WHERE deleted=1",
        [],
        |r| r.get(0),
    )
    .map_err(|e| fatal("Cannot count deleted streams", e))?;
    let pending_orphans: i64 = query_one(
        conn,
        "SELECT COUNT(*) FROM group_epochs e
         WHERE NOT EXISTS (
             SELECT 1 FROM groups g
             WHERE g.stream_id=e.stream_id AND g.active_epoch=e.id)",
        [],
        |r| r.get(0),
    )
    .map_err(|e| fatal("Cannot count orphan epochs", e))?;
    if progress && (pending_deleted > 0 || pending_orphans > 0) {
        effects.followups.push(Continuation::Gc);
    }
    Ok((StreamReply::Unit, effects))
}

pub fn prune_keys(
    conn: &Connection,
    after: Option<([u8; 16], i64)>,
) -> Result<(StreamReply, Effects), CmdError> {
    let mut effects = Effects::default();
    let mut budget = BATCH_LIMIT as i64;
    let page_limit = (KEY_GC_PAGE as i64).min(budget);

    let candidates: Vec<(Vec<u8>, i64)> = match after {
        Some((sid, kid)) => query_vec(
            conn,
            "SELECT stream_id, key_id FROM stream_keys
             WHERE key_id > 0 AND last_pos = retained_after_pos
               AND (stream_id, key_id) > (?1, ?2)
             ORDER BY stream_id, key_id LIMIT ?3",
            params![sid.as_slice(), kid, page_limit],
            |r| Ok((r.get(0)?, r.get(1)?)),
            "Cannot scan retired stream keys",
        )?,
        None => query_vec(
            conn,
            "SELECT stream_id, key_id FROM stream_keys
             WHERE key_id > 0 AND last_pos = retained_after_pos
             ORDER BY stream_id, key_id LIMIT ?1",
            params![page_limit],
            |r| Ok((r.get(0)?, r.get(1)?)),
            "Cannot scan retired stream keys",
        )?,
    };
    let page_full = candidates.len() as i64 == page_limit;

    let mut cursor: Option<([u8; 16], i64)> = after;

    for (sid_blob, key_id) in &candidates {
        budget -= 1;
        if budget <= 0 {
            effects
                .followups
                .push(Continuation::PruneKeys { after: cursor });
            return Ok((StreamReply::Unit, effects));
        }
        let stream_id = blob16(sid_blob, "stream id")?;
        let blocked: bool = query_one(
            conn,
            "SELECT EXISTS(SELECT 1 FROM events
                WHERE stream_id = ?1 AND key_id = ?2)
              OR EXISTS(SELECT 1 FROM key_lanes
                WHERE stream_id = ?1 AND key_id = ?2 AND state <> 0)
              OR EXISTS(SELECT 1 FROM replay_intents
                WHERE stream_id = ?1 AND key_id = ?2)
              OR EXISTS(SELECT 1 FROM dls_ranges
                WHERE stream_id = ?1 AND key_id = ?2)",
            params![stream_id.as_slice(), key_id],
            |r| r.get(0),
        )
        .map_err(|e| fatal("Cannot check key obligations", e))?;
        if blocked {
            cursor = Some((stream_id, *key_id));
            continue;
        }

        let deleted = sql(
            exec(
                conn,
                "DELETE FROM key_lanes WHERE rowid IN (
                   SELECT rowid FROM key_lanes
                   WHERE stream_id = ?1 AND key_id = ?2 AND state = 0
                   LIMIT ?3)",
                params![stream_id.as_slice(), key_id, budget],
            ),
            "Cannot prune empty key lanes",
        )? as i64;
        budget -= deleted;

        let lanes_left: bool = query_one(
            conn,
            "SELECT EXISTS(SELECT 1 FROM key_lanes
                WHERE stream_id = ?1 AND key_id = ?2)",
            params![stream_id.as_slice(), key_id],
            |r| r.get(0),
        )
        .map_err(|e| fatal("Cannot recount key lanes", e))?;
        if budget <= 0 || lanes_left {
            effects
                .followups
                .push(Continuation::PruneKeys { after: cursor });
            return Ok((StreamReply::Unit, effects));
        }

        sql(
            exec(
                conn,
                "DELETE FROM stream_keys
                 WHERE stream_id = ?1 AND key_id = ?2
                   AND last_pos = retained_after_pos",
                params![stream_id.as_slice(), key_id],
            ),
            "Cannot prune retired stream key",
        )?;
        budget -= 1;
        cursor = Some((stream_id, *key_id));
    }

    if page_full || budget <= 0 {
        effects
            .followups
            .push(Continuation::PruneKeys { after: cursor });
    }
    Ok((StreamReply::Unit, effects))
}

/// Dispatch a continuation item inside the shared writer transaction.
pub fn run_continuation(
    conn: &Connection,
    cont: &Continuation,
    ctx: &ExecCtx,
) -> Result<(StreamReply, Effects), CmdError> {
    match cont {
        Continuation::InitEpoch { epoch_id } => init_epoch(conn, epoch_id),
        Continuation::ExpireLeases => expire_leases(conn, ctx.now_ms),
        Continuation::Retention => retention_tick(conn, ctx),
        Continuation::Gc => gc_slice(conn),
        Continuation::PruneKeys { after } => prune_keys(conn, *after),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::brokers::stream::domain::storage::SCHEMA;
    use rusqlite::Connection;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
        conn
    }

    /// A deleted stream with more child rows than `BATCH_LIMIT` must still be
    /// fully reclaimed: children drain before parents across slices, so the
    /// deferred `groups.active_epoch` and lane/delivery FKs never break.
    #[test]
    fn gc_reclaims_deleted_stream_beyond_batch_limit() {
        let mut conn = test_conn();
        let stream_id = [7u8; 16];
        let group_id = [8u8; 16];
        let epoch_id = [9u8; 16];

        exec(
            &conn,
            "INSERT INTO streams VALUES (?1,'s',1,'{}',?2,?2,0,0)",
            params![stream_id.as_slice(), encode_u64(0).as_slice()],
        )
        .unwrap();
        // The keyless key row (key_id=0, empty key) required by the schema.
        exec(
            &conn,
            "INSERT INTO stream_keys(stream_id,key_id,key,last_pos,retained_after_pos)
             VALUES (?1,0,'',?2,?2)",
            params![stream_id.as_slice(), encode_u64(0).as_slice()],
        )
        .unwrap();
        {
            // groups.active_epoch → group_epochs is DEFERRED: the pair must
            // be inserted inside one transaction.
            let tx = conn.transaction().unwrap();
            exec(
                &tx,
                "INSERT INTO groups(id,stream_id,name,active_epoch)
                 VALUES (?1,?2,'g',?3)",
                params![
                    group_id.as_slice(),
                    stream_id.as_slice(),
                    epoch_id.as_slice()
                ],
            )
            .unwrap();
            exec(
                &tx,
                "INSERT INTO group_epochs(id,group_id,stream_id,start_after_seq,initialized,
                    init_key_cursor,init_key_target,keyless_cursor_pos,keyless_ticket,
                    next_ready_ticket,pending_count,max_pending)
                 VALUES (?1,?2,?3,?4,1,0,0,?4,0,1,0,100)",
                params![
                    epoch_id.as_slice(),
                    group_id.as_slice(),
                    stream_id.as_slice(),
                    encode_u64(0).as_slice()
                ],
            )
            .unwrap();
            tx.commit().unwrap();
        }

        // More keyed lanes than one slice can hold.
        let total_keys = BATCH_LIMIT + 37;
        for key_id in 1..=total_keys as i64 {
            exec(
                &conn,
                "INSERT INTO stream_keys(stream_id,key_id,key,last_pos,retained_after_pos)
                 VALUES (?1,?2,?3,?4,?4)",
                params![
                    stream_id.as_slice(),
                    key_id,
                    format!("k{key_id}").as_bytes(),
                    encode_u64(1).as_slice()
                ],
            )
            .unwrap();
            exec(
                &conn,
                "INSERT INTO events(stream_id,seq,key_id,key_pos,timestamp_ms,payload,
                    payload_bytes,logical_bytes)
                 VALUES (?1,?2,?3,?4,0,'x',1,1)",
                params![
                    stream_id.as_slice(),
                    encode_u64(key_id as u64).as_slice(),
                    key_id,
                    encode_u64(1).as_slice()
                ],
            )
            .unwrap();
            exec(
                &conn,
                "INSERT INTO key_lanes(epoch_id,stream_id,key_id,cursor_pos,state,
                    attempts,ready_ticket)
                 VALUES (?1,?2,?3,?4,3,0,0)",
                params![
                    epoch_id.as_slice(),
                    stream_id.as_slice(),
                    key_id,
                    encode_u64(1).as_slice()
                ],
            )
            .unwrap();
        }

        // Drive slices until the stream row is gone. Each slice runs inside
        // an explicit transaction, mirroring the worker's batch commit.
        for _ in 0..64 {
            conn.execute_batch("BEGIN").unwrap();
            match gc_slice(&conn) {
                Ok(_) => {}
                Err(e) => panic!("gc_slice failed: {}", e.into_error()),
            }
            conn.execute_batch("COMMIT").unwrap();
            let left: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM streams WHERE id=?1",
                    [stream_id.as_slice()],
                    |r| r.get(0),
                )
                .unwrap();
            if left == 0 {
                break;
            }
        }

        for (table, col) in [
            ("streams", "id"),
            ("groups", "stream_id"),
            ("group_epochs", "stream_id"),
            ("stream_keys", "stream_id"),
            ("events", "stream_id"),
            ("key_lanes", "stream_id"),
        ] {
            let left: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE {col}=?1"),
                    [stream_id.as_slice()],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(left, 0, "{table} must be fully reclaimed");
        }
    }

    /// A merged update run consumes keyless leases in bulk while keeping
    /// per-item outcomes: duplicate and stale receipts are FENCED alone;
    /// valid completions drop `pending` and delete the rows.
    #[test]
    fn update_batches_consumes_keyless_run_with_per_item_outcomes() {
        let conn = test_conn();
        let stream_id = [7u8; 16];
        let group_id = [8u8; 16];
        let epoch_id = [9u8; 16];
        let identity = crate::brokers::stream::domain::message::ConsumerIdentity {
            connection_id: "conn1".to_string(),
            consumer_id: "c1".to_string(),
            epoch: epoch_id,
        };

        exec(
            &conn,
            "INSERT INTO streams VALUES (?1,'s',0,?3,?2,?2,0,0)",
            params![
                stream_id.as_slice(),
                encode_u64(0).as_slice(),
                r#"{"retention":{"max_age_ms":null,"max_bytes":null},"max_ack_pending":1000,"max_ack_pending_bytes":67108864,"ack_wait_ms":30000,"max_deliveries":3}"#
            ],
        )
        .unwrap();
        exec(
            &conn,
            "INSERT INTO stream_keys(stream_id,key_id,key,last_pos,retained_after_pos)
             VALUES (?1,0,'',?2,?2)",
            params![stream_id.as_slice(), encode_u64(0).as_slice()],
        )
        .unwrap();
        conn.execute_batch("BEGIN").unwrap();
        exec(
            &conn,
            "INSERT INTO groups(id,stream_id,name,active_epoch)
             VALUES (?1,?2,'g',?3)",
            params![
                group_id.as_slice(),
                stream_id.as_slice(),
                epoch_id.as_slice()
            ],
        )
        .unwrap();
        exec(
            &conn,
            "INSERT INTO group_epochs(id,group_id,stream_id,start_after_seq,initialized,
                init_key_cursor,init_key_target,keyless_cursor_pos,keyless_ticket,
                next_ready_ticket,pending_count,max_pending)
             VALUES (?1,?2,?3,?4,1,0,0,?4,0,1,0,100)",
            params![
                epoch_id.as_slice(),
                group_id.as_slice(),
                stream_id.as_slice(),
                encode_u64(0).as_slice()
            ],
        )
        .unwrap();
        conn.execute_batch("COMMIT").unwrap();
        exec(
            &conn,
            "INSERT INTO members(epoch_id,stream_id,group_id,consumer_id,connection_id)
             VALUES (?1,?2,?3,'c1','conn1')",
            params![
                epoch_id.as_slice(),
                stream_id.as_slice(),
                group_id.as_slice()
            ],
        )
        .unwrap();
        for seq in 1..=5u64 {
            exec(
                &conn,
                "INSERT INTO events(stream_id,seq,key_id,key_pos,timestamp_ms,payload,
                    payload_bytes,logical_bytes)
                 VALUES (?1,?2,0,?3,1,X'78',1,1)",
                params![
                    stream_id.as_slice(),
                    encode_u64(seq).as_slice(),
                    encode_u64(seq).as_slice()
                ],
            )
            .unwrap();
        }

        let ctx = ExecCtx {
            now_ms: 1_000,
            fetch_response_bytes: 1 << 20,
        };
        let (reply, _) = match fetch(
            &conn,
            "s",
            "g",
            &identity,
            FetchLimits {
                max_messages: 10,
                max_key_messages: 1,
                max_key_batches: 10,
                max_bytes: 1 << 20,
            },
            &ctx,
        ) {
            Ok(out) => out,
            Err(e) => panic!("fetch failed: {}", e.into_error()),
        };
        let StreamReply::Fetch(page) = reply else {
            panic!("expected a fetch reply")
        };
        assert_eq!(page.batches.len(), 5);
        // The deferred bulk admit actually wrote the lease rows.
        let leased: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM keyless_deliveries WHERE epoch_id=?1 AND state=2",
                [epoch_id.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(leased, 5);

        // Run: [ok 1, dup 1, stale receipt, unknown receipt, ok 3, ok 4, ok 5]
        let done = |receipt: [u8; 16]| BatchUpdate {
            receipt,
            completed: 1,
            disposition: BatchDisposition::Checkpoint,
        };
        let mut run: Vec<BatchUpdate> = vec![
            done(page.batches[0].receipt),
            done(page.batches[0].receipt),
            done([0xAA; 16]),
            done([0xBB; 16]),
        ];
        for b in &page.batches[2..] {
            run.push(done(b.receipt));
        }

        let (outcomes, _) = match update_batches(&conn, "s", "g", &identity, &run, &ctx) {
            Ok(out) => out,
            Err(e) => panic!("update_batches failed: {}", e.into_error()),
        };
        assert_eq!(outcomes.len(), run.len());
        assert!(outcomes[0].is_none());
        assert!(
            outcomes[1].is_some(),
            "second update of a consumed receipt is fenced"
        );
        assert!(outcomes[2].is_some(), "stale receipt is fenced");
        assert!(outcomes[3].is_some(), "unknown receipt is fenced");
        for o in &outcomes[4..] {
            assert!(o.is_none());
        }

        let leased: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM keyless_deliveries WHERE epoch_id=?1 AND state=2",
                [epoch_id.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(leased, 1, "only the stale-receipt lease survives");
        let pending: i64 = conn
            .query_row(
                "SELECT pending_count FROM group_epochs WHERE id=?1",
                [epoch_id.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pending, 1);
    }

    fn test_config() -> StreamConfig {
        StreamConfig {
            retention: crate::brokers::stream::options::RetentionOptions {
                max_age_ms: None,
                max_bytes: None,
            },
            max_ack_pending: 1000,
            max_ack_pending_bytes: 64 * 1024 * 1024,
            ack_wait_ms: 30_000,
            max_deliveries: 3,
        }
    }

    #[test]
    fn init_epoch_skips_superseded_epochs() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(
            &conn,
            "s",
            &[crate::brokers::stream::domain::message::PubItem {
                key: Bytes::from_static(b"k"),
                payload: Bytes::from_static(b"p"),
            }],
            &ctx,
        )
        .unwrap();

        let stream = require_stream(&conn, "s").unwrap();
        let epoch0;
        let epoch1;
        let epoch2;
        {
            let tx = conn.transaction().unwrap();
            let (_group, epoch) = create_group_and_epoch(&tx, &stream, "g", 0).unwrap();
            epoch0 = epoch.id;
            seek(&tx, "s", "g", &SeekTarget::Beginning, &ctx).unwrap();
            epoch1 = load_group(&tx, &stream.id, "g")
                .unwrap()
                .unwrap()
                .active_epoch
                .unwrap();
            seek(&tx, "s", "g", &SeekTarget::Beginning, &ctx).unwrap();
            epoch2 = load_group(&tx, &stream.id, "g")
                .unwrap()
                .unwrap()
                .active_epoch
                .unwrap();
            tx.commit().unwrap();
        }
        assert_ne!(epoch0, epoch1);
        assert_ne!(epoch1, epoch2);

        for stale in [epoch0, epoch1] {
            let (_reply, effects) = init_epoch(&conn, &stale).unwrap();
            assert!(effects.followups.is_empty());
            let initialized: i64 = conn
                .query_row(
                    "SELECT initialized FROM group_epochs WHERE id=?1",
                    [stale.as_slice()],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(initialized, 0, "superseded epoch must stay uninitialized");
            let lanes: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM key_lanes WHERE epoch_id=?1",
                    [stale.as_slice()],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(lanes, 0);
        }

        let (_reply, effects) = init_epoch(&conn, &epoch2).unwrap();
        assert!(effects.followups.is_empty());
        let initialized: i64 = conn
            .query_row(
                "SELECT initialized FROM group_epochs WHERE id=?1",
                [epoch2.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(initialized, 1);
        let lanes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM key_lanes WHERE epoch_id=?1",
                [epoch2.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(lanes, 1, "only the key with pending events gets a lane");
    }

    #[test]
    fn recover_resumes_only_active_epoch_and_schedules_orphan_gc() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let orphan_epoch;
        let active_epoch;
        {
            let mut store = crate::durable::Store::open(
                dir.path(),
                crate::brokers::stream::domain::storage::SPEC,
            )
            .unwrap();
            {
                let tx = store.conn.transaction().unwrap();
                create_stream_with_config(&tx, "s", &test_config()).unwrap();
                tx.commit().unwrap();
            }
            publish(
                &store.conn,
                "s",
                &[crate::brokers::stream::domain::message::PubItem {
                    key: Bytes::from_static(b"k"),
                    payload: Bytes::from_static(b"p"),
                }],
                &ctx,
            )
            .unwrap();
            {
                let tx = store.conn.transaction().unwrap();
                let stream = require_stream(&tx, "s").unwrap();
                let (_group, epoch) = create_group_and_epoch(&tx, &stream, "g", 0).unwrap();
                orphan_epoch = epoch.id;
                seek(&tx, "s", "g", &SeekTarget::Beginning, &ctx).unwrap();
                active_epoch = load_group(&tx, &stream.id, "g")
                    .unwrap()
                    .unwrap()
                    .active_epoch
                    .unwrap();
                tx.commit().unwrap();
            }
        }
        let mut store = crate::durable::Store::open(
            dir.path(),
            crate::brokers::stream::domain::storage::SPEC,
        )
        .unwrap();

        let deleted: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM streams WHERE deleted=1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(deleted, 0);
        let continuations =
            crate::brokers::stream::domain::storage::recover(&mut store).unwrap();
        let init = |continuations: &[Continuation], id: [u8; 16]| {
            continuations
                .iter()
                .any(|c| matches!(c, Continuation::InitEpoch { epoch_id } if *epoch_id == id))
        };
        assert!(init(&continuations, active_epoch));
        assert!(
            !init(&continuations, orphan_epoch),
            "orphan epoch must not be initialized"
        );
        assert!(
            continuations.iter().any(|c| matches!(c, Continuation::Gc)),
            "recovery must schedule GC for the orphan epoch alone"
        );

        let mut queue = continuations;
        for _ in 0..128 {
            let Some(cont) = queue.pop() else { break };
            let followups = {
                let tx = store.conn.transaction().unwrap();
                let (_reply, effects) = run_continuation(&tx, &cont, &ctx).unwrap();
                tx.commit().unwrap();
                effects.followups
            };
            queue.extend(followups);
        }

        let orphan_rows: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM group_epochs WHERE id=?1",
                [orphan_epoch.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(orphan_rows, 0, "orphan epoch reclaimed by GC");
        let initialized: i64 = store
            .conn
            .query_row(
                "SELECT initialized FROM group_epochs WHERE id=?1",
                [active_epoch.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(initialized, 1);

        let (reply, _) = join(&store.conn, "s", "g", "conn1", &ctx).unwrap();
        let StreamReply::Join {
            consumer_id, epoch, ..
        } = reply
        else {
            panic!("expected a join reply")
        };
        assert_eq!(epoch, active_epoch);
        let identity = crate::brokers::stream::domain::message::ConsumerIdentity {
            connection_id: "conn1".to_string(),
            consumer_id,
            epoch,
        };
        let (reply, _) = fetch(
            &store.conn,
            "s",
            "g",
            &identity,
            FetchLimits {
                max_messages: 10,
                max_key_messages: 1,
                max_key_batches: 10,
                max_bytes: 1 << 20,
            },
            &ctx,
        )
        .unwrap();
        let StreamReply::Fetch(page) = reply else {
            panic!("expected a fetch reply")
        };
        assert_eq!(page.batches.len(), 1);
        assert_eq!(page.batches[0].messages[0].payload.as_ref(), b"p");

        let dead_epoch = [14u8; 16];
        {
            let tx = store.conn.transaction().unwrap();
            create_stream_with_config(&tx, "dead", &test_config()).unwrap();
            let dead_stream_id: Vec<u8> = tx
                .query_row("SELECT id FROM streams WHERE name='dead'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            exec(&tx, "UPDATE streams SET deleted=1 WHERE name='dead'", []).unwrap();
            let dead_group = [13u8; 16];
            exec(
                &tx,
                "INSERT INTO groups(id,stream_id,name,active_epoch)
                 VALUES (?1,?2,'g',?3)",
                params![
                    dead_group.as_slice(),
                    dead_stream_id.as_slice(),
                    dead_epoch.as_slice()
                ],
            )
            .unwrap();
            exec(
                &tx,
                "INSERT INTO group_epochs(id,group_id,stream_id,start_after_seq,initialized,
                    init_key_cursor,init_key_target,keyless_cursor_pos,keyless_ticket,
                    next_ready_ticket,pending_count,max_pending)
                 VALUES (?1,?2,?3,?4,0,0,0,?4,0,1,0,100)",
                params![
                    dead_epoch.as_slice(),
                    dead_group.as_slice(),
                    dead_stream_id.as_slice(),
                    encode_u64(0).as_slice()
                ],
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let continuations =
            crate::brokers::stream::domain::storage::recover(&mut store).unwrap();
        assert!(
            !init(&continuations, dead_epoch),
            "deleted-stream epoch must not be initialized"
        );
        assert!(
            continuations.iter().any(|c| matches!(c, Continuation::Gc)),
            "deleted stream must schedule GC"
        );

        let (_reply, effects) = init_epoch(&store.conn, &dead_epoch).unwrap();
        assert!(effects.followups.is_empty());
        assert!(effects.wakes.is_empty());
        let initialized: i64 = store
            .conn
            .query_row(
                "SELECT initialized FROM group_epochs WHERE id=?1",
                [dead_epoch.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(initialized, 0, "deleted-stream epoch stays uninitialized");
    }


    use crate::brokers::stream::domain::message::{ConsumerIdentity, PubItem};
    use crate::brokers::BrokerErrorKind;

    fn keyed(key: &[u8], payload: &[u8]) -> PubItem {
        PubItem {
            key: Bytes::copy_from_slice(key),
            payload: Bytes::copy_from_slice(payload),
        }
    }

    fn keyless(payload: &[u8]) -> PubItem {
        PubItem {
            key: Bytes::new(),
            payload: Bytes::copy_from_slice(payload),
        }
    }

    fn new_member(
        conn: &mut Connection,
        name: &str,
        group: &str,
        connection_id: &str,
        ctx: &ExecCtx,
    ) -> ConsumerIdentity {
        let (reply, effects) = {
            let tx = conn.transaction().unwrap();
            let out = join(&tx, name, group, connection_id, ctx).unwrap();
            tx.commit().unwrap();
            out
        };
        let mut queue: std::collections::VecDeque<Continuation> =
            effects.followups.into();
        while let Some(c) = queue.pop_front() {
            let (_r, more) = run_continuation(conn, &c, ctx).unwrap();
            queue.extend(more.followups);
        }
        let StreamReply::Join {
            consumer_id, epoch, ..
        } = reply
        else {
            panic!("expected a join reply")
        };
        ConsumerIdentity {
            connection_id: connection_id.to_string(),
            consumer_id,
            epoch,
        }
    }

    fn fetch_page(
        conn: &Connection,
        name: &str,
        identity: &ConsumerIdentity,
        max_messages: usize,
        max_key_messages: usize,
        max_key_batches: usize,
        max_bytes: usize,
        ctx: &ExecCtx,
    ) -> FetchPage {
        let (reply, _) = fetch(
            conn,
            name,
            "g",
            identity,
            FetchLimits {
                max_messages,
                max_key_messages,
                max_key_batches,
                max_bytes,
            },
            ctx,
        )
        .unwrap();
        let StreamReply::Fetch(page) = reply else {
            panic!("expected a fetch page")
        };
        page
    }

    fn upd(
        conn: &Connection,
        name: &str,
        identity: &ConsumerIdentity,
        receipt: [u8; 16],
        completed: u32,
        disposition: BatchDisposition,
        ctx: &ExecCtx,
    ) -> Option<BrokerErrorKind> {
        let (outcomes, _) = update_batches(
            conn,
            name,
            "g",
            identity,
            &[BatchUpdate {
                receipt,
                completed,
                disposition,
            }],
            ctx,
        )
        .unwrap();
        outcomes.into_iter().next().flatten()
    }

    fn pending(conn: &Connection, epoch: &[u8; 16]) -> (i64, i64) {
        conn.query_row(
            "SELECT pending_count, pending_bytes FROM group_epochs WHERE id=?1",
            [epoch.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    }

    fn lane_of(conn: &Connection, epoch: &[u8; 16], key: &[u8]) -> LaneRow {
        conn.query_row(
            &format!(
                "SELECT {} FROM key_lanes l
                 JOIN stream_keys k ON k.stream_id=l.stream_id AND k.key_id=l.key_id
                 WHERE l.epoch_id=?1 AND k.key=?2",
                LANE_COLS
                    .split(',')
                    .map(|c| format!("l.{}", c.trim()))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            params![epoch.as_slice(), key],
            |r| lane_row(r, 0),
        )
        .unwrap()
    }

    fn seqs(batch: &DeliveryBatch) -> Vec<u64> {
        batch.messages.iter().map(|m| m.seq).collect()
    }

    #[test]
    fn keyed_batches_interleave_keys_and_isolate_leases() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(
            &conn,
            "s",
            &[
                keyed(b"a", b"1"),
                keyed(b"b", b"2"),
                keyed(b"a", b"3"),
                keyed(b"b", b"4"),
                keyed(b"a", b"5"),
            ],
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let c2 = new_member(&mut conn, "s", "g", "connB", &ctx);

        let page = fetch_page(&conn, "s", &c1, 5, 3, 2, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 2);
        assert!(page.batches[0]
            .messages
            .iter()
            .all(|m| m.key.as_ref() == b"a"));
        assert_eq!(seqs(&page.batches[0]), vec![1, 3, 5]);
        assert!(page.batches[1]
            .messages
            .iter()
            .all(|m| m.key.as_ref() == b"b"));
        assert_eq!(seqs(&page.batches[1]), vec![2, 4]);

        let other = fetch_page(&conn, "s", &c2, 5, 3, 2, 1 << 20, &ctx);
        assert!(other.batches.is_empty());

        assert_eq!(
            upd(
                &conn,
                "s",
                &c1,
                page.batches[1].receipt,
                2,
                BatchDisposition::Checkpoint,
                &ctx
            ),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 3);
        let other = fetch_page(&conn, "s", &c2, 5, 3, 2, 1 << 20, &ctx);
        assert!(other.batches.is_empty());
    }

    #[test]
    fn keyed_prefix_checkpoint_is_cumulative_and_idempotent() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(
            &conn,
            "s",
            &(1..=5u8)
                .map(|i| keyed(b"a", &[i]))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let c2 = new_member(&mut conn, "s", "g", "connB", &ctx);

        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 1);
        let receipt = page.batches[0].receipt;
        assert_eq!(seqs(&page.batches[0]), vec![1, 2, 3, 4, 5]);
        assert_eq!(pending(&conn, &c1.epoch).0, 5);

        assert_eq!(
            upd(&conn, "s", &c1, receipt, 2, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 3);
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Leased as i64);
        assert_eq!(lane.lease_acked, 2);
        assert_eq!(lane.cursor_pos, 2);
        assert_eq!(lane.normal_seq, Some(3));
        let stream = require_stream(&conn, "s").unwrap();
        let epoch = load_epoch(&conn, &c1.epoch).unwrap().unwrap();
        assert_eq!(ack_floor(&conn, &epoch, &stream).unwrap(), 2);

        assert!(fetch_page(&conn, "s", &c2, 5, 5, 1, 1 << 20, &ctx)
            .batches
            .is_empty());

        assert_eq!(
            upd(&conn, "s", &c1, receipt, 2, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 3);
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 6, BatchDisposition::Checkpoint, &ctx),
            Some(BrokerErrorKind::InvalidArgument)
        );
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 1, BatchDisposition::Checkpoint, &ctx),
            Some(BrokerErrorKind::InvalidArgument)
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 3);

        assert_eq!(
            upd(&conn, "s", &c1, receipt, 5, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.cursor_pos, 5);
        let epoch = load_epoch(&conn, &c1.epoch).unwrap().unwrap();
        assert_eq!(ack_floor(&conn, &epoch, &stream).unwrap(), 5);
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 5, BatchDisposition::Checkpoint, &ctx),
            Some(BrokerErrorKind::Fenced)
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
    }

    #[test]
    fn keyed_checkpoint_at_position_max() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        let stream = require_stream(&conn, "s").unwrap();
        conn.execute(
            "INSERT INTO stream_keys(stream_id, key_id, key, last_pos, retained_after_pos)
             VALUES (?1,1,?2,?3,?3)",
            params![
                stream.id.as_slice(),
                b"a".as_slice(),
                encode_u64(u64::MAX - 2).as_slice()
            ],
        )
        .unwrap();
        conn.execute(
            "UPDATE streams SET last_key_id=1 WHERE id=?1",
            params![stream.id.as_slice()],
        )
        .unwrap();
        publish(&conn, "s", &[keyed(b"a", b"1"), keyed(b"a", b"2")], &ctx).unwrap();
        let c = new_member(&mut conn, "s", "g", "conn", &ctx);

        let page = fetch_page(&conn, "s", &c, 4, 4, 1, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 1);
        assert_eq!(seqs(&page.batches[0]), vec![1, 2]);
        let receipt = page.batches[0].receipt;

        assert_eq!(
            upd(&conn, "s", &c, receipt, 1, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(lane_of(&conn, &c.epoch, b"a").cursor_pos, u64::MAX - 1);
        assert_eq!(
            upd(&conn, "s", &c, receipt, 2, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(pending(&conn, &c.epoch).0, 0);
        assert_eq!(lane_of(&conn, &c.epoch, b"a").cursor_pos, u64::MAX);
        let epoch = load_epoch(&conn, &c.epoch).unwrap().unwrap();
        let stream = require_stream(&conn, "s").unwrap();
        assert_eq!(ack_floor(&conn, &epoch, &stream).unwrap(), 2);
    }

    #[test]
    fn keyed_retry_preserves_prefix_and_parks_head() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let mut config = test_config();
        config.max_deliveries = 5;
        create_stream_with_config(&conn, "s", &config).unwrap();
        publish(
            &conn,
            "s",
            &(1..=5u8)
                .map(|i| keyed(b"a", &[i]))
                .chain(std::iter::once(keyed(b"b", b"x")))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);

        let page = fetch_page(&conn, "s", &c1, 6, 5, 2, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 2);
        let a_receipt = page.batches[0].receipt;
        let b_receipt = page.batches[1].receipt;
        assert_eq!(seqs(&page.batches[0]), vec![1, 2, 3, 4, 5]);
        assert_eq!(seqs(&page.batches[1]), vec![6]);
        assert_eq!(pending(&conn, &c1.epoch).0, 6);

        assert_eq!(
            upd(&conn, "s", &c1, b_receipt, 1, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(
            upd(&conn, "s", &c1, a_receipt, 2, BatchDisposition::Retry, &ctx),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Waiting as i64);
        assert_eq!(lane.head_pos, Some(3));
        assert_eq!(lane.attempts, 1);
        assert_eq!(lane.cursor_pos, 2);

        assert!(fetch_page(&conn, "s", &c1, 6, 5, 2, 1 << 20, &ctx)
            .batches
            .is_empty());
        expire_leases(&conn, config.ack_wait_ms + 1).unwrap();

        let mut last_receipt = [0u8; 16];
        for _ in 0..4 {
            let retry = fetch_page(&conn, "s", &c1, 6, 5, 2, 1 << 20, &ctx);
            assert_eq!(retry.batches.len(), 1);
            assert_eq!(seqs(&retry.batches[0]), vec![3, 4, 5]);
            last_receipt = retry.batches[0].receipt;
            assert_eq!(
                upd(&conn, "s", &c1, last_receipt, 0, BatchDisposition::Retry, &ctx),
                None
            );
            let lane = lane_of(&conn, &c1.epoch, b"a");
            if lane.state == LaneState::Waiting as i64 {
                expire_leases(&conn, 2 * (config.ack_wait_ms + 1)).unwrap();
            }
        }
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Parked as i64);
        let (first, last): (Vec<u8>, Option<Vec<u8>>) = conn
            .query_row(
                "SELECT first_pos, last_pos FROM dls_ranges WHERE epoch_id=?1",
                [c1.epoch.as_slice()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(decode_u64(&first).unwrap(), 3);
        assert!(last.is_none(), "parked suffix is the open tail range");
        assert!(fetch_page(&conn, "s", &c1, 6, 5, 2, 1 << 20, &ctx)
            .batches
            .is_empty());
        let _ = last_receipt;
    }

    #[test]
    fn keyed_release_redelivers_tail_immediately() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(
            &conn,
            "s",
            &(1..=5u8)
                .map(|i| keyed(b"a", &[i]))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let c2 = new_member(&mut conn, "s", "g", "connB", &ctx);

        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        let receipt = page.batches[0].receipt;
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 2, BatchDisposition::Release, &ctx),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Ready as i64);
        assert_eq!(lane.attempts, 0, "unstarted head's charge is rolled back");
        assert_eq!(lane.cursor_pos, 2);

        let page = fetch_page(&conn, "s", &c2, 5, 5, 1, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 1);
        assert_ne!(page.batches[0].receipt, receipt);
        assert_eq!(seqs(&page.batches[0]), vec![3, 4, 5]);
        assert_eq!(
            upd(
                &conn,
                "s",
                &c2,
                page.batches[0].receipt,
                3,
                BatchDisposition::Release,
                &ctx
            ),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Empty as i64);
    }

    #[test]
    fn keyed_cursor_survives_retry_release_cycles() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let config = test_config();
        create_stream_with_config(&conn, "s", &config).unwrap();
        publish(
            &conn,
            "s",
            &(1..=5u8).map(|i| keyed(b"a", &[i])).collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);

        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        let receipt = page.batches[0].receipt;
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 2, BatchDisposition::Retry, &ctx),
            None
        );
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Waiting as i64);
        assert_eq!(lane.cursor_pos, 2);

        expire_leases(&conn, config.ack_wait_ms + 1).unwrap();
        let page = fetch_page(
            &conn,
            "s",
            &c1,
            5,
            5,
            1,
            1 << 20,
            &ExecCtx {
                now_ms: config.ack_wait_ms + 1,
                ..ctx
            },
        );
        assert_eq!(seqs(&page.batches[0]), vec![3, 4, 5]);
        let receipt = page.batches[0].receipt;

        assert_eq!(
            upd(&conn, "s", &c1, receipt, 0, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 0, BatchDisposition::Release, &ctx),
            None
        );
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.cursor_pos, 2);

        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        assert_eq!(seqs(&page.batches[0]), vec![3, 4, 5]);
        assert_eq!(
            upd(
                &conn,
                "s",
                &c1,
                page.batches[0].receipt,
                3,
                BatchDisposition::Checkpoint,
                &ctx
            ),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.cursor_pos, 5);
        assert!(fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx)
            .batches
            .is_empty());
    }

    #[test]
    fn keyed_lease_expiry_fences_stale_receipt() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 1_000,
            fetch_response_bytes: 1 << 20,
        };
        let config = test_config();
        create_stream_with_config(&conn, "s", &config).unwrap();
        publish(
            &conn,
            "s",
            &(1..=3u8)
                .map(|i| keyed(b"a", &[i]))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let c2 = new_member(&mut conn, "s", "g", "connB", &ctx);

        let page = fetch_page(&conn, "s", &c1, 3, 3, 1, 1 << 20, &ctx);
        let receipt = page.batches[0].receipt;
        assert_eq!(page.lease_ms, config.ack_wait_ms);

        let ctx2 = ExecCtx { now_ms: 2_000, ..ctx };
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 0, BatchDisposition::Checkpoint, &ctx2),
            None
        );
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Leased as i64);
        assert_eq!(
            lane.deadline_ms,
            Some((2_000 + config.ack_wait_ms) as i64),
            "heartbeat renews the deadline"
        );

        let late = ExecCtx {
            now_ms: 2_000 + config.ack_wait_ms + 1,
            ..ctx
        };
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 0, BatchDisposition::Checkpoint, &late),
            Some(BrokerErrorKind::Fenced)
        );
        expire_leases(&conn, late.now_ms).unwrap();
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Ready as i64);
        assert_eq!(pending(&conn, &c1.epoch).0, 0, "expiry refunds the suffix");

        let page = fetch_page(&conn, "s", &c2, 3, 3, 1, 1 << 20, &late);
        assert_eq!(page.batches.len(), 1);
        assert_ne!(page.batches[0].receipt, receipt);
        assert_eq!(seqs(&page.batches[0]), vec![1, 2, 3]);
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 3, BatchDisposition::Checkpoint, &late),
            Some(BrokerErrorKind::Fenced),
            "reassigned work cannot be settled by the old receipt"
        );
        let active: Vec<u8> = conn
            .query_row(
                "SELECT g.active_epoch FROM groups g JOIN streams s ON s.id=g.stream_id
                 WHERE s.name='s' AND g.name='g'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(active.as_slice(), c1.epoch.as_slice());
    }

    #[test]
    fn keyed_lease_update_requires_owner() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(&conn, "s", &[keyed(b"a", b"1"), keyed(b"a", b"2")], &ctx).unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let c2 = new_member(&mut conn, "s", "g", "connB", &ctx);

        let page = fetch_page(&conn, "s", &c1, 2, 2, 1, 1 << 20, &ctx);
        let receipt = page.batches[0].receipt;
        assert_eq!(
            upd(&conn, "s", &c2, receipt, 0, BatchDisposition::Checkpoint, &ctx),
            Some(BrokerErrorKind::Fenced),
            "another member cannot renew the lease"
        );
        assert_eq!(
            upd(&conn, "s", &c2, receipt, 2, BatchDisposition::Checkpoint, &ctx),
            Some(BrokerErrorKind::Fenced),
            "another member cannot confirm the lease"
        );
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 2, BatchDisposition::Checkpoint, &ctx),
            None
        );
    }

    #[test]
    fn seek_supersedes_active_batch_epoch() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(&conn, "s", &[keyed(b"a", b"1"), keyed(b"a", b"2")], &ctx).unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let page = fetch_page(&conn, "s", &c1, 2, 2, 1, 1 << 20, &ctx);
        let receipt = page.batches[0].receipt;
        assert_eq!(pending(&conn, &c1.epoch).0, 2);

        let seek_effects = {
            let tx = conn.transaction().unwrap();
            let (_r, effects) = seek(&tx, "s", "g", &SeekTarget::Beginning, &ctx).unwrap();
            tx.commit().unwrap();
            effects
        };
        let mut queue: std::collections::VecDeque<Continuation> = seek_effects.followups.into();
        while let Some(c) = queue.pop_front() {
            let (_r, more) = run_continuation(&conn, &c, &ctx).unwrap();
            queue.extend(more.followups);
        }

        let err = update_batches(
            &conn,
            "s",
            "g",
            &c1,
            &[BatchUpdate {
                receipt,
                completed: 2,
                disposition: BatchDisposition::Checkpoint,
            }],
            &ctx,
        )
        .unwrap_err();
        match err {
            CmdError::Expected(e) => assert_eq!(e.kind, BrokerErrorKind::Fenced),
            other => panic!("expected a fenced epoch error, got {other:?}"),
        }
        let c1b = new_member(&mut conn, "s", "g", "connA2", &ctx);
        assert_ne!(c1b.epoch, c1.epoch);
        assert_eq!(pending(&conn, &c1b.epoch).0, 0);
        let page = fetch_page(&conn, "s", &c1b, 2, 2, 1, 1 << 20, &ctx);
        assert_eq!(seqs(&page.batches[0]), vec![1, 2]);
    }

    #[test]
    fn recovery_recovers_committed_prefix_progress() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let identity;
        let epoch_id;
        {
            let mut store = crate::durable::Store::open(
                dir.path(),
                crate::brokers::stream::domain::storage::SPEC,
            )
            .unwrap();
            create_stream_with_config(&store.conn, "s", &test_config()).unwrap();
            publish(
                &store.conn,
                "s",
                &(1..=5u8)
                    .map(|i| keyed(b"a", &[i]))
                    .collect::<Vec<_>>(),
                &ctx,
            )
            .unwrap();
            identity = {
                let (reply, effects) = {
                    let tx = store.conn.transaction().unwrap();
                    let out = join(&tx, "s", "g", "connA", &ctx).unwrap();
                    tx.commit().unwrap();
                    out
                };
                let mut queue: std::collections::VecDeque<Continuation> =
                    effects.followups.into();
                while let Some(c) = queue.pop_front() {
                    let (_r, more) = run_continuation(&store.conn, &c, &ctx).unwrap();
                    queue.extend(more.followups);
                }
                let StreamReply::Join {
                    consumer_id, epoch, ..
                } = reply
                else {
                    panic!("expected a join reply")
                };
                epoch_id = epoch;
                ConsumerIdentity {
                    connection_id: "connA".to_string(),
                    consumer_id,
                    epoch,
                }
            };
            let page = fetch_page(&store.conn, "s", &identity, 5, 5, 1, 1 << 20, &ctx);
            let receipt = page.batches[0].receipt;

            {
                let tx = store.conn.transaction().unwrap();
                assert_eq!(
                    upd(&tx, "s", &identity, receipt, 3, BatchDisposition::Checkpoint, &ctx),
                    None
                );
                tx.rollback().unwrap();
            }
            assert_eq!(lane_of(&store.conn, &epoch_id, b"a").lease_acked, 0);

            assert_eq!(
                upd(&store.conn, "s", &identity, receipt, 2, BatchDisposition::Checkpoint, &ctx),
                None
            );
        }

        let mut store = crate::durable::Store::open(
            dir.path(),
            crate::brokers::stream::domain::storage::SPEC,
        )
        .unwrap();
        let continuations =
            crate::brokers::stream::domain::storage::recover(&mut store).unwrap();
        {
            let tx = store.conn.transaction().unwrap();
            for c in &continuations {
                run_continuation(&tx, c, &ctx).unwrap();
            }
            tx.commit().unwrap();
        }

        let identity = new_member(&mut store.conn, "s", "g", "connA", &ctx);
        assert_eq!(identity.epoch, epoch_id);

        let page = fetch_page(&store.conn, "s", &identity, 5, 5, 1, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 1);
        assert_eq!(seqs(&page.batches[0]), vec![3, 4, 5]);

        let receipt = page.batches[0].receipt;
        assert_eq!(
            upd(&store.conn, "s", &identity, receipt, 3, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(pending(&store.conn, &epoch_id).0, 0);
        assert_eq!(
            upd(&store.conn, "s", &identity, receipt, 3, BatchDisposition::Checkpoint, &ctx),
            Some(BrokerErrorKind::Fenced)
        );
        assert_eq!(pending(&store.conn, &epoch_id).0, 0);
    }

    #[test]
    fn replay_intents_are_singleton_and_preserve_cursor() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let mut config = test_config();
        config.max_deliveries = 1;
        create_stream_with_config(&conn, "s", &config).unwrap();
        publish(
            &conn,
            "s",
            &(1..=5u8)
                .map(|i| keyed(b"a", &[i]))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);

        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        let receipt = page.batches[0].receipt;
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 2, BatchDisposition::Retry, &ctx),
            None
        );
        assert_eq!(lane_of(&conn, &c1.epoch, b"a").state, LaneState::Parked as i64);

        dls_point_op(&conn, "s", "g", 3, true).unwrap();
        dls_point_op(&conn, "s", "g", 4, true).unwrap();
        purge_dls(&conn, "s", "g").unwrap();

        let intents = |conn: &Connection| -> Vec<u64> {
            query_vec(
                conn,
                "SELECT key_pos FROM replay_intents WHERE epoch_id=?1 ORDER BY key_pos",
                [c1.epoch.as_slice()],
                |r| blob8(r, 0, "key_pos"),
                "Cannot list replay intents",
            )
            .unwrap()
        };
        assert_eq!(intents(&conn), vec![3, 4]);
        let tail: u64 = conn
            .query_row(
                "SELECT last_pos FROM stream_keys k JOIN streams s ON s.id=k.stream_id
                 WHERE s.name='s' AND k.key=?1",
                [b"a".as_slice()],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .map(|b| decode_u64(&b).unwrap())
            .unwrap();
        assert_eq!(
            lane_of(&conn, &c1.epoch, b"a").cursor_pos,
            tail,
            "unpark anchors the cursor at the parked tail"
        );

        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 1);
        assert_eq!(seqs(&page.batches[0]), vec![3]);
        assert_eq!(
            upd(&conn, "s", &c1, page.batches[0].receipt, 1, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(
            intents(&conn),
            vec![4],
            "consuming one intent must not delete the other"
        );
        assert_eq!(
            lane_of(&conn, &c1.epoch, b"a").cursor_pos,
            tail,
            "replays never rewind the original cursor"
        );

        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        assert_eq!(seqs(&page.batches[0]), vec![4]);
        assert_eq!(
            upd(&conn, "s", &c1, page.batches[0].receipt, 1, BatchDisposition::Checkpoint, &ctx),
            None
        );

        publish(&conn, "s", &[keyed(b"a", b"6")], &ctx).unwrap();
        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        assert_eq!(seqs(&page.batches[0]), vec![6]);
    }

    #[test]
    fn retention_refunds_unconfirmed_suffix_and_stales_receipt() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let mut config = test_config();
        config.retention.max_age_ms = Some(500);
        create_stream_with_config(&conn, "s", &config).unwrap();
        publish(
            &conn,
            "s",
            &(1..=5u8)
                .map(|i| keyed(b"a", &[i]))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx);
        let receipt = page.batches[0].receipt;
        assert_eq!(pending(&conn, &c1.epoch).0, 5);

        let old = ExecCtx {
            now_ms: 1_000,
            ..ctx
        };
        retention_tick(&conn, &old).unwrap();
        assert_eq!(pending(&conn, &c1.epoch), (0, 0));
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 5, BatchDisposition::Checkpoint, &old),
            Some(BrokerErrorKind::Fenced),
            "retention-invalidated receipt stays fenced"
        );

        publish(&conn, "s", &[keyed(b"a", b"6")], &old).unwrap();
        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &old);
        assert_eq!(seqs(&page.batches[0]), vec![6]);
    }

    #[test]
    fn retention_below_unconfirmed_head_keeps_lease() {
        let mut conn = test_conn();
        let mut config = test_config();
        config.retention.max_age_ms = Some(500);
        let ctx0 = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &config).unwrap();
        publish(
            &conn,
            "s",
            &[keyed(b"a", b"1"), keyed(b"a", b"2")],
            &ctx0,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx0);
        let ctx600 = ExecCtx {
            now_ms: 600,
            ..ctx0
        };
        publish(
            &conn,
            "s",
            &[keyed(b"a", b"3"), keyed(b"a", b"4"), keyed(b"a", b"5")],
            &ctx600,
        )
        .unwrap();

        let page = fetch_page(&conn, "s", &c1, 5, 5, 1, 1 << 20, &ctx600);
        let receipt = page.batches[0].receipt;
        assert_eq!(page.batches[0].messages.len(), 5);
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 2, BatchDisposition::Checkpoint, &ctx600),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 3);

        let ctx1000 = ExecCtx {
            now_ms: 1_000,
            ..ctx0
        };
        retention_tick(&conn, &ctx1000).unwrap();
        let lane = lane_of(&conn, &c1.epoch, b"a");
        assert_eq!(lane.state, LaneState::Leased as i64);
        assert_eq!(
            upd(&conn, "s", &c1, receipt, 5, BatchDisposition::Checkpoint, &ctx1000),
            None,
            "only confirmed positions expired — the lease survives"
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
    }

    #[test]
    fn keyed_fairness_caps_and_mixed_keyless() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let mut config = test_config();
        config.max_ack_pending = 4;
        create_stream_with_config(&conn, "s", &config).unwrap();
        publish(
            &conn,
            "s",
            &(1..=5u8)
                .map(|i| keyed(b"a", &[i]))
                .chain(std::iter::once(keyless(b"z")))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);

        let page = fetch_page(&conn, "s", &c1, 10, 100, 3, 1 << 20, &ctx);
        assert_eq!(pending(&conn, &c1.epoch).0, 3);
        let per_key: Vec<(Bytes, usize)> = page
            .batches
            .iter()
            .map(|b| (b.messages[0].key.clone(), b.messages.len()))
            .collect();
        assert_eq!(per_key[0], (Bytes::new(), 1));
        assert!(per_key.contains(&(Bytes::from_static(b"a"), 2)));
        let a_receipt = page
            .batches
            .iter()
            .find(|b| b.messages[0].key.as_ref() == b"a")
            .unwrap()
            .receipt;

        publish(
            &conn,
            "s",
            &(1..=3u8).map(|i| keyed(b"b", &[0x80 + i])).collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let page = fetch_page(&conn, "s", &c1, 10, 100, 3, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 1, "B must be served while A is held");
        assert!(page.batches[0]
            .messages
            .iter()
            .all(|m| m.key.as_ref() == b"b"));
        assert_eq!(seqs(&page.batches[0]), vec![7]);
        assert_eq!(pending(&conn, &c1.epoch).0, 4);
        assert!(fetch_page(&conn, "s", &c1, 10, 100, 3, 1 << 20, &ctx)
            .batches
            .is_empty());

        assert_eq!(
            upd(&conn, "s", &c1, a_receipt, 2, BatchDisposition::Release, &ctx),
            None
        );
        let page = fetch_page(&conn, "s", &c1, 10, 100, 3, 1 << 20, &ctx);
        let sizes: Vec<usize> = page.batches.iter().map(|b| b.messages.len()).collect();
        assert!(sizes.iter().all(|&n| n <= 2), "per-key grant holds: {sizes:?}");

        let g2 = new_member(&mut conn, "s", "g2", "connC", &ctx);
        let one_batch = FETCH_PAGE_HEADER_BYTES
            + fetch_batch_header_bytes(1)
            + fetch_event_encoded_bytes(1);
        let (reply, _) = fetch(
            &conn,
            "s",
            "g2",
            &g2,
            FetchLimits {
                max_messages: 10,
                max_key_messages: 100,
                max_key_batches: 3,
                max_bytes: one_batch as usize,
            },
            &ctx,
        )
        .unwrap();
        let StreamReply::Fetch(page) = reply else {
            panic!("expected a fetch page")
        };
        let total: u64 = FETCH_PAGE_HEADER_BYTES
            + page
                .batches
                .iter()
                .map(|b| {
                    fetch_batch_header_bytes(b.messages[0].key.len())
                        + b.messages
                            .iter()
                            .map(|m| fetch_event_encoded_bytes(m.payload.len()))
                            .sum::<u64>()
                })
                .sum::<u64>();
        assert!(total <= one_batch, "response never exceeds the budget");
        assert_eq!(page.batches.len(), 1);
        assert_eq!(page.batches[0].messages.len(), 1);
        assert_eq!(page.batches[0].messages[0].payload.as_ref(), b"z");
    }

    #[test]
    fn bulk_update_isolates_keyed_failures() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(
            &conn,
            "s",
            &[
                keyed(b"a", b"1"),
                keyed(b"a", b"2"),
                keyed(b"a", b"3"),
                keyed(b"b", b"4"),
            ],
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let page = fetch_page(&conn, "s", &c1, 4, 3, 2, 1 << 20, &ctx);
        let (a, b) = (
            page.batches[0].receipt,
            page.batches[1].receipt,
        );

        let run = vec![
            BatchUpdate {
                receipt: a,
                completed: 3,
                disposition: BatchDisposition::Checkpoint,
            },
            BatchUpdate {
                receipt: [0xAA; 16],
                completed: 1,
                disposition: BatchDisposition::Checkpoint,
            },
            BatchUpdate {
                receipt: b,
                completed: 9,
                disposition: BatchDisposition::Checkpoint,
            },
        ];
        let (outcomes, _) = update_batches(&conn, "s", "g", &c1, &run, &ctx).unwrap();
        assert_eq!(outcomes[0], None);
        assert_eq!(outcomes[1], Some(BrokerErrorKind::Fenced));
        assert_eq!(outcomes[2], Some(BrokerErrorKind::InvalidArgument));
        assert_eq!(pending(&conn, &c1.epoch).0, 1, "only A's completion applied");

        assert_eq!(
            upd(&conn, "s", &c1, b, 1, BatchDisposition::Checkpoint, &ctx),
            None
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
    }

    #[test]
    fn keyless_byte_budget_never_skips_unassigned_event() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(
            &conn,
            "s",
            &[keyless(b"a"), keyless(&[b'b'; 100]), keyless(b"c")],
            &ctx,
        )
        .unwrap();
        let c = new_member(&mut conn, "s", "g", "conn", &ctx);

        let first = fetch_page(&conn, "s", &c, 3, 1, 0, 100, &ctx);
        let first_seqs: Vec<u64> = first.batches.iter().flat_map(seqs).collect();
        assert_eq!(first_seqs, vec![1]);
        let second = fetch_page(&conn, "s", &c, 3, 1, 0, 512, &ctx);
        let second_seqs: Vec<u64> = second.batches.iter().flat_map(seqs).collect();
        assert_eq!(second_seqs, vec![2, 3]);
    }

    #[test]
    fn fetch_bulk_payloads_preserve_many_keys_and_keyless() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        let keys: Vec<Vec<u8>> = (0..32u8)
            .map(|i| vec![0xC0, i, 0x5A, 0x7F])
            .collect();
        let mut items: Vec<PubItem> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| keyed(k, format!("payload-{i}").as_bytes()))
            .collect();
        items.push(keyless(b"free-a"));
        items.push(keyless(b"free-b"));
        publish(&conn, "s", &items, &ctx).unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);

        let page = fetch_page(&conn, "s", &c1, 64, 32, 32, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 34);
        let mut seen: std::collections::HashMap<u64, (Vec<u8>, Vec<u8>)> =
            std::collections::HashMap::new();
        for batch in &page.batches {
            assert_eq!(batch.messages.len(), 1);
            let m = &batch.messages[0];
            assert!(seen
                .insert(m.seq, (m.key.to_vec(), m.payload.to_vec()))
                .is_none());
        }
        for (i, key) in keys.iter().enumerate() {
            let (k, p) = seen.get(&(i as u64 + 1)).expect("keyed event");
            assert_eq!(k, key);
            assert_eq!(p, format!("payload-{i}").as_bytes());
        }
        let (k, p) = seen.get(&33).expect("keyless event");
        assert!(k.is_empty());
        assert_eq!(p, b"free-a");
        let (k, p) = seen.get(&34).expect("keyless event");
        assert!(k.is_empty());
        assert_eq!(p, b"free-b");

        let updates: Vec<BatchUpdate> = page
            .batches
            .iter()
            .map(|b| BatchUpdate {
                receipt: b.receipt,
                completed: 1,
                disposition: BatchDisposition::Checkpoint,
            })
            .collect();
        let (outcomes, _) =
            update_batches(&conn, "s", "g", &c1, &updates, &ctx).unwrap();
        assert!(outcomes.iter().all(|o| o.is_none()));
        assert_eq!(pending(&conn, &c1.epoch).0, 0);
        let stream = require_stream(&conn, "s").unwrap();
        let epoch = load_epoch(&conn, &c1.epoch).unwrap().unwrap();
        assert_eq!(ack_floor(&conn, &epoch, &stream).unwrap(), 34);
    }

    #[test]
    fn bulk_update_revisits_same_receipt() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(
            &conn,
            "s",
            &(1..=5u8)
                .map(|i| keyed(b"a", &[i]))
                .chain(std::iter::once(keyless(b"z")))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let page = fetch_page(&conn, "s", &c1, 6, 5, 1, 1 << 20, &ctx);
        let keyless_receipt = page
            .batches
            .iter()
            .find(|b| b.messages[0].key.is_empty())
            .unwrap()
            .receipt;
        let a_receipt = page
            .batches
            .iter()
            .find(|b| b.messages[0].key.as_ref() == b"a")
            .unwrap()
            .receipt;

        let run = [2, 2, 5].map(|completed| BatchUpdate {
            receipt: a_receipt,
            completed,
            disposition: BatchDisposition::Checkpoint,
        });
        let (outcomes, _) = update_batches(&conn, "s", "g", &c1, &run, &ctx).unwrap();
        assert_eq!(outcomes, vec![None, None, None]);
        assert_eq!(lane_of(&conn, &c1.epoch, b"a").cursor_pos, 5);
        assert_eq!(pending(&conn, &c1.epoch).0, 1, "only keyless z remains");

        let run = [
            BatchUpdate {
                receipt: keyless_receipt,
                completed: 0,
                disposition: BatchDisposition::Checkpoint,
            },
            BatchUpdate {
                receipt: keyless_receipt,
                completed: 1,
                disposition: BatchDisposition::Checkpoint,
            },
            BatchUpdate {
                receipt: keyless_receipt,
                completed: 1,
                disposition: BatchDisposition::Checkpoint,
            },
        ];
        let (outcomes, _) = update_batches(&conn, "s", "g", &c1, &run, &ctx).unwrap();
        assert_eq!(
            outcomes,
            vec![None, None, Some(BrokerErrorKind::Fenced)],
            "heartbeat renews, first completion consumes, the duplicate fences"
        );
        assert_eq!(pending(&conn, &c1.epoch).0, 0);

        publish(&conn, "s", &[keyed(b"a", b"6"), keyed(b"a", b"7")], &ctx).unwrap();
        let page = fetch_page(&conn, "s", &c1, 2, 2, 1, 1 << 20, &ctx);
        let a_receipt = page.batches[0].receipt;
        let run = [
            BatchUpdate {
                receipt: a_receipt,
                completed: 9,
                disposition: BatchDisposition::Checkpoint,
            },
            BatchUpdate {
                receipt: a_receipt,
                completed: 2,
                disposition: BatchDisposition::Checkpoint,
            },
        ];
        let (outcomes, _) = update_batches(&conn, "s", "g", &c1, &run, &ctx).unwrap();
        assert_eq!(
            outcomes,
            vec![Some(BrokerErrorKind::InvalidArgument), None]
        );
        assert_eq!(lane_of(&conn, &c1.epoch, b"a").cursor_pos, 7);
    }

    #[test]
    fn bulk_update_unknown_receipts_fence_per_item() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);

        let updates: Vec<BatchUpdate> = (0u32..5000)
            .map(|i| {
                let mut receipt = [0u8; 16];
                receipt[..4].copy_from_slice(&i.to_be_bytes());
                BatchUpdate {
                    receipt,
                    completed: 1,
                    disposition: BatchDisposition::Checkpoint,
                }
            })
            .collect();
        let (outcomes, _) = update_batches(&conn, "s", "g", &c1, &updates, &ctx).unwrap();
        assert_eq!(outcomes.len(), 5000);
        assert!(outcomes
            .iter()
            .all(|o| *o == Some(BrokerErrorKind::Fenced)));
    }

    #[test]
    fn bulk_keyless_completions_delete_in_one_run() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        create_stream_with_config(&conn, "s", &test_config()).unwrap();
        publish(
            &conn,
            "s",
            &(0u8..100).map(|i| keyless(&[i])).collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let page = fetch_page(&conn, "s", &c1, 100, 1, 0, 1 << 20, &ctx);
        assert_eq!(page.batches.len(), 100);
        let updates: Vec<BatchUpdate> = page
            .batches
            .iter()
            .map(|b| BatchUpdate {
                receipt: b.receipt,
                completed: 1,
                disposition: BatchDisposition::Checkpoint,
            })
            .collect();
        let (outcomes, _) = update_batches(&conn, "s", "g", &c1, &updates, &ctx).unwrap();
        assert_eq!(outcomes.len(), 100);
        assert!(outcomes.iter().all(|o| o.is_none()));
        assert_eq!(pending(&conn, &c1.epoch), (0, 0));
        let left: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM keyless_deliveries WHERE epoch_id=?1",
                [c1.epoch.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
    }

    #[test]
    fn recover_keeps_waiting_leases_until_their_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let config = test_config();
        {
            let mut store = crate::durable::Store::open(
                dir.path(),
                crate::brokers::stream::domain::storage::SPEC,
            )
            .unwrap();
            create_stream_with_config(&store.conn, "s", &config).unwrap();
            publish(
                &store.conn,
                "s",
                &[keyed(b"a", b"1"), keyed(b"a", b"2"), keyless(b"z")],
                &ctx,
            )
            .unwrap();
            let c = new_member(&mut store.conn, "s", "g", "connA", &ctx);
            let page = fetch_page(&store.conn, "s", &c, 3, 3, 2, 1 << 20, &ctx);
            assert_eq!(page.batches.len(), 2);
            for batch in &page.batches {
                assert_eq!(
                    upd(
                        &store.conn,
                        "s",
                        &c,
                        batch.receipt,
                        0,
                        BatchDisposition::Retry,
                        &ctx
                    ),
                    None
                );
            }
            let lane = lane_of(&store.conn, &c.epoch, b"a");
            assert_eq!(lane.state, LaneState::Waiting as i64);
            assert_eq!(lane.attempts, 1);
        }

        let mut store = crate::durable::Store::open(
            dir.path(),
            crate::brokers::stream::domain::storage::SPEC,
        )
        .unwrap();
        let continuations =
            crate::brokers::stream::domain::storage::recover(&mut store).unwrap();
        {
            let tx = store.conn.transaction().unwrap();
            for c in &continuations {
                run_continuation(&tx, c, &ctx).unwrap();
            }
            tx.commit().unwrap();
        }
        let c = new_member(&mut store.conn, "s", "g", "connA", &ctx);

        let early = ExecCtx {
            now_ms: config.ack_wait_ms - 1,
            ..ctx
        };
        expire_leases(&store.conn, config.ack_wait_ms - 1).unwrap();
        assert!(fetch_page(&store.conn, "s", &c, 3, 3, 2, 1 << 20, &early)
            .batches
            .is_empty());
        let lane = lane_of(&store.conn, &c.epoch, b"a");
        assert_eq!(lane.state, LaneState::Waiting as i64);
        assert_eq!(lane.attempts, 1);

        let late = ExecCtx {
            now_ms: config.ack_wait_ms,
            ..ctx
        };
        expire_leases(&store.conn, config.ack_wait_ms).unwrap();
        let page = fetch_page(&store.conn, "s", &c, 3, 3, 2, 1 << 20, &late);
        assert_eq!(page.batches.len(), 2);
        let mut keyed_seqs: Vec<u64> = Vec::new();
        let mut keyless_seqs: Vec<u64> = Vec::new();
        for b in &page.batches {
            if b.messages[0].key.is_empty() {
                keyless_seqs.extend(seqs(b));
            } else {
                keyed_seqs.extend(seqs(b));
            }
        }
        assert_eq!(keyed_seqs, vec![1, 2]);
        assert_eq!(keyless_seqs, vec![3]);
        assert_eq!(lane_of(&store.conn, &c.epoch, b"a").attempts, 2);
    }

    #[test]
    fn prune_keys_reclaims_retained_out_metadata() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let mut config = test_config();
        config.retention.max_age_ms = Some(500);
        create_stream_with_config(&conn, "s", &config).unwrap();
        let keys: Vec<Vec<u8>> = (0u8..8)
            .map(|i| format!("k{i}").into_bytes())
            .collect();
        publish(
            &conn,
            "s",
            &keys
                .iter()
                .enumerate()
                .map(|(i, k)| keyed(k, &[i as u8]))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let c2 = new_member(&mut conn, "s", "g2", "connB", &ctx);
        let page = fetch_page(&conn, "s", &c1, 10, 10, 8, 1 << 20, &ctx);
        let stale_receipt = page.batches[0].receipt;

        let old = ExecCtx {
            now_ms: 1_000,
            ..ctx
        };
        let mut queue: std::collections::VecDeque<Continuation> = {
            let tx = conn.transaction().unwrap();
            let (_r, fx) = retention_tick(&tx, &old).unwrap();
            tx.commit().unwrap();
            fx.followups.into()
        };
        let mut slices = 0;
        while let Some(c) = queue.pop_front() {
            slices += 1;
            assert!(slices < 64, "prune queue must terminate");
            let tx = conn.transaction().unwrap();
            let (_r, more) = run_continuation(&tx, &c, &old).unwrap();
            tx.commit().unwrap();
            queue.extend(more.followups);
        }

        let count = |sql_text: &str| -> i64 {
            conn.query_row(sql_text, [], |r| r.get(0)).unwrap()
        };
        assert_eq!(count("SELECT COUNT(*) FROM events"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM key_lanes"), 0);
        assert_eq!(
            count("SELECT COUNT(*) FROM stream_keys WHERE key_id > 0"),
            0,
            "retired key metadata must be reclaimed"
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM stream_keys WHERE key_id = 0"),
            1,
            "keyless sentinel is never reclaimed"
        );
        assert_eq!(count("SELECT COUNT(*) FROM groups"), 2);
        assert_eq!(count("SELECT COUNT(*) FROM group_epochs"), 2);
        assert_eq!(
            upd(&conn, "s", &c1, stale_receipt, 1, BatchDisposition::Checkpoint, &old),
            Some(BrokerErrorKind::Fenced),
            "old receipt stays fenced after reclamation"
        );

        publish(&conn, "s", &[keyed(b"k0", b"9"), keyed(b"new", b"10")], &old)
            .unwrap();
        let new_key_id: i64 = conn
            .query_row(
                "SELECT key_id FROM stream_keys WHERE key = ?1",
                [b"k0".as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert!(new_key_id > 8, "republished key gets a fresh key_id");
        let new_seq: Vec<u8> = conn
            .query_row(
                "SELECT seq FROM events WHERE key_id = ?1",
                [new_key_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            new_seq.as_slice() > encode_u64(8).as_slice(),
            "global sequence stays monotonic"
        );

        let page1 = fetch_page(&conn, "s", &c1, 10, 10, 8, 1 << 20, &old);
        assert_eq!(page1.batches.len(), 2);
        let (reply2, _) = fetch(
            &conn,
            "s",
            "g2",
            &c2,
            FetchLimits {
                max_messages: 10,
                max_key_messages: 10,
                max_key_batches: 8,
                max_bytes: 1 << 20,
            },
            &old,
        )
        .unwrap();
        let StreamReply::Fetch(page2) = reply2 else {
            panic!("expected a fetch page")
        };
        assert_eq!(page2.batches.len(), 2);
    }

    #[test]
    fn prune_keys_is_bounded_and_resumes_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        {
            let mut store = crate::durable::Store::open(
                dir.path(),
                crate::brokers::stream::domain::storage::SPEC,
            )
            .unwrap();
            {
                let tx = store.conn.transaction().unwrap();
                create_stream_with_config(&tx, "s", &test_config()).unwrap();
                let stream_id = require_stream(&tx, "s").unwrap().id;
                exec(
                    &tx,
                    "INSERT INTO stream_keys(stream_id,key_id,key,last_pos,retained_after_pos)
                     VALUES (?1,1,'k',?2,?2)",
                    params![stream_id.as_slice(), encode_u64(1).as_slice()],
                )
                .unwrap();
                for i in 0..(BATCH_LIMIT + 76) as i64 {
                    let mut group_id = [0u8; 16];
                    group_id[8..].copy_from_slice(&i.to_be_bytes());
                    let mut epoch_id = [1u8; 16];
                    epoch_id[8..].copy_from_slice(&i.to_be_bytes());
                    exec(
                        &tx,
                        "INSERT INTO groups(id,stream_id,name,active_epoch)
                         VALUES (?1,?2,?3,?4)",
                        params![
                            group_id.as_slice(),
                            stream_id.as_slice(),
                            format!("g{i}"),
                            epoch_id.as_slice()
                        ],
                    )
                    .unwrap();
                    exec(
                        &tx,
                        "INSERT INTO group_epochs(id,group_id,stream_id,start_after_seq,
                            initialized,init_key_cursor,init_key_target,
                            keyless_cursor_pos,keyless_ticket,next_ready_ticket,
                            pending_count,max_pending)
                         VALUES (?1,?2,?3,?4,1,0,0,?4,0,1,0,100)",
                        params![
                            epoch_id.as_slice(),
                            group_id.as_slice(),
                            stream_id.as_slice(),
                            encode_u64(0).as_slice()
                        ],
                    )
                    .unwrap();
                    exec(
                        &tx,
                        "INSERT INTO key_lanes(epoch_id,stream_id,key_id,cursor_pos,
                            state,attempts,ready_ticket)
                         VALUES (?1,?2,1,?3,0,0,0)",
                        params![
                            epoch_id.as_slice(),
                            stream_id.as_slice(),
                            encode_u64(1).as_slice()
                        ],
                    )
                    .unwrap();
                }
                tx.commit().unwrap();
            }

            let tx = store.conn.transaction().unwrap();
            let (_r, fx) = prune_keys(&tx, None).unwrap();
            tx.commit().unwrap();
            let lanes_left: i64 = store
                .conn
                .query_row("SELECT COUNT(*) FROM key_lanes", [], |r| r.get(0))
                .unwrap();
            assert!(lanes_left > 0, "slice must leave lanes beyond the budget");
            assert_eq!(
                lanes_left,
                (BATCH_LIMIT + 76 - (BATCH_LIMIT - 1)) as i64,
                "one candidate check plus 1023 lane deletions exhausts the 1024 budget"
            );
            let key_row: i64 = store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM stream_keys WHERE key_id = 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(key_row, 1, "partial lane cleanup keeps the key row");
            assert!(
                fx.followups
                    .iter()
                    .any(|c| matches!(c, Continuation::PruneKeys { .. })),
                "budget exhaustion must reschedule the prune"
            );
        }

        let mut store = crate::durable::Store::open(
            dir.path(),
            crate::brokers::stream::domain::storage::SPEC,
        )
        .unwrap();
        let continuations =
            crate::brokers::stream::domain::storage::recover(&mut store).unwrap();
        assert!(
            continuations
                .iter()
                .any(|c| matches!(c, Continuation::PruneKeys { after: None })),
            "recovery must schedule key pruning for retired metadata"
        );

        let mut queue: std::collections::VecDeque<Continuation> =
            continuations.into();
        let mut slices = 0;
        while let Some(c) = queue.pop_front() {
            slices += 1;
            assert!(slices < 64, "prune queue must terminate");
            let tx = store.conn.transaction().unwrap();
            let (_r, more) = run_continuation(&tx, &c, &ctx).unwrap();
            tx.commit().unwrap();
            queue.extend(more.followups);
        }
        let lanes: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM key_lanes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(lanes, 0);
        let retired: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM stream_keys
                 WHERE key_id > 0 AND last_pos = retained_after_pos",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(retired, 0);
        let keyless: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM stream_keys WHERE key_id = 0",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(keyless, 1, "keyless sentinel is never reclaimed");
    }

    #[test]
    fn prune_keys_revived_after_partial_lane_cleanup() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let mut queue: std::collections::VecDeque<Continuation>;
        {
            let tx = conn.transaction().unwrap();
            create_stream_with_config(&tx, "s", &test_config()).unwrap();
            let stream_id = require_stream(&tx, "s").unwrap().id;
            exec(
                &tx,
                "UPDATE streams SET last_key_id=1, last_seq=?2, retained_after_seq=?2
                 WHERE id=?1",
                params![stream_id.as_slice(), encode_u64(1).as_slice()],
            )
            .unwrap();
            exec(
                &tx,
                "INSERT INTO stream_keys(stream_id,key_id,key,last_pos,retained_after_pos)
                 VALUES (?1,1,?2,?3,?3)",
                params![
                    stream_id.as_slice(),
                    b"k".as_slice(),
                    encode_u64(1).as_slice()
                ],
            )
            .unwrap();
            for i in 0..(BATCH_LIMIT + 76) as i64 {
                let mut group_id = [0u8; 16];
                group_id[8..].copy_from_slice(&i.to_be_bytes());
                let mut epoch_id = [1u8; 16];
                epoch_id[8..].copy_from_slice(&i.to_be_bytes());
                exec(
                    &tx,
                    "INSERT INTO groups(id,stream_id,name,active_epoch)
                     VALUES (?1,?2,?3,?4)",
                    params![
                        group_id.as_slice(),
                        stream_id.as_slice(),
                        format!("g{i}"),
                        epoch_id.as_slice()
                    ],
                )
                .unwrap();
                exec(
                    &tx,
                    "INSERT INTO group_epochs(id,group_id,stream_id,start_after_seq,
                        initialized,init_key_cursor,init_key_target,
                        keyless_cursor_pos,keyless_ticket,next_ready_ticket,
                        pending_count,max_pending)
                     VALUES (?1,?2,?3,?4,1,0,0,?4,0,1,0,100)",
                    params![
                        epoch_id.as_slice(),
                        group_id.as_slice(),
                        stream_id.as_slice(),
                        encode_u64(0).as_slice()
                    ],
                )
                .unwrap();
                exec(
                    &tx,
                    "INSERT INTO key_lanes(epoch_id,stream_id,key_id,cursor_pos,
                        state,attempts,ready_ticket)
                     VALUES (?1,?2,1,?3,0,0,0)",
                    params![
                        epoch_id.as_slice(),
                        stream_id.as_slice(),
                        encode_u64(1).as_slice()
                    ],
                )
                .unwrap();
            }
            let (_r, fx) = prune_keys(&tx, None).unwrap();
            tx.commit().unwrap();
            queue = fx.followups.into();
        }
        let stream_id = require_stream(&conn, "s").unwrap().id;
        let lanes_left: i64 = conn
            .query_row("SELECT COUNT(*) FROM key_lanes", [], |r| r.get(0))
            .unwrap();
        assert!(
            lanes_left > 0,
            "first slice must leave some empty lanes behind"
        );
        let laneless: String = conn
            .query_row(
                "SELECT g.name FROM groups g
                 JOIN group_epochs e ON e.group_id=g.id AND e.stream_id=g.stream_id
                 WHERE g.stream_id=?1
                   AND NOT EXISTS(SELECT 1 FROM key_lanes l WHERE l.epoch_id=e.id)
                 ORDER BY g.name LIMIT 1",
                [stream_id.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        let kept: String = conn
            .query_row(
                "SELECT g.name FROM groups g
                 JOIN group_epochs e ON e.group_id=g.id AND e.stream_id=g.stream_id
                 WHERE g.stream_id=?1
                   AND EXISTS(SELECT 1 FROM key_lanes l WHERE l.epoch_id=e.id)
                 ORDER BY g.name LIMIT 1",
                [stream_id.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        {
            let tx = conn.transaction().unwrap();
            publish(&tx, "s", &[keyed(b"k", b"revived")], &ctx).unwrap();
            tx.commit().unwrap();
        }
        for group in [&laneless, &kept] {
            let identity = new_member(&mut conn, "s", group, "connX", &ctx);
            let (reply, _) = fetch(
                &conn,
                "s",
                group,
                &identity,
                FetchLimits {
                    max_messages: 10,
                    max_key_messages: 10,
                    max_key_batches: 4,
                    max_bytes: 1 << 20,
                },
                &ctx,
            )
            .unwrap();
            let StreamReply::Fetch(page) = reply else {
                panic!("expected a fetch page")
            };
            assert_eq!(page.batches.len(), 1);
            assert_eq!(
                page.batches[0]
                    .messages
                    .iter()
                    .map(|m| m.seq)
                    .collect::<Vec<_>>(),
                vec![2]
            );
            let (outcomes, _) = update_batches(
                &conn,
                "s",
                group,
                &identity,
                &[BatchUpdate {
                    receipt: page.batches[0].receipt,
                    completed: 1,
                    disposition: BatchDisposition::Checkpoint,
                }],
                &ctx,
            )
            .unwrap();
            assert_eq!(outcomes, vec![None], "new receipt must ack the lease");
            let (outcomes, _) = update_batches(
                &conn,
                "s",
                group,
                &identity,
                &[BatchUpdate {
                    receipt: [0xAA; 16],
                    completed: 1,
                    disposition: BatchDisposition::Checkpoint,
                }],
                &ctx,
            )
            .unwrap();
            assert_eq!(
                outcomes,
                vec![Some(crate::brokers::BrokerErrorKind::Fenced)],
                "no old receipt validates the new lease"
            );
        }
        let mut slices = 0;
        while let Some(c) = queue.pop_front() {
            slices += 1;
            assert!(slices < 64, "prune queue must terminate");
            let tx = conn.transaction().unwrap();
            let (_r, more) = run_continuation(&tx, &c, &ctx).unwrap();
            tx.commit().unwrap();
            queue.extend(more.followups);
        }
        let key_row: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM stream_keys WHERE stream_id=?1 AND key_id=1",
                [stream_id.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(key_row, 1, "revived key must not be pruned");
        let events_left: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE stream_id=?1 AND seq=?2",
                params![stream_id.as_slice(), encode_u64(2).as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(events_left, 1, "revived event must survive pruning");
    }

    #[test]
    fn prune_keys_skips_parked_keys_without_starvation() {
        let mut conn = test_conn();
        let stream_id = [7u8; 16];
        let group_id = [8u8; 16];
        let epoch_id = [9u8; 16];
        exec(
            &conn,
            "INSERT INTO streams VALUES (?1,'s',0,?3,?2,?2,0,0)",
            params![
                stream_id.as_slice(),
                encode_u64(0).as_slice(),
                r#"{"retention":{"max_age_ms":null,"max_bytes":null},"max_ack_pending":1000,"max_ack_pending_bytes":67108864,"ack_wait_ms":30000,"max_deliveries":3}"#
            ],
        )
        .unwrap();
        exec(
            &conn,
            "INSERT INTO stream_keys(stream_id,key_id,key,last_pos,retained_after_pos)
             VALUES (?1,0,'',?2,?2)",
            params![stream_id.as_slice(), encode_u64(0).as_slice()],
        )
        .unwrap();
        {
            let tx = conn.transaction().unwrap();
            exec(
                &tx,
                "INSERT INTO groups(id,stream_id,name,active_epoch)
                 VALUES (?1,?2,'g',?3)",
                params![
                    group_id.as_slice(),
                    stream_id.as_slice(),
                    epoch_id.as_slice()
                ],
            )
            .unwrap();
            exec(
                &tx,
                "INSERT INTO group_epochs(id,group_id,stream_id,start_after_seq,
                    initialized,init_key_cursor,init_key_target,
                    keyless_cursor_pos,keyless_ticket,next_ready_ticket,
                    pending_count,max_pending)
                 VALUES (?1,?2,?3,?4,1,0,0,?4,0,1,0,100)",
                params![
                    epoch_id.as_slice(),
                    group_id.as_slice(),
                    stream_id.as_slice(),
                    encode_u64(0).as_slice()
                ],
            )
            .unwrap();
            exec(
                &tx,
                "INSERT INTO events(stream_id,seq,key_id,key_pos,timestamp_ms,
                    payload,payload_bytes,logical_bytes)
                 VALUES (?1,?2,0,?2,1,X'78',1,1)",
                params![stream_id.as_slice(), encode_u64(1).as_slice()],
            )
            .unwrap();
            for key_id in 1..=70i64 {
                exec(
                    &tx,
                    "INSERT INTO stream_keys(stream_id,key_id,key,last_pos,retained_after_pos)
                     VALUES (?1,?2,?3,?4,?4)",
                    params![
                        stream_id.as_slice(),
                        key_id,
                        format!("k{key_id}").as_bytes(),
                        encode_u64(1).as_slice()
                    ],
                )
                .unwrap();
                match key_id {
                    1 => exec(
                        &tx,
                        "INSERT INTO key_lanes(epoch_id,stream_id,key_id,cursor_pos,
                            state,attempts,ready_ticket)
                         VALUES (?1,?2,?3,?4,3,0,0)",
                        params![
                            epoch_id.as_slice(),
                            stream_id.as_slice(),
                            key_id,
                            encode_u64(1).as_slice()
                        ],
                    ),
                    3 => exec(
                        &tx,
                        "INSERT INTO key_lanes(epoch_id,stream_id,key_id,cursor_pos,
                            state,normal_seq,head_seq,head_pos,head_origin,receipt,
                            owner,connection_id,deadline_ms,lease_first_pos,
                            lease_count,lease_acked,lease_bytes,attempts,ready_ticket)
                         VALUES (?1,?2,?3,?4,2,?5,?5,?5,1,?6,'owner','conn1',
                            1000,?5,2,0,100,0,0)",
                        params![
                            epoch_id.as_slice(),
                            stream_id.as_slice(),
                            key_id,
                            encode_u64(1).as_slice(),
                            encode_u64(1).as_slice(),
                            [9u8; 16].as_slice()
                        ],
                    ),
                    _ => exec(
                        &tx,
                        "INSERT INTO key_lanes(epoch_id,stream_id,key_id,cursor_pos,
                            state,attempts,ready_ticket)
                         VALUES (?1,?2,?3,?4,0,0,0)",
                        params![
                            epoch_id.as_slice(),
                            stream_id.as_slice(),
                            key_id,
                            encode_u64(1).as_slice()
                        ],
                    ),
                }
                .unwrap();
            }
            exec(
                &tx,
                "INSERT INTO dls_ranges(epoch_id,stream_id,key_id,first_pos,
                    reason,attempts)
                 VALUES (?1,?2,1,?3,0,0)",
                params![
                    epoch_id.as_slice(),
                    stream_id.as_slice(),
                    encode_u64(1).as_slice()
                ],
            )
            .unwrap();
            exec(
                &tx,
                "INSERT INTO replay_intents(epoch_id,stream_id,key_id,key_pos,seq)
                 VALUES (?1,?2,2,?3,?3)",
                params![
                    epoch_id.as_slice(),
                    stream_id.as_slice(),
                    encode_u64(1).as_slice()
                ],
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let mut queue: std::collections::VecDeque<Continuation> =
            [Continuation::PruneKeys { after: None }].into();
        let mut slices = 0;
        while let Some(c) = queue.pop_front() {
            slices += 1;
            assert!(slices < 64, "prune queue must terminate");
            let tx = conn.transaction().unwrap();
            let (_r, more) = run_continuation(&tx, &c, &ctx).unwrap();
            tx.commit().unwrap();
            queue.extend(more.followups);
        }

        let keys_left: Vec<i64> = conn
            .prepare("SELECT key_id FROM stream_keys WHERE key_id > 0 ORDER BY key_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(keys_left, vec![1, 2, 3]);
        let lanes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM key_lanes WHERE key_id IN (1, 3)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(lanes, 2, "parked and leased lanes must remain");
        let dls: i64 = conn
            .query_row("SELECT COUNT(*) FROM dls_ranges", [], |r| r.get(0))
            .unwrap();
        assert_eq!(dls, 1);
        let intents: i64 = conn
            .query_row("SELECT COUNT(*) FROM replay_intents", [], |r| r.get(0))
            .unwrap();
        assert_eq!(intents, 1);
    }

    #[test]
    fn publish_between_prune_slices_preserves_live_key() {
        let mut conn = test_conn();
        let ctx = ExecCtx {
            now_ms: 0,
            fetch_response_bytes: 1 << 20,
        };
        let mut config = test_config();
        config.retention.max_age_ms = Some(500);
        create_stream_with_config(&conn, "s", &config).unwrap();
        let keys: Vec<Vec<u8>> = (0u8..70)
            .map(|i| format!("k{i}").into_bytes())
            .collect();
        publish(
            &conn,
            "s",
            &keys
                .iter()
                .enumerate()
                .map(|(i, k)| keyed(k, &[i as u8]))
                .collect::<Vec<_>>(),
            &ctx,
        )
        .unwrap();
        let c1 = new_member(&mut conn, "s", "g", "connA", &ctx);
        let c2 = new_member(&mut conn, "s", "g2", "connB", &ctx);

        let old = ExecCtx {
            now_ms: 1_000,
            ..ctx
        };
        let mut queue: std::collections::VecDeque<Continuation> = {
            let tx = conn.transaction().unwrap();
            let (_r, fx) = retention_tick(&tx, &old).unwrap();
            tx.commit().unwrap();
            fx.followups.into()
        };

        let c = queue.pop_front().unwrap();
        {
            let tx = conn.transaction().unwrap();
            let (_r, more) = run_continuation(&tx, &c, &old).unwrap();
            tx.commit().unwrap();
            queue.extend(more.followups);
        }
        {
            let tx = conn.transaction().unwrap();
            publish(&tx, "s", &[keyed(b"k69", b"late"), keyed(b"fresh", b"new")], &old)
                .unwrap();
            tx.commit().unwrap();
        }

        let mut slices = 0;
        while let Some(c) = queue.pop_front() {
            slices += 1;
            assert!(slices < 64, "prune queue must terminate");
            let tx = conn.transaction().unwrap();
            let (_r, more) = run_continuation(&tx, &c, &old).unwrap();
            tx.commit().unwrap();
            queue.extend(more.followups);
        }

        let keys_left: Vec<i64> = conn
            .prepare("SELECT key_id FROM stream_keys WHERE key_id > 0 ORDER BY key_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(keys_left.len(), 2, "live keys keep their metadata");
        let k69: i64 = conn
            .query_row(
                "SELECT key_id FROM stream_keys WHERE key = ?1",
                [b"k69".as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(k69, 70, "republished before pruning keeps its key_id");
        let fresh: i64 = conn
            .query_row(
                "SELECT key_id FROM stream_keys WHERE key = ?1",
                [b"fresh".as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(fresh, 71);

        let page1 = fetch_page(&conn, "s", &c1, 10, 10, 8, 1 << 20, &old);
        assert_eq!(page1.batches.len(), 2);
        let (reply2, _) = fetch(
            &conn,
            "s",
            "g2",
            &c2,
            FetchLimits {
                max_messages: 10,
                max_key_messages: 10,
                max_key_batches: 8,
                max_bytes: 1 << 20,
            },
            &old,
        )
        .unwrap();
        let StreamReply::Fetch(page2) = reply2 else {
            panic!("expected a fetch page")
        };
        assert_eq!(page2.batches.len(), 2);
    }
}

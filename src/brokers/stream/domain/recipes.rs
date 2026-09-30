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
use crate::brokers::stream::domain::message::{Delivery, DlsEntry, Message};
use crate::brokers::stream::domain::ops::{Continuation, Effects, StreamReply, StreamRequest};
use crate::brokers::stream::domain::types::{
    decode_u64, encode_u64, event_logical_bytes, fetch_item_encoded_bytes, DlsReason, LaneState,
    Origin,
};
use crate::brokers::stream::options::SeekTarget;
use crate::brokers::{config_conflict_error, BrokerError, ProvisionOutcome, ProvisionResult};
use crate::durable::{exec, expected, fatal, query_one, query_vec, sql, CmdError, R};
use bytes::Bytes;
use uuid::Uuid;

/// Rows processed per continuation slice / maintenance batch.
pub const BATCH_LIMIT: usize = 1024;
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
    pub max_pending: i64,
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
    generation: u64,
    active_epoch: Option<[u8; 16]>,
}

fn load_group(conn: &Connection, stream_id: &[u8; 16], name: &str) -> R<Option<GroupRow>> {
    query_one(
        conn,
        "SELECT id, generation, active_epoch FROM groups WHERE stream_id = ?1 AND name = ?2",
        params![stream_id.as_slice(), name],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                blob8(r, 1, "generation")?,
                r.get::<_, Option<Vec<u8>>>(2)?,
            ))
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot load group", e))?
    .map(|(id, generation, epoch)| {
        Ok(GroupRow {
            id: blob16(&id, "group id")?,
            generation,
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
                pending_count, max_pending
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
            max_pending,
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
                max_pending,
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
                keyless_cursor_pos=?5, keyless_ticket=?6, next_ready_ticket=?7, pending_count=?8
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
/// the EMPTY/READY state. PARKED and LEASED lanes must not call this.
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
    // cursor (ack) mutate the copy first, fusing the advance into this write.
    sql(
        exec(
            conn,
            "UPDATE key_lanes SET cursor_pos=?3, normal_seq=?4, head_seq=?5, head_pos=?6,
                head_origin=?7, state=?8, attempts=?9, ready_ticket=?10, receipt=NULL,
                owner=NULL, connection_id=NULL, deadline_ms=NULL
             WHERE epoch_id=?1 AND key_id=?2",
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
    }))
}

/// Lane row mapping shared by every `key_lanes` select: the nine columns in
/// canonical order starting at `base`, so point lookups and IN-scans map the
/// same shape.
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
    })
}

/// Lane columns in `lane_row` order — every `key_lanes` select uses this
/// exact list so the mapping stays single-sourced by convention.
/// key_id, cursor_pos, normal_seq, head_seq, head_pos, head_origin, state,
/// attempts, receipt
fn load_lane(conn: &Connection, epoch_id: &[u8; 16], key_id: i64) -> R<Option<LaneRow>> {
    query_one(
        conn,
        "SELECT key_id, cursor_pos, normal_seq, head_seq, head_pos, head_origin, state,
                attempts, receipt
         FROM key_lanes WHERE epoch_id=?1 AND key_id=?2",
        params![epoch_id.as_slice(), key_id],
        |r| lane_row(r, 0),
    )
    .optional()
    .map_err(|e| fatal("Cannot load lane", e))
}

fn set_lane_ready(conn: &Connection, epoch: &mut EpochRow, lane: &LaneRow, attempts: i64) -> R<()> {
    let ticket = take_ticket(epoch)?;
    sql(
        exec(
            conn,
            "UPDATE key_lanes SET state=1, attempts=?3, ready_ticket=?4, receipt=NULL, owner=NULL,
                connection_id=NULL, deadline_ms=NULL
             WHERE epoch_id=?1 AND key_id=?2",
            params![epoch.id.as_slice(), lane.key_id, attempts, ticket],
        ),
        "Cannot ready lane",
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
            "UPDATE key_lanes SET state=3, normal_seq=NULL, head_seq=NULL, head_pos=NULL,
                head_origin=NULL, attempts=0, ready_ticket=0, receipt=NULL, owner=NULL,
                connection_id=NULL, deadline_ms=NULL
             WHERE epoch_id=?1 AND key_id=?2",
            params![epoch.id.as_slice(), lane.key_id],
        ),
        "Cannot park lane",
    )?;
    Ok(())
}

/// Release one leased keyed lane: READY again while attempts remain, PARKED
/// once `max_deliveries` is exhausted. `charge_pending` decrements the epoch
/// credit (recovery sweeps reconcile counters wholesale instead).
fn release_lane_lease(
    conn: &Connection,
    epoch: &mut EpochRow,
    lane: &LaneRow,
    max_deliveries: u32,
    charge_pending: bool,
) -> R<()> {
    if charge_pending {
        epoch.pending -= 1;
    }
    if lane.attempts >= max_deliveries as i64 {
        park_lane(conn, epoch, lane, DlsReason::MaxDeliveries)?;
    } else {
        set_lane_ready(conn, epoch, lane, lane.attempts)?;
    }
    Ok(())
}

fn release_keyless_lease(
    conn: &Connection,
    epoch: &mut EpochRow,
    seq: u64,
    key_pos: u64,
    attempts: i64,
    max_deliveries: u32,
    charge_pending: bool,
) -> R<()> {
    if charge_pending {
        epoch.pending -= 1;
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
                    connection_id=NULL, deadline_ms=NULL
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
    release_lane_lease(conn, &mut epoch, &lane, config.max_deliveries, false)
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
    let key_pos = query_one(
        conn,
        "SELECT key_pos FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2",
        params![epoch_id.as_slice(), encode_u64(seq).as_slice()],
        |r| blob8(r, 0, "key_pos"),
    )
    .optional()
    .map_err(|e| BrokerError::storage(format!("Corrupt keyless delivery: {e}")))?
    .ok_or_else(|| BrokerError::storage("Corrupt storage: leased delivery missing"))?;
    let config = load_config_for_stream(conn, &epoch.stream_id)?;
    release_keyless_lease(
        conn,
        &mut epoch,
        seq,
        key_pos,
        attempts,
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
/// those identities are already fenced by the generation check above.
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
                g.id, g.generation,
                e.id, e.group_id, e.stream_id, e.start_after_seq, e.initialized,
                e.init_key_cursor, e.init_key_target, e.keyless_cursor_pos,
                e.keyless_ticket, e.next_ready_ticket, e.pending_count, e.max_pending,
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
                r.get::<_, Option<Vec<u8>>>(10)?,
                opt_blob8(r, 11, "start_after_seq")?,
                r.get::<_, Option<i64>>(12)?,
                r.get::<_, Option<i64>>(13)?,
                r.get::<_, Option<i64>>(14)?,
                opt_blob8(r, 15, "keyless_cursor_pos")?,
                r.get::<_, Option<i64>>(16)?,
                r.get::<_, Option<i64>>(17)?,
                r.get::<_, Option<i64>>(18)?,
                r.get::<_, Option<i64>>(19)?,
                r.get::<_, Option<String>>(20)?,
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
        generation,
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
        max_pending,
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
    let generation = generation
        .map(|g| decode_u64(&g))
        .transpose()
        .map_err(CmdError::Fatal)?
        .ok_or_else(|| CmdError::Fatal(BrokerError::storage("Corrupt storage: group row")))?;
    if identity.generation != generation {
        return Err(expected(BrokerError::fenced()));
    }
    let (Some(epoch_id), Some(epoch_group), Some(epoch_stream)) =
        (epoch_id, epoch_group, epoch_stream)
    else {
        return Err(CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: group without active epoch",
        )));
    };
    let epoch = EpochRow {
        id: blob16(&epoch_id, "epoch id")?,
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
        max_pending: max_pending.unwrap_or(0),
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
            limit,
        } => fetch(conn, name, group, identity, *limit, ctx),
        StreamRequest::Ack {
            name,
            group,
            identity,
            seq,
            receipt,
        } => ack(conn, name, group, identity, *seq, receipt),
        StreamRequest::AckMany {
            name,
            group,
            identity,
            acks,
        } => {
            let (results, effects) = ack_many(conn, name, group, identity, acks)?;
            let failed = acks
                .iter()
                .zip(&results)
                .filter(|(_, r)| r.is_err())
                .map(|((seq, _), _)| *seq)
                .collect();
            Ok((StreamReply::AckOutcome(failed), effects))
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
        logical_added += event_logical_bytes(item.key.len(), item.payload.len());
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
                "SELECT key_id, cursor_pos, normal_seq, head_seq, head_pos, head_origin,
                        state, attempts, receipt FROM key_lanes
                 WHERE epoch_id=?1 AND key_id IN (",
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
                Some(LaneState::Ready) | Some(LaneState::Leased) => {
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
            "INSERT INTO groups(id, stream_id, name, generation, active_epoch)
             VALUES (?1,?2,?3,?4,?5)",
            params![
                group_id.as_slice(),
                stream.id.as_slice(),
                group_name,
                encode_u64(1).as_slice(),
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
                next_ready_ticket, pending_count, max_pending)
             VALUES (?1,?2,?3,?4,0,0,?5,?6,1,2,0,?7)",
            params![
                epoch_id.as_slice(),
                group_id.as_slice(),
                stream.id.as_slice(),
                encode_u64(start_after).as_slice(),
                stream.last_key_id,
                encode_u64(keyless_cursor).as_slice(),
                stream.config.max_ack_pending as i64,
            ],
        ),
        "Cannot create epoch",
    )?;
    let group = GroupRow {
        id: group_id,
        generation: 1,
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
            generation: group.generation,
        },
        effects,
    ))
}

/// One fetch attempt: validate the lease identity, then claim READY lane
/// heads, ready keyless retries and the keyless fresh source in ticket order
/// until the credit or the byte budget is exhausted.
fn fetch(
    conn: &Connection,
    name: &str,
    group_name: &str,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    limit: usize,
    ctx: &ExecCtx,
) -> Result<(StreamReply, Effects), CmdError> {
    let (stream, mut epoch) = resolve_member(conn, name, group_name, identity)?;
    if limit == 0 {
        return Ok((StreamReply::Fetch(Vec::new()), Effects::default()));
    }
    let credit = ((epoch.max_pending - epoch.pending).max(0) as usize).min(limit);
    if credit == 0 {
        return Ok((StreamReply::Fetch(Vec::new()), Effects::default()));
    }

    // Candidate scans carry event metadata (timestamp, payload size, key size)
    // so the byte-budget check needs no per-message SELECT. Payloads are
    // loaded in bulk afterwards, only for the claims actually taken.
    struct LaneCand {
        ticket: i64,
        key_id: i64,
        head_seq: u64,
        timestamp_ms: i64,
        encoded_bytes: u64,
    }
    struct RetryCand {
        ticket: i64,
        seq: u64,
        timestamp_ms: i64,
        encoded_bytes: u64,
    }

    let mut lanes: std::collections::VecDeque<LaneCand> = query_vec(
        conn,
        "SELECT l.ready_ticket, l.key_id, l.head_seq, e.timestamp_ms,
                e.payload_bytes, LENGTH(k.key)
         FROM key_lanes l
         JOIN events e ON e.stream_id=l.stream_id AND e.seq=l.head_seq
         JOIN stream_keys k ON k.stream_id=l.stream_id AND k.key_id=l.key_id
         WHERE l.epoch_id=?1 AND l.state=1
         ORDER BY l.ready_ticket, l.key_id LIMIT ?2",
        params![epoch.id.as_slice(), (credit + 1) as i64],
        |r| {
            Ok(LaneCand {
                ticket: r.get(0)?,
                key_id: r.get(1)?,
                head_seq: blob8(r, 2, "head_seq")?,
                timestamp_ms: r.get(3)?,
                encoded_bytes: fetch_item_encoded_bytes(
                    r.get::<_, i64>(5)? as usize,
                    r.get::<_, i64>(4)? as usize,
                ),
            })
        },
        "Cannot scan ready lanes",
    )?
    .into();
    let mut retries: std::collections::VecDeque<RetryCand> = query_vec(
        conn,
        "SELECT d.ready_ticket, d.seq, e.timestamp_ms, e.payload_bytes
         FROM keyless_deliveries d
         JOIN events e ON e.stream_id=d.stream_id AND e.seq=d.seq
         WHERE d.epoch_id=?1 AND d.state=1 ORDER BY d.seq LIMIT ?2",
        params![epoch.id.as_slice(), (credit + 1) as i64],
        |r| {
            Ok(RetryCand {
                ticket: r.get(0)?,
                seq: blob8(r, 1, "seq")?,
                timestamp_ms: r.get(2)?,
                encoded_bytes: fetch_item_encoded_bytes(0, r.get::<_, i64>(3)? as usize),
            })
        },
        "Cannot scan keyless retries",
    )?
    .into();

    let deadline = (ctx.now_ms + stream.config.ack_wait_ms) as i64;
    let mut used_bytes: u64 = 4; // FETCH response count prefix

    // Selection pass in ticket order: decides the claims without touching the
    // lease tables. Fresh keyless events are scanned in pages; a short page
    // marks the source exhausted for this fetch.
    enum Claim {
        Lane {
            key_id: i64,
            seq: u64,
            timestamp_ms: i64,
        },
        Retry {
            seq: u64,
            timestamp_ms: i64,
        },
        Source {
            ev: FreshSource,
        },
    }
    let mut claims: Vec<Claim> = Vec::with_capacity(credit);
    let mut sources: std::collections::VecDeque<FreshSource> = std::collections::VecDeque::new();
    let mut fresh_exhausted = false;
    // Sources consume ready tickets at selection time; the commit pass calls
    // take_ticket in the same order, so a virtual ticket keeps comparisons
    // honest before any row is written. The scan cursor likewise advances
    // only at commit — page queries use a local copy.
    let mut next_source_ticket = epoch.keyless_ticket;
    let mut scan_cursor = epoch.keyless_cursor;

    while claims.len() < credit && used_bytes < ctx.fetch_response_bytes {
        if sources.is_empty() && !fresh_exhausted {
            let page = ((credit - claims.len()).min(256)) as i64;
            let found: Vec<FreshSource> = query_vec(
                conn,
                "SELECT key_pos, seq, timestamp_ms, payload FROM events
                 WHERE stream_id=?1 AND key_id=0 AND key_pos>?2
                 ORDER BY key_pos LIMIT ?3",
                params![
                    epoch.stream_id.as_slice(),
                    encode_u64(scan_cursor).as_slice(),
                    page
                ],
                |r| {
                    Ok(FreshSource {
                        key_pos: blob8(r, 0, "key_pos")?,
                        seq: blob8(r, 1, "seq")?,
                        timestamp_ms: r.get(2)?,
                        payload: r.get(3)?,
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

        let (encoded_bytes, claim) =
            if lane_ticket != i64::MAX && lane_ticket <= keyless_ticket {
                let Some(c) = lanes.pop_front() else {
                    break;
                };
                (
                    c.encoded_bytes,
                    Claim::Lane {
                        key_id: c.key_id,
                        seq: c.head_seq,
                        timestamp_ms: c.timestamp_ms,
                    },
                )
            } else if retries.front().is_some() {
                let Some(c) = retries.pop_front() else {
                    break;
                };
                (
                    c.encoded_bytes,
                    Claim::Retry {
                        seq: c.seq,
                        timestamp_ms: c.timestamp_ms,
                    },
                )
            } else if source_ticket != i64::MAX {
                let Some(ev) = sources.pop_front() else {
                    break;
                };
                next_source_ticket += 1;
                scan_cursor = ev.key_pos;
                (fetch_item_encoded_bytes(0, ev.payload.len()), Claim::Source { ev })
            } else {
                break;
            };

        if used_bytes + encoded_bytes > ctx.fetch_response_bytes {
            if claims.is_empty() {
                return Err(expected(BrokerError::invalid_argument(format!(
                    "Single stream event ({} bytes) exceeds the fetch response budget ({} bytes)",
                    encoded_bytes, ctx.fetch_response_bytes
                ))));
            }
            break;
        }
        used_bytes += encoded_bytes;
        claims.push(claim);
    }

    // Payloads + key bytes for claimed lanes/retries load in one chunked
    // IN-lookup. Seq is unique per stream.
    let keyed_seqs: Vec<u64> = claims
        .iter()
        .filter_map(|c| match c {
            Claim::Lane { seq, .. } | Claim::Retry { seq, .. } => Some(*seq),
            Claim::Source { .. } => None,
        })
        .collect();
    let mut events: std::collections::HashMap<u64, (Vec<u8>, Vec<u8>)> =
        std::collections::HashMap::with_capacity(keyed_seqs.len());
    for chunk in keyed_seqs.chunks(400) {
        let mut text = String::from(
            "SELECT e.seq, e.payload, k.key FROM events e
             JOIN stream_keys k ON k.stream_id=e.stream_id AND k.key_id=e.key_id
             WHERE e.stream_id=?1 AND e.seq IN (",
        );
        let mut params: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(chunk.len() + 1);
        let sid: &[u8] = epoch.stream_id.as_slice();
        params.push(&sid);
        let mut enc: Vec<Vec<u8>> = Vec::with_capacity(chunk.len());
        for (i, seq) in chunk.iter().enumerate() {
            if i > 0 {
                text.push(',');
            }
            text.push('?');
            enc.push(encode_u64(*seq).to_vec());
        }
        text.push(')');
        for b in &enc {
            params.push(b);
        }
        for row in query_vec(
            conn,
            &text,
            rusqlite::params_from_iter(params),
            |r| {
                Ok((
                    blob8(r, 0, "seq")?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            },
            "Cannot load events for delivery",
        )? {
            events.insert(row.0, (row.1, row.2));
        }
    }

    // Commit pass: lease rows for the chosen claims in selection order.
    let mut deliveries: Vec<Delivery> = Vec::with_capacity(claims.len());
    let mut fresh_claims: Vec<(u64, u64, [u8; 16])> = Vec::new();
    for claim in claims {
        let receipt = Uuid::new_v4().into_bytes();
        match claim {
            Claim::Source { ev } => {
                fresh_claims.push((ev.seq, ev.key_pos, receipt));
                epoch.keyless_cursor = ev.key_pos;
                epoch.keyless_ticket = take_ticket(&mut epoch)?;
                deliveries.push(Delivery {
                    message: Message {
                        seq: ev.seq,
                        timestamp: ev.timestamp_ms as u64,
                        key: Bytes::new(),
                        payload: Bytes::from(ev.payload),
                    },
                    receipt,
                });
            }
            Claim::Lane {
                key_id,
                seq,
                timestamp_ms,
            } => {
                let Some((payload, key)) = events.remove(&seq) else {
                    // Retention removed the row between candidate scan and
                    // claim; the lease tables keep no stale reference after
                    // normalization.
                    continue;
                };
                let changed = sql(
                    exec(
                        conn,
                        "UPDATE key_lanes SET state=2, receipt=?3, owner=?4, connection_id=?5,
                            deadline_ms=?6, attempts=attempts+1
                         WHERE epoch_id=?1 AND key_id=?2 AND state=1",
                        params![
                            epoch.id.as_slice(),
                            key_id,
                            receipt.as_slice(),
                            identity.consumer_id,
                            identity.connection_id,
                            deadline,
                        ],
                    ),
                    "Cannot lease lane",
                )?;
                if changed == 0 {
                    continue;
                }
                deliveries.push(Delivery {
                    message: Message {
                        seq,
                        timestamp: timestamp_ms as u64,
                        key: Bytes::from(key),
                        payload: Bytes::from(payload),
                    },
                    receipt,
                });
            }
            Claim::Retry { seq, timestamp_ms } => {
                let Some((payload, key)) = events.remove(&seq) else {
                    continue;
                };
                let changed = sql(
                    exec(
                        conn,
                        "UPDATE keyless_deliveries SET state=2, receipt=?3, owner=?4,
                            connection_id=?5, deadline_ms=?6, attempts=attempts+1
                         WHERE epoch_id=?1 AND seq=?2 AND state=1",
                        params![
                            epoch.id.as_slice(),
                            encode_u64(seq).as_slice(),
                            receipt.as_slice(),
                            identity.consumer_id,
                            identity.connection_id,
                            deadline,
                        ],
                    ),
                    "Cannot lease keyless delivery",
                )?;
                if changed == 0 {
                    continue;
                }
                deliveries.push(Delivery {
                    message: Message {
                        seq,
                        timestamp: timestamp_ms as u64,
                        key: Bytes::from(key),
                        payload: Bytes::from(payload),
                    },
                    receipt,
                });
            }
        }
    }

    if deliveries.is_empty() {
        return Ok((StreamReply::Fetch(deliveries), Effects::default()));
    }
    admit_fresh_keyless(conn, &epoch, &fresh_claims, identity, deadline)?;
    epoch.pending += deliveries.len() as i64;
    save_epoch(conn, &epoch)?;
    Ok((StreamReply::Fetch(deliveries), Effects::default()))
}

/// Fresh keyless event prefetched for the claim loop: the whole payload is
/// read up front so the byte budget needs no second query.
struct FreshSource {
    key_pos: u64,
    seq: u64,
    timestamp_ms: i64,
    payload: Vec<u8>,
}

/// Admission rows for fresh keyless claims, written as chunked multi-row
/// VALUES inserts — identical rows to the per-message INSERT, at ~1/Nth the
/// statement count.
fn admit_fresh_keyless(
    conn: &Connection,
    epoch: &EpochRow,
    claims: &[(u64, u64, [u8; 16])],
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    deadline: i64,
) -> R<()> {
    // 8 bound params per row; chunks stay under the 999-variable limit.
    for chunk in claims.chunks(120) {
        let mut text = String::from(
            "INSERT INTO keyless_deliveries(epoch_id, stream_id, seq, key_pos, origin,
                state, attempts, ready_ticket, receipt, owner, connection_id, deadline_ms)
             VALUES ",
        );
        let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(chunk.len() * 8);
        for (row, (seq, key_pos, receipt)) in chunk.iter().enumerate() {
            if row > 0 {
                text.push(',');
            }
            text.push_str("(?,?,?,?,0,2,1,0,?,?,?,?)");
            values.extend([
                rusqlite::types::Value::Blob(epoch.id.to_vec()),
                rusqlite::types::Value::Blob(epoch.stream_id.to_vec()),
                rusqlite::types::Value::Blob(encode_u64(*seq).to_vec()),
                rusqlite::types::Value::Blob(encode_u64(*key_pos).to_vec()),
                rusqlite::types::Value::Blob(receipt.to_vec()),
                rusqlite::types::Value::Text(identity.consumer_id.clone()),
                rusqlite::types::Value::Text(identity.connection_id.clone()),
                rusqlite::types::Value::Integer(deadline),
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
// ACK / leave / disconnect / seek
// ---------------------------------------------------------------------------

/// Consume the lease for `seq` fenced by `receipt`. A lease that is missing or
/// carries a different receipt is a stale epoch/receipt → FENCED.
fn ack(
    conn: &Connection,
    name: &str,
    group_name: &str,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    seq: u64,
    receipt: &[u8; 16],
) -> Result<(StreamReply, Effects), CmdError> {
    let (_stream, mut epoch) = resolve_member(conn, name, group_name, identity)?;
    let mut effects = Effects::default();

    // Keyed lease: one full-row hit on the (stream, head_seq) index.
    let lane = query_one(
        conn,
        "SELECT key_id, cursor_pos, normal_seq, head_seq, head_pos, head_origin, state,
                attempts, receipt
         FROM key_lanes
         WHERE stream_id=?1 AND head_seq=?2 AND epoch_id=?3 AND state=2",
        params![
            epoch.stream_id.as_slice(),
            encode_u64(seq).as_slice(),
            epoch.id.as_slice()
        ],
        |r| lane_row(r, 0),
    )
    .optional()
    .map_err(|e| fatal("Cannot locate lease", e))?;

    if let Some(lane) = lane {
        let stored = lane.receipt.as_deref().unwrap_or(&[]);
        if stored != receipt.as_slice() {
            return Err(expected(BrokerError::fenced()));
        }
        consume_lane_lease(conn, &mut epoch, &lane, name, group_name, &mut effects)?;
        save_epoch(conn, &epoch)?;
        // Credit freed: pending fetches retry even when the lane went EMPTY.
        effects.wake(name, group_name);
        return Ok((StreamReply::Unit, effects));
    }

    // Keyless lease: primary-key lookup on the epoch table.
    let keyless = query_one(
        conn,
        "SELECT receipt, state FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2",
        params![epoch.id.as_slice(), encode_u64(seq).as_slice()],
        |r| Ok((r.get::<_, Option<Vec<u8>>>(0)?, r.get::<_, i64>(1)?)),
    )
    .optional()
    .map_err(|e| fatal("Cannot locate keyless lease", e))?;
    match keyless {
        Some((Some(stored), 2)) if stored.as_slice() == receipt.as_slice() => {
            sql(
                exec(
                    conn,
                    "DELETE FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2",
                    params![epoch.id.as_slice(), encode_u64(seq).as_slice()],
                ),
                "Cannot consume keyless delivery",
            )?;
            epoch.pending -= 1;
            save_epoch(conn, &epoch)?;
            effects.wake(name, group_name);
            Ok((StreamReply::Unit, effects))
        }
        _ => Err(expected(BrokerError::fenced())),
    }
}

/// Consume one leased lane head after the receipt check: drop the replay
/// obligation or advance the durable cursor, then recompute the next head.
/// Shared by `ack` and `ack_many`; decrements the epoch pending counter.
fn consume_lane_lease(
    conn: &Connection,
    epoch: &mut EpochRow,
    lane: &LaneRow,
    name: &str,
    group_name: &str,
    effects: &mut Effects,
) -> R<()> {
    epoch.pending -= 1;
    let origin = lane.head_origin.unwrap_or(Origin::Original as i64);
    let head_pos = lane.head_pos.ok_or_else(|| {
        CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: leased lane without head",
        ))
    })?;
    let mut consumed = lane.clone();
    if origin == Origin::Replay as i64 {
        // The delivery obligation is consumed only when it is acked.
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
    } else {
        // Original progress: the resolved position becomes the new cursor,
        // persisted by the recompute write below.
        consumed.cursor_pos = head_pos;
    }
    if recompute_lane(conn, epoch, &consumed)? {
        effects.wake(name, group_name);
    }
    Ok(())
}

/// `(text, params)` for `... WHERE epoch_id=?1 AND <col> IN (?2..)`: values
/// bind as numbered placeholders past the epoch. Bounded by the worker's
/// batch cap (256), well under the SQLite variable limit.
fn in_list(
    base: &str,
    epoch_id: &[u8; 16],
    mut values: Vec<rusqlite::types::Value>,
) -> (String, Vec<rusqlite::types::Value>) {
    let mut text = String::from(base);
    let mut bound = Vec::with_capacity(values.len() + 1);
    bound.push(rusqlite::types::Value::Blob(epoch_id.to_vec()));
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
    epoch_id: &[u8; 16],
    seqs: &[u64],
) -> (String, Vec<rusqlite::types::Value>) {
    in_list(
        base,
        epoch_id,
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

/// Consecutive acks for one (stream, group, consumer), merged by the worker:
/// member resolution and the epoch write happen once per run, and keyless
/// releases collapse into a single IN-list delete (~1 statement per run, not
/// per message). Per-ack results are preserved: a fenced seq fails alone.
pub fn ack_many(
    conn: &Connection,
    name: &str,
    group_name: &str,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
    acks: &[(u64, [u8; 16])],
) -> Result<(Vec<Result<StreamReply, BrokerError>>, Effects), CmdError> {
    let (_stream, mut epoch) = resolve_member(conn, name, group_name, identity)?;
    let mut effects = Effects::default();
    let mut ok = vec![false; acks.len()];
    // A seq already released inside this run: a repeated ack behaves like
    // the second sequential call would — the row is gone → FENCED.
    let mut consumed = std::collections::HashSet::with_capacity(acks.len());

    let mut seqs: Vec<u64> = acks.iter().map(|(seq, _)| *seq).collect();
    seqs.sort_unstable();
    seqs.dedup();
    if seqs.is_empty() {
        return Ok((Vec::new(), Effects::default()));
    }

    // Lanes whose leased head is one of the acked seqs, and leased keyless
    // rows: two IN-list scans, one row per lane and per delivery.
    let lane_of: std::collections::HashMap<u64, LaneRow> = {
        let (text, values) = seq_in_list(
            "SELECT head_seq, key_id, cursor_pos, normal_seq, head_seq, head_pos,
                    head_origin, state, attempts, receipt FROM key_lanes
             WHERE epoch_id=?1 AND state=2 AND head_seq IN (",
            &epoch.id,
            &seqs,
        );
        query_vec(
            conn,
            &text,
            rusqlite::params_from_iter(values),
            |r| Ok((blob8(r, 0, "head_seq")?, lane_row(r, 1)?)),
            "Cannot locate lane leases",
        )?
        .into_iter()
        .collect()
    };
    let keyless_of: std::collections::HashMap<u64, Vec<u8>> = {
        let (text, values) = seq_in_list(
            "SELECT seq, receipt FROM keyless_deliveries
             WHERE epoch_id=?1 AND state=2 AND seq IN (",
            &epoch.id,
            &seqs,
        );
        query_vec(
            conn,
            &text,
            rusqlite::params_from_iter(values),
            |r| Ok((blob8(r, 0, "seq")?, r.get::<_, Vec<u8>>(1)?)),
            "Cannot locate keyless leases",
        )?
        .into_iter()
        .collect()
    };

    let mut keyless_done: Vec<u64> = Vec::new();
    for (i, (seq, receipt)) in acks.iter().enumerate() {
        if consumed.contains(seq) {
            continue;
        }
        if let Some(lane) = lane_of.get(seq) {
            if lane.receipt.as_deref() == Some(receipt.as_slice()) {
                consume_lane_lease(conn, &mut epoch, lane, name, group_name, &mut effects)?;
                consumed.insert(*seq);
                ok[i] = true;
            }
            continue;
        }
        if let Some(stored) = keyless_of.get(seq) {
            if stored.as_slice() == receipt.as_slice() {
                keyless_done.push(*seq);
                consumed.insert(*seq);
                ok[i] = true;
            }
        }
    }

    if !keyless_done.is_empty() {
        let (text, values) = seq_in_list(
            "DELETE FROM keyless_deliveries WHERE epoch_id=?1 AND seq IN (",
            &epoch.id,
            &keyless_done,
        );
        sql(
            exec(conn, &text, rusqlite::params_from_iter(values)),
            "Cannot consume keyless deliveries",
        )?;
        epoch.pending -= keyless_done.len() as i64;
    }
    if ok.iter().any(|done| *done) {
        save_epoch(conn, &epoch)?;
        // Credit freed once per run; wakes are deduplicated anyway.
        effects.wake(name, group_name);
    }
    Ok((
        ok.into_iter()
            .map(|done| {
                if done {
                    Ok(StreamReply::Unit)
                } else {
                    Err(BrokerError::fenced())
                }
            })
            .collect(),
        effects,
    ))
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
        release_lane_lease(conn, epoch, &lane, stream.config.max_deliveries, true)?;
    }
    let keyless_rows: Vec<(u64, u64, i64)> = query_vec(
        conn,
        "SELECT seq, key_pos, attempts FROM keyless_deliveries
         WHERE epoch_id=?1 AND owner=?2 AND connection_id=?3 AND state=2",
        params![epoch.id.as_slice(), consumer_id, connection_id],
        |r| {
            Ok((
                blob8(r, 0, "seq")?,
                blob8(r, 1, "key_pos")?,
                r.get::<_, i64>(2)?,
            ))
        },
        "Cannot scan member keyless leases",
    )?;
    for (seq, key_pos, attempts) in keyless_rows {
        release_keyless_lease(
            conn,
            epoch,
            seq,
            key_pos,
            attempts,
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
            let generation = group.generation.checked_add(1).ok_or_else(|| {
                CmdError::Fatal(BrokerError::storage("Group generation exhausted"))
            })?;
            sql(
                exec(conn,
                    "INSERT INTO group_epochs(id, group_id, stream_id, start_after_seq, initialized,
                        init_key_cursor, init_key_target, keyless_cursor_pos, keyless_ticket,
                        next_ready_ticket, pending_count, max_pending)
                     VALUES (?1,?2,?3,?4,0,0,?5,?6,1,2,0,?7)",
                    params![
                        epoch_id.as_slice(),
                        group.id.as_slice(),
                        stream.id.as_slice(),
                        encode_u64(start_after).as_slice(),
                        stream.last_key_id,
                        encode_u64(keyless_cursor).as_slice(),
                        stream.config.max_ack_pending as i64,
                    ],
                ),
                "Cannot create epoch",
            )?;
            sql(
                exec(
                    conn,
                    "UPDATE groups SET generation=?2, active_epoch=?3 WHERE id=?1",
                    params![
                        group.id.as_slice(),
                        encode_u64(generation).as_slice(),
                        epoch_id.as_slice()
                    ],
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
        "SELECT key_id, cursor_pos, normal_seq, head_seq, head_pos, head_origin, state,
                attempts, receipt FROM key_lanes WHERE epoch_id=?1 AND state=3",
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
        "SELECT epoch_id, key_id, cursor_pos, normal_seq, head_seq, head_pos, head_origin,
                state, attempts, receipt FROM key_lanes
         WHERE state=2 AND deadline_ms<=?1 ORDER BY deadline_ms LIMIT ?2",
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
        release_lane_lease(conn, epoch, &lane, max_deliveries, true)?;
        if let Some((sname, gname)) = name_cache.get(&epoch_id) {
            effects.wake(&sname.clone(), &gname.clone());
        }
    }

    let due_keyless: Vec<(Vec<u8>, u64, u64, i64)> = query_vec(
        conn,
        "SELECT epoch_id, seq, key_pos, attempts FROM keyless_deliveries
         WHERE state=2 AND deadline_ms<=?1 ORDER BY deadline_ms LIMIT ?2",
        params![now_ms as i64, BATCH_LIMIT as i64],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                blob8(r, 1, "seq")?,
                blob8(r, 2, "key_pos")?,
                r.get::<_, i64>(3)?,
            ))
        },
        "Cannot scan due keyless leases",
    )?;
    let more_keyless = due_keyless.len() == BATCH_LIMIT;
    for (epoch_id, seq, key_pos, attempts) in due_keyless {
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
        release_keyless_lease(
            conn,
            epoch,
            seq,
            key_pos,
            attempts,
            max_deliveries,
            true,
        )?;
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
    let expired_keyless: Vec<(Vec<u8>, Vec<u8>, i64)> = query_vec(
        conn,
        "SELECT epoch_id, seq, state FROM keyless_deliveries
         WHERE stream_id=?1 AND seq<=?2",
        params![stream.id.as_slice(), encode_u64(boundary_seq).as_slice()],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, i64>(2)?,
            ))
        },
        "Cannot scan keyless deliveries for retention",
    )?;
    for (epoch_blob, seq_blob, state) in expired_keyless {
        let epoch_id = blob16(&epoch_blob, "epoch id")?;
        if epoch_of(conn, &mut dirty_epochs, epoch_id)? && state == LaneState::Leased as i64 {
            if let Some(e) = dirty_epochs.get_mut(&epoch_id) {
                e.pending -= 1;
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
                Some(LaneState::Leased) => {
                    if lane.head_seq.is_some_and(|s| s <= boundary_seq) {
                        // The leased head is gone: retire the lease, refund
                        // the credit, and rebuild the lane.
                        epoch.pending -= 1;
                        sql(
                            exec(
                                conn,
                                "UPDATE key_lanes SET state=0, head_seq=NULL, head_pos=NULL,
                                    head_origin=NULL, receipt=NULL, owner=NULL,
                                    connection_id=NULL, deadline_ms=NULL
                                 WHERE epoch_id=?1 AND key_id=?2",
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
    if epoch.initialized {
        return Ok((StreamReply::Unit, effects));
    }
    // Skip init work for epochs of streams already deleted; GC reclaims them.
    let Some(_stream) = load_stream_by_id(conn, &epoch.stream_id)? else {
        return Ok((StreamReply::Unit, effects));
    };
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
                "INSERT INTO groups(id,stream_id,name,generation,active_epoch)
                 VALUES (?1,?2,'g',?3,?4)",
                params![
                    group_id.as_slice(),
                    stream_id.as_slice(),
                    encode_u64(1).as_slice(),
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

    /// A merged ack run consumes keyless leases in bulk while keeping
    /// per-ack outcomes: duplicate seqs, stale receipts and unknown seqs are
    /// FENCED alone; valid releases drop `pending` and delete the rows.
    #[test]
    fn ack_many_consumes_keyless_run_with_per_ack_outcomes() {
        let conn = test_conn();
        let stream_id = [7u8; 16];
        let group_id = [8u8; 16];
        let epoch_id = [9u8; 16];
        let identity = crate::brokers::stream::domain::message::ConsumerIdentity {
            connection_id: "conn1".to_string(),
            consumer_id: "c1".to_string(),
            generation: 1,
        };

        exec(
            &conn,
            "INSERT INTO streams VALUES (?1,'s',0,?3,?2,?2,0,0)",
            params![
                stream_id.as_slice(),
                encode_u64(0).as_slice(),
                r#"{"retention":{"max_age_ms":null,"max_bytes":null},"max_ack_pending":1000,"ack_wait_ms":30000,"max_deliveries":3}"#
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
            "INSERT INTO groups(id,stream_id,name,generation,active_epoch)
             VALUES (?1,?2,'g',?3,?4)",
            params![
                group_id.as_slice(),
                stream_id.as_slice(),
                encode_u64(1).as_slice(),
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
        let (reply, _) = match fetch(&conn, "s", "g", &identity, 10, &ctx) {
            Ok(out) => out,
            Err(e) => panic!("fetch failed: {}", e.into_error()),
        };
        let StreamReply::Fetch(deliveries) = reply else {
            panic!("expected a fetch reply")
        };
        assert_eq!(deliveries.len(), 5);
        // The deferred bulk admit actually wrote the lease rows.
        let leased: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM keyless_deliveries WHERE epoch_id=?1 AND state=2",
                [epoch_id.as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(leased, 5);

        // Run: [ok 1, dup 1, stale receipt on 2, unknown 99, ok 3, ok 4, ok 5]
        let mut run: Vec<(u64, [u8; 16])> = vec![
            (deliveries[0].message.seq, deliveries[0].receipt),
            (deliveries[0].message.seq, deliveries[0].receipt),
            (deliveries[1].message.seq, [0xAA; 16]),
            (99, [0xBB; 16]),
        ];
        for d in &deliveries[2..] {
            run.push((d.message.seq, d.receipt));
        }

        let (results, _) = match ack_many(&conn, "s", "g", &identity, &run) {
            Ok(out) => out,
            Err(e) => panic!("ack_many failed: {}", e.into_error()),
        };
        assert!(results[0].is_ok());
        assert!(
            results[1].is_err(),
            "second ack of a consumed seq is fenced"
        );
        assert!(results[2].is_err(), "stale receipt is fenced");
        assert!(results[3].is_err(), "unknown seq is fenced");
        for r in &results[4..] {
            assert!(r.is_ok());
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
}

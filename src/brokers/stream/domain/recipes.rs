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

use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::brokers::stream::domain::definition::{StreamConfig, StreamDefinition};
use crate::brokers::stream::domain::message::{Delivery, DlsEntry, Message};
use crate::brokers::stream::domain::ops::{Continuation, Effects, StreamReply, StreamRequest};
use crate::brokers::stream::domain::types::{
    decode_u64, encode_u64, event_logical_bytes, fetch_item_encoded_bytes, DlsReason, LaneState,
    Origin,
};
use crate::brokers::stream::options::SeekTarget;
use crate::brokers::{
    config_conflict_error, BrokerError, ProvisionOutcome, ProvisionResult,
};
use bytes::Bytes;
use uuid::Uuid;

/// Rows processed per continuation slice / maintenance batch.
pub const BATCH_LIMIT: usize = 1024;
/// Groups reclaimed per GC slice; each group deletes its epoch rows + group
/// row atomically (deferred `active_epoch` FK).
const GROUP_GC_BATCH: usize = 64;
/// Keys initialized per `InitEpoch` slice.
pub const INIT_PAGE: i64 = 512;

pub enum CmdError {
    /// Domain failure: roll back the command savepoint, reply with the error.
    Expected(BrokerError),
    /// Storage failure: abort the entire transaction.
    Fatal(BrokerError),
}

impl CmdError {
    fn into_error(self) -> BrokerError {
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

pub type R<T> = Result<T, CmdError>;

fn expected(e: BrokerError) -> CmdError {
    CmdError::Expected(e)
}

fn fatal(context: &str, e: impl std::fmt::Display) -> CmdError {
    CmdError::Fatal(BrokerError::storage(format!("{context}: {e}")))
}

fn sql<T>(result: rusqlite::Result<T>, context: &str) -> R<T> {
    result.map_err(|e| fatal(context, e))
}

/// All statement access goes through `prepare_cached`: every recipe runs on
/// the writer's single connection, so cached plans amortize parse/plan cost
/// across commands and transactions.
fn exec<P: rusqlite::Params>(
    conn: &Connection,
    text: &str,
    params: P,
) -> rusqlite::Result<usize> {
    conn.prepare_cached(text)?.execute(params)
}

fn query_one<P: rusqlite::Params, T>(
    conn: &Connection,
    text: &str,
    params: P,
    map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    conn.prepare_cached(text)?.query_row(params, map)
}

/// Prepare + map + collect in one step. The collect result is bound to a
/// local first so the `MappedRows` temporary dies before the `Statement`
/// it borrows (block-tail temporaries would otherwise outlive `stmt`).
fn query_vec<P: rusqlite::Params, T>(
    conn: &Connection,
    text: &str,
    params: P,
    map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ctx: &'static str,
) -> R<Vec<T>> {
    let mut stmt = sql(conn.prepare_cached(text), ctx)?;
    let rows = stmt.query_map(params, map)?.collect::<Result<Vec<_>, _>>();
    sql(rows, ctx)
}

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
    let row = conn
        .query_row(
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
    load_stream(conn, name)?.ok_or_else(|| {
        expected(BrokerError::not_found(format!("Stream '{name}' not found")))
    })
}

struct GroupRow {
    id: [u8; 16],
    generation: u64,
    active_epoch: Option<[u8; 16]>,
}

fn load_group(conn: &Connection, stream_id: &[u8; 16], name: &str) -> R<Option<GroupRow>> {
    query_one(conn, 
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
            active_epoch: epoch
                .map(|e| blob16(&e, "active epoch"))
                .transpose()?,
        })
    })
    .transpose()
}

fn require_group(conn: &Connection, stream_id: &[u8; 16], name: &str) -> R<GroupRow> {
    load_group(conn, stream_id, name)?.ok_or_else(|| {
        expected(BrokerError::not_found(format!("Group '{name}' not found")))
    })
}

fn load_epoch(conn: &Connection, epoch_id: &[u8; 16]) -> R<Option<EpochRow>> {
    query_one(conn, 
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
        exec(conn, 
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
    query_one(conn, 
        "SELECT key_pos FROM events WHERE stream_id=?1 AND key_id=?2 AND seq<=?3
         ORDER BY seq DESC LIMIT 1",
        params![stream_id.as_slice(), key_id, encode_u64(seq_boundary).as_slice()],
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
    query_one(conn, 
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
    query_one(conn, 
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
    query_one(conn, 
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
    let first_seq = first_retained_seq_in_range(
        conn,
        &epoch.stream_id,
        key_id,
        first_pos,
        last_pos,
    )?;
    sql(
        exec(conn, 
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
fn recompute_lane(
    conn: &Connection,
    epoch: &mut EpochRow,
    lane: &LaneRow,
) -> R<bool> {
    let next_original = next_event_pos(conn, &epoch.stream_id, lane.key_id, lane.cursor_pos)?;
    let normal_seq = next_original.map(|(_, seq)| seq);

    let intent: Option<(u64, u64)> = conn
        .query_row(
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
        Some((seq, pos, origin)) => {
            (LaneState::Ready as i64, take_ticket(epoch)?, Some(seq), Some(pos), Some(origin as i64), 0i64)
        }
        None => (LaneState::Empty as i64, 0, None, None, None, 0),
    };
    sql(
        exec(conn, 
            "UPDATE key_lanes SET normal_seq=?3, head_seq=?4, head_pos=?5, head_origin=?6,
                state=?7, attempts=?8, ready_ticket=?9, receipt=NULL, owner=NULL,
                connection_id=NULL, deadline_ms=NULL
             WHERE epoch_id=?1 AND key_id=?2",
            params![
                epoch.id.as_slice(),
                lane.key_id,
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
        exec(conn, 
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

fn load_lane(conn: &Connection, epoch_id: &[u8; 16], key_id: i64) -> R<Option<LaneRow>> {
    query_one(conn, 
        "SELECT key_id, cursor_pos, normal_seq, head_seq, head_pos, head_origin, state, attempts,
                receipt
         FROM key_lanes WHERE epoch_id=?1 AND key_id=?2",
        params![epoch_id.as_slice(), key_id],
        |r| {
            Ok(LaneRow {
                key_id: r.get(0)?,
                cursor_pos: blob8(r, 1, "cursor_pos")?,
                normal_seq: opt_blob8(r, 2, "normal_seq")?,
                head_seq: opt_blob8(r, 3, "head_seq")?,
                head_pos: opt_blob8(r, 4, "head_pos")?,
                head_origin: r.get(5)?,
                state: r.get(6)?,
                attempts: r.get(7)?,
                receipt: r.get(8)?,
            })
        },
    )
    .optional()
    .map_err(|e| fatal("Cannot load lane", e))
}

fn set_lane_ready(
    conn: &Connection,
    epoch: &mut EpochRow,
    lane: &LaneRow,
    attempts: i64,
) -> R<()> {
    let ticket = take_ticket(epoch)?;
    sql(
        exec(conn, 
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
fn park_lane(
    conn: &Connection,
    epoch: &mut EpochRow,
    lane: &LaneRow,
    reason: DlsReason,
) -> R<()> {
    let head_pos = lane.head_pos.ok_or_else(|| {
        CmdError::Fatal(BrokerError::storage("Cannot park lane without a head"))
    })?;
    match lane.head_origin {
        Some(o) if o == Origin::Replay as i64 => {
            // A re-failed replay parks as a singleton interval.
            insert_range(conn, epoch, lane.key_id, head_pos, Some(head_pos), reason, lane.attempts)?;
            sql(
                exec(conn, 
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
            insert_range(conn, epoch, lane.key_id, head_pos, None, reason, lane.attempts)?;
        }
    }
    sql(
        exec(conn, 
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
            exec(conn, 
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
            exec(conn, 
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
    let key_pos = conn
        .query_row(
            "SELECT key_pos FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2",
            params![epoch_id.as_slice(), encode_u64(seq).as_slice()],
            |r| blob8(r, 0, "key_pos"),
        )
        .optional()
        .map_err(|e| BrokerError::storage(format!("Corrupt keyless delivery: {e}")))?
        .ok_or_else(|| BrokerError::storage("Corrupt storage: leased delivery missing"))?;
    let config = load_config_for_stream(conn, &epoch.stream_id)?;
    release_keyless_lease(conn, &mut epoch, seq, key_pos, attempts, config.max_deliveries, false)
        .map_err(|e| e.into_error())?;
    save_epoch(conn, &epoch).map_err(|e| e.into_error())
}

fn load_config_for_stream(conn: &Connection, stream_id: &[u8; 16]) -> Result<StreamConfig, BrokerError> {
    let json: Option<String> = conn
        .query_row(
            "SELECT config_json FROM streams WHERE id=?1",
            [stream_id.as_slice()],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| BrokerError::storage(format!("Cannot load stream config: {e}")))?;
    let json = json
        .ok_or_else(|| BrokerError::storage("Corrupt storage: epoch references missing stream"))?;
    serde_json::from_str(&json)
        .map_err(|e| BrokerError::storage(format!("Corrupt stream config: {e}")))
}

// ---------------------------------------------------------------------------
// Membership validation
// ---------------------------------------------------------------------------

/// Resolve stream/group/epoch and validate a durable membership:
/// consumer bound to the active epoch, owning connection, current generation.
/// Stale (pre-seek) member rows exist but live on dead epochs → FENCED.
fn resolve_member(
    conn: &Connection,
    name: &str,
    group: &str,
    identity: &crate::brokers::stream::domain::message::ConsumerIdentity,
) -> R<(StreamRow, EpochRow)> {
    let stream = require_stream(conn, name)?;
    let group = require_group(conn, &stream.id, group)?;
    if identity.generation != group.generation {
        return Err(expected(BrokerError::fenced()));
    }
    let epoch = require_epoch(conn, &group)?;
    let member = conn
        .query_row(
            "SELECT epoch_id, connection_id FROM members WHERE group_id=?1 AND consumer_id=?2",
            params![group.id.as_slice(), identity.consumer_id],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|e| fatal("Cannot load member", e))?;
    let Some((member_epoch, member_conn)) = member else {
        return Err(expected(BrokerError::not_member()));
    };
    if member_epoch.as_slice() != epoch.id.as_slice() {
        return Err(expected(BrokerError::fenced()));
    }
    if member_conn != identity.connection_id {
        return Err(expected(BrokerError::not_member()));
    }
    Ok((stream, epoch))
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
        StreamRequest::ReplayDls { name, group, seq } => dls_point_op(conn, name, group, *seq, true),
        StreamRequest::DeleteDls { name, group, seq } => dls_point_op(conn, name, group, *seq, false),
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
    let config_json = serde_json::to_string(requested)
        .map_err(|e| fatal("Cannot serialize stream config", e))?;
    sql(
        exec(conn, 
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
        exec(conn, 
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
        exec(conn, 
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

/// Insert one event batch in a single transaction: seqs are assigned in order,
/// key interning is a `stream_keys` read-or-write, and per-key live lanes are
/// updated in place. Keyless items only move the source watermark lazily at
/// fetch time — no per-group fan-out rows are created.
fn publish(
    conn: &Connection,
    name: &str,
    items: &[crate::brokers::stream::domain::message::PubItem],
    ctx: &ExecCtx,
) -> Result<(StreamReply, Effects), CmdError> {
    let mut stream = require_stream(conn, name)?;
    let mut effects = Effects::default();

    // Live epochs for lane fan-out, loaded once per batch.
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
    let group_names: std::collections::HashMap<[u8; 16], String> = {
        let mut map = std::collections::HashMap::new();
        let rows = query_vec(
            conn,
            "SELECT id, name FROM groups WHERE stream_id=?1",
            [stream.id.as_slice()],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)),
            "Cannot list groups",
        )?;
        for (id, name) in rows {
            map.insert(blob16(&id, "group id")?, name);
        }
        map
    };
    let epoch_group: std::collections::HashMap<[u8; 16], String> = live_epochs
        .iter()
        .filter_map(|e| group_names.get(&e.group_id).map(|n| (e.id, n.clone())))
        .collect();

    let mut seqs = Vec::with_capacity(items.len());
    let mut logical_added: u64 = 0;
    let mut dirty_epochs: std::collections::HashMap<[u8; 16], usize> =
        std::collections::HashMap::new();

    for item in items {
        let seq = stream.last_seq.checked_add(1).ok_or_else(|| {
            CmdError::Fatal(BrokerError::storage("Stream sequence exhausted"))
        })?;
        stream.last_seq = seq;
        seqs.push(seq);

        let key_id = if item.key.is_empty() {
            0
        } else {
            intern_key(conn, &mut stream, &item.key)?
        };
        let key_pos = bump_key_pos(conn, &stream.id, key_id)?;
        logical_added += event_logical_bytes(item.key.len(), item.payload.len());
        sql(
            exec(conn, 
                "INSERT INTO events(stream_id, seq, key_id, key_pos, timestamp_ms, payload,
                                    payload_bytes, logical_bytes)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    stream.id.as_slice(),
                    encode_u64(seq).as_slice(),
                    key_id,
                    encode_u64(key_pos).as_slice(),
                    ctx.now_ms as i64,
                    item.payload.as_ref(),
                    item.payload.len() as i64,
                    event_logical_bytes(item.key.len(), item.payload.len()) as i64,
                ],
            ),
            "Cannot insert event",
        )?;

        if key_id == 0 {
            // Keyless: the epoch source cursor admits lazily; a wake for every
            // group is staged once after the batch.
            continue;
        }

        for (idx, epoch) in live_epochs.iter_mut().enumerate() {
            dirty_epochs.insert(epoch.id, idx);
            let lane = match load_lane(conn, &epoch.id, key_id)? {
                Some(lane) => Some(lane),
                None => ensure_lane(conn, epoch, key_id, key_pos)?,
            };
            let Some(lane) = lane else { continue };
            match LaneState::from_i64(lane.state) {
                Some(LaneState::Parked) => {
                    // The open interval already covers the new position;
                    // populate first_seq lazily where it was still empty.
                    sql(
                        exec(conn, 
                            "UPDATE dls_ranges SET first_seq=?4
                             WHERE epoch_id=?1 AND key_id=?2 AND first_seq IS NULL
                               AND first_pos<=?3 AND (last_pos IS NULL OR ?3<=last_pos)",
                            params![
                                epoch.id.as_slice(),
                                key_id,
                                encode_u64(key_pos).as_slice(),
                                encode_u64(seq).as_slice(),
                            ],
                        ),
                        "Cannot populate DLS first_seq",
                    )?;
                }
                Some(LaneState::Empty) => {
                    if recompute_lane(conn, epoch, &lane)? {
                        if let Some(g) = epoch_group.get(&epoch.id) {
                            effects.wake(name, g);
                        }
                    }
                }
                Some(LaneState::Ready) | Some(LaneState::Leased) => {
                    if lane.normal_seq.is_none() {
                        sql(
                            exec(conn, 
                                "UPDATE key_lanes SET normal_seq=?3
                                 WHERE epoch_id=?1 AND key_id=?2",
                                params![
                                    epoch.id.as_slice(),
                                    key_id,
                                    encode_u64(seq).as_slice()
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

    sql(
        exec(conn, 
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
    for idx in dirty_epochs.values() {
        save_epoch(conn, &live_epochs[*idx])?;
    }
    Ok((StreamReply::Published(seqs), effects))
}

/// Read-or-insert the key row; the keyless row is never interned.
fn intern_key(conn: &Connection, stream: &mut StreamRow, key: &Bytes) -> R<i64> {
    let found = conn
        .query_row(
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
        exec(conn, 
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

/// Advance `last_pos` for a key and return the new position.
fn bump_key_pos(conn: &Connection, stream_id: &[u8; 16], key_id: i64) -> R<u64> {
    let row = conn
        .query_row(
            "SELECT last_pos FROM stream_keys WHERE stream_id=?1 AND key_id=?2",
            params![stream_id.as_slice(), key_id],
            |r| blob8(r, 0, "last_pos"),
        )
        .optional()
        .map_err(|e| fatal("Cannot load key row", e))?;
    let Some(last_pos) = row else {
        return Err(CmdError::Fatal(BrokerError::storage(
            "Corrupt storage: key row missing",
        )));
    };
    let new_pos = last_pos
        .checked_add(1)
        .ok_or_else(|| fatal("Key position space exhausted", ""))?;
    sql(
        exec(conn, 
            "UPDATE stream_keys SET last_pos=?3 WHERE stream_id=?1 AND key_id=?2",
            params![
                stream_id.as_slice(),
                key_id,
                encode_u64(new_pos).as_slice()
            ],
        ),
        "Cannot advance key position",
    )?;
    Ok(new_pos)
}

fn read(conn: &Connection, name: &str, from_seq: u64, limit: usize) -> Result<(StreamReply, Effects), CmdError> {
    let stream = require_stream(conn, name)?;
    let rows = query_vec(
        conn,
        "SELECT e.seq, e.timestamp_ms, k.key, e.payload
         FROM events e JOIN stream_keys k ON k.stream_id=e.stream_id AND k.key_id=e.key_id
         WHERE e.stream_id=?1 AND e.seq>=?2 ORDER BY e.seq LIMIT ?3",
        params![stream.id.as_slice(), encode_u64(from_seq).as_slice(), limit as i64],
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

fn epoch_keyless_boundary(
    conn: &Connection,
    stream_id: &[u8; 16],
    start_after: u64,
) -> R<u64> {
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
        exec(conn, 
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
        exec(conn, 
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
    let min_lane: Option<u64> = conn
        .query_row(
            "SELECT MIN(normal_seq) FROM key_lanes WHERE epoch_id=?1 AND normal_seq IS NOT NULL
             AND state<>3",
            [epoch.id.as_slice()],
            |r| r.get::<_, Option<Vec<u8>>>(0),
        )
        .map_err(|e| fatal("Cannot compute ack floor", e))?
        .map(|b| decode_u64(&b))
        .transpose()
        .map_err(|e| CmdError::Fatal(e))?;
    let min_keyless_delivery: Option<u64> = conn
        .query_row(
            "SELECT MIN(seq) FROM keyless_deliveries WHERE epoch_id=?1 AND origin=0",
            [epoch.id.as_slice()],
            |r| r.get::<_, Option<Vec<u8>>>(0),
        )
        .map_err(|e| fatal("Cannot compute ack floor", e))?
        .map(|b| decode_u64(&b))
        .transpose()
        .map_err(|e| CmdError::Fatal(e))?;
    let next_keyless_fresh: Option<u64> = conn
        .query_row(
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
        exec(conn, 
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

    struct LaneCand {
        ticket: i64,
        key_id: i64,
        head_seq: u64,
    }
    struct RetryCand {
        ticket: i64,
        seq: u64,
    }

    let mut lanes: std::collections::VecDeque<LaneCand> = query_vec(
        conn,
        "SELECT ready_ticket, key_id, head_seq
         FROM key_lanes WHERE epoch_id=?1 AND state=1
         ORDER BY ready_ticket, key_id LIMIT ?2",
        params![epoch.id.as_slice(), (credit + 1) as i64],
        |r| {
            Ok(LaneCand {
                ticket: r.get(0)?,
                key_id: r.get(1)?,
                head_seq: blob8(r, 2, "head_seq")?,
            })
        },
        "Cannot scan ready lanes",
    )?
    .into();
    let mut retries: std::collections::VecDeque<RetryCand> = query_vec(
        conn,
        "SELECT ready_ticket, seq FROM keyless_deliveries
         WHERE epoch_id=?1 AND state=1 ORDER BY seq LIMIT ?2",
        params![epoch.id.as_slice(), (credit + 1) as i64],
        |r| {
            Ok(RetryCand {
                ticket: r.get(0)?,
                seq: blob8(r, 1, "seq")?,
            })
        },
        "Cannot scan keyless retries",
    )?
    .into();

    let deadline = (ctx.now_ms + stream.config.ack_wait_ms) as i64;
    let mut deliveries: Vec<Delivery> = Vec::with_capacity(credit);
    let mut used_bytes: u64 = 4; // FETCH response count prefix
    let mut claimed = 0usize;

    while claimed < credit && used_bytes < ctx.fetch_response_bytes {
        // Source candidate: next keyless event after the epoch watermark.
        let source = next_event_pos(conn, &epoch.stream_id, 0, epoch.keyless_cursor)?;
        let source_ticket = source.map(|_| epoch.keyless_ticket).unwrap_or(i64::MAX);
        let lane_ticket = lanes.front().map(|c| c.ticket).unwrap_or(i64::MAX);
        // Retries sit below the keyless cursor, so their seq always precedes
        // the next fresh event: the earlier of the two keyless tickets claims
        // the slot, and pending retries win over the source on ties.
        let retry_ticket = retries.front().map(|c| c.ticket).unwrap_or(i64::MAX);
        let keyless_ticket = retry_ticket.min(source_ticket);

        // The chosen claim: seq to deliver plus the row to mutate.
        enum Choice {
            Lane { key_id: i64 },
            Retry { seq: u64 },
            Source { seq: u64, key_pos: u64 },
        }
        let (event_seq, choice) = if lane_ticket != i64::MAX && lane_ticket <= keyless_ticket {
            let c = lanes.pop_front().unwrap();
            (
                c.head_seq,
                Choice::Lane {
                    key_id: c.key_id,
                },
            )
        } else if let Some(c) = retries.front() {
            let seq = c.seq;
            retries.pop_front();
            (seq, Choice::Retry { seq })
        } else if source_ticket != i64::MAX {
            let (pos, seq) = source.unwrap();
            (seq, Choice::Source { seq, key_pos: pos })
        } else {
            break;
        };

        // Load the immutable event + key before claiming so the byte budget
        // is checked against what would actually be encoded.
        let event = conn
            .query_row(
                "SELECT e.timestamp_ms, e.payload, k.key FROM events e
                 JOIN stream_keys k ON k.stream_id=e.stream_id AND k.key_id=e.key_id
                 WHERE e.stream_id=?1 AND e.seq=?2",
                params![epoch.stream_id.as_slice(), encode_u64(event_seq).as_slice()],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| fatal("Cannot load event for delivery", e))?;
        let Some((timestamp_ms, payload, key)) = event else {
            // Retention removed the row between candidate scan and claim;
            // the lease tables keep no stale reference after normalization.
            continue;
        };
        let item_bytes = fetch_item_encoded_bytes(key.len(), payload.len());
        if used_bytes + item_bytes > ctx.fetch_response_bytes {
            if deliveries.is_empty() {
                return Err(expected(BrokerError::invalid_argument(format!(
                    "Single stream event ({} bytes) exceeds the fetch response budget ({} bytes)",
                    item_bytes, ctx.fetch_response_bytes
                ))));
            }
            break;
        }

        let receipt = Uuid::new_v4().into_bytes();
        match choice {
            Choice::Lane { key_id } => {
                let changed = sql(
                    exec(conn, 
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
            }
            Choice::Retry { seq } => {
                let changed = sql(
                    exec(conn, 
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
            }
            Choice::Source { seq, key_pos } => {
                // Admission creates the delivery row already leased to this
                // consumer; the epoch watermark moves past the position and
                // the source re-queues at the back of the ticket order.
                sql(
                    exec(conn, 
                        "INSERT INTO keyless_deliveries(epoch_id, stream_id, seq, key_pos, origin,
                            state, attempts, ready_ticket, receipt, owner, connection_id,
                            deadline_ms)
                         VALUES (?1,?2,?3,?4,0,2,1,0,?5,?6,?7,?8)",
                        params![
                            epoch.id.as_slice(),
                            epoch.stream_id.as_slice(),
                            encode_u64(seq).as_slice(),
                            encode_u64(key_pos).as_slice(),
                            receipt.as_slice(),
                            identity.consumer_id,
                            identity.connection_id,
                            deadline,
                        ],
                    ),
                    "Cannot admit keyless delivery",
                )?;
                epoch.keyless_cursor = key_pos;
                epoch.keyless_ticket = take_ticket(&mut epoch)?;
            }
        }

        used_bytes += item_bytes;
        claimed += 1;
        deliveries.push(Delivery {
            message: Message {
                seq: event_seq,
                timestamp: timestamp_ms as u64,
                key: Bytes::from(key),
                payload: Bytes::from(payload),
            },
            receipt,
        });
    }

    epoch.pending += claimed as i64;
    save_epoch(conn, &epoch)?;
    Ok((StreamReply::Fetch(deliveries), Effects::default()))
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

    // Keyed lease: located by (stream, head_seq) then filtered to this epoch.
    let lane = query_vec(
        conn,
        "SELECT key_id FROM key_lanes
         WHERE stream_id=?1 AND head_seq=?2 AND epoch_id=?3 AND state=2",
        params![
            epoch.stream_id.as_slice(),
            encode_u64(seq).as_slice(),
            epoch.id.as_slice()
        ],
        |r| r.get::<_, i64>(0),
        "Cannot locate lease",
    )?
    .into_iter()
    .next()
    .map(|key_id| load_lane(conn, &epoch.id, key_id))
    .transpose()?
    .flatten();

    if let Some(lane) = lane {
        let stored = lane.receipt.as_deref().unwrap_or(&[]);
        if stored != receipt.as_slice() {
            return Err(expected(BrokerError::fenced()));
        }
        epoch.pending -= 1;
        let origin = lane.head_origin.unwrap_or(Origin::Original as i64);
        let head_pos = lane.head_pos.ok_or_else(|| {
            CmdError::Fatal(BrokerError::storage("Corrupt storage: leased lane without head"))
        })?;
        if origin == Origin::Replay as i64 {
            // The delivery obligation is consumed only when it is acked.
            sql(
                exec(conn, 
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
            // Original progress: the resolved position becomes the new cursor.
            lane_refresh_cursor(conn, &epoch.id, lane.key_id, head_pos)?;
        }
        let refreshed = load_lane(conn, &epoch.id, lane.key_id)?.ok_or_else(|| {
            CmdError::Fatal(BrokerError::storage("Corrupt storage: lane vanished on ack"))
        })?;
        if recompute_lane(conn, &mut epoch, &refreshed)? {
            effects.wake(name, group_name);
        }
        save_epoch(conn, &epoch)?;
        // Credit freed: pending fetches retry even when the lane went EMPTY.
        effects.wake(name, group_name);
        return Ok((StreamReply::Unit, effects));
    }

    // Keyless lease: primary-key lookup on the epoch table.
    let keyless = conn
        .query_row(
            "SELECT receipt, state FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2",
            params![epoch.id.as_slice(), encode_u64(seq).as_slice()],
            |r| Ok((r.get::<_, Option<Vec<u8>>>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|e| fatal("Cannot locate keyless lease", e))?;
    match keyless {
        Some((Some(stored), 2)) if stored.as_slice() == receipt.as_slice() => {
            sql(
                exec(conn, 
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

/// Rewrite the cursor column in place (helper split out for readability).
fn lane_refresh_cursor(conn: &Connection, epoch_id: &[u8; 16], key_id: i64, cursor: u64) -> R<()> {
    sql(
        exec(conn, 
            "UPDATE key_lanes SET cursor_pos=?3 WHERE epoch_id=?1 AND key_id=?2",
            params![epoch_id.as_slice(), key_id, encode_u64(cursor).as_slice()],
        ),
        "Cannot advance lane cursor",
    )?;
    Ok(())
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
        exec(conn, 
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
            exec(conn, 
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
    let row = conn
        .query_row(
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
    query_one(conn, 
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
    let mut effects = Effects::default();
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
                exec(conn, 
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
            let (_group, epoch) =
                create_group_and_epoch(conn, &stream, group_name, start_after)?;
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
            Ok((blob8(r, 0, "seq")?, reason_kind, r.get::<_, i64>(2)?, r.get::<_, Vec<u8>>(3)?))
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
    let result = query_one(conn, 
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
fn remove_dls_point(
    conn: &Connection,
    epoch: &EpochRow,
    key_id: i64,
    pos: u64,
) -> R<bool> {
    let Some(range) = find_range(conn, &epoch.id, key_id, pos)? else {
        return Ok(false);
    };
    let reason = DlsReason::from_i64(range.reason).unwrap_or(DlsReason::MaxDeliveries);
    sql(
        exec(conn, 
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
        insert_range(conn, epoch, key_id, pos + 1, range.last_pos, reason, range.attempts)?;
    }
    Ok(true)
}

/// A parked lane resumes when no retained DLS member remains for the key:
/// the original cursor jumps to the key tail covered by parking and the head
/// is rebuilt from remaining replay intents. Returns true if it became READY.
fn try_unpark_lane(
    conn: &Connection,
    epoch: &mut EpochRow,
    key_id: i64,
) -> R<bool> {
    let Some(lane) = load_lane(conn, &epoch.id, key_id)? else {
        return Ok(false);
    };
    if lane.state != LaneState::Parked as i64 {
        return Ok(false);
    }
    let has_members: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM dls_ranges WHERE epoch_id=?1 AND key_id=?2
             AND first_seq IS NOT NULL",
            params![epoch.id.as_slice(), key_id],
            |r| r.get::<_, i64>(0),
        )
        .map_err(|e| fatal("Cannot check DLS membership", e))?
        > 0;
    if has_members {
        return Ok(false);
    }
    // All ranges of this key are memberless: drop them and resume originals.
    sql(
        exec(conn, 
            "DELETE FROM dls_ranges WHERE epoch_id=?1 AND key_id=?2",
            params![epoch.id.as_slice(), key_id],
        ),
        "Cannot clear memberless ranges",
    )?;
    let tail = key_tail_pos(conn, &epoch.stream_id, key_id)?;
    sql(
        exec(conn, 
            "UPDATE key_lanes SET cursor_pos=?3 WHERE epoch_id=?1 AND key_id=?2",
            params![epoch.id.as_slice(), key_id, encode_u64(tail).as_slice()],
        ),
        "Cannot advance parked cursor",
    )?;
    let lane = load_lane(conn, &epoch.id, key_id)?.ok_or_else(|| {
        CmdError::Fatal(BrokerError::storage("Corrupt storage: lane vanished on unpark"))
    })?;
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
    let event = conn
        .query_row(
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
                exec(conn, 
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
                exec(conn, 
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

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM dls_ranges r
             JOIN events e ON e.stream_id=r.stream_id AND e.key_id=r.key_id
                 AND e.key_pos >= r.first_pos AND (r.last_pos IS NULL OR e.key_pos <= r.last_pos)
             WHERE r.epoch_id=?1",
            [epoch.id.as_slice()],
            |r| r.get(0),
        )
        .map_err(|e| fatal("Cannot count DLS members", e))?;

    sql(
        exec(conn, 
            "DELETE FROM dls_ranges WHERE epoch_id=?1",
            [epoch.id.as_slice()],
        ),
        "Cannot purge DLS",
    )?;

    let parked: Vec<i64> = query_vec(
        conn,
        "SELECT key_id FROM key_lanes WHERE epoch_id=?1 AND state=3",
        [epoch.id.as_slice()],
        |r| r.get::<_, i64>(0),
        "Cannot scan parked lanes",
    )?;
    let mut resumed = false;
    for key_id in parked {
        let tail = key_tail_pos(conn, &epoch.stream_id, key_id)?;
        sql(
            exec(conn, 
                "UPDATE key_lanes SET cursor_pos=?3 WHERE epoch_id=?1 AND key_id=?2",
                params![epoch.id.as_slice(), key_id, encode_u64(tail).as_slice()],
            ),
            "Cannot advance parked cursor",
        )?;
        let lane = load_lane(conn, &epoch.id, key_id)?.ok_or_else(|| {
            CmdError::Fatal(BrokerError::storage("Corrupt storage: lane vanished on purge"))
        })?;
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

    let due_lanes: Vec<(Vec<u8>, i64)> = query_vec(
        conn,
        "SELECT epoch_id, key_id FROM key_lanes
         WHERE state=2 AND deadline_ms<=?1 ORDER BY deadline_ms LIMIT ?2",
        params![now_ms as i64, BATCH_LIMIT as i64],
        |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)),
        "Cannot scan due lanes",
    )?;
    let more_lanes = due_lanes.len() == BATCH_LIMIT;
    for (epoch_id, key_id) in due_lanes {
        let Ok(epoch_id) = <[u8; 16]>::try_from(epoch_id.as_slice()) else {
            continue;
        };
        if !epoch_cache.contains_key(&epoch_id) {
            if let Some(e) = load_epoch(conn, &epoch_id)? {
                epoch_cache.insert(epoch_id, e);
            }
        }
        if !epoch_cache.contains_key(&epoch_id) {
            continue;
        }
        let stream_id = epoch_cache[&epoch_id].stream_id;
        if !config_cache.contains_key(&stream_id) {
            if let Ok(cfg) = load_config_for_stream(conn, &stream_id) {
                config_cache.insert(stream_id, cfg);
            }
        }
        let Some(config) = config_cache.get(&stream_id) else {
            continue;
        };
        if !name_cache.contains_key(&epoch_id) {
            if let Ok(n) = group_stream_names(conn, &epoch_cache[&epoch_id]) {
                name_cache.insert(epoch_id, n);
            }
        }
        let Some(lane) = load_lane(conn, &epoch_id, key_id)? else {
            continue;
        };
        if lane.state != LaneState::Leased as i64 {
            continue;
        }
        let epoch = epoch_for!(&epoch_id);
        release_lane_lease(conn, epoch, &lane, config.max_deliveries, true)?;
        if let Some((sname, gname)) = name_cache.get(&epoch_id) {
            effects.wake(&sname.clone(), &gname.clone());
        }
    }

    let due_keyless: Vec<(Vec<u8>, Vec<u8>)> = query_vec(
        conn,
        "SELECT epoch_id, seq FROM keyless_deliveries
         WHERE state=2 AND deadline_ms<=?1 ORDER BY deadline_ms LIMIT ?2",
        params![now_ms as i64, BATCH_LIMIT as i64],
        |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
        "Cannot scan due keyless leases",
    )?;
    let more_keyless = due_keyless.len() == BATCH_LIMIT;
    for (epoch_id, seq_blob) in due_keyless {
        let Ok(epoch_id) = <[u8; 16]>::try_from(epoch_id.as_slice()) else {
            continue;
        };
        let seq = decode_u64(&seq_blob).map_err(CmdError::Fatal)?;
        if !epoch_cache.contains_key(&epoch_id) {
            if let Some(e) = load_epoch(conn, &epoch_id)? {
                epoch_cache.insert(epoch_id, e);
            }
        }
        if !epoch_cache.contains_key(&epoch_id) {
            continue;
        }
        let stream_id = epoch_cache[&epoch_id].stream_id;
        if !config_cache.contains_key(&stream_id) {
            if let Ok(cfg) = load_config_for_stream(conn, &stream_id) {
                config_cache.insert(stream_id, cfg);
            }
        }
        let Some(config) = config_cache.get(&stream_id) else {
            continue;
        };
        if !name_cache.contains_key(&epoch_id) {
            if let Ok(n) = group_stream_names(conn, &epoch_cache[&epoch_id]) {
                name_cache.insert(epoch_id, n);
            }
        }
        let key_pos = conn
            .query_row(
                "SELECT key_pos, attempts FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2 AND state=2",
                params![epoch_id.as_slice(), seq_blob.as_slice()],
                |r| Ok((blob8(r, 0, "key_pos")?, r.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(|e| fatal("Cannot load keyless lease", e))?;
        let Some((key_pos, attempts)) = key_pos else {
            continue;
        };
        let epoch = epoch_for!(&epoch_id);
        release_keyless_lease(
            conn,
            epoch,
            seq,
            key_pos,
            attempts,
            config.max_deliveries,
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
    let boundary_seq = deleted.last().unwrap().0;
    let mut key_deleted_pos: std::collections::HashMap<i64, u64> =
        std::collections::HashMap::new();
    let mut deleted_bytes: u64 = 0;
    for (_, key_id, key_pos, logical) in &deleted {
        *key_deleted_pos.entry(*key_id).or_insert(0) = (*key_deleted_pos
            .get(key_id)
            .unwrap_or(&0))
        .max(*key_pos);
        deleted_bytes += logical;
    }

    let mut dirty_epochs: std::collections::HashMap<[u8; 16], EpochRow> =
        std::collections::HashMap::new();
    let epoch_of = |conn: &Connection,
                        dirty: &mut std::collections::HashMap<[u8; 16], EpochRow>,
                        epoch_id: [u8; 16]| {
        if !dirty.contains_key(&epoch_id) {
            if let Some(e) = load_epoch(conn, &epoch_id)? {
                dirty.insert(epoch_id, e);
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
        if epoch_of(conn, &mut dirty_epochs, epoch_id)?
            && state == LaneState::Leased as i64
        {
            dirty_epochs.get_mut(&epoch_id).unwrap().pending -= 1;
        }
        sql(
            exec(conn, 
                "DELETE FROM keyless_deliveries WHERE epoch_id=?1 AND seq=?2",
                params![epoch_id.as_slice(), seq_blob.as_slice()],
            ),
            "Cannot expire keyless delivery",
        )?;
    }
    sql(
        exec(conn, 
            "DELETE FROM replay_intents WHERE stream_id=?1 AND seq<=?2",
            params![stream.id.as_slice(), encode_u64(boundary_seq).as_slice()],
        ),
        "Cannot expire replay intents",
    )?;

    // Phase C: delete the event prefix and charge the stream.
    sql(
        exec(conn, 
            "DELETE FROM events WHERE stream_id=?1 AND seq<=?2",
            params![stream.id.as_slice(), encode_u64(boundary_seq).as_slice()],
        ),
        "Cannot delete expired events",
    )?;
    sql(
        exec(conn, 
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
            exec(conn, 
                "UPDATE stream_keys SET retained_after_pos=?3
                 WHERE stream_id=?1 AND key_id=?2 AND retained_after_pos<?3",
                params![
                    stream.id.as_slice(),
                    key_id,
                    encode_u64(*dpos).as_slice()
                ],
            ),
            "Cannot advance retained position",
        )?;
    }

    // Phase D: derived state against the post-delete view.
    // Keyless source watermark moves past deleted positions.
    if let Some(dpos0) = key_deleted_pos.get(&0).copied() {
        sql(
            exec(conn, 
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
                    exec(conn, 
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
                    exec(conn, 
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
            let needs_refresh: bool = conn
                .query_row(
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
                    exec(conn, 
                        "UPDATE dls_ranges SET first_seq=?4
                         WHERE epoch_id=?1 AND key_id=?2 AND first_pos=?3",
                        params![
                            epoch_id.as_slice(),
                            key_id,
                            encode_u64(if clipped_front { dpos + 1 } else { first_pos })
                                .as_slice(),
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
            if !dirty_epochs.contains_key(&epoch_id) {
                if let Some(e) = load_epoch(conn, &epoch_id)? {
                    dirty_epochs.insert(epoch_id, e);
                } else {
                    continue;
                }
            }
            let epoch = dirty_epochs.get_mut(&epoch_id).unwrap();
            let Some(mut lane) = load_lane(conn, &epoch_id, *key_id)? else {
                continue;
            };
            if lane.cursor_pos < *dpos {
                sql(
                    exec(conn, 
                        "UPDATE key_lanes SET cursor_pos=?3 WHERE epoch_id=?1 AND key_id=?2",
                        params![
                            epoch_id.as_slice(),
                            key_id,
                            encode_u64(*dpos).as_slice()
                        ],
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
                            exec(conn, 
                                "UPDATE key_lanes SET state=0, head_seq=NULL, head_pos=NULL,
                                    head_origin=NULL, receipt=NULL, owner=NULL,
                                    connection_id=NULL, deadline_ms=NULL
                                 WHERE epoch_id=?1 AND key_id=?2",
                                params![epoch_id.as_slice(), key_id],
                            ),
                            "Cannot retire expired lease",
                        )?;
                        let lane = load_lane(conn, &epoch_id, *key_id)?.unwrap();
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
    let mut processed = 0i64;
    let mut last_key = epoch.init_cursor;
    for (key_id, last_pos) in &keys {
        if processed == INIT_PAGE {
            break;
        }
        processed += 1;
        last_key = *key_id;
        let boundary = key_boundary_pos(conn, &epoch.stream_id, *key_id, epoch.start_after)?;
        // Materialize only lanes with pending originals; a key with nothing to
        // deliver lazily gets its lane on the next publish.
        if boundary < *last_pos && next_event_pos(conn, &epoch.stream_id, *key_id, boundary)?.is_some()
        {
            sql(
                exec(conn, 
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
                if lane.state == LaneState::Empty as i64
                    && recompute_lane(conn, &mut epoch, &lane)?
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
        effects.followups.push(Continuation::InitEpoch {
            epoch_id: epoch.id,
        });
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
    let deleted_stream: Option<Vec<u8>> = conn
        .query_row(
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
                    exec(conn, 
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
                    exec(conn, 
                        "DELETE FROM group_epochs WHERE stream_id=?1 AND group_id=?2",
                        params![stream_id.as_slice(), gid.as_slice()],
                    ),
                    "Cannot GC group epochs",
                )?;
                sql(
                    exec(conn, 
                        "DELETE FROM groups WHERE stream_id=?1 AND id=?2",
                        params![stream_id.as_slice(), gid.as_slice()],
                    ),
                    "Cannot GC group row",
                )?;
            }
            if !remaining_groups {
                sql(
                    exec(conn, 
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
    let orphan: Option<Vec<u8>> = conn
        .query_row(
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
                    exec(conn, 
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
                exec(conn, 
                    "DELETE FROM group_epochs WHERE id=?1",
                    params![epoch_id.as_slice()],
                ),
                "Cannot delete epoch row",
            )?;
        }
        progress = true;
    }

    let pending_deleted: i64 = conn
        .query_row("SELECT COUNT(*) FROM streams WHERE deleted=1", [], |r| r.get(0))
        .map_err(|e| fatal("Cannot count deleted streams", e))?;
    let pending_orphans: i64 = conn
        .query_row(
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

        exec(&conn, 
            "INSERT INTO streams VALUES (?1,'s',1,'{}',?2,?2,0,0)",
            params![
                stream_id.as_slice(),
                encode_u64(0).as_slice()
            ],
        )
        .unwrap();
        // The keyless key row (key_id=0, empty key) required by the schema.
        exec(&conn, 
            "INSERT INTO stream_keys(stream_id,key_id,key,last_pos,retained_after_pos)
             VALUES (?1,0,'',?2,?2)",
            params![stream_id.as_slice(), encode_u64(0).as_slice()],
        )
        .unwrap();
        {
            // groups.active_epoch → group_epochs is DEFERRED: the pair must
            // be inserted inside one transaction.
            let tx = conn.transaction().unwrap();
            exec(&tx, 
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
            exec(&tx, 
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
            exec(&conn, 
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
            exec(&conn, 
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
            exec(&conn, 
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
}

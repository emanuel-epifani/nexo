//! Stream broker TCP surface: opcodes, request parsing, response encoding.
//!
//! Split-phase dispatch: `submit()` parses and enqueues on the connection's
//! read path (preserving command order and applying admission backpressure),
//! while `StreamCall::complete()` runs on a spawned task that awaits the
//! writer's post-commit reply and encodes it. The reader never blocks on a
//! SQLite commit.

use bytes::Bytes;

use crate::brokers::stream::domain::definition::StreamDefinition;
use crate::brokers::stream::domain::message::{
    BatchDisposition, BatchUpdate, DlsEntry, FetchLimits, FetchPage, PubItem,
};
use crate::brokers::stream::domain::ops::{StreamReply, StreamRequest};
use crate::brokers::stream::domain::types::FETCH_PAGE_HEADER_BYTES;
use crate::brokers::stream::manager::PendingReply;
use crate::brokers::stream::options::{RetentionOptions, SeekTarget, StreamCreateOptions};
use crate::brokers::{BrokerErrorKind, ProvisionOutcome, ProvisionResult};
use crate::protocol::wire::{PayloadCursor, PayloadWriter};
use crate::protocol::{
    ErrorCode, ParseError, ProvisionStatus, Response, FLAG_STREAM_S_CREATE_HAS_MAX_AGE,
    FLAG_STREAM_S_CREATE_HAS_MAX_BYTES, FLAG_STREAM_S_UPDATE_BATCH_RELEASE,
    FLAG_STREAM_S_UPDATE_BATCH_RETRY,
};
use crate::transport::tcp::error_response;
use crate::NexoEngine;

// ==========================================
// OPCODES
// ==========================================

use crate::protocol::STREAM_MAX_FETCH_BATCH_SIZE;
use crate::protocol::STREAM_MAX_KEY_BATCH_SIZE;
use crate::protocol::STREAM_MAX_PUBLISH_BATCH as MAX_PUBLISH_BATCH;
pub use crate::protocol::{
    OP_S_CREATE, OP_S_DELETE, OP_S_DELETE_DLS, OP_S_DESCRIBE, OP_S_EXISTS, OP_S_FETCH, OP_S_JOIN,
    OP_S_LEAVE, OP_S_MOVE_TO_STREAM, OP_S_PEEK_DLS, OP_S_PUB, OP_S_PURGE_DLS, OP_S_SEEK,
    OP_S_UPDATE_BATCH, OP_S_UPDATE_BATCHES, STREAM_OPCODE_MAX as OPCODE_MAX,
    STREAM_OPCODE_MIN as OPCODE_MIN,
};
const MIN_PUBLISH_ITEM_BYTES: usize = 6;
const MIN_UPDATE_ITEM_BYTES: usize = 21;

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
pub enum StreamCommand {
    Create {
        name: String,
        options: StreamCreateOptions,
    },
    Publish {
        name: String,
        items: Vec<PubItem>,
    },
    Fetch {
        name: String,
        group: String,
        consumer_id: String,
        epoch: [u8; 16],
        limits: FetchLimits,
        wait_ms: u32,
    },
    Join {
        name: String,
        group: String,
    },
    UpdateBatch {
        name: String,
        group: String,
        consumer_id: String,
        epoch: [u8; 16],
        update: BatchUpdate,
    },
    /// Bulk update: one frame carries N cumulative batch updates so a fetch
    /// page is settled in a single round trip instead of N awaits.
    UpdateBatches {
        name: String,
        group: String,
        consumer_id: String,
        epoch: [u8; 16],
        updates: Vec<BatchUpdate>,
    },
    Seek {
        name: String,
        group: String,
        target: SeekTarget,
    },
    Exists {
        name: String,
    },
    Describe {
        name: String,
    },
    Delete {
        name: String,
    },
    Leave {
        name: String,
        group: String,
        consumer_id: String,
        epoch: [u8; 16],
    },
    PeekDls {
        name: String,
        group: String,
        limit: u32,
        offset: u32,
    },
    MoveToStream {
        name: String,
        group: String,
        seq: u64,
    },
    DeleteDls {
        name: String,
        group: String,
        seq: u64,
    },
    PurgeDls {
        name: String,
        group: String,
    },
}

impl StreamCommand {
    pub fn parse(opcode: u8, cursor: &mut PayloadCursor) -> Result<Self, ParseError> {
        match opcode {
            OP_S_CREATE => {
                let name = cursor.read_string()?;
                let flags = cursor.read_u8()?;
                let max_age_ms = if flags & FLAG_STREAM_S_CREATE_HAS_MAX_AGE != 0 {
                    Some(cursor.read_u64()?)
                } else {
                    None
                };
                let max_bytes = if flags & FLAG_STREAM_S_CREATE_HAS_MAX_BYTES != 0 {
                    Some(cursor.read_u64()?)
                } else {
                    None
                };
                let retention = if max_age_ms.is_some() || max_bytes.is_some() {
                    Some(RetentionOptions {
                        max_age_ms,
                        max_bytes,
                    })
                } else {
                    None
                };
                Ok(Self::Create {
                    name,
                    options: StreamCreateOptions { retention },
                })
            }
            OP_S_PUB => {
                let name = cursor.read_string()?;
                let count = cursor.read_u32()? as usize;
                if count > MAX_PUBLISH_BATCH {
                    return Err(ParseError::Invalid(format!(
                        "Publish batch too large: {} items (max: {})",
                        count, MAX_PUBLISH_BATCH
                    )));
                }
                if count > cursor.len() / MIN_PUBLISH_ITEM_BYTES {
                    return Err(ParseError::Invalid(format!(
                        "Publish batch count {} exceeds remaining payload",
                        count
                    )));
                }
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let key_len = cursor.read_u16()? as usize;
                    let key = if key_len > 0 {
                        cursor.read_bytes(key_len)?
                    } else {
                        Bytes::new()
                    };
                    let payload_len = cursor.read_u32()? as usize;
                    let payload = cursor.read_bytes(payload_len)?;
                    items.push(PubItem { key, payload });
                }
                Ok(Self::Publish { name, items })
            }
            OP_S_FETCH => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                let consumer_id = cursor.read_string()?;
                let epoch = cursor.read_uuid_bytes()?;
                let max_messages = cursor.read_u32()?;
                let max_key_messages = cursor.read_u32()?;
                let max_key_batches = cursor.read_u32()?;
                let max_bytes = cursor.read_u32()?;
                let wait_ms = cursor.read_u32()?;
                if max_messages as usize > STREAM_MAX_FETCH_BATCH_SIZE {
                    return Err(ParseError::Invalid(format!(
                        "Fetch max_messages {max_messages} exceeds {STREAM_MAX_FETCH_BATCH_SIZE}"
                    )));
                }
                if max_key_messages == 0 || max_key_messages as usize > STREAM_MAX_KEY_BATCH_SIZE
                {
                    return Err(ParseError::Invalid(format!(
                        "Fetch max_key_messages must be 1..={STREAM_MAX_KEY_BATCH_SIZE}"
                    )));
                }
                if max_key_batches > max_messages {
                    return Err(ParseError::Invalid(
                        "Fetch max_key_batches exceeds max_messages".to_string(),
                    ));
                }
                if (max_bytes as u64) < FETCH_PAGE_HEADER_BYTES {
                    return Err(ParseError::Invalid(format!(
                        "Fetch max_bytes must be at least {FETCH_PAGE_HEADER_BYTES}"
                    )));
                }
                Ok(Self::Fetch {
                    name,
                    group,
                    consumer_id,
                    epoch,
                    limits: FetchLimits {
                        max_messages: max_messages as usize,
                        max_key_messages: max_key_messages as usize,
                        max_key_batches: max_key_batches as usize,
                        max_bytes: max_bytes as usize,
                    },
                    wait_ms,
                })
            }
            OP_S_JOIN => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                Ok(Self::Join { name, group })
            }
            OP_S_UPDATE_BATCH => {
                let (name, group, consumer_id, epoch) = read_identity(cursor)?;
                let update = read_batch_update(cursor)?;
                Ok(Self::UpdateBatch {
                    name,
                    group,
                    consumer_id,
                    epoch,
                    update,
                })
            }
            OP_S_UPDATE_BATCHES => {
                let (name, group, consumer_id, epoch) = read_identity(cursor)?;
                let count = cursor.read_u32()? as usize;
                if count > STREAM_MAX_FETCH_BATCH_SIZE
                    || count > cursor.len() / MIN_UPDATE_ITEM_BYTES
                {
                    return Err(ParseError::Invalid(format!(
                        "Update batch count {count} exceeds remaining payload"
                    )));
                }
                let mut updates = Vec::with_capacity(count);
                for _ in 0..count {
                    updates.push(read_batch_update(cursor)?);
                }
                Ok(Self::UpdateBatches {
                    name,
                    group,
                    consumer_id,
                    epoch,
                    updates,
                })
            }
            OP_S_SEEK => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                let target_byte = cursor.read_u8()?;
                let target = match target_byte {
                    0 => SeekTarget::Beginning,
                    1 => SeekTarget::End,
                    _ => {
                        return Err(ParseError::Invalid(format!(
                            "Invalid seek target: {}",
                            target_byte
                        )))
                    }
                };
                Ok(Self::Seek {
                    name,
                    group,
                    target,
                })
            }
            OP_S_EXISTS => {
                let name = cursor.read_string()?;
                Ok(Self::Exists { name })
            }
            OP_S_DESCRIBE => {
                let name = cursor.read_string()?;
                Ok(Self::Describe { name })
            }
            OP_S_DELETE => {
                let name = cursor.read_string()?;
                Ok(Self::Delete { name })
            }
            OP_S_LEAVE => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                let consumer_id = cursor.read_string()?;
                let epoch = cursor.read_uuid_bytes()?;
                Ok(Self::Leave {
                    name,
                    group,
                    consumer_id,
                    epoch,
                })
            }
            OP_S_PEEK_DLS => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                let limit = cursor.read_u32()?;
                let offset = cursor.read_u32()?;
                Ok(Self::PeekDls {
                    name,
                    group,
                    limit,
                    offset,
                })
            }
            OP_S_MOVE_TO_STREAM => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                let seq = cursor.read_u64()?;
                Ok(Self::MoveToStream { name, group, seq })
            }
            OP_S_DELETE_DLS => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                let seq = cursor.read_u64()?;
                Ok(Self::DeleteDls { name, group, seq })
            }
            OP_S_PURGE_DLS => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                Ok(Self::PurgeDls { name, group })
            }
            _ => Err(ParseError::Invalid(format!(
                "Unknown Stream opcode: 0x{:02X}",
                opcode
            ))),
        }
    }
}

fn read_identity(
    cursor: &mut PayloadCursor,
) -> Result<(String, String, String, [u8; 16]), ParseError> {
    Ok((
        cursor.read_string()?,
        cursor.read_string()?,
        cursor.read_string()?,
        cursor.read_uuid_bytes()?,
    ))
}

fn read_batch_update(cursor: &mut PayloadCursor) -> Result<BatchUpdate, ParseError> {
    let receipt = cursor.read_uuid_bytes()?;
    let completed = cursor.read_u32()?;
    let flags = cursor.read_u8()?;
    let known = FLAG_STREAM_S_UPDATE_BATCH_RETRY | FLAG_STREAM_S_UPDATE_BATCH_RELEASE;
    if flags & !known != 0 {
        return Err(ParseError::Invalid(format!(
            "Invalid update flags: 0x{flags:02X}"
        )));
    }
    let disposition = match flags {
        0 => BatchDisposition::Checkpoint,
        f if f == FLAG_STREAM_S_UPDATE_BATCH_RETRY => BatchDisposition::Retry,
        f if f == FLAG_STREAM_S_UPDATE_BATCH_RELEASE => BatchDisposition::Release,
        _ => {
            return Err(ParseError::Invalid(format!(
                "Invalid update flags: 0x{flags:02X}"
            )))
        }
    };
    Ok(BatchUpdate {
        receipt,
        completed,
        disposition,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_parser_rejects_huge_count_before_allocation() {
        let mut writer = PayloadWriter::new();
        writer.put_str("name").put_u32(u32::MAX);
        let mut cursor = PayloadCursor::new(writer.into_bytes());

        let error = StreamCommand::parse(OP_S_PUB, &mut cursor).unwrap_err();

        assert!(error.to_string().contains("Publish batch too large"));
    }

    #[test]
    fn publish_parser_rejects_count_larger_than_payload() {
        let mut writer = PayloadWriter::new();
        writer.put_str("name").put_u32(2);
        let mut cursor = PayloadCursor::new(writer.into_bytes());

        let error = StreamCommand::parse(OP_S_PUB, &mut cursor).unwrap_err();

        assert!(error.to_string().contains("exceeds remaining payload"));
    }

    #[test]
    fn update_parser_reads_receipt_and_disposition() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_uuid(&[0xAB; 16])
            .put_u32(9)
            .put_u8(FLAG_STREAM_S_UPDATE_BATCH_RETRY);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        match StreamCommand::parse(OP_S_UPDATE_BATCH, &mut cursor).unwrap() {
            StreamCommand::UpdateBatch { update, .. } => {
                assert_eq!(update.receipt, [0xAB; 16]);
                assert_eq!(update.completed, 9);
                assert_eq!(update.disposition, BatchDisposition::Retry);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn update_parser_rejects_truncated_receipt() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_uuid(&[0xAB; 16])
            .put_u32(9);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        assert!(StreamCommand::parse(OP_S_UPDATE_BATCH, &mut cursor).is_err());
    }

    #[test]
    fn update_parser_rejects_combined_flags() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_uuid(&[0xAB; 16])
            .put_u32(9)
            .put_u8(FLAG_STREAM_S_UPDATE_BATCH_RETRY | FLAG_STREAM_S_UPDATE_BATCH_RELEASE);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        assert!(StreamCommand::parse(OP_S_UPDATE_BATCH, &mut cursor).is_err());
    }

    #[test]
    fn update_batches_parser_reads_items() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_u32(2)
            .put_uuid(&[0xAB; 16])
            .put_u32(2)
            .put_u8(0)
            .put_uuid(&[0xCD; 16])
            .put_u32(0)
            .put_u8(FLAG_STREAM_S_UPDATE_BATCH_RETRY);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        match StreamCommand::parse(OP_S_UPDATE_BATCHES, &mut cursor).unwrap() {
            StreamCommand::UpdateBatches { updates, .. } => {
                assert_eq!(updates[0].receipt, [0xAB; 16]);
                assert_eq!(updates[0].completed, 2);
                assert_eq!(updates[0].disposition, BatchDisposition::Checkpoint);
                assert_eq!(updates[1].receipt, [0xCD; 16]);
                assert_eq!(updates[1].disposition, BatchDisposition::Retry);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn update_batches_parser_rejects_oversized_count() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_u32(1000);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        assert!(StreamCommand::parse(OP_S_UPDATE_BATCHES, &mut cursor).is_err());
    }

    #[test]
    fn fetch_and_update_parsers_never_panic_on_garbage() {
        let mut fetch = PayloadWriter::new();
        fetch
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_u32(10)
            .put_u32(3)
            .put_u32(2)
            .put_u32(1024)
            .put_u32(5000);
        let mut single = PayloadWriter::new();
        single
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_uuid(&[0xAB; 16])
            .put_u32(2)
            .put_u8(0);
        let mut plural = PayloadWriter::new();
        plural
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_u32(2)
            .put_uuid(&[0xAB; 16])
            .put_u32(2)
            .put_u8(0)
            .put_uuid(&[0xCD; 16])
            .put_u32(0)
            .put_u8(FLAG_STREAM_S_UPDATE_BATCH_RETRY);
        let cases = [
            (OP_S_FETCH, fetch.into_bytes()),
            (OP_S_UPDATE_BATCH, single.into_bytes()),
            (OP_S_UPDATE_BATCHES, plural.into_bytes()),
        ];
        for (opcode, payload) in &cases {
            for cut in 0..payload.len() {
                let mut cursor = PayloadCursor::new(Bytes::copy_from_slice(&payload[..cut]));
                let _ = StreamCommand::parse(*opcode, &mut cursor);
            }
        }

        let mut state = 1u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for len in 0..=256usize {
            let buf: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            for (opcode, _) in &cases {
                let mut cursor = PayloadCursor::new(Bytes::copy_from_slice(&buf));
                let _ = StreamCommand::parse(*opcode, &mut cursor);
            }
        }

        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_uuid(&[3; 16])
            .put_u32(u32::MAX);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        assert!(StreamCommand::parse(OP_S_UPDATE_BATCHES, &mut cursor).is_err());
    }
}

// ==========================================
// WIRE RESPONSES
// ==========================================

fn encode_publish_batch(seqs: &[u64]) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u32(seqs.len() as u32);
    for &seq in seqs {
        w.put_u64(seq);
    }
    w.into_bytes()
}

/// FETCH DATA: `u64 lease_ms | u32 batch_count` then per batch
/// `uuid receipt | u16 key_len | key | u32 message_count` followed by
/// `u64 seq | u64 timestamp | u32 payload_len | payload` per message.
/// A receipt is shared by all messages of its batch and fences the lease.
fn encode_fetch(page: &FetchPage) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u64(page.lease_ms);
    w.put_u32(page.batches.len() as u32);
    for batch in &page.batches {
        w.put_uuid(&batch.receipt);
        let key = batch.messages.first().map(|m| m.key.as_ref()).unwrap_or(&[]);
        w.put_u16(key.len() as u16);
        w.put_raw(key);
        w.put_u32(batch.messages.len() as u32);
        for m in &batch.messages {
            w.put_u64(m.seq);
            w.put_u64(m.timestamp);
            w.put_bytes(&m.payload);
        }
    }
    w.into_bytes()
}

fn encode_join_group(ack_floor: u64, epoch: [u8; 16], consumer_id: &str) -> Bytes {
    let mut w = PayloadWriter::with_capacity(24 + 4 + consumer_id.len());
    w.put_u64(ack_floor);
    w.put_uuid(&epoch);
    w.put_str(consumer_id);
    w.into_bytes()
}

/// S_UPDATE_BATCHES reply: `[u32 failure_count][uuid receipt | u8 code]*` —
/// empty means every update was consumed. Kept as DATA (not ERR) so whole-op
/// membership failures stay distinct from per-batch fencing, and valid
/// updates still commit.
fn encode_batch_outcome(failures: &[([u8; 16], BrokerErrorKind)]) -> Bytes {
    let mut w = PayloadWriter::with_capacity(4 + failures.len() * 17);
    w.put_u32(failures.len() as u32);
    for (receipt, kind) in failures {
        w.put_uuid(receipt);
        w.put_u8(match kind {
            BrokerErrorKind::InvalidArgument => ErrorCode::InvalidArgument as u8,
            _ => ErrorCode::Fenced as u8,
        });
    }
    w.into_bytes()
}

fn encode_bool(value: bool) -> Bytes {
    let mut w = PayloadWriter::with_capacity(1);
    w.put_bool(value);
    w.into_bytes()
}

fn encode_peek_dls(entries: &[DlsEntry]) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u32(entries.len() as u32);
    for entry in entries {
        w.put_u64(entry.seq);
        w.put_str(&entry.reason);
        w.put_u32(entry.attempts);
        w.put_u16(entry.key.len() as u16);
        w.put_raw(&entry.key);
    }
    w.into_bytes()
}

fn encode_purge_dls(count: usize) -> Bytes {
    let mut w = PayloadWriter::with_capacity(4);
    w.put_u32(count as u32);
    w.into_bytes()
}

fn put_definition(writer: &mut PayloadWriter, definition: &StreamDefinition) {
    let max_age = definition.config.retention.max_age_ms;
    let max_bytes = definition.config.retention.max_bytes;
    let flags = (if max_age.is_some() {
        FLAG_STREAM_S_CREATE_HAS_MAX_AGE
    } else {
        0
    }) | (if max_bytes.is_some() {
        FLAG_STREAM_S_CREATE_HAS_MAX_BYTES
    } else {
        0
    });
    writer.put_str(&definition.name).put_u8(flags);
    if let Some(max_age) = max_age {
        writer.put_u64(max_age);
    }
    if let Some(max_bytes) = max_bytes {
        writer.put_u64(max_bytes);
    }
    writer
        .put_u64(definition.config.max_ack_pending as u64)
        .put_u64(definition.config.max_ack_pending_bytes)
        .put_u64(definition.config.ack_wait_ms)
        .put_u32(definition.config.max_deliveries);
}

fn encode_definition(definition: &StreamDefinition) -> Bytes {
    let mut writer = PayloadWriter::new();
    put_definition(&mut writer, definition);
    writer.into_bytes()
}

fn encode_provision_result(result: &ProvisionResult<StreamDefinition>) -> Bytes {
    let mut writer = PayloadWriter::new();
    writer.put_u8(match result.outcome {
        ProvisionOutcome::Created => ProvisionStatus::Created as u8,
        ProvisionOutcome::Unchanged => ProvisionStatus::Unchanged as u8,
    });
    put_definition(&mut writer, &result.definition);
    writer.into_bytes()
}

// ==========================================
// SUBMIT / COMPLETE SEAM
// ==========================================

/// Which reply encoding a submitted command needs at completion time.
#[derive(Debug, Clone, Copy)]
enum CallKind {
    Provision,
    Publish,
    Fetch,
    Join,
    Bool,
    Definition,
    DlsPeek,
    Count,
    Unit,
    /// S_UPDATE_BATCHES: DATA reply listing the receipts that failed.
    BatchOutcome,
}

pub struct StreamCall {
    pending: PendingReply,
    kind: CallKind,
}

impl StreamCall {
    /// Runs on a spawned task: awaits the post-commit reply and encodes it.
    pub async fn complete(self) -> Response {
        match self.pending.wait().await {
            Ok(reply) => encode_reply(self.kind, reply),
            Err(error) => error_response(error),
        }
    }
}

fn encode_reply(kind: CallKind, reply: StreamReply) -> Response {
    match (kind, reply) {
        (CallKind::Provision, StreamReply::Provision(result)) => {
            Response::Data(encode_provision_result(&result))
        }
        (CallKind::Publish, StreamReply::Published(seqs)) => {
            Response::Data(encode_publish_batch(&seqs))
        }
        (CallKind::Fetch, StreamReply::Fetch(page)) => {
            Response::Data(encode_fetch(&page))
        }
        (
            CallKind::Join,
            StreamReply::Join {
                ack_floor,
                consumer_id,
                epoch,
            },
        ) => Response::Data(encode_join_group(ack_floor, epoch, &consumer_id)),
        (CallKind::Bool, StreamReply::Bool(v)) => Response::Data(encode_bool(v)),
        (CallKind::Definition, StreamReply::Definition(def)) => {
            Response::Data(encode_definition(&def))
        }
        (CallKind::DlsPeek, StreamReply::DlsEntries(entries)) => {
            Response::Data(encode_peek_dls(&entries))
        }
        (CallKind::Count, StreamReply::Count(n)) => Response::Data(encode_purge_dls(n)),
        (CallKind::BatchOutcome, StreamReply::BatchOutcome(failures)) => {
            Response::Data(encode_batch_outcome(&failures))
        }
        (CallKind::Unit, StreamReply::Unit) => Response::Ok,
        _ => Response::error(ErrorCode::Internal, "Unexpected stream reply"),
    }
}

/// Parse + submit in TCP read order. `Err` carries the response to send
/// inline (parse/validation/admission failures); `Ok` carries the pending
/// completion to detach onto `request_set`.
pub async fn submit(
    opcode: u8,
    payload: Bytes,
    engine: &NexoEngine,
    session_id: &str,
) -> Result<StreamCall, Response> {
    let mut cursor = PayloadCursor::new(payload);
    let cmd = match StreamCommand::parse(opcode, &mut cursor) {
        Ok(c) => c,
        Err(error) => return Err(Response::error(ErrorCode::ProtocolError, error.to_string())),
    };
    let stream = &engine.stream;
    let connection = session_id.to_string();

    let (request, kind) = match cmd {
        StreamCommand::Create { name, options } => {
            let requested = crate::brokers::stream::domain::definition::StreamConfig::from_options(
                options,
                stream.config(),
            );
            (
                StreamRequest::CreateStream { name, requested },
                CallKind::Provision,
            )
        }
        StreamCommand::Publish { name, items } => {
            (StreamRequest::Publish { name, items }, CallKind::Publish)
        }
        StreamCommand::Fetch {
            name,
            group,
            consumer_id,
            epoch,
            limits,
            wait_ms,
        } => {
            // Fetch is the only command that may long-poll: the wait loop is
            // driven by a detached task so the reader never blocks.
            return Ok(submit_fetch(
                std::sync::Arc::clone(&engine.stream),
                name,
                group,
                consumer_id,
                epoch,
                limits,
                wait_ms,
                connection,
            ));
        }
        StreamCommand::Join { name, group } => (
            StreamRequest::Join {
                name,
                group,
                connection_id: connection,
            },
            CallKind::Join,
        ),
        StreamCommand::UpdateBatch {
            name,
            group,
            consumer_id,
            epoch,
            update,
        } => (
            StreamRequest::UpdateBatch {
                name,
                group,
                identity: crate::brokers::stream::domain::message::ConsumerIdentity {
                    connection_id: connection,
                    consumer_id,
                    epoch,
                },
                update,
            },
            CallKind::Unit,
        ),
        StreamCommand::UpdateBatches {
            name,
            group,
            consumer_id,
            epoch,
            updates,
        } => (
            StreamRequest::UpdateBatches {
                name,
                group,
                identity: crate::brokers::stream::domain::message::ConsumerIdentity {
                    connection_id: connection,
                    consumer_id,
                    epoch,
                },
                updates,
            },
            CallKind::BatchOutcome,
        ),
        StreamCommand::Seek {
            name,
            group,
            target,
        } => (
            StreamRequest::Seek {
                name,
                group,
                target,
            },
            CallKind::Unit,
        ),
        StreamCommand::Exists { name } => (StreamRequest::StreamExists { name }, CallKind::Bool),
        StreamCommand::Describe { name } => {
            (StreamRequest::DescribeStream { name }, CallKind::Definition)
        }
        StreamCommand::Delete { name } => (StreamRequest::DeleteStream { name }, CallKind::Unit),
        StreamCommand::Leave {
            name,
            group,
            consumer_id,
            epoch,
        } => (
            StreamRequest::Leave {
                name,
                group,
                identity: crate::brokers::stream::domain::message::ConsumerIdentity {
                    connection_id: connection,
                    consumer_id,
                    epoch,
                },
            },
            CallKind::Unit,
        ),
        StreamCommand::PeekDls {
            name,
            group,
            limit,
            offset,
        } => (
            StreamRequest::PeekDls {
                name,
                group,
                limit: limit as usize,
                offset: offset as usize,
            },
            CallKind::DlsPeek,
        ),
        StreamCommand::MoveToStream { name, group, seq } => (
            StreamRequest::ReplayDls { name, group, seq },
            CallKind::Unit,
        ),
        StreamCommand::DeleteDls { name, group, seq } => (
            StreamRequest::DeleteDls { name, group, seq },
            CallKind::Unit,
        ),
        StreamCommand::PurgeDls { name, group } => {
            (StreamRequest::PurgeDls { name, group }, CallKind::Count)
        }
    };

    match stream.submit(request).await {
        Ok(pending) => Ok(StreamCall { pending, kind }),
        Err(error) => Err(error_response(error)),
    }
}

/// FETCH drives the long-poll loop on a detached task: the read path stays
/// free, and the delivery order guarantee is unaffected because fetches only
/// observe committed state (leases are receipt-fenced).
fn submit_fetch(
    stream: std::sync::Arc<crate::brokers::stream::StreamManager>,
    name: String,
    group: String,
    consumer_id: String,
    epoch: [u8; 16],
    limits: FetchLimits,
    wait_ms: u32,
    connection: String,
) -> StreamCall {
    let identity = crate::brokers::stream::domain::message::ConsumerIdentity {
        connection_id: connection,
        consumer_id,
        epoch,
    };
    let wait = wait_ms as u64;
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let result = stream.fetch(&name, &group, &identity, limits, wait).await;
        let _ = tx.send(result.map(StreamReply::Fetch));
    });
    StreamCall {
        pending: PendingReply::wrap(rx, "Stream"),
        kind: CallKind::Fetch,
    }
}

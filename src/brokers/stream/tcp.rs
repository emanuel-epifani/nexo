//! Stream broker TCP surface: opcodes, request parsing, response encoding.
//!
//! Split-phase dispatch: `submit()` parses and enqueues on the connection's
//! read path (preserving command order and applying admission backpressure),
//! while `StreamCall::complete()` runs on a spawned task that awaits the
//! writer's post-commit reply and encodes it. The reader never blocks on a
//! SQLite commit.

use bytes::Bytes;

use crate::brokers::stream::domain::definition::StreamDefinition;
use crate::brokers::stream::domain::message::{Delivery, DlsEntry, PubItem};
use crate::brokers::stream::domain::ops::{StreamReply, StreamRequest};
use crate::brokers::stream::manager::PendingReply;
use crate::brokers::stream::options::{RetentionOptions, SeekTarget, StreamCreateOptions};
use crate::brokers::{ProvisionOutcome, ProvisionResult};
use crate::protocol::wire::{PayloadCursor, PayloadWriter};
use crate::protocol::{
    ErrorCode, ParseError, ProvisionStatus, Response, FLAG_STREAM_S_CREATE_HAS_MAX_AGE,
    FLAG_STREAM_S_CREATE_HAS_MAX_BYTES,
};
use crate::transport::tcp::error_response;
use crate::NexoEngine;

// ==========================================
// OPCODES
// ==========================================

use crate::protocol::STREAM_MAX_FETCH_BATCH_SIZE;
use crate::protocol::STREAM_MAX_PUBLISH_BATCH as MAX_PUBLISH_BATCH;
pub use crate::protocol::{
    OP_S_ACK, OP_S_ACK_MANY, OP_S_CREATE, OP_S_DELETE, OP_S_DELETE_DLS, OP_S_DESCRIBE, OP_S_EXISTS,
    OP_S_FETCH, OP_S_JOIN, OP_S_LEAVE, OP_S_MOVE_TO_STREAM, OP_S_PEEK_DLS, OP_S_PUB,
    OP_S_PURGE_DLS, OP_S_SEEK, STREAM_OPCODE_MAX as OPCODE_MAX, STREAM_OPCODE_MIN as OPCODE_MIN,
};
const MIN_PUBLISH_ITEM_BYTES: usize = 6;

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
        generation: u64,
        limit: u32,
        wait_ms: u32,
    },
    Join {
        name: String,
        group: String,
    },
    Ack {
        name: String,
        group: String,
        consumer_id: String,
        generation: u64,
        seq: u64,
        receipt: [u8; 16],
    },
    /// Batched ack: one frame carries N (seq, receipt) pairs so a fetch
    /// batch is released in a single round trip instead of N awaits.
    AckMany {
        name: String,
        group: String,
        consumer_id: String,
        generation: u64,
        acks: Vec<(u64, [u8; 16])>,
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
        generation: u64,
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
                let generation = cursor.read_u64()?;
                let limit = cursor.read_u32()?;
                let wait_ms = cursor.read_u32()?;
                Ok(Self::Fetch {
                    name,
                    group,
                    consumer_id,
                    generation,
                    limit,
                    wait_ms,
                })
            }
            OP_S_JOIN => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                Ok(Self::Join { name, group })
            }
            OP_S_ACK => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                let consumer_id = cursor.read_string()?;
                let generation = cursor.read_u64()?;
                let seq = cursor.read_u64()?;
                let receipt = cursor.read_uuid_bytes()?;
                Ok(Self::Ack {
                    name,
                    group,
                    consumer_id,
                    generation,
                    seq,
                    receipt,
                })
            }
            OP_S_ACK_MANY => {
                let name = cursor.read_string()?;
                let group = cursor.read_string()?;
                let consumer_id = cursor.read_string()?;
                let generation = cursor.read_u64()?;
                let count = cursor.read_u32()? as usize;
                // 24 bytes per item (seq u64 + receipt 16B); the cap mirrors
                // the fetch batch ceiling since a batch can't ack more than
                // a fetch could have delivered.
                if count > STREAM_MAX_FETCH_BATCH_SIZE || count > cursor.len() / 24 {
                    return Err(ParseError::Invalid(format!(
                        "Ack batch count {} exceeds remaining payload",
                        count
                    )));
                }
                let mut acks = Vec::with_capacity(count);
                for _ in 0..count {
                    acks.push((cursor.read_u64()?, cursor.read_uuid_bytes()?));
                }
                Ok(Self::AckMany {
                    name,
                    group,
                    consumer_id,
                    generation,
                    acks,
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
                let generation = cursor.read_u64()?;
                Ok(Self::Leave {
                    name,
                    group,
                    consumer_id,
                    generation,
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
    fn ack_parser_reads_receipt() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_u64(3)
            .put_u64(9)
            .put_uuid(&[0xAB; 16]);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        match StreamCommand::parse(OP_S_ACK, &mut cursor).unwrap() {
            StreamCommand::Ack { receipt, seq, .. } => {
                assert_eq!(seq, 9);
                assert_eq!(receipt, [0xAB; 16]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn ack_parser_rejects_truncated_receipt() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_u64(3)
            .put_u64(9);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        assert!(StreamCommand::parse(OP_S_ACK, &mut cursor).is_err());
    }

    #[test]
    fn ack_many_parser_reads_pairs() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_u64(3)
            .put_u32(2)
            .put_u64(9)
            .put_uuid(&[0xAB; 16])
            .put_u64(10)
            .put_uuid(&[0xCD; 16]);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        match StreamCommand::parse(OP_S_ACK_MANY, &mut cursor).unwrap() {
            StreamCommand::AckMany { acks, .. } => {
                assert_eq!(acks, vec![(9, [0xAB; 16]), (10, [0xCD; 16])]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn ack_many_parser_rejects_oversized_count() {
        let mut writer = PayloadWriter::new();
        writer
            .put_str("s")
            .put_str("g")
            .put_str("c")
            .put_u64(3)
            .put_u32(1000);
        let mut cursor = PayloadCursor::new(writer.into_bytes());
        assert!(StreamCommand::parse(OP_S_ACK_MANY, &mut cursor).is_err());
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

/// FETCH item: seq u64 | receipt 16B | timestamp u64 | key_len u16 | key |
/// payload_len u32 | payload. The receipt fences the lease and is echoed on ACK.
fn encode_fetch(deliveries: &[Delivery]) -> Bytes {
    let mut w = PayloadWriter::new();
    w.put_u32(deliveries.len() as u32);
    for d in deliveries {
        w.put_u64(d.message.seq);
        w.put_uuid(&d.receipt);
        w.put_u64(d.message.timestamp);
        w.put_u16(d.message.key.len() as u16);
        w.put_raw(&d.message.key);
        w.put_bytes(&d.message.payload);
    }
    w.into_bytes()
}

fn encode_join_group(ack_floor: u64, generation: u64, consumer_id: &str) -> Bytes {
    let mut w = PayloadWriter::with_capacity(16 + 4 + consumer_id.len());
    w.put_u64(ack_floor);
    w.put_u64(generation);
    w.put_str(consumer_id);
    w.into_bytes()
}

/// S_ACK_MANY reply: `[u32 failed_count][seq u64]*` — empty means all acks
/// consumed. Kept as DATA (not ERR) so whole-op failures stay distinct from
/// per-seq fencing, and valid acks still commit.
fn encode_ack_outcome(failed: &[u64]) -> Bytes {
    let mut w = PayloadWriter::with_capacity(4 + failed.len() * 8);
    w.put_u32(failed.len() as u32);
    for &seq in failed {
        w.put_u64(seq);
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
    /// S_ACK_MANY: DATA reply listing the seqs that failed fencing.
    AckOutcome,
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
        (CallKind::Fetch, StreamReply::Fetch(deliveries)) => {
            Response::Data(encode_fetch(&deliveries))
        }
        (
            CallKind::Join,
            StreamReply::Join {
                ack_floor,
                consumer_id,
                generation,
            },
        ) => Response::Data(encode_join_group(ack_floor, generation, &consumer_id)),
        (CallKind::Bool, StreamReply::Bool(v)) => Response::Data(encode_bool(v)),
        (CallKind::Definition, StreamReply::Definition(def)) => {
            Response::Data(encode_definition(&def))
        }
        (CallKind::DlsPeek, StreamReply::DlsEntries(entries)) => {
            Response::Data(encode_peek_dls(&entries))
        }
        (CallKind::Count, StreamReply::Count(n)) => Response::Data(encode_purge_dls(n)),
        (CallKind::AckOutcome, StreamReply::AckOutcome(failed)) => {
            Response::Data(encode_ack_outcome(&failed))
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
            generation,
            limit,
            wait_ms,
        } => {
            // Fetch is the only command that may long-poll: the wait loop is
            // driven by a detached task so the reader never blocks.
            return Ok(submit_fetch(
                std::sync::Arc::clone(&engine.stream),
                name,
                group,
                consumer_id,
                generation,
                limit,
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
        StreamCommand::Ack {
            name,
            group,
            consumer_id,
            generation,
            seq,
            receipt,
        } => (
            StreamRequest::Ack {
                name,
                group,
                identity: crate::brokers::stream::domain::message::ConsumerIdentity {
                    connection_id: connection,
                    consumer_id,
                    generation,
                },
                seq,
                receipt,
            },
            CallKind::Unit,
        ),
        StreamCommand::AckMany {
            name,
            group,
            consumer_id,
            generation,
            acks,
        } => (
            StreamRequest::AckMany {
                name,
                group,
                identity: crate::brokers::stream::domain::message::ConsumerIdentity {
                    connection_id: connection,
                    consumer_id,
                    generation,
                },
                acks,
            },
            CallKind::AckOutcome,
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
            generation,
        } => (
            StreamRequest::Leave {
                name,
                group,
                identity: crate::brokers::stream::domain::message::ConsumerIdentity {
                    connection_id: connection,
                    consumer_id,
                    generation,
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
    generation: u64,
    limit: u32,
    wait_ms: u32,
    connection: String,
) -> StreamCall {
    let identity = crate::brokers::stream::domain::message::ConsumerIdentity {
        connection_id: connection,
        consumer_id,
        generation,
    };
    let wait = wait_ms as u64;
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let result = stream
            .fetch(&name, &group, &identity, limit as usize, wait)
            .await;
        let _ = tx.send(result.map(StreamReply::Fetch));
    });
    StreamCall {
        pending: PendingReply::wrap(rx, "Stream"),
        kind: CallKind::Fetch,
    }
}

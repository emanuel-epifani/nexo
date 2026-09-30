//! Thin opcode dispatcher delegating to broker-specific TCP handlers.

use crate::brokers::{pub_sub, queue, store, stream};
use crate::protocol::wire::PayloadCursor;
pub use crate::protocol::OP_DEBUG_ECHO;
use crate::protocol::{ErrorCode, Response};
use crate::NexoEngine;
use bytes::Bytes;

/// Opcodes that must complete in TCP arrival order and are cheap enough to
/// execute without allocating a Tokio task. Stream opcodes are NOT here:
/// they take the dedicated ordered-submit path in `connection.rs` (submit on
/// the reader, completion detached).
///
/// Every pubsub opcode is inline: PUB/SUB/UNSUB are all in-memory operations
/// and MUST run in arrival order — a PUB spawned to a task can overtake a
/// SUB/UNSUB written in the same TCP batch, causing spurious deliveries or
/// lost messages.
pub fn is_inline_opcode(opcode: u8) -> bool {
    matches!(
        opcode,
        OP_DEBUG_ECHO
            | store::tcp::OP_MAP_SET
            | store::tcp::OP_MAP_GET
            | store::tcp::OP_MAP_DEL
            | store::tcp::OP_MAP_INCR
            | queue::tcp::OP_Q_EXISTS
            | pub_sub::tcp::OP_PUB
            | pub_sub::tcp::OP_SUB
            | pub_sub::tcp::OP_UNSUB
    )
}

pub async fn dispatch(
    engine: &NexoEngine,
    session_id: &str,
    opcode: u8,
    payload: Bytes,
) -> Response {
    let mut cursor = PayloadCursor::new(payload);

    match opcode {
        OP_DEBUG_ECHO => Response::Data(cursor.read_remaining()),

        op if (store::tcp::OPCODE_MIN..=store::tcp::OPCODE_MAX).contains(&op) => {
            store::tcp::handle(op, &mut cursor, engine)
        }
        op if (queue::tcp::OPCODE_MIN..=queue::tcp::OPCODE_MAX).contains(&op) => {
            queue::tcp::handle(op, &mut cursor, engine).await
        }
        op if (pub_sub::tcp::OPCODE_MIN..=pub_sub::tcp::OPCODE_MAX).contains(&op) => {
            pub_sub::tcp::handle(op, &mut cursor, engine, session_id).await
        }
        // Stream opcodes are submitted on the read path (ordered) and
        // complete on spawned tasks — see connection.rs.
        op if (stream::tcp::OPCODE_MIN..=stream::tcp::OPCODE_MAX).contains(&op) => {
            let _ = op;
            Response::error(ErrorCode::Internal, "Stream dispatch misrouted")
        }

        _ => Response::error(
            ErrorCode::ProtocolError,
            format!("Unknown opcode: 0x{:02X}", opcode),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_opcodes_are_classified_correctly() {
        assert!(is_inline_opcode(OP_DEBUG_ECHO));

        // Store opcodes that are O(1) in-memory
        assert!(is_inline_opcode(store::tcp::OP_MAP_SET));
        assert!(is_inline_opcode(store::tcp::OP_MAP_GET));
        assert!(is_inline_opcode(store::tcp::OP_MAP_DEL));
        assert!(is_inline_opcode(store::tcp::OP_MAP_INCR));

        assert!(is_inline_opcode(queue::tcp::OP_Q_EXISTS));

        // All PubSub commands are inline: PUB/SUB/UNSUB must execute in TCP order
        assert!(is_inline_opcode(pub_sub::tcp::OP_PUB));
        assert!(is_inline_opcode(pub_sub::tcp::OP_SUB));
        assert!(is_inline_opcode(pub_sub::tcp::OP_UNSUB));
    }

    #[test]
    fn test_blocking_opcodes_are_not_inline() {
        // Stream opcodes take the ordered-submit path, not inline dispatch.
        assert!(!is_inline_opcode(stream::tcp::OP_S_ACK));
        assert!(!is_inline_opcode(stream::tcp::OP_S_SEEK));
        assert!(!is_inline_opcode(stream::tcp::OP_S_LEAVE));
        assert!(!is_inline_opcode(stream::tcp::OP_S_JOIN));
        assert!(!is_inline_opcode(stream::tcp::OP_S_FETCH));
        assert!(!is_inline_opcode(stream::tcp::OP_S_PUB));
        assert!(!is_inline_opcode(stream::tcp::OP_S_CREATE));
        assert!(!is_inline_opcode(stream::tcp::OP_S_DELETE));
        assert!(!is_inline_opcode(store::tcp::OP_MAP_CLEAR_ALL));
        assert!(!is_inline_opcode(store::tcp::OP_MAP_CLEAR_PREFIX));

        // Queue ack/nack now await on the bounded store writer channel
        assert!(!is_inline_opcode(queue::tcp::OP_Q_ACK));
        assert!(!is_inline_opcode(queue::tcp::OP_Q_NACK));
    }

    #[test]
    fn test_unknown_opcode_is_not_inline() {
        assert!(!is_inline_opcode(0xFF));
    }
}

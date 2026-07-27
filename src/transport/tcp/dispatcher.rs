//! Thin opcode dispatcher delegating to broker-specific TCP handlers.

use crate::brokers::{pub_sub, queue, store, stream};
use crate::transport::tcp::protocol::wire::PayloadCursor;
use crate::transport::tcp::protocol::Response;
use crate::NexoEngine;
use bytes::Bytes;

pub const OP_DEBUG_ECHO: u8 = 0x00;

/// Opcodes that mutate state and must be processed in TCP arrival order to
/// prevent races (e.g. ACK processed after LEAVE, SUB processed after PUBLISH).
/// These are fast, in-memory state mutations that do not perform I/O,
/// long-polling, or large fan-out — safe to run inline.
pub fn is_inline_opcode(opcode: u8) -> bool {
    matches!(
        opcode,
        stream::tcp::OP_S_ACK
            | stream::tcp::OP_S_SEEK
            | stream::tcp::OP_S_LEAVE
            | stream::tcp::OP_S_JOIN
            | store::tcp::OP_MAP_SET
            | store::tcp::OP_MAP_GET
            | store::tcp::OP_MAP_DEL
            | store::tcp::OP_MAP_INCR
            | pub_sub::tcp::OP_SUB
            | pub_sub::tcp::OP_UNSUB
    )
}

pub struct Dispatcher<'a> {
    engine: &'a NexoEngine,
    /// Opaque per-connection session id (transport-level). Brokers that need to
    /// identify the caller (pubsub, stream) receive it as a plain `&str`.
    session_id: &'a str,
}

impl<'a> Dispatcher<'a> {
    pub fn new(engine: &'a NexoEngine, session_id: &'a str) -> Self {
        Self { engine, session_id }
    }

    pub async fn dispatch(&self, opcode: u8, payload: Bytes) -> Response {
        let mut cursor = PayloadCursor::new(payload);

        match opcode {
            OP_DEBUG_ECHO => Response::Data(cursor.read_remaining()),

            op if (store::tcp::OPCODE_MIN..=store::tcp::OPCODE_MAX).contains(&op) => {
                store::tcp::handle(op, &mut cursor, self.engine)
            }
            op if (queue::tcp::OPCODE_MIN..=queue::tcp::OPCODE_MAX).contains(&op) => {
                queue::tcp::handle(op, &mut cursor, self.engine).await
            }
            op if (pub_sub::tcp::OPCODE_MIN..=pub_sub::tcp::OPCODE_MAX).contains(&op) => {
                pub_sub::tcp::handle(op, &mut cursor, self.engine, self.session_id).await
            }
            op if (stream::tcp::OPCODE_MIN..=stream::tcp::OPCODE_MAX).contains(&op) => {
                stream::tcp::handle(op, &mut cursor, self.engine, self.session_id).await
            }

            _ => Response::Error(format!("Unknown opcode: 0x{:02X}", opcode)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_opcodes_are_classified_correctly() {
        // Stream opcodes that must be inline
        assert!(is_inline_opcode(stream::tcp::OP_S_ACK));
        assert!(is_inline_opcode(stream::tcp::OP_S_SEEK));
        assert!(is_inline_opcode(stream::tcp::OP_S_LEAVE));
        assert!(is_inline_opcode(stream::tcp::OP_S_JOIN));

        // Store opcodes that are O(1) in-memory
        assert!(is_inline_opcode(store::tcp::OP_MAP_SET));
        assert!(is_inline_opcode(store::tcp::OP_MAP_GET));
        assert!(is_inline_opcode(store::tcp::OP_MAP_DEL));
        assert!(is_inline_opcode(store::tcp::OP_MAP_INCR));

        // PubSub opcodes that must be ordered with publish
        assert!(is_inline_opcode(pub_sub::tcp::OP_SUB));
        assert!(is_inline_opcode(pub_sub::tcp::OP_UNSUB));
    }

    #[test]
    fn test_blocking_opcodes_are_not_inline() {
        // These must NOT be inline — they can block (long-poll, I/O, fan-out, etc.)
        assert!(!is_inline_opcode(stream::tcp::OP_S_FETCH));
        assert!(!is_inline_opcode(stream::tcp::OP_S_PUB));
        assert!(!is_inline_opcode(stream::tcp::OP_S_CREATE));
        assert!(!is_inline_opcode(stream::tcp::OP_S_DELETE));
        assert!(!is_inline_opcode(store::tcp::OP_MAP_CLEAR_ALL));
        assert!(!is_inline_opcode(store::tcp::OP_MAP_CLEAR_PREFIX));
        assert!(!is_inline_opcode(pub_sub::tcp::OP_PUB));

        // Queue ack/nack now await on the bounded store writer channel
        assert!(!is_inline_opcode(queue::tcp::OP_Q_ACK));
        assert!(!is_inline_opcode(queue::tcp::OP_Q_NACK));
    }

    #[test]
    fn test_unknown_opcode_is_not_inline() {
        assert!(!is_inline_opcode(0xFF));
        assert!(!is_inline_opcode(0x00));
    }
}

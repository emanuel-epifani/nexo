//! Thin opcode dispatcher delegating to broker-specific TCP handlers.

use crate::brokers::{pub_sub, queue, store, stream};
use crate::transport::tcp::protocol::wire::PayloadCursor;
use crate::transport::tcp::protocol::Response;
use crate::NexoEngine;
use bytes::Bytes;

pub const OP_DEBUG_ECHO: u8 = 0x00;

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

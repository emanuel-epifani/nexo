//! Request Router: thin opcode dispatcher delegating to broker-specific TCP handlers.

use crate::brokers::pub_sub::ClientId;
use crate::brokers::{pub_sub, queue, store, stream};
use crate::server::protocol::cursor::PayloadCursor;
use crate::server::protocol::Response;
use crate::NexoEngine;
use bytes::Bytes;

pub const OP_DEBUG_ECHO: u8 = 0x00;

pub struct RequestHandler<'a> {
    engine: &'a NexoEngine,
    client_id: &'a ClientId,
}

impl<'a> RequestHandler<'a> {
    pub fn new(engine: &'a NexoEngine, client_id: &'a ClientId) -> Self {
        Self { engine, client_id }
    }

    pub async fn route(&self, opcode: u8, payload: Bytes) -> Response {
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
                pub_sub::tcp::handle(op, &mut cursor, self.engine, self.client_id).await
            }
            op if (stream::tcp::OPCODE_MIN..=stream::tcp::OPCODE_MAX).contains(&op) => {
                stream::tcp::handle(op, &mut cursor, self.engine, self.client_id).await
            }

            _ => Response::Error(format!("Unknown opcode: 0x{:02X}", opcode)),
        }
    }
}

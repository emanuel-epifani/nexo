//! Network Layer: TCP listener + connection handling

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::server::protocol::{encode_response, parse_request, ParseError, Response};
use crate::NexoEngine;

// Import all command enums from centralized commands module
use crate::server::commands::{KvCommand, QueueCommand, TopicCommand, StreamCommand};

/// Handle a single client connection.
/// This function runs in a loop until the client disconnects.
pub async fn handle_connection(mut socket: TcpStream, engine: NexoEngine) -> Result<(), String> {
    // 1. Allocate a buffer for incoming data.
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // 2. Read from the socket into our buffer.
        let n = socket
            .read_buf(&mut buffer)
            .await
            .map_err(|e| format!("Socket read error: {}", e))?;

        // 3. Check for EOF (End Of File).
        if n == 0 {
            return Ok(());
        }

        // 4. Processing Loop (Pipelining support).
        loop {
            // 5. Parse Attempt (Zero-Copy).
            let (req, consumed) = match parse_request(&buffer) {
                Ok(Some((r, c))) => (r, c),
                Ok(None) => break, // Need more data
                Err(ParseError::Invalid(e)) => {
                    return Err(format!("Protocol error: {}", e));
                }
                Err(ParseError::Incomplete) => break,
            };

            // 6. Route opcode to feature handler (inline)
            let response = match req.opcode {
                0x01 => Ok(Response::Ok), // PING
                
                // KV commands (0x02-0x0F)
                0x02..=0x0F => {
                    KvCommand::parse(req.opcode, req.payload)?
                        .execute(&engine.kv)
                },
                
                // Queue commands (0x10-0x1F)
                0x10..=0x1F => {
                    QueueCommand::parse(req.opcode, req.payload)?
                        .execute(&engine.queue)
                },
                
                // Topic commands (0x20-0x2F)
                0x20..=0x2F => {
                    TopicCommand::parse(req.opcode, req.payload)?
                        .execute(&engine.topic)
                },
                
                // Stream commands (0x30-0x3F)
                0x30..=0x3F => {
                    StreamCommand::parse(req.opcode, req.payload)?
                        .execute(&engine.stream)
                },
                
                _ => Err(format!("Unknown opcode: 0x{:02X}", req.opcode)),
            }.unwrap_or_else(Response::Error);

            // 7. Serialize Response.
            let resp_bytes = encode_response(&response);

            // 8. Write to Socket.
            socket
                .write_all(&resp_bytes)
                .await
                .map_err(|e| format!("Socket write error: {}", e))?;

            // 9. Advance Buffer.
            buffer.advance(consumed);
        }
    }
}

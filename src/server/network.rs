//! Network Layer: TCP listener + connection handling

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use crate::server::protocol::{encode_response, parse_request, ParseError, Response};
use crate::NexoEngine;

// Import all command enums from centralized commands module
use crate::server::commands::{KvCommand, QueueCommand, TopicCommand, StreamCommand};

// ========================================
// TCP LISTENER
// ========================================

/// Start the TCP server on port 8080
pub async fn start(engine: NexoEngine) {
    let listener = TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("Failed to bind to port 8080");

    println!("[Server] Nexo listening on :8080");

    loop {
        // Accept new connection
        let (socket, addr) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        // Clone the engine reference (cheap Arc clone) for this connection
        let engine_clone = engine.clone();

        println!("[Server] New connection from {}", addr);

        // Spawn a new lightweight thread (task) for this client
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, engine_clone).await {
                eprintln!("[Server] Error handling connection from {}: {}", addr, e);
            }
            println!("[Server] Connection closed from {}", addr);
        });
    }
}

// ========================================
// CONNECTION HANDLER
// ========================================

/// Handle a single client connection.
/// This function runs in a loop until the client disconnects.
async fn handle_connection(mut socket: TcpStream, engine: NexoEngine) -> Result<(), String> {
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

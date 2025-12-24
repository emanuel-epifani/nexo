//! Network Layer: TCP listener + connection handling

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use crate::server::protocol::{encode_response, parse_request, ParseError, Response, Command};
use crate::server::routing::route;
use crate::NexoEngine;

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

            // 6. Parse Request -> Command
            let response = match Command::from_request(req) {
                Ok(command) => {
                    // 7. Routing (Dispatch Command)
                    route(command, &engine).unwrap_or_else(Response::Error)
                },
                Err(e) => Response::Error(format!("Invalid Command: {}", e)),
            };

            // 8. Serialize Response.
            let resp_bytes = encode_response(&response);

            // 9. Write to Socket.
            socket
                .write_all(&resp_bytes)
                .await
                .map_err(|e| format!("Socket write error: {}", e))?;

            // 10. Advance Buffer.
            buffer.advance(consumed);
        }
    }
}

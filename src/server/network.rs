//! Network Layer: TCP listener + connection handling

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use crate::server::protocol::{encode_response, parse_request, ParseError, Response};
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
    // BytesMut is efficient: it reuses memory when we "consume" (advance) bytes.
    // 4096 bytes (4KB) is a standard page size, good starting point.
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // 2. Read from the socket into our buffer.
        // This is async: if no data is arriving, this task sleeps and consumes 0 CPU.
        // It wakes up only when the OS has new bytes for us.
        let n = socket
            .read_buf(&mut buffer)
            .await
            .map_err(|e| format!("Socket read error: {}", e))?;

        // 3. Check for EOF (End Of File).
        // If read returns 0 bytes, it means the client closed the connection.
        if n == 0 {
            return Ok(());
        }

        // println!("[Socket] Received {} bytes", n);

        // 4. Processing Loop (Pipelining support).
        // Why a loop here?
        // TCP is a stream, not a packet queue. We might receive:
        // - Half a message (wait for more)
        // - Exactly one message (process it)
        // - Two and a half messages (process 2, wait for the rest)
        // This loop tries to extract as many complete messages as possible from the current buffer.
        loop {
            // 5. Parse Attempt (Zero-Copy).
            // We pass a slice of the buffer to the parser.
            // It returns:
            // - Ok(Some): We found a full message!
            // - Ok(None): Incomplete message, need more bytes.
            // - Err: Invalid data protocol violation.
            let (req, consumed) = match parse_request(&buffer) {
                Ok(Some((r, c))) => (r, c),
                Ok(None) => break, // Need more data, go back to socket.read_buf
                Err(ParseError::Invalid(e)) => {
                    // Critical Protocol Error: The client sent garbage.
                    // We can't recover synchronization easily, so we close the connection.
                    return Err(format!("Protocol error: {}", e));
                }
                Err(ParseError::Incomplete) => break, // Should be covered by Ok(None), but safe fallback
            };

            // println!("[Parser] Opcode: 0x{:02X}, Payload: {} bytes", req.opcode, req.payload.len());

            // 6. Routing (The Brain).
            // We have a valid request. Now we ask the Router to execute it.
            // We pass the whole Engine so the router can access KV, Queue, etc.
            let response = match route(req, &engine) {
                Ok(resp) => resp,
                Err(e) => Response::Error(e), // Application error (e.g., Key not found)
            };

            // 7. Serialize Response.
            // Turn the Response enum back into bytes.
            let resp_bytes = encode_response(&response);

            // 8. Write to Socket.
            // Send the answer back to the client.
            socket
                .write_all(&resp_bytes)
                .await
                .map_err(|e| format!("Socket write error: {}", e))?;

            // 9. Advance Buffer.
            // Crucial Step: We successfully processed 'consumed' bytes.
            // We tell BytesMut to mark them as "free" space.
            // The remaining bytes (if any) are moved to the front.
            buffer.advance(consumed);
        }
    }
}

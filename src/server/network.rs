//! Network Layer: TCP listener + connection handling

use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::features::kv::KvManager;
use crate::server::protocol::{encode_response, parse_request, ParseError, Response};
use crate::server::routing::route;

// ========================================
// TCP LISTENER
// ========================================

/// Start the TCP server on port 8080
pub async fn start() {
    let listener = TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("Failed to bind to port 8080");

    let kv_manager = Arc::new(KvManager::new());

    println!("[Server] Nexo listening on :8080");

    loop {
        let (socket, addr) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        let kv = kv_manager.clone();

        println!("[Server] New connection from {}", addr);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, kv).await {
                eprintln!("[Server] Error handling connection: {}", e);
            }
            println!("[Server] Connection closed from {}", addr);
        });
    }
}

// ========================================
// CONNECTION HANDLER
// ========================================

/// Handle a single client connection
async fn handle_connection(mut socket: TcpStream, kv: Arc<KvManager>) -> Result<(), String> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // Read from socket
        let n = socket
            .read_buf(&mut buffer)
            .await
            .map_err(|e| format!("Failed to read from socket: {}", e))?;

        if n == 0 {
            // Connection closed
            return Ok(());
        }

        println!("[Socket] Raw bytes received from client: {:?}", &buffer[..]);

        // Process as many frames as are available in the buffer (pipelining)
        loop {
            // Try to parse a frame (Zero-Copy: req borrows from buffer)
            // We can't borrow from buffer and then advance it mutably in the same scope easily without splitting logic.
            // But since parse_request takes a slice, we are good.
            let (req, consumed) = match parse_request(&buffer) {
                Ok(Some((r, c))) => (r, c),
                Ok(None) => break, // Incomplete frame
                Err(ParseError::Invalid(e)) => {
                    // Send error and close
                    eprintln!("Invalid frame: {}", e);
                    return Err(e); 
                }
                Err(ParseError::Incomplete) => break,
            };

            println!(
                "[Parser] Opcode: 0x{:02X}, PayloadLen: {}",
                req.opcode,
                req.payload.len()
            );

            // Route command
            let response = match route(req, &kv) {
                Ok(resp) => resp,
                Err(e) => Response::Error(e),
            };

            // Encode response
            let resp_bytes = encode_response(&response);

            // Send response
            socket
                .write_all(&resp_bytes)
                .await
                .map_err(|e| format!("Failed to write response: {}", e))?;

            // Remove consumed bytes
            buffer.advance(consumed);
        }
    }
}


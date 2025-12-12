//! Network Layer: TCP listener + connection handling

use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::features::kv::KvManager;
use crate::server::protocol::{encode_resp, parse_resp, ParseError, Response};
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
            let (resp_value, consumed) = match parse_resp(&buffer) {
                Ok(result) => result,
                Err(ParseError::Incomplete) => break, // need more bytes
                Err(ParseError::Invalid(e)) => {
                    // Send error response for invalid protocol
                    let error_resp =
                        encode_resp(&Response::Error(format!("Protocol error: {}", e)));
                    socket
                        .write_all(&error_resp)
                        .await
                        .map_err(|e| format!("Failed to write error response: {}", e))?;
                    buffer.clear();
                    break;
                }
            };

            // Remove consumed bytes from buffer
            buffer.advance(consumed);

            // Convert RESP value to binary-safe args
            let args = match resp_value.into_args() {
                Ok(args) => args,
                Err(e) => {
                    let error_resp =
                        encode_resp(&Response::Error(format!("Invalid command format: {}", e)));
                    socket
                        .write_all(&error_resp)
                        .await
                        .map_err(|e| format!("Failed to write error response: {}", e))?;
                    continue;
                }
            };

            println!(
                "[Parser] Parsed cmd: {:?}, args: {}",
                String::from_utf8_lossy(&args.cmd),
                args.rest.len()
            );

            // Route command
            let response = match route(args, &kv) {
                Ok(resp) => resp,
                Err(e) => Response::Error(e),
            };

            // Encode response
            let resp_bytes = encode_resp(&response);

            // Send response
            socket
                .write_all(&resp_bytes)
                .await
                .map_err(|e| format!("Failed to write response: {}", e))?;
        }
    }
}


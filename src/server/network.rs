//! Socket Network Layer: TCP listener + connection handling
//! Handles raw bytes and spawns tasks for parallel command execution.

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use std::sync::Arc;

use crate::server::protocol::{
    encode_response, parse_frame, ParseError, Response,
    TYPE_REQUEST, TYPE_PING
};
use crate::server::router::route;
use crate::NexoEngine;

/// Internal message type for the write loop
enum WriteMessage {
    Response(u32, Response),
    // Raw(Vec<u8>), // Not used for now, but keeping for future pushes
}

/// Handle a single client connection.
pub async fn handle_connection(socket: TcpStream, engine: NexoEngine) -> Result<(), String> {
    let (mut reader, mut writer) = tokio::io::split(socket);
    let (tx, mut rx) = mpsc::channel::<WriteMessage>(100);
    
    // --- WRITE LOOP ---
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let bytes = match msg {
                WriteMessage::Response(id, resp) => encode_response(id, &resp),
            };

            if let Err(e) = writer.write_all(&bytes).await {
                eprintln!("[Network] Write error: {}", e);
                break;
            }
        }
    });

    // --- READ LOOP ---
    let mut buffer = BytesMut::with_capacity(4096);
    let engine = Arc::new(engine);

    loop {
        let n = reader
            .read_buf(&mut buffer)
            .await
            .map_err(|e| format!("Socket read error: {}", e))?;

        if n == 0 { break; }

        while let Some((frame, consumed)) = match parse_frame(&buffer) {
            Ok(Some(res)) => Some(res),
            Ok(None) => None,
            Err(ParseError::Invalid(e)) => return Err(format!("Protocol error: {}", e)),
            Err(ParseError::Incomplete) => None,
        } {
            let id = frame.id;
            let frame_type = frame.frame_type;
            let payload = frame.payload.to_vec();
            buffer.advance(consumed);

            let tx_clone = tx.clone();
            let engine_clone = Arc::clone(&engine);

            // Parallel execution: One task per request
            tokio::spawn(async move {
                match frame_type {
                    TYPE_REQUEST => {
                        let response = route(&payload, &engine_clone);
                        let _ = tx_clone.send(WriteMessage::Response(id, response)).await;
                    }
                    TYPE_PING => {
                        let _ = tx_clone.send(WriteMessage::Response(id, Response::Ok)).await;
                    }
                    _ => {
                        let _ = tx_clone.send(WriteMessage::Response(id, Response::Error("Unsupported Frame Type".to_string()))).await;
                    }
                }
            });
        }
    }

    drop(tx);
    let _ = write_task.await;
    Ok(())
}

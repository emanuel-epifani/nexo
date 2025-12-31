//! Socket Network Layer: TCP listener + connection handling
//! Handles raw bytes and spawns tasks for parallel command execution.

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use std::sync::Arc;

use crate::server::header_protocol::{
    encode_response, parse_frame, ParseError, Response,
    TYPE_REQUEST, TYPE_PING
};
use crate::server::payload_routing::route;
use crate::NexoEngine;

/// Internal message type for the write loop
enum WriteMessage {
    Response(u32, Response),
}

/// Handle a single client connection.
pub async fn handle_connection(socket: TcpStream, engine: NexoEngine) -> Result<(), String> {
    let (mut reader, writer) = tokio::io::split(socket);
    
    // BATCHING: Wrap writer in a BufWriter for automatic OS-level batching
    let mut buffered_writer = BufWriter::with_capacity(16 * 1024, writer);
    
    let (tx, mut rx) = mpsc::channel::<WriteMessage>(1024);
    
    // --- WRITE LOOP ---
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let bytes = match msg {
                WriteMessage::Response(id, ref resp) => {
                     tracing::debug!("[SRV] -> FRAME RES #{} (Status: {:?})", id, resp);
                     encode_response(id, resp)
                },
            };

            if let Err(e) = buffered_writer.write_all(&bytes).await {
                tracing::error!(error = %e, "[Network] Write error");
                break;
            }
            
            tracing::trace!("[SRV] -> SOCKET WRITE ({} bytes)", bytes.len());

            // If there are no more messages immediately available, flush the buffer
            if rx.is_empty() {
                let _ = buffered_writer.flush().await;
            }
        }
    });

    // --- READ LOOP ---
    // Use a larger buffer to allow for more data in a single read syscall
    let mut buffer = BytesMut::with_capacity(64 * 1024);
    let engine = Arc::new(engine);

    loop {
        let n = reader
            .read_buf(&mut buffer)
            .await
            .map_err(|e| format!("Socket read error: {}", e))?;

        if n > 0 {
             tracing::trace!("[SRV] <- SOCKET READ ({} bytes): {:X?}", n, &buffer[buffer.len() - n..]);
        }

        if n == 0 { break; }

        while let Some((frame_ref, consumed)) = match parse_frame(&buffer) {
            Ok(Some(res)) => Some(res),
            Ok(None) => None,
            Err(ParseError::Invalid(e)) => return Err(format!("Protocol error: {}", e)),
            Err(ParseError::Incomplete) => None,
        } {
            let id = frame_ref.id;
            let frame_type = frame_ref.frame_type;

            tracing::debug!("[SRV] <- FRAME REQ #{} (Type: {:?}, Len: {})", id, frame_type, consumed);
            
            // ZERO-COPY: Split the buffer to get a 'static Bytes object for this frame
            // This is O(1) and does not copy the underlying data.
            let frame_data = buffer.split_to(consumed).freeze();

            let tx_clone = tx.clone();
            let engine_clone = Arc::clone(&engine);

            // Parallel execution: One task per request
            tokio::spawn(async move {
                match frame_type {
                    TYPE_REQUEST => {
                        // Extract payload from the frozen frame_data (offset 9)
                        let payload = frame_data.slice(9..);
                        let response = route(payload, &engine_clone);
                        
                        match response {
                            Response::AsyncConsume(rx) => {
                                tracing::debug!(req_id = %id, "[Network] Request suspended, waiting for queue data...");
                                // Wait for the message in the background task
                                match rx.await {
                                    Ok(msg) => {
                                        tracing::debug!(req_id = %id, msg_id = %msg.id, "[Network] Woke up! Sending QueueData");
                                        let _ = tx_clone.send(WriteMessage::Response(id, Response::QueueData(msg.id, msg.payload))).await;
                                    }
                                    Err(_) => {
                                        tracing::warn!(req_id = %id, "[Network] Consumer channel dropped");
                                        let _ = tx_clone.send(WriteMessage::Response(id, Response::Error("Consumer dropped".into()))).await;
                                    }
                                }
                            }
                            _ => {
                                let _ = tx_clone.send(WriteMessage::Response(id, response)).await;
                            }
                        }
                    }
                    TYPE_PING => {
                        let _ = tx_clone.send(WriteMessage::Response(id, Response::Ok)).await;
                    }
                    _ => {
                        let _ = tx_clone.send(WriteMessage::Response(id, Response::Error("Unsupported".into()))).await;
                    }
                }
            });
        }
    }

    drop(tx);
    let _ = write_task.await;
    Ok(())
}

//! Socket Network Layer: TCP listener + connection handling
//! Handles raw bytes and spawns tasks for parallel command execution.

use bytes::{Buf, BytesMut, Bytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use std::sync::Arc;
use uuid::Uuid;

use crate::server::header_protocol::{
    encode_response, encode_push, parse_frame, ParseError, Response,
    TYPE_REQUEST, TYPE_PING, RequestHeader
};
use crate::server::payload_routing::route;
use crate::NexoEngine;
use crate::brokers::pub_sub::{ClientId, PubSubMessage};
use crate::server::protocol::{NETWORK_BUFFER_READ_SIZE, NETWORK_BUFFER_WRITE_SIZE, CHANNEL_CAPACITY_SOCKET_WRITE};

/// Internal message type for the write loop
enum WriteMessage {
    Response(u32, Response),
    Push(Bytes), // Push notification from broker (e.g. PubSubManager)
}

/// Handle a single client connection.
pub async fn handle_connection(socket: TcpStream, engine: NexoEngine) -> Result<(), String> {
    let (mut reader, writer) = tokio::io::split(socket);
    
    // Generate a unique Client ID
    let client_uuid = Uuid::new_v4();
    let client_id = ClientId(client_uuid.to_string());
    
    // BATCHING: Wrap writer in a BufWriter for automatic OS-level batching
    let mut buffered_writer = BufWriter::with_capacity(NETWORK_BUFFER_WRITE_SIZE, writer);
    
    // Main channel for writing to the socket
    let (tx, mut rx) = mpsc::channel::<WriteMessage>(CHANNEL_CAPACITY_SOCKET_WRITE);
    
    // Channel for PubSubManager to send push notifications to this client
    // We use Unbounded channel for high throughput, but we should monitor for memory usage
    let (push_tx, mut push_rx) = mpsc::unbounded_channel::<Arc<PubSubMessage>>();
    
    // Register with PubSub Manager
    engine.pubsub.connect(client_id.clone(), push_tx);

    // Register with Stream Manager & Hold the Guard
    let _stream_guard = engine.stream.register_session(client_id.0.clone());
    
    // Bridge Task: Forward Pushes to Main Write Loop
    let tx_bridge = tx.clone();
    let bridge_handle = tokio::spawn(async move {
        while let Some(msg_arc) = push_rx.recv().await {
            // Forward push message to the main write loop
            // Use network_cache to get pre-serialized bytes efficiently
            let payload = msg_arc.get_network_packet().clone();
            
            if tx_bridge.send(WriteMessage::Push(payload)).await.is_err() {
                break;
            }
        }
    });
    
    // --- WRITE LOOP ---
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let bytes = match msg {
                WriteMessage::Response(id, ref resp) => encode_response(id, resp),
                WriteMessage::Push(ref payload) => encode_push(0, payload), // ID 0 for server pushes
            };

            if let Err(e) = buffered_writer.write_all(&bytes).await {
                tracing::debug!(error = %e, "[Network] Write error");
                break;
            }

            // If there are no more messages immediately available, flush the buffer
            if rx.is_empty() {
                let _ = buffered_writer.flush().await;
            }
        }
    });
    
    // --- READ LOOP ---
    // Use a larger buffer to allow for more data in a single read syscall
    let mut buffer = BytesMut::with_capacity(NETWORK_BUFFER_READ_SIZE);
    let engine = Arc::new(engine);
    
    // Collect request handler tasks for cleanup on disconnect
    let mut request_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        let n = match reader.read_buf(&mut buffer).await {
            Ok(n) => n,
            Err(e) => {
                // Connection error
                tracing::debug!("Socket read error/close: {}", e);
                break; 
            }
        };

        if n == 0 { break; } // EOF


        while let Some((frame_ref, consumed)) = match parse_frame(&buffer) {
            Ok(Some(res)) => {
                Some(res)
            },
            Ok(None) => {
                None
            },
            Err(ParseError::Invalid(e)) => {
                let _ = tx.send(WriteMessage::Response(0, Response::Error(format!("Proto err: {}", e)))).await;
                return Err(format!("Protocol error: {}", e));
            },
            Err(ParseError::Incomplete) => {
                None
            },
        } {
            let id = frame_ref.header.id;
            let frame_type = frame_ref.header.frame_type;
            let opcode = frame_ref.header.opcode;

            // ZERO-COPY: Split the buffer to get a 'static Bytes object for this frame
            let frame_data = buffer.split_to(consumed).freeze();

            let tx_clone = tx.clone();
            let engine_clone = Arc::clone(&engine);
            let client_id_clone = client_id.clone();

            // Parallel execution: One task per request
            let handle = tokio::spawn(async move {
                match frame_type {
                    TYPE_REQUEST => {
                        let payload = frame_data.slice(RequestHeader::SIZE..);
                        let response = route(opcode, payload, &engine_clone, &client_id_clone).await;
                        
                        let _ = tx_clone.send(WriteMessage::Response(id, response)).await;
                    }
                    TYPE_PING => {
                        let _ = tx_clone.send(WriteMessage::Response(id, Response::Ok)).await;
                    }
                    _ => {
                        let _ = tx_clone.send(WriteMessage::Response(id, Response::Error("Unsupported".into()))).await;
                    }
                }
            });
            request_handles.push(handle);
        }
    }

    // Cleanup: Cancel all in-flight request handlers first.
    // This drops oneshot receivers, making stale waiters (e.g. queue long-poll)
    // detectable by actors via is_closed() / send() failure.
    for handle in &request_handles {
        handle.abort();
    }
    bridge_handle.abort();
    
    tracing::debug!("Client {:?} disconnected", client_id);
    
    // Disconnect from PubSub (cleanup subscriptions, actors)
    engine.pubsub.disconnect(&client_id).await;
    
    // Disconnect from Stream (rebalance)
    engine.stream.disconnect(client_id.0.clone()).await;
    
    // Drop guards and wait for write task
    drop(_stream_guard);
    drop(tx);
    let _ = write_task.await;
    Ok(())
}

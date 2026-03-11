//! Connection Session Layer: lifecycle + routing for a single client session.
//! Owns broker registration, push bridge, and request dispatch.

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::brokers::pub_sub::{ClientId, PubSubMessage};
use crate::config::Config;
use crate::server::payload_routing::RequestHandler;
use crate::server::protocol::{
    InboundFrame, OutboundFrame, ParseError, Response, TYPE_REQUEST, PUSH_TYPE_PUBSUB,
};
use crate::server::socket_network::run_socket;
use crate::NexoEngine;

pub async fn handle_connection(socket: TcpStream, engine: NexoEngine) -> Result<(), String> {
    let config = Config::global();
    let engine = Arc::new(engine); // Wrapped in Arc once for all tasks

    // ==========================================
    // ACT 1: SESSION SETUP & SOCKET CHANNELS
    // ==========================================
    let client_id = ClientId(Uuid::new_v4().to_string());

    // Channels to communicate with the raw TCP socket
    let (inbound_tx, mut inbound_rx) = mpsc::channel(config.server.channel_capacity_socket_write);
    let (outbound_tx, outbound_rx) = mpsc::channel(config.server.channel_capacity_socket_write);

    // Spawn the raw I/O task
    let (reader, writer) = socket.into_split();
    let mut socket_task = tokio::spawn(run_socket(reader, writer, inbound_tx, outbound_rx));

    // ==========================================
    // ACT 2: PUBSUB PUSH BRIDGE
    // ==========================================
    // Channel to receive push notifications from the PubSub Engine
    let (push_tx, mut push_rx) = mpsc::unbounded_channel::<Arc<PubSubMessage>>();
    engine.pubsub.connect(client_id.clone(), push_tx);

    // Background task: forwards PubSub pushes to the socket's outbound channel
    let outbound_bridge = outbound_tx.clone();
    let bridge_handle = tokio::spawn(async move {
        while let Some(msg_arc) = push_rx.recv().await {
            let payload = msg_arc.get_network_packet().clone();
            let frame = OutboundFrame::Push { id: 0, push_type: PUSH_TYPE_PUBSUB, payload };

            if outbound_bridge.send(frame).await.is_err() {
                break; // Socket closed, exit bridge
            }
        }
    });

    // ==========================================
    // ACT 3: MAIN EVENT LOOP (ROUTING)
    // ==========================================
    let mut request_set = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            // EVENT A: We received a command from the Client
            Some(frame) = inbound_rx.recv() => {
                let tx_clone = outbound_tx.clone();
                let engine_clone = Arc::clone(&engine);
                let client_id_clone = client_id.clone();

                request_set.spawn(async move {
                    let id = frame.header.id();
                    let response = match frame.header.frame_type {
                        TYPE_REQUEST => {
                            let handler = RequestHandler::new(&engine_clone, &client_id_clone);
                            handler.route(frame.header.meta, frame.payload).await
                        }
                        _ => Response::Error("Unsupported frame type".into()),
                    };
                    let _ = tx_clone.send(OutboundFrame::Response { id, response }).await;
                });
            }

            // EVENT B: The TCP Socket crashed or disconnected
            socket_result = &mut socket_task => {
                match socket_result {
                    Ok(Ok(())) => break, // Clean disconnect
                    Ok(Err(err)) => return Err(format!("Protocol error: {err:?}")),
                    Err(err) => return Err(format!("Socket task panicked: {err:?}")),
                }
            }

            // EVENT C: A background request finished, clean up its memory
            _ = request_set.join_next(), if !request_set.is_empty() => {}
        }
    }

    // ==========================================
    // ACT 4: CLEANUP & DISCONNECT
    // ==========================================
    tracing::debug!("Client {:?} disconnected", client_id);

    request_set.abort_all();
    bridge_handle.abort();
    engine.pubsub.disconnect(&client_id).await;
    engine.stream.disconnect(client_id.0.clone()).await;

    Ok(())
}

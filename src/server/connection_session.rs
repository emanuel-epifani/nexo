//! Connection Session Layer: lifecycle + routing for a single client session.
//! Owns broker registration, push bridge, and request dispatch.

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::brokers::pub_sub::{ClientId, PubSubMessage};
use crate::config::Config;
use crate::server::payload_routing::route;
use crate::server::protocol::{
    InboundFrame, OutboundFrame, ParseError, Response, TYPE_REQUEST, PUSH_TYPE_PUBSUB,
};
use crate::server::socket_network::run_socket;
use crate::NexoEngine;

/// Handle a single client connection with lifecycle orchestration.
pub async fn handle_connection(socket: TcpStream, engine: NexoEngine) -> Result<(), String> {
    let config = Config::global();
    let (reader, writer) = socket.into_split();

    let client_uuid = Uuid::new_v4();
    let client_id = ClientId(client_uuid.to_string());

    let (inbound_tx, mut inbound_rx) =
        mpsc::channel::<InboundFrame>(config.server.channel_capacity_socket_write);
    let (outbound_tx, outbound_rx) =
        mpsc::channel::<OutboundFrame>(config.server.channel_capacity_socket_write);

    let mut socket_task = tokio::spawn(run_socket(reader, writer, inbound_tx, outbound_rx));

    // Channel for PubSubManager to send push notifications to this client
    // We use Unbounded channel for high throughput, but we should monitor for memory usage
    let (push_tx, mut push_rx) = mpsc::unbounded_channel::<Arc<PubSubMessage>>();

    engine.pubsub.connect(client_id.clone(), push_tx);

    let outbound_bridge = outbound_tx.clone();
    let bridge_handle = tokio::spawn(async move {
        while let Some(msg_arc) = push_rx.recv().await {
            let payload = msg_arc.get_network_packet().clone();

            if outbound_bridge
                .send(OutboundFrame::Push {
                    id: 0,
                    push_type: PUSH_TYPE_PUBSUB,
                    payload,
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let engine = Arc::new(engine);
    let mut request_set = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            inbound = inbound_rx.recv() => {
                let frame = match inbound {
                    Some(frame) => frame,
                    None => break,
                };

                let id = frame.header.id();
                let frame_type = frame.header.frame_type;
                let meta = frame.header.meta;
                let payload = frame.payload;

                let tx_clone = outbound_tx.clone();
                let engine_clone = Arc::clone(&engine);
                let client_id_clone = client_id.clone();

                request_set.spawn(async move {
                    match frame_type {
                        TYPE_REQUEST => {
                            let response = route(meta, payload, &engine_clone, &client_id_clone).await;
                            let _ = tx_clone
                                .send(OutboundFrame::Response { id, response })
                                .await;
                        }
                        _ => {
                            let _ = tx_clone
                                .send(OutboundFrame::Response {
                                    id,
                                    response: Response::Error("Unsupported".into()),
                                })
                                .await;
                        }
                    }
                });
            }
            socket_result = &mut socket_task => {
                match socket_result {
                    Ok(Ok(())) => break,
                    Ok(Err(err)) => {
                        let _ = outbound_tx
                            .send(OutboundFrame::Response {
                                id: 0,
                                response: Response::Error(format!("Proto err: {err:?}")),
                            })
                            .await;
                        return Err(format!("Protocol error: {err:?}"));
                    }
                    Err(err) => {
                        return Err(format!("Socket task error: {err:?}"));
                    }
                }
            }
            _ = request_set.join_next(), if !request_set.is_empty() => {}
        }
    }

    request_set.abort_all();
    bridge_handle.abort();

    tracing::debug!("Client {:?} disconnected", client_id);

    engine.pubsub.disconnect(&client_id).await;
    engine.stream.disconnect(client_id.0.clone()).await;

    drop(outbound_tx);
    let _ = socket_task.await;
    Ok(())
}

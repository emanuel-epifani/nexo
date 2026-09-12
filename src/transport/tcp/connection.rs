//! Connection Session Layer: lifecycle + routing for a single client session.
//! Owns broker registration, push bridge, and request dispatch.
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::brokers::pub_sub::PubSubMessage;
use crate::config::ServerConfig;
use crate::protocol::{
    ErrorCode, InboundFrame, NexoCodec, OutboundFrame, ParseError, Response, TYPE_REQUEST,
    TYPE_REQUEST_NO_RESPONSE,
};
use crate::transport::tcp::dispatcher::{dispatch, is_inline_opcode};
use crate::NexoEngine;

pub async fn handle_connection(
    socket: TcpStream,
    engine: NexoEngine,
    server_config: ServerConfig,
) -> Result<(), String> {
    let engine = Arc::new(engine); // Wrapped in Arc once for all tasks

    // ==========================================
    // ACT 1: SESSION SETUP & SOCKET CHANNELS
    // ==========================================
    let session_id: Arc<str> = Arc::from(Uuid::new_v4().to_string());

    // The write half runs in a dedicated task fed by the outbound channel.
    // Reading remains independent until that bounded channel fills, at which
    // point backpressure intentionally stops the session from buffering more.
    let (outbound_tx, outbound_rx) = mpsc::channel(server_config.channel_capacity_socket_write);
    let (reader, writer) = socket.into_split();
    let mut framed_reader = FramedRead::new(reader, NexoCodec::new(server_config.max_payload_size));
    let mut writer_task = tokio::spawn(run_writer(
        writer,
        outbound_rx,
        server_config.max_payload_size,
    ));

    // ==========================================
    // ACT 2: PUBSUB PUSH BRIDGE
    // ==========================================
    // Channel to receive push notifications from the PubSub Engine
    let (push_tx, mut push_rx) =
        mpsc::channel::<Arc<PubSubMessage>>(engine.pubsub.push_channel_capacity());
    engine.pubsub.connect(&session_id, push_tx);

    // Background task: forwards PubSub pushes to the socket's outbound channel
    let outbound_bridge = outbound_tx.clone();
    let mut bridge_handle = tokio::spawn(async move {
        while let Some(msg_arc) = push_rx.recv().await {
            let payload = msg_arc.get_network_packet().clone();
            let frame = OutboundFrame::PushPubSub { payload };

            if outbound_bridge.send(frame).await.is_err() {
                break; // Socket closed, exit bridge
            }
        }
    });

    // ==========================================
    // ACT 3: MAIN EVENT LOOP (ROUTING)
    // ==========================================
    let mut request_set = tokio::task::JoinSet::new();
    let mut result = Ok(());

    loop {
        tokio::select! {
            // EVENT A: We received a frame from the Client
            frame = framed_reader.next() => {
                let frame = match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(err)) => {
                        result = Err(format!("Protocol error: {err:?}"));
                        break;
                    }
                    None => break, // Clean disconnect
                };

                let opcode = frame.header.meta;
                let inline = !matches!(
                    frame.header.frame_type,
                    TYPE_REQUEST | TYPE_REQUEST_NO_RESPONSE
                ) || is_inline_opcode(opcode);

                if inline {
                    if dispatch_frame(&engine, &session_id, &outbound_tx, frame)
                        .await
                        .is_err()
                    {
                        break;
                    }
                } else {
                    let outbound_tx = outbound_tx.clone();
                    let engine = Arc::clone(&engine);
                    let session_id = Arc::clone(&session_id);

                    request_set.spawn(async move {
                        let _ = dispatch_frame(&engine, &session_id, &outbound_tx, frame).await;
                    });
                }
            }

            // EVENT B: The write side died (socket write error or all senders dropped)
            writer_result = &mut writer_task => {
                match writer_result {
                    Ok(Ok(())) => break,
                    Ok(Err(err)) => {
                        result = Err(format!("Socket write error: {err:?}"));
                        break;
                    }
                    Err(err) => {
                        result = Err(format!("Writer task panicked: {err:?}"));
                        break;
                    }
                }
            }

            // EVENT C: A background request finished, clean up its memory
            _ = request_set.join_next(), if !request_set.is_empty() => {}

            // EVENT D: PubSub bridge exited (slow consumer disconnected by manager)
            _ = &mut bridge_handle => {
                break;
            }
        }
    }

    // ==========================================
    // ACT 4: CLEANUP & DISCONNECT
    // ==========================================
    // Runs on every exit path — including protocol/write errors — otherwise the
    // client's subscriptions, stream bindings and the bridge task would leak.
    tracing::debug!("Client {:?} disconnected", session_id);

    request_set.abort_all();
    bridge_handle.abort();
    writer_task.abort();
    engine.pubsub.disconnect(&session_id);
    engine.stream.disconnect(&*session_id).await;

    result
}

async fn dispatch_frame(
    engine: &NexoEngine,
    session_id: &str,
    outbound_tx: &mpsc::Sender<OutboundFrame>,
    frame: InboundFrame,
) -> Result<(), mpsc::error::SendError<OutboundFrame>> {
    let id = frame.header.id();
    let opcode = frame.header.meta;

    match frame.header.frame_type {
        TYPE_REQUEST => {
            let response = dispatch(engine, session_id, opcode, frame.payload).await;
            outbound_tx
                .send(OutboundFrame::Response { id, response })
                .await
        }
        TYPE_REQUEST_NO_RESPONSE => {
            let _ = dispatch(engine, session_id, opcode, frame.payload).await;
            Ok(())
        }
        _ => {
            outbound_tx
                .send(OutboundFrame::Response {
                    id,
                    response: Response::error(ErrorCode::ProtocolError, "Unsupported frame type"),
                })
                .await
        }
    }
}

/// Drains the outbound channel to the socket. Exits when every sender is
/// dropped or a write fails.
async fn run_writer(
    writer: OwnedWriteHalf,
    mut outbound_rx: mpsc::Receiver<OutboundFrame>,
    max_payload_size: usize,
) -> Result<(), ParseError> {
    let mut framed_writer = FramedWrite::new(writer, NexoCodec::new(max_payload_size));

    while let Some(frame) = outbound_rx.recv().await {
        framed_writer.feed(frame).await?;
        for _ in 1..256 {
            let Ok(frame) = outbound_rx.try_recv() else {
                break;
            };
            framed_writer.feed(frame).await?;
        }
        framed_writer.flush().await?;
    }

    Ok(())
}

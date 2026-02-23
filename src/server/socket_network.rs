//! Socket Network Layer: raw I/O framing for a single connection.

use futures_util::{SinkExt, StreamExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::server::protocol::{InboundFrame, NexoCodec, OutboundFrame, ParseError};

/// Runs framed I/O for a single socket connection.
pub async fn run_socket(
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    inbound_tx: mpsc::Sender<InboundFrame>,
    mut outbound_rx: mpsc::Receiver<OutboundFrame>,
) -> Result<(), ParseError> {
    let mut framed_reader = FramedRead::new(reader, NexoCodec::new());
    let mut framed_writer = FramedWrite::new(writer, NexoCodec::new());

    loop {
        tokio::select! {
            frame = framed_reader.next() => {
                match frame {
                    Some(Ok(frame)) => {
                        if inbound_tx.send(frame).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(err)) => return Err(err),
                    None => break,
                }
            }
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(message) => {
                        if let Err(err) = framed_writer.send(message).await {
                            return Err(err);
                        }
                    }
                    None => break,
                }
            }
        }
    }

    Ok(())
}

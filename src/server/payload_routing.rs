//! Request Router: Routes opcodes to broker handlers
//! Uses specific Command enums for each broker to ensure type safety and clean parsing.

use crate::server::header_protocol::*;
use crate::server::payload_cursor::PayloadCursor;
use crate::NexoEngine;
use bytes::Bytes;
use crate::brokers::pub_sub::ClientId;

// Import Command types from brokers
use crate::brokers::store::commands::StoreCommand;
use crate::brokers::queues::commands::QueueCommand;
use crate::brokers::pub_sub::commands::PubSubCommand;
use crate::brokers::stream::commands::StreamCommand;

// ========================================
// OPCODES (Main dispatch)
// ========================================
pub const OP_DEBUG_ECHO: u8 = 0x00;

// ========================================
// ROUTING
// ========================================

pub fn route(payload: Bytes, engine: &NexoEngine, client_id: &ClientId) -> Response {
    if payload.is_empty() { return Response::Error("Empty payload".to_string()); }
    let opcode = payload[0];
    let mut cursor = PayloadCursor::new(payload.slice(1..));

    match opcode {
        // DEBUG
        OP_DEBUG_ECHO => { Response::Data(cursor.read_remaining()) }

        // KV STORE (0x02 - 0x0F)
        0x02..=0x0F => {
            match StoreCommand::parse(opcode, &mut cursor) {
                Ok(cmd) => handle_store(cmd, engine),
                Err(e) => Response::Error(e),
            }
        }

        // QUEUE (0x10 - 0x1F)
        0x10..=0x1F => {
            match QueueCommand::parse(opcode, &mut cursor) {
                Ok(cmd) => handle_queue(cmd, engine),
                Err(e) => Response::Error(e),
            }
        }

        // PUBSUB (0x21 - 0x2F)
        0x21..=0x2F => {
            match PubSubCommand::parse(opcode, &mut cursor) {
                Ok(cmd) => handle_pubsub(cmd, engine, client_id),
                Err(e) => Response::Error(e),
            }
        }

        // STREAM (0x30 - 0x3F)
        0x30..=0x3F => {
            match StreamCommand::parse(opcode, &mut cursor) {
                Ok(cmd) => handle_stream(cmd, engine, client_id),
                Err(e) => Response::Error(e),
            }
        }

        _ => Response::Error(format!("Unknown opcode: 0x{:02X}", opcode)),
    }
}

// ========================================
// HANDLERS
// ========================================

fn handle_store(cmd: StoreCommand, engine: &NexoEngine) -> Response {
    match cmd {
        StoreCommand::Set { ttl, key, value } => {
            engine.store.set(key, value, ttl)
                .map(|_| Response::Ok)
                .unwrap_or_else(Response::Error)
        }
        StoreCommand::Get { key } => {
            engine.store.get(&key)
                .map(|res| res.map(Response::Data).unwrap_or(Response::Null))
                .unwrap_or_else(Response::Error)
        }
        StoreCommand::Del { key } => {
            engine.store.del(&key)
                .map(|_| Response::Ok)
                .unwrap_or_else(Response::Error)
        }
    }
}

fn handle_queue(cmd: QueueCommand, engine: &NexoEngine) -> Response {
    match cmd {
        QueueCommand::Create { passive, config, q_name } => {
            match engine.queue.declare_queue(q_name, config, passive) {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Push { priority, delay, q_name, payload } => {
            match engine.queue.push(q_name, payload, priority, delay) {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Consume { max_batch, wait_ms, q_name } => {
            let queue_manager = engine.queue.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let result = queue_manager.consume_batch(q_name, max_batch, wait_ms).await;
                let response = match result {
                    Ok(messages) => {
                        let mut buf = Vec::new();
                        buf.extend_from_slice(&(messages.len() as u32).to_be_bytes());
                        for msg in messages {
                            buf.extend_from_slice(msg.id.as_bytes());
                            buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
                            buf.extend_from_slice(&msg.payload);
                        }
                        Ok(Bytes::from(buf))
                    }
                    Err(e) => Err(e),
                };
                let _ = tx.send(response);
            });
            Response::Async(rx)
        }
        QueueCommand::Ack { id, q_name } => {
            if engine.queue.ack(&q_name, id) { Response::Ok } else { Response::Error("ACK failed".to_string()) }
        }
    }
}

fn handle_pubsub(cmd: PubSubCommand, engine: &NexoEngine, client_id: &ClientId) -> Response {
    let pubsub = engine.pubsub.clone();
    let client = client_id.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();

    match cmd {
        PubSubCommand::Publish { flags, topic, payload } => {
            tokio::spawn(async move {
                let _count = pubsub.publish(&topic, payload, flags).await;
                let _ = tx.send(Ok(Bytes::new()));
            });
        }
        PubSubCommand::Subscribe { topic } => {
            tokio::spawn(async move {
                pubsub.subscribe(&topic, client).await;
                let _ = tx.send(Ok(Bytes::new()));
            });
        }
        PubSubCommand::Unsubscribe { topic } => {
            tokio::spawn(async move {
                pubsub.unsubscribe(&topic, &client).await;
                let _ = tx.send(Ok(Bytes::new()));
            });
        }
    }
    Response::Async(rx)
}

fn handle_stream(cmd: StreamCommand, engine: &NexoEngine, client_id: &ClientId) -> Response {
    let stream = engine.stream.clone();
    let client = client_id.0.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();

    match cmd {
        StreamCommand::Create { topic } => {
            tokio::spawn(async move {
                let res = stream.create_topic(topic).await;
                let response = match res { Ok(_) => Ok(Bytes::new()), Err(e) => Err(e) };
                let _ = tx.send(response);
            });
        }
        StreamCommand::Publish { topic, payload } => {
            tokio::spawn(async move {
                let result = stream.publish(&topic, payload).await;
                let response = match result {
                    Ok(offset_id) => Ok(Bytes::from(offset_id.to_be_bytes().to_vec())),
                    Err(e) => Err(e),
                };
                let _ = tx.send(response);
            });
        }
        StreamCommand::Fetch { gen_id, topic, group, partition, offset, limit } => {
            tokio::spawn(async move {
                let result = stream.fetch_group(&group, &client, gen_id, partition, offset, limit as usize, &topic).await;
                let response = match result {
                    Ok(msgs) => {
                        let mut buf = Vec::new();
                        buf.extend_from_slice(&(msgs.len() as u32).to_be_bytes());
                        for msg in msgs {
                            buf.extend_from_slice(&msg.offset.to_be_bytes());
                            buf.extend_from_slice(&msg.timestamp.to_be_bytes());
                            buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
                            buf.extend_from_slice(&msg.payload);
                        }
                        Ok(Bytes::from(buf))
                    },
                    Err(e) => Err(e),
                };
                let _ = tx.send(response);
            });
        }
        StreamCommand::Join { group, topic } => {
            tokio::spawn(async move {
                let result = stream.join_group(&group, &topic, &client).await;
                let response = match result {
                    Ok((gen_id, partitions, start_offsets)) => {
                        let mut buf = Vec::new();
                        buf.extend_from_slice(&gen_id.to_be_bytes());
                        buf.extend_from_slice(&(partitions.len() as u32).to_be_bytes());
                        for p in partitions {
                            buf.extend_from_slice(&p.to_be_bytes());
                            let start_offset = start_offsets.get(&p).cloned().unwrap_or(0);
                            buf.extend_from_slice(&start_offset.to_be_bytes());
                        }
                        Ok(Bytes::from(buf))
                    },
                    Err(e) => Err(e),
                };
                let _ = tx.send(response);
            });
        }
        StreamCommand::Commit { gen_id, group, topic, partition, offset } => {
            tokio::spawn(async move {
                let res = stream.commit_offset(&group, &topic, partition, offset, &client, gen_id).await;
                let response = match res { Ok(_) => Ok(Bytes::new()), Err(e) => Err(e) };
                let _ = tx.send(response);
            });
        }
    }
    Response::Async(rx)
}

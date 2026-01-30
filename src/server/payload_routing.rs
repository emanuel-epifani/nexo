//! Request Router: Routes opcodes to broker handlers
//! Uses specific Command enums for each broker to ensure type safety and clean parsing.

use crate::server::header_protocol::*;
use crate::server::payload_cursor::PayloadCursor;
use crate::NexoEngine;
use bytes::Bytes;
use crate::brokers::pub_sub::ClientId;

// Import Command types from brokers
use crate::brokers::store::commands::StoreCommand;
use crate::brokers::store::map::commands::MapCommand;
use crate::brokers::queues::commands::{QueueCommand, QueueCreateOptions, OP_Q_DELETE};
use crate::brokers::pub_sub::commands::PubSubCommand;
use crate::brokers::stream::commands::StreamCommand;
use crate::brokers::queues::QueueConfig;
use crate::brokers::queues::commands::PersistenceOptions;
use crate::brokers::queues::persistence::types::PersistenceMode;
use crate::config::Config;

// ========================================
// OPCODES (Main dispatch)
// ========================================
pub const OP_DEBUG_ECHO: u8 = 0x00;

// ========================================
// ROUTING
// ========================================

pub async fn route(payload: Bytes, engine: &NexoEngine, client_id: &ClientId) -> Response {
    if payload.is_empty() { return Response::Error("Empty payload".to_string()); }
    let opcode = payload[0];
    let mut cursor = PayloadCursor::new(payload.slice(1..));

    match opcode {
        // DEBUG
        OP_DEBUG_ECHO => { Response::Data(cursor.read_remaining()) }

        // STORE (0x02 - 0x0F)
        0x02..=0x0F => {
            match StoreCommand::parse(opcode, &mut cursor) {
                Ok(cmd) => handle_store(cmd, engine),
                Err(e) => Response::Error(e),
            }
        }

        // QUEUE (0x10 - 0x1F)
        0x10..=0x1F => {
            match QueueCommand::parse(opcode, &mut cursor) {
                Ok(cmd) => handle_queue(cmd, engine).await,
                Err(e) => Response::Error(e),
            }
        }

        // PUBSUB (0x21 - 0x2F)
        0x21..=0x2F => {
            match PubSubCommand::parse(opcode, &mut cursor) {
                Ok(cmd) => handle_pubsub(cmd, engine, client_id).await,
                Err(e) => Response::Error(e),
            }
        }

        // STREAM (0x30 - 0x3F)
        0x30..=0x3F => {
            match StreamCommand::parse(opcode, &mut cursor) {
                Ok(cmd) => handle_stream(cmd, engine, client_id).await,
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
        StoreCommand::Map(map_cmd) => match map_cmd {
            MapCommand::Set { key, options, value } => {
                let ttl = options.ttl;
                // Updated to use the new nested structure
                engine.store.map.set(key, value, ttl);
                Response::Ok
            }
            MapCommand::Get { key } => {
                engine.store.map.get(&key)
                    .map(Response::Data)
                    .unwrap_or(Response::Null)
            }
            MapCommand::Del { key } => {
                engine.store.map.del(&key);
                Response::Ok
            }
        }
    }
}

async fn handle_queue(cmd: QueueCommand, engine: &NexoEngine) -> Response {
    let queue_manager = &engine.queue;

    match cmd {
        QueueCommand::Create { options, q_name } => {
            let visibility_timeout_ms = Config::global().queue.visibility_timeout_ms;
            let max_retries = Config::global().queue.max_retries;
            let ttl_ms = Config::global().queue.ttl_ms;
            
            let persistence = match options.persistence {
                Some(PersistenceOptions::Memory) => PersistenceMode::Memory,
                Some(PersistenceOptions::FileSync) => PersistenceMode::Sync,
                Some(PersistenceOptions::FileAsync { flush_interval_ms }) => PersistenceMode::Async {
                    flush_ms: flush_interval_ms.unwrap_or(100),
                },
                None => PersistenceMode::default(),
            };

            let config = QueueConfig {
                visibility_timeout_ms: options.visibility_timeout_ms.unwrap_or(visibility_timeout_ms),
                max_retries: options.max_retries.unwrap_or(max_retries),
                ttl_ms: options.ttl_ms.unwrap_or(ttl_ms),
                persistence,
            };

            match queue_manager.declare_queue(q_name, config).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Push { q_name, options, payload } => {
            let priority = options.priority.unwrap_or(0);
            let delay = options.delay_ms;
            match queue_manager.push(q_name, payload, priority, delay).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Consume { q_name, options } => {
            let max_batch = options.batch_size;
            let wait_ms = options.wait_ms;
            
            match queue_manager.consume_batch(q_name, max_batch, wait_ms).await {
                Ok(messages) => {
                    let mut buf = Vec::new();
                    buf.extend_from_slice(&(messages.len() as u32).to_be_bytes());
                    for msg in messages {
                        buf.extend_from_slice(msg.id.as_bytes());
                        buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
                        buf.extend_from_slice(&msg.payload);
                    }
                    Response::Data(Bytes::from(buf))
                }
                Err(e) => Response::Error(e),
            }
        }
        QueueCommand::Ack { id, q_name } => {
            match queue_manager.ack(&q_name, id).await {
                true => Response::Ok,
                false => Response::Error("ACK failed".to_string()),
            }
        }
        QueueCommand::Exists { q_name } => {
            match queue_manager.exists(&q_name).await {
                true => Response::Ok,
                false => Response::Error("Queue not found".to_string()),
            }
        }
        QueueCommand::Delete { q_name } => {
            match queue_manager.delete_queue(q_name).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
    }
}

async fn handle_pubsub(cmd: PubSubCommand, engine: &NexoEngine, client_id: &ClientId) -> Response {
    let pubsub = &engine.pubsub;
    let client = client_id.clone();

    match cmd {
        PubSubCommand::Publish { options, topic, payload } => {
            let retain = options.retain.unwrap_or(false);
            let _count = pubsub.publish(&topic, payload, retain).await;
            Response::Ok
        }
        PubSubCommand::Subscribe { topic } => {
            pubsub.subscribe(&topic, client).await;
            Response::Ok
        }
        PubSubCommand::Unsubscribe { topic } => {
            pubsub.unsubscribe(&topic, &client).await;
            Response::Ok
        }
    }
}

async fn handle_stream(cmd: StreamCommand, engine: &NexoEngine, client_id: &ClientId) -> Response {
    let stream = &engine.stream;
    let client = client_id.0.clone();

    match cmd {
        StreamCommand::Create { topic, options } => {
            match stream.create_topic(topic, options).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        StreamCommand::Publish { topic, options, payload } => {
            match stream.publish(&topic, options, payload).await {
                Ok(offset_id) => Response::Data(Bytes::from(offset_id.to_be_bytes().to_vec())),
                Err(e) => Response::Error(e),
            }
        }
        StreamCommand::Fetch { gen_id, topic, group, partition, offset, limit } => {
            match stream.fetch_group(&group, &client, gen_id, partition, offset, limit as usize, &topic).await {
                Ok(msgs) => {
                    let mut buf = Vec::new();
                    buf.extend_from_slice(&(msgs.len() as u32).to_be_bytes());
                    for msg in msgs {
                        buf.extend_from_slice(&msg.offset.to_be_bytes());
                        buf.extend_from_slice(&msg.timestamp.to_be_bytes());
                        buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
                        buf.extend_from_slice(&msg.payload);
                    }
                    Response::Data(Bytes::from(buf))
                }
                Err(e) => Response::Error(e),
            }
        }
        StreamCommand::Join { group, topic } => {
            match stream.join_group(&group, &topic, &client).await {
                Ok((gen_id, partitions, start_offsets)) => {
                    let mut buf = Vec::new();
                    buf.extend_from_slice(&gen_id.to_be_bytes());
                    buf.extend_from_slice(&(partitions.len() as u32).to_be_bytes());
                    for p in partitions {
                        buf.extend_from_slice(&p.to_be_bytes());
                        let start_offset = start_offsets.get(&p).cloned().unwrap_or(0);
                        buf.extend_from_slice(&start_offset.to_be_bytes());
                    }
                    Response::Data(Bytes::from(buf))
                }
                Err(e) => Response::Error(e),
            }
        }
        StreamCommand::Commit { gen_id, group, topic, partition, offset } => {
            match stream.commit_offset(&group, &topic, partition, offset, &client, gen_id).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        StreamCommand::Exists { topic } => {
            match stream.exists(&topic).await {
                true => Response::Ok,
                false => Response::Error("Stream not found".to_string()),
            }
        }
        StreamCommand::Delete { topic } => {
            match stream.delete_topic(topic).await {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
    }
}

//! Request Router: Routes opcodes to broker handlers
//! Uses specific Command enums for each broker to ensure type safety and clean parsing.

use crate::server::protocol::*;
use crate::server::protocol::cursor::PayloadCursor;
use crate::NexoEngine;
use bytes::Bytes;
use crate::brokers::pub_sub::ClientId;

// Import Command types from brokers
use crate::brokers::store::commands::StoreCommand;
use crate::brokers::queue::commands::{QueueCommand, QueueCreateOptions, OP_Q_DELETE, OP_Q_NACK};
use crate::brokers::queue::responses::{ConsumeBatchResponse, PeekDlqResponse, BoolResponse, CountResponse};
use crate::brokers::pub_sub::commands::{PubSubCommand, PubSubPublishConfig};
use crate::brokers::store::map::MapCommand;
use crate::brokers::stream::commands::StreamCommand;
use crate::brokers::stream::responses::{PublishResponse, FetchResponse, JoinGroupResponse};
use crate::config::Config;

// ========================================
// OPCODES (Main dispatch)
// ========================================
pub const OP_DEBUG_ECHO: u8 = 0x00;

// ========================================
// ROUTING
// ========================================

pub struct RequestHandler<'a> {
    engine: &'a NexoEngine,
    client_id: &'a ClientId,
}

impl<'a> RequestHandler<'a> {
    pub fn new(engine: &'a NexoEngine, client_id: &'a ClientId) -> Self {
        Self { engine, client_id }
    }

    pub async fn route(&self, opcode: u8, payload: Bytes) -> Response {
        let mut cursor = PayloadCursor::new(payload);

        match opcode {
            // DEBUG
            OP_DEBUG_ECHO => { Response::Data(cursor.read_remaining()) }

            // STORE
            OPCODE_MIN_STORE..=OPCODE_MAX_STORE => {
                match StoreCommand::parse(opcode, &mut cursor) {
                    Ok(cmd) => self.handle_store(cmd),
                    Err(e) => Response::Error(e.to_string()),
                }
            }

            // QUEUE
            OPCODE_MIN_QUEUE..=OPCODE_MAX_QUEUE => {
                match QueueCommand::parse(opcode, &mut cursor) {
                    Ok(cmd) => self.handle_queue(cmd).await,
                    Err(e) => Response::Error(e.to_string()),
                }
            }

            // PUBSUB
            OPCODE_MIN_PUBSUB..=OPCODE_MAX_PUBSUB => {
                match PubSubCommand::parse(opcode, &mut cursor) {
                    Ok(cmd) => self.handle_pubsub(cmd).await,
                    Err(e) => Response::Error(e.to_string()),
                }
            }

            // STREAM
            OPCODE_MIN_STREAM..=OPCODE_MAX_STREAM => {
                match StreamCommand::parse(opcode, &mut cursor) {
                    Ok(cmd) => self.handle_stream(cmd).await,
                    Err(e) => Response::Error(e.to_string()),
                }
            }

            _ => Response::Error(format!("Unknown opcode: 0x{:02X}", opcode)),
        }
    }

    // ========================================
    // HANDLERS
    // ========================================

    fn handle_store(&self, cmd: StoreCommand) -> Response {
        match cmd {
            StoreCommand::Map(map_cmd) => match map_cmd {
                MapCommand::Set { key, options, value } => {
                    let ttl = options.ttl;
                    // Updated to use the new nested structure
                    self.engine.store.map.set(key, value, ttl);
                    Response::Ok
                }
                MapCommand::Get { key } => {
                    self.engine.store.map.get(&key)
                        .map(Response::Data)
                        .unwrap_or(Response::Null)
                }
                MapCommand::Del { key } => {
                    self.engine.store.map.del(&key);
                    Response::Ok
                }
            }
        }
    }

    async fn handle_queue(&self, cmd: QueueCommand) -> Response {
        let queue_manager = &self.engine.queue;

        match cmd {
            QueueCommand::Create { options, q_name } => {
                match queue_manager.create_queue(q_name, options).await {
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
                    Ok(messages) => Response::Data(ConsumeBatchResponse { messages }.to_wire()),
                    Err(e) => Response::Error(e),
                }
            }
            QueueCommand::Ack { id, q_name } => {
                match queue_manager.ack(&q_name, id).await {
                    true => Response::Ok,
                    false => Response::Error("ACK failed".to_string()),
                }
            }
            QueueCommand::Nack { id, q_name, reason } => {
                match queue_manager.nack(&q_name, id, reason).await {
                    true => Response::Ok,
                    false => Response::Error("NACK failed".to_string()),
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
            QueueCommand::PeekDLQ { q_name, limit, offset } => {
                match queue_manager.peek_dlq(&q_name, limit, offset).await {
                    Ok((total, messages)) => Response::Data(PeekDlqResponse { total, messages }.to_wire()),
                    Err(e) => Response::Error(e),
                }
            }
            QueueCommand::MoveToQueue { q_name, message_id } => {
                match queue_manager.move_to_queue(&q_name, message_id).await {
                    Ok(found) => Response::Data(BoolResponse { value: found }.to_wire()),
                    Err(e) => Response::Error(e),
                }
            }
            QueueCommand::DeleteDLQ { q_name, message_id } => {
                match queue_manager.delete_dlq(&q_name, message_id).await {
                    Ok(found) => Response::Data(BoolResponse { value: found }.to_wire()),
                    Err(e) => Response::Error(e),
                }
            }
            QueueCommand::PurgeDLQ { q_name } => {
                match queue_manager.purge_dlq(&q_name).await {
                    Ok(count) => Response::Data(CountResponse { count }.to_wire()),
                    Err(e) => Response::Error(e),
                }
            }
        }
    }

    async fn handle_pubsub(&self, cmd: PubSubCommand) -> Response {
        let pubsub = &self.engine.pubsub;
        let client = self.client_id.clone();

        match cmd {
            PubSubCommand::Publish { options, topic, payload } => {
                let config = PubSubPublishConfig::from_options(options, &Config::global().pubsub);
                let _count = pubsub.publish(&topic, payload, config.retain, Some(config.ttl_seconds));
                Response::Ok
            }
            PubSubCommand::Subscribe { topic } => {
                pubsub.subscribe(&client, &topic);
                Response::Ok
            }
            PubSubCommand::Unsubscribe { topic } => {
                pubsub.unsubscribe(&client, &topic);
                Response::Ok
            }
        }
    }

    async fn handle_stream(&self, cmd: StreamCommand) -> Response {
        let stream = &self.engine.stream;
        let client = self.client_id.0.clone();

        match cmd {
            StreamCommand::Create { topic, options } => {
                match stream.create_topic(topic, options).await {
                    Ok(_) => Response::Ok,
                    Err(e) => Response::Error(e),
                }
            }
            StreamCommand::Publish { topic, payload } => {
                match stream.publish(&topic, payload).await {
                    Ok(seq) => Response::Data(PublishResponse { seq }.to_wire()),
                    Err(e) => Response::Error(e),
                }
            }
            StreamCommand::Fetch { topic, group, limit, wait_ms } => {
                match stream.fetch(&group, &client, limit as usize, &topic, wait_ms as u64).await {
                    Ok(messages) => Response::Data(FetchResponse { messages }.to_wire()),
                    Err(e) => Response::Error(e),
                }
            }
            StreamCommand::Join { group, topic } => {
                match stream.join_group(&group, &topic, &client).await {
                    Ok(ack_floor) => Response::Data(JoinGroupResponse { ack_floor }.to_wire()),
                    Err(e) => Response::Error(e),
                }
            }
            StreamCommand::Ack { topic, group, seq } => {
                match stream.ack(&group, &topic, seq).await {
                    Ok(_) => Response::Ok,
                    Err(e) => Response::Error(e),
                }
            }
            StreamCommand::Nack { topic, group, seq } => {
                match stream.nack(&group, &topic, seq).await {
                    Ok(_) => Response::Ok,
                    Err(e) => Response::Error(e),
                }
            }
            StreamCommand::Seek { topic, group, target } => {
                match stream.seek(&group, &topic, target).await {
                    Ok(_) => Response::Ok,
                    Err(e) => Response::Error(e),
                }
            }
            StreamCommand::Leave { topic, group } => {
                match stream.leave_group(&group, &topic, &client).await {
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
}

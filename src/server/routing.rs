//! Routing Layer: Dispatching commands to managers

use crate::NexoEngine;
use crate::server::protocol::{Command, KvCommand, QueueCommand, TopicCommand, StreamCommand, Response};
use crate::features::topic::ClientId;

/// Route parsed command to appropriate manager
pub fn route(command: Command, engine: &NexoEngine) -> Result<Response, String> {
    match command {
        Command::Ping => Ok(Response::Ok),

        Command::Kv(kv_cmd) => match kv_cmd {
            KvCommand::Set { key, value, ttl } => {
                engine.kv.set(key, value, ttl)?;
                Ok(Response::Ok)
            },
            KvCommand::Get { key } => {
                match engine.kv.get(&key)? {
                    Some(val) => Ok(Response::Data(val)),
                    None => Ok(Response::Null),
                }
            },
            KvCommand::Del { key } => {
                engine.kv.del(&key)?;
                Ok(Response::Ok)
            }
        },

        Command::Queue(queue_cmd) => match queue_cmd {
            QueueCommand::Push { queue_name, value } => {
                engine.queue.push(queue_name, value);
                Ok(Response::Ok)
            },
            QueueCommand::Pop { queue_name } => {
                match engine.queue.pop(&queue_name) {
                    Some(val) => Ok(Response::Data(val)),
                    None => Ok(Response::Null),
                }
            }
        },

        Command::Topic(topic_cmd) => match topic_cmd {
            TopicCommand::Publish { topic, value: _ } => {
                // TODO: ClientID should come from connection context
                // TODO: use value to send to clients
                let _clients = engine.topic.publish(&topic);
                Ok(Response::Ok)
            },
            TopicCommand::Subscribe { topic } => {
                let dummy_client_id = ClientId("test-client".to_string());
                engine.topic.subscribe(&topic, dummy_client_id);
                Ok(Response::Ok)
            }
        },

        Command::Stream(stream_cmd) => match stream_cmd {
            StreamCommand::Add { topic, value } => {
                let offset = engine.stream.append(topic, value);
                Ok(Response::Data(offset.to_be_bytes().to_vec()))
            },
            StreamCommand::Read { topic, offset } => {
                let messages = engine.stream.read(&topic, offset as usize);
                if let Some(msg) = messages.first() {
                    Ok(Response::Data(msg.payload.clone()))
                } else {
                    Ok(Response::Null)
                }
            }
        },
    }
}

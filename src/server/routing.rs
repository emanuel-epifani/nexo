//! Routing Layer: Command parsing + dispatching to managers

use crate::NexoEngine;
use crate::server::protocol::*;
use std::convert::TryInto;

/// Route parsed command to appropriate manager based on Opcode
pub fn route(req: Request, engine: &NexoEngine) -> Result<Response, String> {
    match req.opcode {
        // =================================
        // META (Ping)
        // =================================
        OP_PING => Ok(Response::Ok),

        // =================================
        // KV STORE (Set/Get/Del)
        // =================================
        OP_KV_SET | OP_KV_GET | OP_KV_DEL => {
            let cmd = crate::features::kv::KvCommand::parse(req.opcode, req.payload)?;
            engine.kv.execute(cmd)
        }

        // =================================
        // QUEUE (Push/Pop)
        // =================================
        OP_Q_PUSH | OP_Q_POP => {
            let cmd = crate::features::queue::QueueCommand::parse(req.opcode, req.payload)?;
            engine.queue.execute(cmd)
        }

        // =================================
        // MQTT / TOPIC (Pub/Sub)
        // =================================
        OP_PUB => {
            let cmd = crate::features::topic::TopicCommand::parse(req.opcode, req.payload)?;
            // TODO: ClientID should come from connection context
            let dummy_sender = crate::features::topic::ClientId("sender".to_string());
            engine.topic.execute(cmd, dummy_sender)
        }

        OP_SUB => {
             let cmd = crate::features::topic::TopicCommand::parse(req.opcode, req.payload)?;
             let dummy_client_id = crate::features::topic::ClientId("test-client".to_string());
             engine.topic.execute(cmd, dummy_client_id)
        }

        // =================================
        // STREAM (Append/Read)
        // =================================
        OP_S_ADD | OP_S_READ => {
            let cmd = crate::features::stream::StreamCommand::parse(req.opcode, req.payload)?;
            engine.stream.execute(cmd)
        }

        _ => Err(format!("Unknown Opcode: 0x{:02X}", req.opcode)),
    }
}

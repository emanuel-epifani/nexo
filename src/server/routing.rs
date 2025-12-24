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
        OP_KV_SET => {
            // Payload: [KeyLen: 4 bytes] [Key bytes] [Value bytes]
            let payload = req.payload;
            if payload.len() < 4 {
                return Err("Payload too short".to_string());
            }

            let key_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
            if payload.len() < 4 + key_len {
                return Err("Invalid frame format".to_string());
            }

            let key_bytes = &payload[4..4 + key_len];
            let value = &payload[4 + key_len..];

            let key = std::str::from_utf8(key_bytes)
                .map_err(|_| "Key must be UTF-8".to_string())?;

            engine.kv.set(key.to_string(), value.to_vec(), None)?;
            Ok(Response::Ok)
        }

        OP_KV_GET => {
            let key = std::str::from_utf8(req.payload)
                .map_err(|_| "Key must be UTF-8".to_string())?;

            match engine.kv.get(key)? {
                Some(val) => Ok(Response::Data(val)),
                None => Ok(Response::Null),
            }
        }

        OP_KV_DEL => {
            let key = std::str::from_utf8(req.payload)
                .map_err(|_| "Key must be UTF-8".to_string())?;
            
            engine.kv.del(key)?;
            Ok(Response::Ok)
        }

        // =================================
        // QUEUE (Push/Pop)
        // =================================
        OP_Q_PUSH => {
            // Payload: [QueueNameLen: 4 bytes] [QueueName] [Value]
            if req.payload.len() < 4 {
                return Err("Payload too short".to_string());
            }
            let q_len = u32::from_be_bytes(req.payload[0..4].try_into().unwrap()) as usize;
            if req.payload.len() < 4 + q_len {
                return Err("Invalid frame format".to_string());
            }

            let q_name_bytes = &req.payload[4..4 + q_len];
            let value = &req.payload[4 + q_len..];

            let q_name = std::str::from_utf8(q_name_bytes)
                .map_err(|_| "Queue name must be UTF-8".to_string())?;

            engine.queue.push(q_name.to_string(), value.to_vec());
            Ok(Response::Ok)
        }

        OP_Q_POP => {
            // Payload: [QueueName]
            let q_name = std::str::from_utf8(req.payload)
                .map_err(|_| "Queue name must be UTF-8".to_string())?;

            match engine.queue.pop(q_name) {
                Some(val) => Ok(Response::Data(val)),
                None => Ok(Response::Null),
            }
        }

        // =================================
        // MQTT / TOPIC (Pub/Sub)
        // =================================
        OP_PUB => {
            // Payload: [TopicLen: 4 bytes] [Topic] [Value]
            // Note: Currently TopicManager::publish returns clients to notify.
            // In a real implementation, we would loop over them and send data.
            // For now, we just acknowledge receipt.
            
            if req.payload.len() < 4 { return Err("Payload too short".to_string()); }
            let t_len = u32::from_be_bytes(req.payload[0..4].try_into().unwrap()) as usize;
            let topic_bytes = &req.payload[4..4 + t_len];
            let topic = std::str::from_utf8(topic_bytes)
                .map_err(|_| "Topic must be UTF-8".to_string())?;

            let _clients = engine.topic.publish(topic);
            // TODO: Iterate clients and send data
            
            Ok(Response::Ok)
        }

        OP_SUB => {
             // Payload: [Topic]
             // TODO: ClientID should come from connection context, not payload (or handshake)
             // For now we use a dummy ID for testing
             let topic = std::str::from_utf8(req.payload)
                .map_err(|_| "Topic must be UTF-8".to_string())?;
             
             let dummy_client_id = crate::features::topic::ClientId("test-client".to_string());
             engine.topic.subscribe(topic, dummy_client_id);
             Ok(Response::Ok)
        }

        // =================================
        // STREAM (Append/Read)
        // =================================
        OP_S_ADD => {
            // Payload: [TopicLen: 4] [Topic] [Value]
            if req.payload.len() < 4 { return Err("Payload too short".to_string()); }
            let t_len = u32::from_be_bytes(req.payload[0..4].try_into().unwrap()) as usize;
            let topic_bytes = &req.payload[4..4 + t_len];
            let value = &req.payload[4 + t_len..];

            let topic = std::str::from_utf8(topic_bytes)
                .map_err(|_| "Topic must be UTF-8".to_string())?;

            let offset = engine.stream.append(topic.to_string(), value.to_vec());
            
            // Return offset as Data (8 bytes big endian)
            Ok(Response::Data(offset.to_be_bytes().to_vec()))
        }

        OP_S_READ => {
            // Payload: [TopicLen: 4] [Topic] [Offset: 8 bytes] (we used usize/u64)
            // Let's assume protocol sends [TopicLen: 4] [Topic] [Offset: 8]
             if req.payload.len() < 12 { return Err("Payload too short".to_string()); }
            let t_len = u32::from_be_bytes(req.payload[0..4].try_into().unwrap()) as usize;
            let topic_bytes = &req.payload[4..4 + t_len];
            
            // Offset starts after Topic
            let offset_start = 4 + t_len;
            if req.payload.len() < offset_start + 8 { return Err("Missing offset".to_string()); }
            
            let offset_bytes = &req.payload[offset_start..offset_start+8];
            let offset = u64::from_be_bytes(offset_bytes.try_into().unwrap());

            let topic = std::str::from_utf8(topic_bytes)
                .map_err(|_| "Topic must be UTF-8".to_string())?;

            let messages = engine.stream.read(topic, offset as usize);
            
            // Serialization of list of messages is complex. 
            // For now, let's just return the number of messages found as a debug string
            // or just the first payload if any.
            // Simplified: return payload of first message found
            if let Some(msg) = messages.first() {
                Ok(Response::Data(msg.payload.clone()))
            } else {
                Ok(Response::Null)
            }
        }

        _ => Err(format!("Unknown Opcode: 0x{:02X}", req.opcode)),
    }
}

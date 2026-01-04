//! Request Router: Routes opcodes to broker handlers
//! Contains the main routing switch and all parsing helpers.

use crate::server::header_protocol::*;
use crate::NexoEngine;
use bytes::Bytes;
use std::convert::TryInto;
use uuid::Uuid;
use crate::brokers::pub_sub::ClientId;

// ========================================
// OPCODES (First byte of Request Payload)
// ========================================
pub const OP_DEBUG_ECHO: u8 = 0x00;

// KV: 0x02 - 0x0F
pub const OP_KV_SET: u8 = 0x02;
pub const OP_KV_GET: u8 = 0x03;
pub const OP_KV_DEL: u8 = 0x04;

// Queue: 0x10 - 0x1F
pub const OP_Q_DECLARE: u8 = 0x10;
pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_CONSUME: u8 = 0x12;
pub const OP_Q_ACK: u8 = 0x13;

// Topic: 0x20 - 0x2F (Renamed to PubSub internally, but opcodes remain same)
pub const OP_PUB: u8 = 0x21;
pub const OP_SUB: u8 = 0x22;
pub const OP_UNSUB: u8 = 0x23;

// Stream: 0x30 - 0x3F
pub const OP_S_CREATE: u8 = 0x30;
pub const OP_S_PUB: u8 = 0x31;
pub const OP_S_FETCH: u8 = 0x32;
pub const OP_S_JOIN: u8 = 0x33;

// ========================================
// PARSING HELPERS
// ========================================

pub fn parse_string(payload: &[u8]) -> Result<(&str, &[u8]), String> {
    if payload.len() < 4 { 
        return Err("Payload too short for length prefix".to_string()); 
    }
    let len = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    if payload.len() < 4 + len { 
        return Err(format!("Incomplete string: expected {} bytes, got {}", 4 + len, payload.len())); 
    }
    let s = std::str::from_utf8(&payload[4..4+len]).map_err(|e| format!("Invalid UTF-8 in string: {}", e))?;
    Ok((s, &payload[4+len..]))
}

pub fn parse_string_u64(payload: &[u8]) -> Result<(&str, u64), String> {
    let (s, rest) = parse_string(payload)?;
    if rest.len() < 8 { return Err("Missing u64 value".to_string()); }
    let n = u64::from_be_bytes([rest[0], rest[1], rest[2], rest[3], rest[4], rest[5], rest[6], rest[7]]);
    Ok((s, n))
}

// ========================================
// ROUTING - One place to route every command to its broker
// ========================================

pub fn route(payload: Bytes, engine: &NexoEngine, client_id: &ClientId) -> Response {
    if payload.is_empty() { return Response::Error("Empty payload".to_string()); }
    let opcode = payload[0];
    let body = payload.slice(1..);

    match opcode {
        // ==========================================
        // DEBUG
        // ==========================================
        
        // DEBUG_ECHO: [Data]
        OP_DEBUG_ECHO => {
            Response::Data(body)
        }

        // ==========================================
        // STORE BROKER (KV operations)
        // ==========================================
        
        // KV_SET: [TTL:8][KeyLen:4][Key][Value]
        OP_KV_SET => {
            if body.len() < 12 { return Response::Error("Payload too short for SET".to_string()); }
            let ttl_secs = u64::from_be_bytes(body[0..8].as_ref().try_into().unwrap());
            let ttl = if ttl_secs == 0 { None } else { Some(ttl_secs) };
            let (key, val_ptr) = match parse_string(&body[8..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let offset = body.len() - val_ptr.len();
            engine.store.set(key.to_string(), body.slice(offset..), ttl)
                .map(|_| Response::Ok)
                .unwrap_or_else(Response::Error)
        }
        
        // KV_GET: [KeyLen:4][Key]
        OP_KV_GET => {
            let (key, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.store.get(key)
                .map(|res| res.map(Response::Data).unwrap_or(Response::Null))
                .unwrap_or_else(Response::Error)
        }
        
        // KV_DEL: [KeyLen:4][Key]
        OP_KV_DEL => {
            let (key, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.store.del(key)
                .map(|_| Response::Ok)
                .unwrap_or_else(Response::Error)
        }

        // ==========================================
        // QUEUE BROKER
        // ==========================================
        
        // Q_DECLARE: [Visibility:8][MaxRetries:4][TTL:8][Delay:8][NameLen:4][Name]
        OP_Q_DECLARE => {
            if body.len() < 32 { return Response::Error("Payload too short for Q_DECLARE".to_string()); }
            let visibility = u64::from_be_bytes(body[0..8].as_ref().try_into().unwrap());
            let max_retries = u32::from_be_bytes(body[8..12].as_ref().try_into().unwrap());
            let ttl = u64::from_be_bytes(body[12..20].as_ref().try_into().unwrap());
            let delay = u64::from_be_bytes(body[20..28].as_ref().try_into().unwrap());
            let (q_name, _) = match parse_string(&body[28..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };

            let config = crate::brokers::queues::QueueConfig {
                visibility_timeout_ms: visibility,
                max_retries,
                ttl_ms: ttl,
                default_delay_ms: delay,
            };
            engine.queue.declare_queue(q_name.to_string(), config);
            Response::Ok
        }
        
        // Q_PUSH: [Priority:1][Delay:8][NameLen:4][Name][Data]
        OP_Q_PUSH => {
            if body.len() < 13 { return Response::Error("Payload too short for Q_PUSH".to_string()); }
            let priority = body[0];
            let delay = u64::from_be_bytes(body[1..9].as_ref().try_into().unwrap());
            let delay_opt = if delay == 0 { None } else { Some(delay) };
            let (q_name, data_ptr) = match parse_string(&body[9..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let offset = body.len() - data_ptr.len();
            engine.queue.push(q_name.to_string(), body.slice(offset..), priority, delay_opt);
            Response::Ok
        }
        
        // Q_CONSUME: [NameLen:4][Name]
        OP_Q_CONSUME => {
            let (q_name, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            Response::AsyncConsume(engine.queue.consume(q_name.to_string()))
        }
        
        // Q_ACK: [UUID:16][NameLen:4][Name]
        OP_Q_ACK => {
            if body.len() < 20 { return Response::Error("Payload too short for Q_ACK".to_string()); }
            let id = Uuid::from_slice(&body[0..16]).unwrap_or_default();
            let (q_name, _) = match parse_string(&body[16..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            if engine.queue.ack(q_name, id) {
                Response::Ok
            } else {
                Response::Error("ACK failed: Message not found".to_string())
            }
        }

        // ==========================================
        // PUBSUB BROKER
        // ==========================================
        
        // PUB: [Flags:1][TopicLen:4][Topic][Data]
        OP_PUB => {
            if body.len() < 1 { return Response::Error("Payload too short".to_string()); }
            let flags = body[0];
            
            // Slice off the flags byte before parsing topic string
            let remaining = &body[1..];
            
            let (topic, val_ptr) = match parse_string(remaining) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            
            // Calculate data offset. 
            // body = [Flags][Len][Topic][Data]
            // val_ptr points to Data inside 'remaining'.
            // We want zero-copy from 'body' Bytes object.
            // Offset from body start = 1 (Flags) + (Data pos in remaining)
            let data_offset_in_remaining = remaining.len() - val_ptr.len();
            let absolute_offset = 1 + data_offset_in_remaining;
            
            // Publish using zero-copy slice for the message data
            let _count = engine.pubsub.publish(topic, body.slice(absolute_offset..), flags);
            Response::Ok
        }
        
        // SUB: [TopicLen:4][Topic]
        OP_SUB => {
            let (topic, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.pubsub.subscribe(topic, client_id.clone());
            Response::Ok
        }

        // UNSUB: [TopicLen:4][Topic]
        OP_UNSUB => {
            let (topic, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.pubsub.unsubscribe(topic, client_id);
            Response::Ok
        }

        // ==========================================
        // STREAM BROKER
        // ==========================================
        
        // S_CREATE: [Partitions:4][TopicLen:4][Topic]
        OP_S_CREATE => {
            if body.len() < 4 { return Response::Error("Payload too short".to_string()); }
            let partitions = u32::from_be_bytes(body[0..4].as_ref().try_into().unwrap());
            let (topic, _) = match parse_string(&body[4..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.stream.create_topic(topic.to_string(), partitions);
            Response::Ok
        }

        // S_PUB: [KeyLen:4][Key][TopicLen:4][Topic][Data]
        OP_S_PUB => {
            // 1. Parse Key
            let (key_str, rest1) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let key = if key_str.is_empty() { None } else { Some(key_str.to_string()) };
            
            // 2. Parse Topic
            let (topic, data_ptr) = match parse_string(rest1) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            
            // 3. Extract Data (Zero Copy)
            // body = [KeyLen][Key][TopicLen][Topic][Data]
            let offset = body.len() - data_ptr.len();
            
            match engine.stream.publish(topic, body.slice(offset..), key) {
                Ok(offset_id) => Response::Data(Bytes::from(offset_id.to_be_bytes().to_vec())),
                Err(e) => Response::Error(e),
            }
        }
        
        // S_FETCH: [TopicLen:4][Topic][Partition:4][Offset:8][Limit:4]
        OP_S_FETCH => {
            let (topic, rest1) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            if rest1.len() < 16 { return Response::Error("Payload too short for fetch params".to_string()); }
            
            let partition = u32::from_be_bytes(rest1[0..4].as_ref().try_into().unwrap());
            let offset = u64::from_be_bytes(rest1[4..12].as_ref().try_into().unwrap());
            let limit = u32::from_be_bytes(rest1[12..16].as_ref().try_into().unwrap());
            
            let msgs = engine.stream.read(topic, partition, offset, limit as usize);
            
            // Serialize messages: [NumMsgs:4] + ([Offset:8][Ts:8][KeyLen:4][Key][Len:4][Payload]...)
            // This is getting complex for zero-copy.
            // For now, let's just return JSON or a simplified binary list.
            // Let's use binary.
            
            let mut buf = Vec::new();
            buf.extend_from_slice(&(msgs.len() as u32).to_be_bytes());
            
            for msg in msgs {
                buf.extend_from_slice(&msg.offset.to_be_bytes());
                buf.extend_from_slice(&msg.timestamp.to_be_bytes());
                
                let k = msg.key.as_deref().unwrap_or("");
                buf.extend_from_slice(&(k.len() as u32).to_be_bytes());
                buf.extend_from_slice(k.as_bytes());
                
                buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
                buf.extend_from_slice(&msg.payload);
            }
            
            Response::Data(Bytes::from(buf))
        }

        // S_JOIN: [GroupLen:4][Group][TopicLen:4][Topic]
        OP_S_JOIN => {
             let (group, rest1) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let (topic, _) = match parse_string(rest1) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            
            match engine.stream.join_group(group, topic, &client_id.0) {
                Ok(partitions) => {
                    // Return [Num:4][P1:4][P2:4]...
                    let mut buf = Vec::new();
                    buf.extend_from_slice(&(partitions.len() as u32).to_be_bytes());
                    for p in partitions {
                        buf.extend_from_slice(&p.to_be_bytes());
                    }
                    Response::Data(Bytes::from(buf))
                },
                Err(e) => Response::Error(e),
            }
        }

        _ => Response::Error(format!("Unknown opcode: 0x{:02X}", opcode)),
    }
}

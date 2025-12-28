//! Request Router: Routes opcodes to broker handlers
//! Contains the main routing switch and all parsing helpers.

use crate::server::header_protocol::*;
use crate::NexoEngine;
use bytes::Bytes;
use std::convert::TryInto;
use uuid::Uuid;

// ========================================
// OPCODES (First byte of Request Payload)
// ========================================
// KV: 0x02 - 0x0F
pub const OP_KV_SET: u8 = 0x02;
pub const OP_KV_GET: u8 = 0x03;
pub const OP_KV_DEL: u8 = 0x04;

// Queue: 0x10 - 0x1F
pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_CONSUME: u8 = 0x12;
pub const OP_Q_ACK: u8 = 0x13;

// Topic: 0x20 - 0x2F
pub const OP_PUB: u8 = 0x21;
pub const OP_SUB: u8 = 0x22;

// Stream: 0x30 - 0x3F
pub const OP_S_ADD: u8 = 0x31;
pub const OP_S_READ: u8 = 0x32;

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

pub fn route(payload: Bytes, engine: &NexoEngine) -> Response {
    if payload.is_empty() { return Response::Error("Empty payload".to_string()); }
    let opcode = payload[0];
    let body = payload.slice(1..);

    match opcode {
        // KV BROKER
        OP_KV_SET => {
            if body.len() < 12 { return Response::Error("Payload too short for SET".to_string()); }
            let ttl_secs = u64::from_be_bytes(body[0..8].as_ref().try_into().unwrap());
            let ttl = if ttl_secs == 0 { None } else { Some(ttl_secs) };
            let (key, val_ptr) = match parse_string(&body[8..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let offset = body.len() - val_ptr.len();
            engine.kv.set(key.to_string(), body.slice(offset..), ttl)
                .map(|_| Response::Ok).unwrap_or_else(Response::Error)
        }
        OP_KV_GET => {
            let (key, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.kv.get(key)
                .map(|res| res.map(Response::Data).unwrap_or(Response::Null))
                .unwrap_or_else(Response::Error)
        }
        OP_KV_DEL => {
            let (key, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.kv.del(key).map(|_| Response::Ok).unwrap_or_else(Response::Error)
        }

        // QUEUE BROKER
        OP_Q_PUSH => {
            // [Priority: 1][Delay: 8][NameLen: 4][Name][Data]
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
        OP_Q_CONSUME => {
            let (q_name, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            Response::AsyncConsume(engine.queue.consume(q_name.to_string()))
        }
        OP_Q_ACK => {
            // [UUID: 16][NameLen: 4][Name]
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

        // TOPIC BROKER
        OP_PUB => {
            let (topic, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.topic.publish(topic);
            Response::Ok
        }
        OP_SUB => {
            let (topic, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.topic.subscribe(topic, crate::brokers::topic::ClientId("test-client".to_string()));
            Response::Ok
        }

        // STREAM BROKER
        OP_S_ADD => {
            let (topic, val_ptr) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let offset = body.len() - val_ptr.len();
            let id = engine.stream.append(topic.to_string(), body.slice(offset..));
            Response::Data(Bytes::from(id.to_be_bytes().to_vec()))
        }
        OP_S_READ => {
            let (topic, offset) = match parse_string_u64(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let msgs = engine.stream.read(topic, offset as usize);
            msgs.first().map(|m| Response::Data(m.payload.clone())).unwrap_or(Response::Null)
        }

        _ => Response::Error(format!("Unknown opcode: 0x{:02X}", opcode)),
    }
}

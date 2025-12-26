//! Request Router: Routes opcodes to broker handlers
//! Contains the main routing switch and all parsing helpers.

use crate::server::protocol::*;
use crate::NexoEngine;

// ========================================
// PARSING HELPERS
// ========================================

/// Parse a single length-prefixed string (returns string + remaining bytes)
fn parse_string(payload: &[u8]) -> Result<(&str, &[u8]), String> {
    if payload.len() < 4 { 
        return Err("Payload too short for length prefix".to_string()); 
    }
    
    let len = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    
    if payload.len() < 4 + len { 
        return Err(format!("Incomplete string: expected {} bytes, got {}", 4 + len, payload.len())); 
    }
    
    let s = std::str::from_utf8(&payload[4..4+len])
        .map_err(|e| format!("Invalid UTF-8 in string: {}", e))?;
    
    Ok((s, &payload[4+len..]))
}

/// Parse length-prefixed string + return all remaining bytes as value
fn parse_string_and_bytes(payload: &[u8]) -> Result<(&str, &[u8]), String> {
    let (s, rest) = parse_string(payload)?;
    Ok((s, rest))
}

/// Parse length-prefixed string + u64
fn parse_string_u64(payload: &[u8]) -> Result<(&str, u64), String> {
    let (s, rest) = parse_string(payload)?;
    
    if rest.len() < 8 { 
        return Err("Missing u64 value".to_string()); 
    }
    
    let n = u64::from_be_bytes([
        rest[0], rest[1], rest[2], rest[3],
        rest[4], rest[5], rest[6], rest[7]
    ]);
    
    Ok((s, n))
}

// ========================================
// ROUTING - One place to route every command to its broker
// ========================================

pub fn route(payload: &[u8], engine: &NexoEngine) -> Response {
    if payload.is_empty() { return Response::Error("Empty payload".to_string()); }
    let opcode = payload[0];
    let body = &payload[1..];

    match opcode {
        // ========================================
        // KV BROKER (0x02 - 0x0F)
        // ========================================
        OP_KV_SET => {
            let (key, val) = match parse_string(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.kv.set(key.to_string(), val.to_vec(), None)
                .map(|_| Response::Ok)
                .unwrap_or_else(Response::Error)
        }
        OP_KV_GET => {
            let (key, _) = match parse_string(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.kv.get(key)
                .map(|res| res.map(Response::Data).unwrap_or(Response::Null))
                .unwrap_or_else(Response::Error)
        }
        OP_KV_DEL => {
            let (key, _) = match parse_string(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.kv.del(key)
                .map(|_| Response::Ok)
                .unwrap_or_else(Response::Error)
        }

        // ========================================
        // QUEUE BROKER (0x10 - 0x1F)
        // ========================================
        OP_Q_PUSH => {
            let (q, val) = match parse_string_and_bytes(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.queue.push(q.to_string(), val.to_vec());
            Response::Ok
        }
        OP_Q_POP => {
            let (q, _) = match parse_string(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.queue.pop(q).map(Response::Data).unwrap_or(Response::Null)
        }

        // ========================================
        // TOPIC BROKER (0x20 - 0x2F)
        // ========================================
        OP_PUB => {
            let (topic, _) = match parse_string_and_bytes(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.topic.publish(topic);
            Response::Ok
        }
        OP_SUB => {
            let (topic, _) = match parse_string(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.topic.subscribe(topic, crate::brokers::topic::ClientId("test-client".to_string()));
            Response::Ok
        }

        // ========================================
        // STREAM BROKER (0x30 - 0x3F)
        // ========================================
        OP_S_ADD => {
            let (topic, val) = match parse_string_and_bytes(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let id = engine.stream.append(topic.to_string(), val.to_vec());
            Response::Data(id.to_be_bytes().to_vec())
        }
        OP_S_READ => {
            let (topic, offset) = match parse_string_u64(body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let msgs = engine.stream.read(topic, offset as usize);
            msgs.first()
                .map(|m| Response::Data(m.payload.clone()))
                .unwrap_or(Response::Null)
        }

        // ========================================
        // UNKNOWN OPCODE
        // ========================================
        _ => Response::Error(format!("Unknown opcode: 0x{:02X}", opcode)),
    }
}


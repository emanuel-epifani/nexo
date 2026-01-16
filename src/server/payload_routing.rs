//! Request Router: Routes opcodes to broker handlers
//! Contains the main routing switch and all parsing helpers.

use crate::server::header_protocol::*;
use crate::NexoEngine;
use bytes::Bytes;
use std::convert::TryInto;
use uuid::Uuid;
use crate::brokers::pub_sub::ClientId;

// ========================================
// OPCODES
// ========================================
pub const OP_DEBUG_ECHO: u8 = 0x00;

// KV
pub const OP_KV_SET: u8 = 0x02;
pub const OP_KV_GET: u8 = 0x03;
pub const OP_KV_DEL: u8 = 0x04;

// Queue
pub const OP_Q_CREATE: u8 = 0x10;
pub const OP_Q_PUSH: u8 = 0x11;
pub const OP_Q_CONSUME: u8 = 0x12;
pub const OP_Q_ACK: u8 = 0x13;

// PubSub
pub const OP_PUB: u8 = 0x21;
pub const OP_SUB: u8 = 0x22;
pub const OP_UNSUB: u8 = 0x23;

// Stream
pub const OP_S_CREATE: u8 = 0x30;
pub const OP_S_PUB: u8 = 0x31;
pub const OP_S_FETCH: u8 = 0x32;
pub const OP_S_JOIN: u8 = 0x33;
pub const OP_S_COMMIT: u8 = 0x34;

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

// ========================================
// ROUTING
// ========================================

pub fn route(payload: Bytes, engine: &NexoEngine, client_id: &ClientId) -> Response {
    if payload.is_empty() { return Response::Error("Empty payload".to_string()); }
    let opcode = payload[0];
    let body = payload.slice(1..);

    match opcode {
        // DEBUG
        OP_DEBUG_ECHO => { Response::Data(body) }

        // KV STORE
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
        OP_KV_GET => {
            let (key, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.store.get(key)
                .map(|res| res.map(Response::Data).unwrap_or(Response::Null))
                .unwrap_or_else(Response::Error)
        }
        OP_KV_DEL => {
            let (key, _) = match parse_string(&body) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            engine.store.del(key)
                .map(|_| Response::Ok)
                .unwrap_or_else(Response::Error)
        }

        // QUEUE
        OP_Q_CREATE => {
            if body.len() < 33 { return Response::Error("Payload too short".to_string()); }
            let flags = body[0];
            let passive = (flags & 0x01) != 0;
            let visibility = u64::from_be_bytes(body[1..9].as_ref().try_into().unwrap());
            let max_retries = u32::from_be_bytes(body[9..13].as_ref().try_into().unwrap());
            let ttl = u64::from_be_bytes(body[13..21].as_ref().try_into().unwrap());
            let delay = u64::from_be_bytes(body[21..29].as_ref().try_into().unwrap());
            let (q_name, _) = match parse_string(&body[29..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let config = crate::brokers::queues::QueueConfig {
                visibility_timeout_ms: visibility, max_retries, ttl_ms: ttl, default_delay_ms: delay,
            };
            match engine.queue.declare_queue(q_name.to_string(), config, passive) {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        OP_Q_PUSH => {
            if body.len() < 13 { return Response::Error("Payload too short".to_string()); }
            let priority = body[0];
            let delay = u64::from_be_bytes(body[1..9].as_ref().try_into().unwrap());
            let delay_opt = if delay == 0 { None } else { Some(delay) };
            let (q_name, data_ptr) = match parse_string(&body[9..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let offset = body.len() - data_ptr.len();
            match engine.queue.push(q_name.to_string(), body.slice(offset..), priority, delay_opt) {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e),
            }
        }
        OP_Q_CONSUME => {
            if body.len() < 16 { return Response::Error("Payload too short".to_string()); }
            let max_batch = u32::from_be_bytes(body[0..4].as_ref().try_into().unwrap()) as usize;
            let wait_ms = u64::from_be_bytes(body[4..12].as_ref().try_into().unwrap());
            let (q_name, _) = match parse_string(&body[12..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let queue_name = q_name.to_string();
            let queue_manager = engine.queue.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let result = queue_manager.consume_batch(queue_name, max_batch, wait_ms).await;
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
        OP_Q_ACK => {
            if body.len() < 20 { return Response::Error("Payload too short".to_string()); }
            let id = Uuid::from_slice(&body[0..16]).unwrap_or_default();
            let (q_name, _) = match parse_string(&body[16..]) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            if engine.queue.ack(q_name, id) { Response::Ok } else { Response::Error("ACK failed".to_string()) }
        }

        // PUBSUB
        OP_PUB => {
            if body.len() < 1 { return Response::Error("Payload too short".to_string()); }
            let flags = body[0];
            let remaining = &body[1..];
            let (topic, val_ptr) = match parse_string(remaining) {
                Ok(res) => res,
                Err(e) => return Response::Error(e),
            };
            let data_offset_in_remaining = remaining.len() - val_ptr.len();
            let absolute_offset = 1 + data_offset_in_remaining;
            let _count = engine.pubsub.publish(topic, body.slice(absolute_offset..), flags);
            Response::Ok
        }
        OP_SUB => {
            let (topic, _) = match parse_string(&body) { Ok(res) => res, Err(e) => return Response::Error(e) };
            engine.pubsub.subscribe(topic, client_id.clone());
            Response::Ok
        }
        OP_UNSUB => {
            let (topic, _) = match parse_string(&body) { Ok(res) => res, Err(e) => return Response::Error(e) };
            engine.pubsub.unsubscribe(topic, client_id);
            Response::Ok
        }

        // ==========================================
        // STREAM BROKER (V1 Protocol with GenID)
        // ==========================================
        
        // S_CREATE: [TopicLen:4][Topic]
        OP_S_CREATE => {
            let (topic, _) = match parse_string(&body) { Ok(res) => res, Err(e) => return Response::Error(e) };
            let topic_name = topic.to_string();
            let stream = engine.stream.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let res = stream.create_topic(topic_name).await;
                let response = match res { Ok(_) => Ok(Bytes::new()), Err(e) => Err(e) };
                let _ = tx.send(response);
            });
            Response::Async(rx)
        }

        // S_PUB: [TopicLen:4][Topic][Data]
        OP_S_PUB => {
            let (topic, data_ptr) = match parse_string(&body) { Ok(res) => res, Err(e) => return Response::Error(e) };
            let offset = body.len() - data_ptr.len();
            let payload = body.slice(offset..);
            let topic_name = topic.to_string();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let stream = engine.stream.clone();
            tokio::spawn(async move {
                let result = stream.publish(&topic_name, payload).await;
                let response = match result {
                    Ok(offset_id) => Ok(Bytes::from(offset_id.to_be_bytes().to_vec())),
                    Err(e) => Err(e),
                };
                let _ = tx.send(response);
            });
            Response::Async(rx)
        }
        
        // S_FETCH: [GenID:8][TopicLen:4][Topic][GroupLen:4][Group][Partition:4][Offset:8][Limit:4]
        // Updated for Rebalancing Support
        OP_S_FETCH => {
            if body.len() < 8 { return Response::Error("Missing GenID".to_string()); }
            let gen_id = u64::from_be_bytes(body[0..8].as_ref().try_into().unwrap());
            
            let (topic, rest1) = match parse_string(&body[8..]) { Ok(res) => res, Err(e) => return Response::Error(e) };
            let (group, rest2) = match parse_string(rest1) { Ok(res) => res, Err(e) => return Response::Error(e) };
            
            if rest2.len() < 16 { return Response::Error("Payload too short for fetch params".to_string()); }
            
            let partition = u32::from_be_bytes(rest2[0..4].as_ref().try_into().unwrap());
            let offset = u64::from_be_bytes(rest2[4..12].as_ref().try_into().unwrap());
            let limit = u32::from_be_bytes(rest2[12..16].as_ref().try_into().unwrap());
            
            let topic_name = topic.to_string();
            let group_id = group.to_string();
            let client = client_id.0.clone();
            
            let (tx, rx) = tokio::sync::oneshot::channel();
            let stream = engine.stream.clone();
            
            tokio::spawn(async move {
                // Now calls fetch_group with all Epoch data
                let result = stream.fetch_group(&group_id, &client, gen_id, partition, offset, limit as usize, &topic_name).await;
                
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
                    Err(e) => Err(e), // Will propagate REBALANCE_NEEDED
                };
                let _ = tx.send(response);
            });
            Response::Async(rx)
        }

        // S_JOIN: [GroupLen:4][Group][TopicLen:4][Topic]
        OP_S_JOIN => {
            let (group, rest1) = match parse_string(&body) { Ok(res) => res, Err(e) => return Response::Error(e) };
            let (topic, _) = match parse_string(rest1) { Ok(res) => res, Err(e) => return Response::Error(e) };
            let group_id = group.to_string();
            let topic_name = topic.to_string();
            let client = client_id.0.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let stream = engine.stream.clone();
            tokio::spawn(async move {
                let result = stream.join_group(&group_id, &topic_name, &client).await;
                let response = match result {
                    Ok((gen_id, partitions, _start_offsets)) => {
                        let mut buf = Vec::new();
                        buf.extend_from_slice(&gen_id.to_be_bytes());
                        buf.extend_from_slice(&(partitions.len() as u32).to_be_bytes());
                        for p in partitions {
                            buf.extend_from_slice(&p.to_be_bytes());
                        }
                        // Send start offset for partition 0 (Compat / Simplification)
                        // Ideally we should send map, but for now client will Fetch(0) and get offset 0 if not committed
                        let start_offset = _start_offsets.get(&0).cloned().unwrap_or(0);
                        buf.extend_from_slice(&start_offset.to_be_bytes());
                        Ok(Bytes::from(buf))
                    },
                    Err(e) => Err(e),
                };
                let _ = tx.send(response);
            });
            Response::Async(rx)
        }

        // S_COMMIT: [GenID:8][GroupLen:4][Group][TopicLen:4][Topic][Partition:4][Offset:8]
        // Updated for Rebalancing Support
        OP_S_COMMIT => {
            if body.len() < 8 { return Response::Error("Missing GenID".to_string()); }
            let gen_id = u64::from_be_bytes(body[0..8].as_ref().try_into().unwrap());

            let (group, rest1) = match parse_string(&body[8..]) { Ok(res) => res, Err(e) => return Response::Error(e) };
            let (topic, rest2) = match parse_string(rest1) { Ok(res) => res, Err(e) => return Response::Error(e) };
            
            if rest2.len() < 12 { return Response::Error("Payload too short for commit".to_string()); }
            
            let partition = u32::from_be_bytes(rest2[0..4].as_ref().try_into().unwrap());
            let offset = u64::from_be_bytes(rest2[4..12].as_ref().try_into().unwrap());
            
            let g = group.to_string();
            let t = topic.to_string();
            let c = client_id.0.clone();
            let stream = engine.stream.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            
            tokio::spawn(async move {
                let res = stream.commit_offset(&g, &t, offset, &c, gen_id).await; // Now passing GenID
                let response = match res { Ok(_) => Ok(Bytes::new()), Err(e) => Err(e) };
                let _ = tx.send(response);
            });
            Response::Async(rx)
        }

        _ => Response::Error(format!("Unknown opcode: 0x{:02X}", opcode)),
    }
}

use bytes::{Bytes, BufMut, BytesMut};
use crate::server::protocol::ToWire;
use crate::brokers::stream::message::Message;

// ========================================
// PUBLISH (returns sequence number)
// ========================================

pub struct PublishResponse {
    pub seq: u64,
}

impl ToWire for PublishResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(self.seq);
        buf.freeze()
    }
}

// ========================================
// FETCH (returns list of messages)
// ========================================

pub struct FetchResponse {
    pub messages: Vec<Message>,
}

impl ToWire for FetchResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(self.messages.len() as u32);
        for msg in &self.messages {
            buf.put_u64(msg.seq);
            buf.put_u64(msg.timestamp);
            buf.put_u32(msg.payload.len() as u32);
            buf.put_slice(&msg.payload);
        }
        buf.freeze()
    }
}

// ========================================
// JOIN GROUP (returns ack_floor offset)
// ========================================

pub struct JoinGroupResponse {
    pub ack_floor: u64,
}

impl ToWire for JoinGroupResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(self.ack_floor);
        buf.freeze()
    }
}

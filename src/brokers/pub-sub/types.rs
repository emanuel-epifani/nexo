//! PubSub Types: Public types used across PubSub modules

use std::sync::OnceLock;
use bytes::{Bytes, BytesMut, BufMut};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

#[derive(Debug)]
pub struct PubSubMessage {
    pub topic: String,
    pub payload: Bytes,
    network_cache: OnceLock<Bytes>,
}

impl PubSubMessage {
    pub fn new(topic: String, payload: Bytes) -> Self {
        Self {
            topic,
            payload,
            network_cache: OnceLock::new(),
        }
    }

    pub fn get_network_packet(&self) -> &Bytes {
        self.network_cache.get_or_init(|| {
            let topic_len = self.topic.len();
            let mut buf = BytesMut::with_capacity(4 + topic_len + self.payload.len());
            buf.put_u32(topic_len as u32);
            buf.put_slice(self.topic.as_bytes());
            buf.put_slice(&self.payload);
            buf.freeze()
        })
    }
}

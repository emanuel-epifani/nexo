use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub seq: u64,
    pub timestamp: u64,
    pub key: Option<Bytes>,
    pub payload: Bytes,
}

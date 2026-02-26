use bytes::Bytes;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub seq: u64,
    pub timestamp: u64,
    pub payload: Bytes,
}

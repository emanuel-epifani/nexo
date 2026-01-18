use std::time::Instant;
use crate::brokers::store::structures::kv::KvValue;

#[derive(Clone, Debug)]
pub enum Value {
    Kv(KvValue),
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

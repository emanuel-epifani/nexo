use std::time::Instant;
use crate::brokers::store::map::MapValue;

#[derive(Clone, Debug)]
pub enum Value {
    Map(MapValue),
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

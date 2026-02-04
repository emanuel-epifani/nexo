use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
pub struct StoreBrokerSnapshot {
    pub keys: Vec<KeyDetail>,
}

#[derive(Serialize)]
pub struct KeyDetail {
    pub key: String,
    pub value: Value,
    pub exp_at: String, // ISO8601
}

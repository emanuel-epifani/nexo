use serde::Serialize;

#[derive(Serialize)]
pub struct StoreBrokerSnapshot {
    pub keys: Vec<KeyDetail>,
}

#[derive(Serialize)]
pub struct KeyDetail {
    pub key: String,
    pub value: String,
    pub expires_at: String, // ISO8601
}

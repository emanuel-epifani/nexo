use serde::Serialize;

#[derive(Serialize)]
pub struct StoreBrokerSnapshot {
    pub total_keys: usize,
    pub expiring_keys: usize,
    pub keys: Vec<KeyDetail>,
}

#[derive(Serialize)]
pub struct KeyDetail {
    pub key: String,
    pub value_preview: String, // Truncated or "[Binary]"
    pub created_at: Option<String>, // ISO8601
    pub expires_at: Option<String>, // ISO8601
}

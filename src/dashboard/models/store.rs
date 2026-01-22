use serde::Serialize;

#[derive(Serialize)]
pub struct StoreBrokerSnapshot {
    pub total_keys: usize,
    pub expiring_keys: usize,
    pub map: MapStructure,
}

#[derive(Serialize)]
pub struct MapStructure {
    pub keys: Vec<KeyDetail>,
}

#[derive(Serialize)]
pub struct KeyDetail {
    pub key: String,
    pub value_preview: String, // Truncated or "[Binary]"
    pub expires_at: Option<String>, // ISO8601
}

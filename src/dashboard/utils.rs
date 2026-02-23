// ========================================
// DASHBOARD UTILS
// ========================================

use crate::server::header_protocol::{DATA_TYPE_JSON, DATA_TYPE_RAW, DATA_TYPE_STRING};

/// Converts a protocol-compliant data payload into a serde_json::Value for dashboard visualization.
/// Format: [DataType: 1 byte][Data...]
pub fn payload_to_dashboard_value(payload: &[u8]) -> serde_json::Value {
    if payload.is_empty() {
        return serde_json::Value::Null;
    }

    // Se il primo byte è un DataType valido (0-2), usalo. Altrimenti tratta tutto come JSON.
    let (data_type, content) = if payload.len() >= 1 && payload[0] <= DATA_TYPE_JSON {
        let data_type = payload[0];
        let content = &payload[1..];
        (data_type, content)
    } else {
        // Legacy: tratta tutto come JSON
        (DATA_TYPE_JSON, payload)
    };

    match data_type {
        DATA_TYPE_JSON => {
            serde_json::from_slice(content).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(content).to_string())
            })
        }
        DATA_TYPE_RAW => {
            serde_json::Value::String(format!("0x{}", hex::encode(content)))
        }
        DATA_TYPE_STRING => {
            serde_json::Value::String(String::from_utf8_lossy(content).to_string())
        }
        _ => {
            serde_json::Value::String(String::from_utf8_lossy(content).to_string())
        }
    }
}

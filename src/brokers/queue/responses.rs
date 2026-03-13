use bytes::Bytes;
use crate::server::protocol::ToWire;
use crate::brokers::queue::queue::Message;
use crate::brokers::queue::dlq::DlqMessage;

// ========================================
// CONSUME BATCH
// ========================================

pub struct ConsumeBatchResponse {
    pub messages: Vec<Message>,
}

impl ToWire for ConsumeBatchResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.messages.len() as u32).to_be_bytes());
        for msg in &self.messages {
            buf.extend_from_slice(msg.id.as_bytes());
            buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&msg.payload);
        }
        Bytes::from(buf)
    }
}

// ========================================
// PEEK DLQ
// ========================================

pub struct PeekDlqResponse {
    pub total: usize,
    pub messages: Vec<DlqMessage>,
}

impl ToWire for PeekDlqResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = Vec::new();

        // Metadata: Total Count & Item Count
        buf.extend_from_slice(&(self.total as u32).to_be_bytes());
        buf.extend_from_slice(&(self.messages.len() as u32).to_be_bytes());

        for msg in &self.messages {
            buf.extend_from_slice(msg.id.as_bytes());
            buf.extend_from_slice(&(msg.payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&msg.payload);
            buf.extend_from_slice(&msg.attempts.to_be_bytes());

            let reason_bytes = msg.failure_reason.as_bytes();
            buf.extend_from_slice(&(reason_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(reason_bytes);
        }
        Bytes::from(buf)
    }
}

// ========================================
// BOOL RESULT (MoveToQueue, DeleteDLQ)
// ========================================

pub struct BoolResponse {
    pub value: bool,
}

impl ToWire for BoolResponse {
    fn to_wire(&self) -> Bytes {
        Bytes::from(vec![if self.value { 1u8 } else { 0u8 }])
    }
}

// ========================================
// COUNT RESULT (PurgeDLQ)
// ========================================

pub struct CountResponse {
    pub count: usize,
}

impl ToWire for CountResponse {
    fn to_wire(&self) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.count as u32).to_be_bytes());
        Bytes::from(buf)
    }
}

//! Stream Manager: Append-only logs for topics
use dashmap::DashMap;
use std::sync::RwLock;
use std::convert::TryInto;
use crate::server::protocol::{OP_S_ADD, OP_S_READ, Response};

// ========================================
// COMMAND PATTERN
// ========================================

#[derive(Debug)]
pub enum StreamCommand {
    Add { topic: String, value: Vec<u8> },
    Read { topic: String, offset: u64 },
}

impl StreamCommand {
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_S_ADD => {
                // Payload: [TopicLen: 4] [Topic] [Value]
                if payload.len() < 4 { return Err("Payload too short".to_string()); }
                let t_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + t_len { return Err("Invalid frame format".to_string()); }
                
                let topic_bytes = &payload[4..4 + t_len];
                let value = payload[4 + t_len..].to_vec();

                let topic = std::str::from_utf8(topic_bytes)
                    .map_err(|_| "Topic must be UTF-8".to_string())?
                    .to_string();

                Ok(StreamCommand::Add { topic, value })
            },
            OP_S_READ => {
                // Payload: [TopicLen: 4] [Topic] [Offset: 8 bytes]
                if payload.len() < 4 { return Err("Payload too short".to_string()); }
                let t_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                
                // Offset starts after Topic
                let offset_start = 4 + t_len;
                if payload.len() < offset_start + 8 { return Err("Missing offset or invalid format".to_string()); }
                
                let topic_bytes = &payload[4..4 + t_len];
                let offset_bytes = &payload[offset_start..offset_start+8];
                let offset = u64::from_be_bytes(offset_bytes.try_into().unwrap());

                let topic = std::str::from_utf8(topic_bytes)
                    .map_err(|_| "Topic must be UTF-8".to_string())?
                    .to_string();

                Ok(StreamCommand::Read { topic, offset })
            },
            _ => Err(format!("Unknown Stream Opcode: 0x{:02X}", opcode)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogMessage {
    pub id: u64,
    pub payload: Vec<u8>,
}

pub struct StreamManager {
    // DashMap separa i topic (Topic A non blocca Topic B)
    // RwLock permette a tanti consumer di leggere lo stesso topic insieme
    topics: DashMap<String, RwLock<Vec<LogMessage>>>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self {
            topics: DashMap::new(),
        }
    }

    pub fn execute(&self, cmd: StreamCommand) -> Result<Response, String> {
        match cmd {
            StreamCommand::Add { topic, value } => {
                let offset = self.append(topic, value);
                Ok(Response::Data(offset.to_be_bytes().to_vec()))
            },
            StreamCommand::Read { topic, offset } => {
                let messages = self.read(&topic, offset as usize);
                // Simplified: return payload of first message found or Null
                if let Some(msg) = messages.first() {
                    Ok(Response::Data(msg.payload.clone()))
                } else {
                    Ok(Response::Null)
                }
            }
        }
    }

    /// Append a message to the topic's stream. Returns the offset (id).
    pub fn append(&self, topic: String, payload: Vec<u8>) -> u64 {
        // Ottieni il lock sul topic specifico.
        // entry() di DashMap blocca solo il bucket di quella chiave, non tutta la mappa.
        let entry = self.topics.entry(topic).or_insert_with(|| RwLock::new(Vec::new()));
        
        // Acquisiamo il lock di scrittura SOLO per questo topic (Log)
        let mut stream = entry.write().unwrap();
        
        let id = stream.len() as u64; // Semplice offset incrementale
        stream.push(LogMessage { id, payload });
        id
    }

    /// Read messages starting from a specific offset
    pub fn read(&self, topic: &str, start_offset: usize) -> Vec<LogMessage> {
        if let Some(entry) = self.topics.get(topic) {
            // Acquisiamo il lock di lettura (più veloce, concorrente)
            let stream = entry.read().unwrap();
            if start_offset < stream.len() {
                // TODO: in produzione limitare il numero di messaggi ritornati (es. max 100)
                return stream[start_offset..].to_vec();
            }
        }
        Vec::new()
    }
}

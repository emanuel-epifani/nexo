//! Stream Manager: Append-only logs for topics
use dashmap::DashMap;
use std::sync::RwLock;

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

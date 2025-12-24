//! Topic Manager: MQTT-style Pub/Sub with wildcard routing
use std::sync::RwLock;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use crate::server::protocol::{OP_PUB, OP_SUB, Response};

// ========================================
// COMMAND PATTERN
// ========================================

#[derive(Debug)]
pub enum TopicCommand {
    Publish { topic: String, value: Vec<u8> },
    Subscribe { topic: String },
}

impl TopicCommand {
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> {
        match opcode {
            OP_PUB => {
                // Payload: [TopicLen: 4 bytes] [Topic] [Value]
                if payload.len() < 4 { return Err("Payload too short".to_string()); }
                let t_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                if payload.len() < 4 + t_len { return Err("Invalid frame format".to_string()); }
                
                let topic_bytes = &payload[4..4 + t_len];
                let value = payload[4 + t_len..].to_vec();
                
                let topic = std::str::from_utf8(topic_bytes)
                    .map_err(|_| "Topic must be UTF-8".to_string())?
                    .to_string();

                Ok(TopicCommand::Publish { topic, value })
            },
            OP_SUB => {
                // Payload: [Topic]
                let topic = std::str::from_utf8(payload)
                    .map_err(|_| "Topic must be UTF-8".to_string())?
                    .to_string();
                Ok(TopicCommand::Subscribe { topic })
            },
            _ => Err(format!("Unknown Topic Opcode: 0x{:02X}", opcode)),
        }
    }
}

// ========================================
// TYPES
// ========================================

// ClientId semplificato per ora (una stringa)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

struct Node {
    children: HashMap<String, Node>,
    subscribers: HashSet<ClientId>, 
}

impl Node {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            subscribers: HashSet::new(),
        }
    }
}

pub struct TopicManager {
    // RwLock permette a 1000 publish di cercare nel trie INSIEME
    root: RwLock<Node>,
}

// ========================================
// IMPLEMENTATION
// ========================================

impl TopicManager {
    pub fn new() -> Self {
        Self {
            root: RwLock::new(Node::new()),
        }
    }

    pub fn execute(&self, cmd: TopicCommand, client_id: ClientId) -> Result<Response, String> {
        match cmd {
            TopicCommand::Publish { topic, value: _ } => {
                // TODO: use value to send to clients
                let _clients = self.publish(&topic);
                Ok(Response::Ok)
            },
            TopicCommand::Subscribe { topic } => {
                self.subscribe(&topic, client_id);
                Ok(Response::Ok)
            }
        }
    }

    /// Iscrive un client a un topic (es. "device/temp")
    pub fn subscribe(&self, topic: &str, client: ClientId) {
        let mut node = self.root.write().unwrap(); // Lock Scrittura (raro)
        let parts: Vec<&str> = topic.split('/').collect();
        
        let mut current = &mut *node;
        for part in parts {
            current = current.children.entry(part.to_string()).or_insert_with(Node::new);
        }
        current.subscribers.insert(client);
    }

    /// Trova tutti i client interessati a un topic
    /// Ritorna la lista di ID a cui dovremmo inviare il messaggio
    pub fn publish(&self, topic: &str) -> Vec<ClientId> {
        let node = self.root.read().unwrap(); // Lock Lettura (frequente)
        let parts: Vec<&str> = topic.split('/').collect();
        
        let mut clients = Vec::new();
        let mut current = &*node;
        
        // Logica semplificata (match esatto). 
        // TODO: Implementare wildcard '+' e '#'
        for part in parts {
            match current.children.get(part) {
                Some(next) => current = next,
                None => return vec![], // Nessuno ascolta
            }
        }
        clients.extend(current.subscribers.iter().cloned());
        clients
    }
}

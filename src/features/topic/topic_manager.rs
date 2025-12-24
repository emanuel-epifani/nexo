//! Topic Manager: MQTT-style Pub/Sub with wildcard routing
use std::sync::RwLock;
use std::collections::{HashMap, HashSet};

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

impl TopicManager {
    pub fn new() -> Self {
        Self {
            root: RwLock::new(Node::new()),
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

//! Topic Manager: MQTT-style Pub/Sub with wildcard routing
//! Supports:
//! - Exact matching: "home/kitchen/temp"
//! - Single-level wildcard: "home/+/temp"
//! - Multi-level wildcard: "home/#"
//! - Active Push to connected clients

use std::sync::RwLock;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use bytes::{Bytes, BytesMut, BufMut};
use dashmap::DashMap;

// ClientId represents a connected client
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

/// A Node in the Radix Tree (Trie) for topic routing
struct Node {
    // Exact match children: "kitchen" -> Node
    children: HashMap<String, Node>,
    
    // Wildcard '+' child: matches any single token at this level
    plus_child: Option<Box<Node>>,
    
    // Wildcard '#' child: matches everything remaining from this level down
    hash_child: Option<Box<Node>>,
    
    // Clients subscribed exactly to the path ending at this node
    subscribers: HashSet<ClientId>, 
}

impl Node {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            plus_child: None,
            hash_child: None,
            subscribers: HashSet::new(),
        }
    }
}

pub struct TopicManager {
    // RwLock allows concurrent reads (publish) while protecting writes (subscribe)
    router: RwLock<Node>,
    
    // Registry of active client connections to send messages to
    // Maps ClientId -> Sender channel (Unbounded for high throughput)
    // DashMap handles concurrency internally (sharding)
    clients: DashMap<ClientId, mpsc::UnboundedSender<Bytes>>,
}

impl TopicManager {
    pub fn new() -> Self {
        Self {
            router: RwLock::new(Node::new()),
            clients: DashMap::new(),
        }
    }

    /// Register a new client connection
    pub fn connect(&self, client_id: ClientId, sender: mpsc::UnboundedSender<Bytes>) {
        self.clients.insert(client_id, sender);
    }

    /// Remove a client connection
    pub fn disconnect(&self, client_id: &ClientId) {
        self.clients.remove(client_id);
    }

    /// Subscribe a client to a topic pattern (supports 'device/+', 'device/#')
    pub fn subscribe(&self, topic: &str, client: ClientId) {
        let mut root = self.router.write().unwrap();
        let parts: Vec<&str> = topic.split('/').collect();
        
        let mut current = &mut *root;
        
        for part in parts.iter() {
            if *part == "#" {
                if current.hash_child.is_none() {
                    current.hash_child = Some(Box::new(Node::new()));
                }
                let hash_node = current.hash_child.as_mut().unwrap();
                hash_node.subscribers.insert(client);
                return; 
            } else if *part == "+" {
                 if current.plus_child.is_none() {
                    current.plus_child = Some(Box::new(Node::new()));
                }
                current = current.plus_child.as_mut().unwrap();
            } else {
                current = current.children.entry(part.to_string()).or_insert_with(Node::new);
            }
        }
        
        current.subscribers.insert(client);
    }

    /// Unsubscribe a client from a specific topic pattern
    pub fn unsubscribe(&self, topic: &str, client: &ClientId) {
        let mut root = self.router.write().unwrap();
        let parts: Vec<&str> = topic.split('/').collect();
        Self::remove_recursive(&mut *root, &parts, client);
    }

    /// Publish a message to all matching subscribers
    /// Returns the number of clients the message was sent to
    pub fn publish(&self, topic: &str, data: Bytes) -> usize {
        // 1. Find target clients (Read lock on Trie)
        let targets = {
            let root = self.router.read().unwrap();
            let parts: Vec<&str> = topic.split('/').collect();
            let mut matched_clients = HashSet::new();
            Self::match_recursive(&root, &parts, &mut matched_clients);
            matched_clients
        };

        if targets.is_empty() {
            return 0;
        }

        // 2. Prepare PUSH Payload: [TopicLen:4][Topic][Data]
        // This format matches what the Client SDK expects in `onPush`
        let topic_len = topic.len();
        let mut push_payload = BytesMut::with_capacity(4 + topic_len + data.len());
        push_payload.put_u32(topic_len as u32);
        push_payload.put_slice(topic.as_bytes());
        push_payload.put_slice(&data);
        let push_payload = push_payload.freeze();

        // 3. Dispatch messages (Lock-free via DashMap)
        let mut sent_count = 0;
        for client_id in targets {
            if let Some(sender) = self.clients.get(&client_id) {
                if sender.send(push_payload.clone()).is_ok() {
                    sent_count += 1;
                }
            }
        }
        sent_count
    }

    // --- Helper Methods ---

    fn match_recursive(node: &Node, parts: &[&str], results: &mut HashSet<ClientId>) {
        if let Some(hash_node) = &node.hash_child {
            results.extend(hash_node.subscribers.iter().cloned());
        }

        if parts.is_empty() {
            results.extend(node.subscribers.iter().cloned());
            return;
        }

        let head = parts[0];
        let tail = &parts[1..];

        if let Some(next_node) = node.children.get(head) {
            Self::match_recursive(next_node, tail, results);
        }

        if let Some(plus_node) = &node.plus_child {
            Self::match_recursive(plus_node, tail, results);
        }
    }

    fn remove_recursive(node: &mut Node, parts: &[&str], client: &ClientId) {
        if parts.is_empty() {
            node.subscribers.remove(client);
            return;
        }

        let head = parts[0];
        let tail = &parts[1..];

        if head == "#" {
             if let Some(ref mut hash_node) = node.hash_child {
                 hash_node.subscribers.remove(client);
             }
        } else if head == "+" {
             if let Some(ref mut plus_node) = node.plus_child {
                 Self::remove_recursive(plus_node, tail, client);
             }
        } else {
             if let Some(next_node) = node.children.get_mut(head) {
                 Self::remove_recursive(next_node, tail, client);
             }
        }
    }
}

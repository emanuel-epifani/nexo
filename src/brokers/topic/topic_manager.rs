//! Topic Manager: MQTT-style Pub/Sub with wildcard routing
//! Supports:
//! - Exact matching: "home/kitchen/temp"
//! - Single-level wildcard: "home/+/temp"
//! - Multi-level wildcard: "home/#"

use std::sync::RwLock;
use std::collections::{HashMap, HashSet};

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
    root: RwLock<Node>,
}

impl TopicManager {
    pub fn new() -> Self {
        Self {
            root: RwLock::new(Node::new()),
        }
    }

    /// Subscribe a client to a topic pattern (supports 'device/+', 'device/#')
    pub fn subscribe(&self, topic: &str, client: ClientId) {
        let mut root = self.root.write().unwrap();
        let parts: Vec<&str> = topic.split('/').collect();
        
        let mut current = &mut *root;
        
        for (i, part) in parts.iter().enumerate() {
            if *part == "#" {
                // '#' must be the last part of the subscription
                // If it's not last, we treat it as an error or just ignore the rest.
                // For safety/simplicity here, we treat it as the terminator.
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
        
        // If we reached here, it's an exact match (or ending with normal token or +)
        current.subscribers.insert(client);
    }

    /// Unsubscribe a client from a specific topic pattern
    pub fn unsubscribe(&self, topic: &str, client: &ClientId) {
        let mut root = self.root.write().unwrap();
        let parts: Vec<&str> = topic.split('/').collect();
        Self::remove_recursive(&mut *root, &parts, client);
    }

    /// Find all clients subscribed to patterns matching this topic
    /// Returns a deduplicated list of ClientIds to fan-out to
    pub fn publish(&self, topic: &str) -> Vec<ClientId> {
        let root = self.root.read().unwrap();
        let parts: Vec<&str> = topic.split('/').collect();
        
        let mut matched_clients = HashSet::new();
        
        Self::match_recursive(&root, &parts, &mut matched_clients);
        
        matched_clients.into_iter().collect()
    }

    // --- Helper Methods ---

    fn match_recursive(node: &Node, parts: &[&str], results: &mut HashSet<ClientId>) {
        // 1. Check '#' wildcard match at this level
        // Matches "sport/tennis/#" against "sport/tennis" (parent level match)
        // And "sport/#" against "sport/tennis"
        if let Some(hash_node) = &node.hash_child {
            results.extend(hash_node.subscribers.iter().cloned());
        }

        if parts.is_empty() {
            // We reached the end of the published topic.
            // Match any exact subscribers at this level.
            results.extend(node.subscribers.iter().cloned());
            return;
        }

        let head = parts[0];
        let tail = &parts[1..];

        // 2. Check exact match
        if let Some(next_node) = node.children.get(head) {
            Self::match_recursive(next_node, tail, results);
        }

        // 3. Check '+' wildcard match
        // The '+' consumes one token ('head') and continues
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

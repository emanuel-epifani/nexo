//! PubSub Manager: MQTT-style Publish/Subscribe with wildcard routing
//! Supports:
//! - Exact matching: "home/kitchen/temp"
//! - Single-level wildcard: "home/+/temp"
//! - Multi-level wildcard: "home/#"
//! - Active Push to connected clients
//! - Retained Messages (Last Value Caching)

use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use bytes::{Bytes, BytesMut, BufMut};
use dashmap::DashMap;
use crate::dashboard::models::pubsub::WildcardSubscription;

// ClientId represents a connected client
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

/// Session Guard for PubSub
/// Automatically disconnects the client when dropped (RAII pattern)
pub struct PubSubSession {
    client_id: ClientId,
    manager: Arc<PubSubManager>,
}

impl Drop for PubSubSession {
    fn drop(&mut self) {
        self.manager.disconnect(&self.client_id);
    }
}

/// A Node in the Radix Tree (Trie) for pub-sub routing
struct Node {
    // Exact match children: "kitchen" -> Node
    children: HashMap<String, Node>,
    
    // Wildcard '+' child: matches any single token at this level
    // Note: Used for routing messages TO subscribers who used '+'
    plus_child: Option<Box<Node>>,
    
    // Wildcard '#' child: matches everything remaining
    // Note: Used for routing messages TO subscribers who used '#'
    hash_child: Option<Box<Node>>,
    
    // Clients subscribed exactly to the path ending at this node
    subscribers: HashSet<ClientId>, 
    
    // Last Retained Message for this pub-sub node
    // Protected by RwLock to allow updates during publish without locking the whole tree structure
    last_retained: RwLock<Option<Bytes>>,
}

impl Node {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            plus_child: None,
            hash_child: None,
            subscribers: HashSet::new(),
            last_retained: RwLock::new(None),
        }
    }
}

pub struct PubSubManager {
    // RwLock allows concurrent reads (publish) while protecting writes (subscribe)
    router: RwLock<Node>,
    
    // Registry of active client connections to send messages to
    clients: DashMap<ClientId, mpsc::UnboundedSender<Bytes>>,

    // Reverse index: ClientId -> Set of subscribed topics
    // Used for efficient cleanup on disconnect
    client_subscriptions: DashMap<ClientId, HashSet<String>>,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            router: RwLock::new(Node::new()),
            clients: DashMap::new(),
            client_subscriptions: DashMap::new(),
        }
    }

    /// Register a new client connection
    /// Returns a Session Guard that automatically handles cleanup when dropped.
    pub fn connect(self: &Arc<Self>, client_id: ClientId, sender: mpsc::UnboundedSender<Bytes>) -> PubSubSession {
        self.clients.insert(client_id.clone(), sender);
        
        PubSubSession {
            client_id,
            manager: self.clone(),
        }
    }

    /// Remove a client connection and cleanup all subscriptions
    pub fn disconnect(&self, client_id: &ClientId) {
        self.clients.remove(client_id);

        // Cleanup subscriptions using reverse index
        // We acquire the lock ONCE and clean everything (Simpler Single Lock Strategy)
        if let Some((_, topics)) = self.client_subscriptions.remove(client_id) {
            let mut root = self.router.write().unwrap();
            for topic in topics {
                let parts: Vec<&str> = topic.split('/').collect();
                // We ignore the return value here because we can't remove the root node
                Self::remove_recursive(&mut *root, &parts, client_id);
            }
        }
    }

    /// Subscribe a client to a topic pattern
    /// If the pattern matches existing nodes with retained messages, they are sent immediately.
    pub fn subscribe(&self, topic: &str, client: ClientId) {
        // 0. Update reverse index
        self.client_subscriptions
            .entry(client.clone())
            .or_insert_with(HashSet::new)
            .insert(topic.to_string());

        let mut root = self.router.write().unwrap();
        let parts: Vec<&str> = topic.split('/').collect();
        
        // 1. Add subscription to the tree
        let mut current = &mut *root;
        for part in parts.iter() {
            if *part == "#" {
                if current.hash_child.is_none() {
                    current.hash_child = Some(Box::new(Node::new()));
                }
                let hash_node = current.hash_child.as_mut().unwrap();
                hash_node.subscribers.insert(client.clone());
                
                // For '#' subscription, we need to traverse everything below 'current'
                // to find retained messages.
                break; // '#' is terminal
            } else if *part == "+" {
                 if current.plus_child.is_none() {
                    current.plus_child = Some(Box::new(Node::new()));
                }
                current = current.plus_child.as_mut().unwrap();
            } else {
                current = current.children.entry(part.to_string()).or_insert_with(Node::new);
            }
        }
        
        // If not ended with '#', add to current node
        if parts.last() != Some(&"#") {
            current.subscribers.insert(client.clone());
        }

        // 2. Send Retained Messages matching this subscription
        // We need to traverse the tree finding nodes that match the 'topic' pattern.
        // If 'topic' is "a/+/b", we find nodes "a/x/b", "a/y/b" etc.
        // If 'topic' is "a/#", we find "a", "a/x", "a/x/y" etc.
        // Since we hold the lock (write lock on router), we can pass 'root' to helper.
        // Note: collect_retained expects immutable ref, which we have (downgrade logic or just ref).
        let mut retained_msgs = Vec::new();
        // We construct the "topic so far" to rebuild the topic string for the PUSH message
        Self::collect_retained(&*root, &parts, "", &mut retained_msgs);

        // Release lock before sending
        drop(root);

        if !retained_msgs.is_empty() {
            if let Some(sender) = self.clients.get(&client) {
                for (topic_str, payload) in retained_msgs {
                    // Re-package as PUSH frame
                    let topic_len = topic_str.len();
                    let mut push_payload = BytesMut::with_capacity(4 + topic_len + payload.len());
                    push_payload.put_u32(topic_len as u32);
                    push_payload.put_slice(topic_str.as_bytes());
                    push_payload.put_slice(&payload);
                    let _ = sender.send(push_payload.freeze());
                }
            }
        }
    }

    /// Unsubscribe a client from a specific topic pattern
    pub fn unsubscribe(&self, topic: &str, client: &ClientId) {
        let mut root = self.router.write().unwrap();
        let parts: Vec<&str> = topic.split('/').collect();
        Self::remove_recursive(&mut *root, &parts, client);
    }

    /// Publish a message to all matching subscribers
    /// flags: Bit 0 = RETAIN
    pub fn publish(&self, topic: &str, data: Bytes, flags: u8) -> usize {
        let retain = (flags & 0x01) != 0;

        // 1. Find target clients (Read lock on Trie) & Update Retained if needed
        let targets = {
            let mut matched_clients = HashSet::new();
            let parts: Vec<&str> = topic.split('/').collect();

            if retain {
                let mut root = self.router.write().unwrap();
                // Update retained value in a scope to drop mutable references to 'current'
                {
                    let mut current = &mut *root;
                    for part in parts.iter() {
                        current = current.children.entry(part.to_string()).or_insert_with(Node::new);
                    }
                    let mut val_lock = current.last_retained.write().unwrap();
                    *val_lock = Some(data.clone());
                }
                
                // Now we can reuse 'root' for matching (reborrow as immutable)
                Self::match_recursive(&*root, &parts, &mut matched_clients);
            } else {
                let root = self.router.read().unwrap();
                Self::match_recursive(&root, &parts, &mut matched_clients);
            }
            matched_clients
        };

        if targets.is_empty() {
            return 0;
        }

        // 2. Prepare PUSH Payload
        let topic_len = topic.len();
        let mut push_payload = BytesMut::with_capacity(4 + topic_len + data.len());
        push_payload.put_u32(topic_len as u32);
        push_payload.put_slice(topic.as_bytes());
        push_payload.put_slice(&data);
        let push_payload = push_payload.freeze();

        // 3. Dispatch messages
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

    pub fn get_snapshot(&self) -> crate::dashboard::models::pubsub::PubSubBrokerSnapshot {
        let root = self.router.read().unwrap();
        
        let mut wildcards = Vec::new();
        for kv in self.client_subscriptions.iter() {
            let client_id = kv.key().0.clone();
            for pattern in kv.value() {
                if pattern.contains('+') || pattern.contains('#') {
                    wildcards.push(WildcardSubscription {
                        pattern: pattern.clone(),
                        client_id: client_id.clone(),
                    });
                }
            }
        }
        
        crate::dashboard::models::pubsub::PubSubBrokerSnapshot {
            active_clients: self.clients.len(),
            topic_tree: Self::build_tree_snapshot("root", "", &root),
            wildcard_subscriptions: wildcards,
        }
    }

    fn build_tree_snapshot(name: &str, path: &str, node: &Node) -> crate::dashboard::models::pubsub::TopicNodeSnapshot {
        let mut children = Vec::new();

        // 1. Regular Children
        for (key, child) in &node.children {
            let next_path = if path.is_empty() { key.clone() } else { format!("{}/{}", path, key) };
            children.push(Self::build_tree_snapshot(key, &next_path, child));
        }

        // 2. Plus Child
        if let Some(plus) = &node.plus_child {
            let next_path = if path.is_empty() { "+".to_string() } else { format!("{}/+", path) };
            children.push(Self::build_tree_snapshot("+", &next_path, plus));
        }

        // 3. Hash Child
        if let Some(hash) = &node.hash_child {
            let next_path = if path.is_empty() { "#".to_string() } else { format!("{}/#", path) };
            children.push(Self::build_tree_snapshot("#", &next_path, hash));
        }

        let retained_val = node.last_retained.read().unwrap()
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string());

        crate::dashboard::models::pubsub::TopicNodeSnapshot {
            name: name.to_string(),
            full_path: path.to_string(),
            subscribers: node.subscribers.len(),
            retained_value: retained_val,
            children,
        }
    }

    // Finds subscribers whose patterns match the published topic
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

    // Traverses the tree to find retained messages that match the subscription pattern
    // pattern: ["sensors", "+", "temp"]
    // current_path: accumulator for the topic string (e.g. "sensors/kitchen/temp")
    fn collect_retained(node: &Node, pattern: &[&str], current_path: &str, results: &mut Vec<(String, Bytes)>) {
        if pattern.is_empty() {
            // End of pattern. If this node has a retained value, collect it.
            // Note: If pattern ended, we matched an exact path.
            if let Some(val) = &*node.last_retained.read().unwrap() {
                results.push((current_path.to_string(), val.clone()));
            }
            return;
        }

        let head = pattern[0];
        let tail = &pattern[1..];

        if head == "#" {
            // Match EVERYTHING below this node recursively
            Self::collect_all_retained_below(node, current_path, results);
            return;
        }

        if head == "+" {
            // Match all immediate children
            for (key, child) in &node.children {
                let next_path = if current_path.is_empty() { key.clone() } else { format!("{}/{}", current_path, key) };
                Self::collect_retained(child, tail, &next_path, results);
            }
        } else {
            // Exact match
            if let Some(child) = node.children.get(head) {
                let next_path = if current_path.is_empty() { head.to_string() } else { format!("{}/{}", current_path, head) };
                Self::collect_retained(child, tail, &next_path, results);
            }
        }
    }

    fn collect_all_retained_below(node: &Node, current_path: &str, results: &mut Vec<(String, Bytes)>) {
        // 1. Check current node
        if let Some(val) = &*node.last_retained.read().unwrap() {
            if !current_path.is_empty() {
                results.push((current_path.to_string(), val.clone()));
            }
        }

        // 2. Recurse children
        for (key, child) in &node.children {
            let next_path = if current_path.is_empty() { key.clone() } else { format!("{}/{}", current_path, key) };
            Self::collect_all_retained_below(child, &next_path, results);
        }
    }

    fn is_node_empty(node: &Node) -> bool {
        node.subscribers.is_empty()
            && node.children.is_empty()
            && node.plus_child.is_none()
            && node.hash_child.is_none()
            && node.last_retained.read().unwrap().is_none()
    }

    fn remove_recursive(node: &mut Node, parts: &[&str], client: &ClientId) -> bool {
        if parts.is_empty() {
            node.subscribers.remove(client);
            return Self::is_node_empty(node);
        }

        let head = parts[0];
        let tail = &parts[1..];
        let mut should_remove_child = false;

        if head == "#" {
             if let Some(ref mut hash_node) = node.hash_child {
                 if Self::remove_recursive(hash_node, tail, client) {
                     should_remove_child = true;
                 }
             }
             if should_remove_child {
                 node.hash_child = None;
             }
        } else if head == "+" {
             if let Some(ref mut plus_node) = node.plus_child {
                 if Self::remove_recursive(plus_node, tail, client) {
                     should_remove_child = true;
                 }
             }
             if should_remove_child {
                 node.plus_child = None;
             }
        } else {
             if let Some(next_node) = node.children.get_mut(head) {
                 if Self::remove_recursive(next_node, tail, client) {
                     should_remove_child = true;
                 }
             }
             if should_remove_child {
                 node.children.remove(head);
             }
        }
        
        Self::is_node_empty(node)
    }
}

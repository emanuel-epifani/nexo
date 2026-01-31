//! PubSub Manager: MQTT-style Publish/Subscribe with wildcard routing
//! 
//! Architecture: Actor per Root
//! - Each root topic (first segment) has its own actor with a radix tree
//! - Actors process commands sequentially (no internal locking)
//! - Parallelism between different root topics
//! - Global "#" subscribers handled separately
//!
//! Supports:
//! - Exact matching: "home/kitchen/temp"
//! - Single-level wildcard: "home/+/temp"
//! - Multi-level wildcard: "home/#"
//! - Active Push to connected clients
//! - Retained Messages (Last Value Caching)

use std::sync::{Arc, OnceLock};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot, RwLock};
use bytes::{Bytes, BytesMut, BufMut};
use dashmap::DashMap;
use crate::dashboard::models::pubsub::TopicSnapshot;

// ==========================================
// PUBLIC TYPES
// ==========================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

#[derive(Debug)]
pub struct PubSubMessage {
    pub topic: String,
    pub payload: Bytes,
    network_cache: OnceLock<Bytes>,
}

impl PubSubMessage {
    pub fn new(topic: String, payload: Bytes) -> Self {
        Self {
            topic,
            payload,
            network_cache: OnceLock::new(),
        }
    }

    pub fn get_network_packet(&self) -> &Bytes {
        self.network_cache.get_or_init(|| {
            let topic_len = self.topic.len();
            let mut buf = BytesMut::with_capacity(4 + topic_len + self.payload.len());
            buf.put_u32(topic_len as u32);
            buf.put_slice(self.topic.as_bytes());
            buf.put_slice(&self.payload);
            buf.freeze()
        })
    }
}

/// Session Guard for PubSub - RAII cleanup on drop
pub struct PubSubSession {
    client_id: ClientId,
    manager: Arc<PubSubManager>,
}

impl Drop for PubSubSession {
    fn drop(&mut self) {
        let manager = self.manager.clone();
        let client_id = self.client_id.clone();
        tokio::spawn(async move {
            manager.disconnect(&client_id).await;
        });
    }
}

// ==========================================
// ACTOR COMMANDS
// ==========================================

enum RootCommand {
    Subscribe {
        pattern: Vec<String>,  // Path segments after root (e.g., ["kitchen", "temp"])
        client: ClientId,
        reply: oneshot::Sender<Vec<(String, Bytes)>>,  // Retained messages to send
    },
    Unsubscribe {
        pattern: Vec<String>,
        client: ClientId,
    },
    Publish {
        parts: Vec<String>,  // Path segments after root
        full_topic: String,  // Full topic for push payload
        data: Bytes,
        retain: bool,
        reply: oneshot::Sender<HashSet<ClientId>>,  // Matched clients
    },
    Disconnect {
        client: ClientId,
    },
    GetFlatSnapshot {
        reply: oneshot::Sender<Vec<crate::dashboard::models::pubsub::TopicSnapshot>>,
    },
}

// ==========================================
// RADIX TREE NODE
// ==========================================

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
    retained: Option<Bytes>,
}

impl Node {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            plus_child: None,
            hash_child: None,
            subscribers: HashSet::new(),
            retained: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
            && self.children.is_empty()
            && self.plus_child.is_none()
            && self.hash_child.is_none()
            && self.retained.is_none()
    }
}

// ==========================================
// ROOT ACTOR
// ==========================================

struct RootActor {
    name: String,
    tree: Node,
    client_patterns: HashMap<ClientId, HashSet<String>>,  // Reverse index for cleanup
    rx: mpsc::Receiver<RootCommand>,
}

impl RootActor {
    fn new(name: String, rx: mpsc::Receiver<RootCommand>) -> Self {
        Self {
            name,
            tree: Node::new(),
            client_patterns: HashMap::new(),
            rx,
        }
    }

    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                RootCommand::Subscribe { pattern, client, reply } => {
                    let retained = self.subscribe(&pattern, &client);
                    let _ = reply.send(retained);
                }
                RootCommand::Unsubscribe { pattern, client } => {
                    self.unsubscribe(&pattern, &client);
                }
                RootCommand::Publish { parts, full_topic, data, retain, reply } => {
                    let clients = self.publish(&parts, &full_topic, data, retain);
                    let _ = reply.send(clients);
                }
                RootCommand::Disconnect { client } => {
                    self.disconnect(&client);
                }
                RootCommand::GetFlatSnapshot { reply } => {
                    let flat_snapshot = self.build_flat_snapshot();
                    let _ = reply.send(flat_snapshot);
                }
            }
        }
    }

    // --- SUBSCRIBE ---
    fn subscribe(&mut self, pattern: &[String], client: &ClientId) -> Vec<(String, Bytes)> {
        // Track pattern for cleanup
        let pattern_str = pattern.join("/");
        self.client_patterns
            .entry(client.clone())
            .or_default()
            .insert(pattern_str);

        // Navigate/create path for subscription
        let mut current = &mut self.tree;
        for (i, part) in pattern.iter().enumerate() {
            if part == "#" {
                if current.hash_child.is_none() {
                    current.hash_child = Some(Box::new(Node::new()));
                }
                current.hash_child.as_mut().unwrap().subscribers.insert(client.clone());
                
                // Collect retained from current node down (not from root!)
                // Navigate to the correct position first for collection
                let mut collect_node = &self.tree;
                for j in 0..i {
                    if let Some(child) = collect_node.children.get(&pattern[j]) {
                        collect_node = child;
                    } else {
                        // Path doesn't exist in tree, no retained to collect
                        return Vec::new();
                    }
                }
                
                let prefix = if i == 0 { 
                    self.name.clone() 
                } else { 
                    format!("{}/{}", self.name, pattern[..i].join("/"))
                };
                let mut retained = Vec::new();
                Self::collect_all_retained(collect_node, &prefix, &mut retained);
                return retained;
            } else if part == "+" {
                if current.plus_child.is_none() {
                    current.plus_child = Some(Box::new(Node::new()));
                }
                current = current.plus_child.as_mut().unwrap();
            } else {
                current = current.children.entry(part.clone()).or_insert_with(Node::new);
            }
        }

        current.subscribers.insert(client.clone());

        // Collect retained matching this exact pattern
        let mut retained = Vec::new();
        Self::collect_retained_for_pattern(&self.tree, pattern, &self.name, &mut retained);
        retained
    }

    // --- UNSUBSCRIBE ---
    fn unsubscribe(&mut self, pattern: &[String], client: &ClientId) {
        let pattern_str = pattern.join("/");
        if let Some(patterns) = self.client_patterns.get_mut(client) {
            patterns.remove(&pattern_str);
        }
        Self::remove_recursive(&mut self.tree, pattern, client);
    }

    // --- PUBLISH ---
    fn publish(&mut self, parts: &[String], full_topic: &str, data: Bytes, retain: bool) -> HashSet<ClientId> {
        // Update retained if needed
            if retain {
            let mut current = &mut self.tree;
            for part in parts {
                current = current.children.entry(part.clone()).or_insert_with(Node::new);
            }
            current.retained = Some(data.clone());
        }

        // Match subscribers
        let mut matched = HashSet::new();
        Self::match_recursive(&self.tree, parts, &mut matched);
        matched
    }

    // --- DISCONNECT ---
    fn disconnect(&mut self, client: &ClientId) {
        if let Some(patterns) = self.client_patterns.remove(client) {
            for pattern_str in patterns {
                let parts: Vec<String> = pattern_str.split('/').map(|s| s.to_string()).collect();
                Self::remove_recursive(&mut self.tree, &parts, client);
            }
        }
    }

    // --- FLAT SNAPSHOT ---
    fn build_flat_snapshot(&self) -> Vec<TopicSnapshot> {
        let mut topics = Vec::new();
        self.collect_flat_topics(&self.tree, &self.name, &mut topics);
        topics
    }

    fn collect_flat_topics(&self, node: &Node, base_path: &str, topics: &mut Vec<TopicSnapshot>) {
        topics.push(TopicSnapshot {
            full_path: base_path.to_string(),
            subscribers: node.subscribers.len(),
            retained_value: node.retained.as_ref()
                .and_then(|bytes| {
                    let payload_withouht_datatype = &bytes[1..];
                    let json_str = String::from_utf8_lossy(payload_withouht_datatype);
                    serde_json::from_str(&json_str).ok()
                }),
        });

        // Process exact match children
        for (child_name, child_node) in &node.children {
            let full_path = format!("{}/{}", base_path, child_name);
            self.collect_flat_topics(child_node, &full_path, topics);
        }
        
        // Process wildcard '+' child
        if let Some(plus_node) = &node.plus_child {
            let full_path = format!("{}/+", base_path);
            self.collect_flat_topics(plus_node, &full_path, topics);
        }
        
        // Process wildcard '#' child
        if let Some(hash_node) = &node.hash_child {
            let full_path = format!("{}/#", base_path);
            self.collect_flat_topics(hash_node, &full_path, topics);
        }
    }

    // --- HELPERS ---

    fn match_recursive(node: &Node, parts: &[String], results: &mut HashSet<ClientId>) {
        // "#" matches everything from here
        if let Some(hash_node) = &node.hash_child {
            results.extend(hash_node.subscribers.iter().cloned());
        }

        if parts.is_empty() {
            results.extend(node.subscribers.iter().cloned());
            return;
        }

        let head = &parts[0];
        let tail = &parts[1..];

        // Exact match
        if let Some(child) = node.children.get(head) {
            Self::match_recursive(child, tail, results);
        }

        // "+" matches any single level
        if let Some(plus_node) = &node.plus_child {
            Self::match_recursive(plus_node, tail, results);
        }
    }

    fn collect_retained_for_pattern(node: &Node, pattern: &[String], current_path: &str, results: &mut Vec<(String, Bytes)>) {
        if pattern.is_empty() {
            if let Some(val) = &node.retained {
                results.push((current_path.to_string(), val.clone()));
            }
            return;
        }

        let head = &pattern[0];
        let tail = &pattern[1..];

        if head == "+" {
            // Single-level wildcard: match any single level
            for (key, child) in &node.children {
                let next_path = format!("{}/{}", current_path, key);
                Self::collect_retained_for_pattern(child, tail, &next_path, results);
            }
        } else if head == "#" {
            // Multi-level wildcard: collect all retained from here down
            Self::collect_all_retained(node, current_path, results);
        } else {
            // Exact match
            if let Some(child) = node.children.get(head) {
                let next_path = format!("{}/{}", current_path, head);
                Self::collect_retained_for_pattern(child, tail, &next_path, results);
            }
        }
    }

    fn collect_all_retained(node: &Node, current_path: &str, results: &mut Vec<(String, Bytes)>) {
        if let Some(val) = &node.retained {
                results.push((current_path.to_string(), val.clone()));
        }
        for (key, child) in &node.children {
            let next_path = format!("{}/{}", current_path, key);
            Self::collect_all_retained(child, &next_path, results);
        }
    }

    fn remove_recursive(node: &mut Node, parts: &[String], client: &ClientId) -> bool {
        if parts.is_empty() {
            node.subscribers.remove(client);
            return node.is_empty();
        }

        let head = &parts[0];
        let tail = &parts[1..];
        let mut should_remove = false;

        if head == "#" {
             if let Some(ref mut hash_node) = node.hash_child {
                hash_node.subscribers.remove(client);
                if hash_node.is_empty() {
                    should_remove = true;
                 }
             }
            if should_remove {
                 node.hash_child = None;
             }
        } else if head == "+" {
             if let Some(ref mut plus_node) = node.plus_child {
                 if Self::remove_recursive(plus_node, tail, client) {
                    should_remove = true;
                 }
             }
            if should_remove {
                 node.plus_child = None;
             }
        } else {
            if let Some(child) = node.children.get_mut(head) {
                if Self::remove_recursive(child, tail, client) {
                    should_remove = true;
                }
            }
            if should_remove {
                 node.children.remove(head);
             }
        }
        
        node.is_empty()
    }
}

// ==========================================
// PUBSUB MANAGER
// ==========================================

pub struct PubSubManager {
    actors: DashMap<String, mpsc::Sender<RootCommand>>,
    clients: DashMap<ClientId, mpsc::UnboundedSender<Arc<PubSubMessage>>>,
    client_subscriptions: DashMap<ClientId, HashSet<String>>,  // For global tracking
    global_hash_subscribers: RwLock<HashSet<ClientId>>,  // Subscribers to "#"
    config: crate::config::PubSubConfig,
}

impl PubSubManager {
    pub fn new(config: crate::config::PubSubConfig) -> Self {
        Self {
            actors: DashMap::new(),
            clients: DashMap::new(),
            client_subscriptions: DashMap::new(),
            global_hash_subscribers: RwLock::new(HashSet::new()),
            config,
        }
    }

    /// Register a new client connection
    pub fn connect(self: &Arc<Self>, client_id: ClientId, sender: mpsc::UnboundedSender<Arc<PubSubMessage>>) -> PubSubSession {
        self.clients.insert(client_id.clone(), sender);
        PubSubSession {
            client_id,
            manager: self.clone(),
        }
    }

    /// Subscribe to a topic pattern
    pub async fn subscribe(&self, topic: &str, client: ClientId) {
        // Track subscription globally
        self.client_subscriptions
            .entry(client.clone())
            .or_default()
            .insert(topic.to_string());

        let parts: Vec<&str> = topic.split('/').collect();

        // Handle global "#" subscription
        if parts.len() == 1 && parts[0] == "#" {
            self.global_hash_subscribers.write().await.insert(client);
            return;
        }

        let root = parts[0].to_string();
        let pattern: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();

        let actor = self.get_or_create_actor(&root);
        let (tx, rx) = oneshot::channel();
        
        if actor.send(RootCommand::Subscribe { pattern, client: client.clone(), reply: tx }).await.is_ok() {
            // Send retained messages
            if let Ok(retained) = rx.await {
                if let Some(sender) = self.clients.get(&client) {
                    for (topic_str, payload) in retained {
                        let msg = Arc::new(PubSubMessage::new(topic_str, payload));
                        let _ = sender.send(msg);
                    }
                }
            }
        }
    }

    /// Unsubscribe from a topic pattern
    pub async fn unsubscribe(&self, topic: &str, client: &ClientId) {
        // Remove from global tracking
        if let Some(mut subs) = self.client_subscriptions.get_mut(client) {
            subs.remove(topic);
        }

        let parts: Vec<&str> = topic.split('/').collect();

        // Handle global "#"
        if parts.len() == 1 && parts[0] == "#" {
            self.global_hash_subscribers.write().await.remove(client);
            return;
        }

        let root = parts[0];
        let pattern: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();

        if let Some(actor) = self.actors.get(root) {
            let _ = actor.send(RootCommand::Unsubscribe { pattern, client: client.clone() }).await;
        }
    }

    /// Publish a message
    pub async fn publish(&self, topic: &str, data: Bytes, retain: bool) -> usize {
        let parts: Vec<&str> = topic.split('/').collect();
        
        if parts.is_empty() {
            return 0;
        }

        let root = parts[0];
        let sub_parts: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();

        // 1. Get matched clients from actor
        let matched_clients = if let Some(actor) = self.actors.get(root) {
            let (tx, rx) = oneshot::channel();
            if actor.send(RootCommand::Publish {
                parts: sub_parts,
                full_topic: topic.to_string(),
                data: data.clone(),
                retain,
                reply: tx,
            }).await.is_ok() {
                rx.await.unwrap_or_default()
            } else {
                HashSet::new()
            }
        } else if retain {
            // Create actor for retained message even if no subscribers yet
            let actor = self.get_or_create_actor(root);
            let (tx, rx) = oneshot::channel();
            if actor.send(RootCommand::Publish {
                parts: sub_parts,
                full_topic: topic.to_string(),
                data: data.clone(),
                retain,
                reply: tx,
            }).await.is_ok() {
                rx.await.unwrap_or_default()
            } else {
                HashSet::new()
            }
        } else {
            HashSet::new()
        };

        // 2. Add global "#" subscribers
        let global_subs = self.global_hash_subscribers.read().await;
        let all_clients: HashSet<_> = matched_clients.iter()
            .chain(global_subs.iter())
            .cloned()
            .collect();

        // 3. Dispatch to all matched clients
        let msg = Arc::new(PubSubMessage::new(topic.to_string(), data));
        let mut sent = 0;

        for client_id in all_clients {
            if let Some(sender) = self.clients.get(&client_id) {
                if sender.send(msg.clone()).is_ok() {
                    sent += 1;
                }
            }
        }

        sent
    }

    /// Disconnect a client and cleanup subscriptions
    pub async fn disconnect(&self, client_id: &ClientId) {
        self.clients.remove(client_id);

        // Remove from global "#" subscribers
        self.global_hash_subscribers.write().await.remove(client_id);

        // Get all subscriptions for this client
        if let Some((_, topics)) = self.client_subscriptions.remove(client_id) {
            // Group by root
            let mut roots: HashSet<String> = HashSet::new();
            for topic in &topics {
                let parts: Vec<&str> = topic.split('/').collect();
                if !parts.is_empty() && parts[0] != "#" {
                    roots.insert(parts[0].to_string());
                }
            }

            // Send disconnect to each relevant actor
            for root in roots {
                if let Some(actor) = self.actors.get(&root) {
                    let _ = actor.send(RootCommand::Disconnect { client: client_id.clone() }).await;
                }
            }
        }
    }

    /// Get snapshot for dashboard
    pub async fn get_snapshot(&self) -> crate::dashboard::models::pubsub::PubSubBrokerSnapshot {
        use crate::dashboard::models::pubsub::{PubSubBrokerSnapshot, WildcardSubscription, WildcardSubscriptions};

        println!("🔍 DEBUG: Clients in manager: {}", self.clients.len());
        println!("🔍 DEBUG: Client subscriptions: {}", self.client_subscriptions.len());
        println!("🔍 DEBUG: Actors: {}", self.actors.len());

        let mut multi_level = Vec::new();
        let mut single_level = Vec::new();
        for entry in self.client_subscriptions.iter() {
            let client_id = entry.key().0.clone();
            println!("🔍 DEBUG: Client {} has {} subscriptions", client_id, entry.value().len());
            for pattern in entry.value().iter() {
                if pattern.contains('#') {
                    println!("🔍 DEBUG: Found multi-level wildcard: {} for client {}", pattern, client_id);
                    multi_level.push(WildcardSubscription {
                        pattern: pattern.clone(),
                        client_id: client_id.clone(),
                    });
                } else if pattern.contains('+') {
                    println!("🔍 DEBUG: Found single-level wildcard: {} for client {}", pattern, client_id);
                    single_level.push(WildcardSubscription {
                        pattern: pattern.clone(),
                        client_id: client_id.clone(),
                    });
                }
            }
        }

        // Build flat topics list by querying each actor
        let mut all_topics = Vec::new();
        for entry in self.actors.iter() {
            println!("🔍 DEBUG: Querying actor for root: {}", entry.key());
            let (tx, rx) = oneshot::channel();
            if entry.value().send(RootCommand::GetFlatSnapshot { reply: tx }).await.is_ok() {
                if let Ok(topics) = rx.await {
                    println!("🔍 DEBUG: Actor {} returned {} topics", entry.key(), topics.len());
                    all_topics.extend(topics);
                }
            }
        }

        PubSubBrokerSnapshot {
            active_clients: self.clients.len(),
            topics: all_topics,
            wildcards: WildcardSubscriptions {
                multi_level,
                single_level,
            },
        }
    }

    // --- HELPERS ---

    fn get_or_create_actor(&self, root: &str) -> mpsc::Sender<RootCommand> {
        self.actors.entry(root.to_string()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(self.config.actor_channel_capacity);
            let actor = RootActor::new(root.to_string(), rx);
            tokio::spawn(actor.run());
            tx
        }).clone()
    }
}

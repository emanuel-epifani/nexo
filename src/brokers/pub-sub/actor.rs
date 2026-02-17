//! PubSub Root Actor: Async actor owning a radix tree for a root topic
//! 
//! Each root topic (first segment) has its own actor with a radix tree.
//! Actors process commands sequentially (no internal locking).

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use dashmap::DashMap;
use crate::dashboard::models::pubsub::TopicSnapshot;
use crate::dashboard::utils::payload_to_dashboard_value;
use crate::brokers::pub_sub::types::{ClientId, PubSubMessage};
use crate::brokers::pub_sub::radix_tree::Node;
use crate::brokers::pub_sub::retained::RetainedMessage;

// ==========================================
// ACTOR COMMANDS
// ==========================================

pub(crate) enum RootCommand {
    Subscribe {
        pattern: Vec<String>,
        client: ClientId,
        reply: oneshot::Sender<Vec<(String, Bytes)>>,  // Retained messages to send
    },
    Unsubscribe {
        pattern: Vec<String>,
        client: ClientId,
    },
    Publish {
        parts: Vec<String>,
        full_topic: String,
        data: Bytes,
        retain: bool,
        ttl_seconds: Option<u64>,
        reply: oneshot::Sender<usize>,
    },
    Disconnect {
        client: ClientId,
        reply: oneshot::Sender<()>,
    },
    GetFlatSnapshot {
        reply: oneshot::Sender<Vec<crate::dashboard::models::pubsub::TopicSnapshot>>,
    },
    IsEmpty {
        reply: oneshot::Sender<bool>,
    },
    CleanupExpired,
}

// ==========================================
// ROOT ACTOR
// ==========================================

pub(crate) type ClientRegistry = Arc<DashMap<ClientId, mpsc::UnboundedSender<Arc<PubSubMessage>>>>;

pub(crate) struct RootActor {
    name: String,
    tree: Node,
    client_patterns: HashMap<ClientId, HashSet<String>>,
    /// Shared read-only reference to the global client registry (single source of truth for Senders)
    clients: ClientRegistry,
    rx: mpsc::Receiver<RootCommand>,
    retained_file_path: PathBuf,
    default_ttl_seconds: u64,
}

impl RootActor {
    pub(crate) fn new(
        name: String,
        rx: mpsc::Receiver<RootCommand>,
        persistence_path: &str,
        default_ttl_seconds: u64,
        clients: ClientRegistry,
    ) -> Self {
        let retained_file_path = PathBuf::from(format!("{}/pubsub/retained/{}.json", persistence_path, name));
        
        let mut actor = Self {
            name,
            tree: Node::new(),
            client_patterns: HashMap::new(),
            clients,
            rx,
            retained_file_path,
            default_ttl_seconds,
        };
        
        actor.load_retained();
        actor
    }

    pub(crate) async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                RootCommand::Subscribe { pattern, client, reply } => {
                    let retained = self.subscribe(&pattern, &client);
                    let _ = reply.send(retained);
                }
                RootCommand::Unsubscribe { pattern, client } => {
                    self.unsubscribe(&pattern, &client);
                }
                RootCommand::Publish { parts, full_topic, data, retain, ttl_seconds, reply } => {
                    let count = self.publish(&parts, &full_topic, data, retain, ttl_seconds);
                    let _ = reply.send(count);
                }
                RootCommand::Disconnect { client, reply } => {
                    self.disconnect(&client);
                    let _ = reply.send(());
                }
                RootCommand::GetFlatSnapshot { reply } => {
                    let flat_snapshot = self.build_flat_snapshot();
                    let _ = reply.send(flat_snapshot);
                }
                RootCommand::IsEmpty { reply } => {
                    let is_empty = self.tree.is_empty() && self.client_patterns.is_empty();
                    tracing::debug!("RootActor '{}' IsEmpty check: {}", self.name, is_empty);
                    let _ = reply.send(is_empty);
                }
                RootCommand::CleanupExpired => {
                    self.cleanup_expired_retained();
                }
            }
        }
        tracing::debug!("RootActor '{}' channel closed, exiting", self.name);
    }

    // --- SUBSCRIBE ---
    fn subscribe(&mut self, pattern: &[String], client: &ClientId) -> Vec<(String, Bytes)> {
        let pattern_str = pattern.join("/");
        self.client_patterns
            .entry(client.clone())
            .or_default()
            .insert(pattern_str);

        let mut current = &mut self.tree;
        for (i, part) in pattern.iter().enumerate() {
            if part == "#" {
                if current.hash_child.is_none() {
                    current.hash_child = Some(Box::new(Node::new()));
                }
                current.hash_child.as_mut().unwrap().subscribers.insert(client.clone());
                
                let mut collect_node = &self.tree;
                for j in 0..i {
                    if let Some(child) = collect_node.children.get(&pattern[j]) {
                        collect_node = child;
                    } else {
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
    fn publish(&mut self, parts: &[String], full_topic: &str, data: Bytes, retain: bool, ttl_seconds: Option<u64>) -> usize {
        if retain {
            let mut current = &mut self.tree;
            for part in parts {
                current = current.children.entry(part.clone()).or_insert_with(Node::new);
            }
            
            if data.is_empty() {
                current.retained = None;
            } else {
                let effective_ttl = ttl_seconds.unwrap_or(self.default_ttl_seconds);
                current.retained = Some(RetainedMessage::new(data.clone(), Some(effective_ttl)));
            }
            
            self.save_retained_async();
        }

        // Match subscriber IDs from radix tree
        let mut matched = Vec::new();
        Self::match_recursive(&self.tree, parts, &mut matched);
        
        // Deduplicate (MQTT standard: same client receives message only once)
        let mut seen = HashSet::new();
        matched.retain(|id| seen.insert(id.clone()));
        
        // Resolve ClientId → Sender from shared registry and send
        let msg = Arc::new(PubSubMessage::new(full_topic.to_string(), data));
        let mut sent_count = 0;
        let mut zombies = Vec::new();

        for client_id in matched {
            match self.clients.get(&client_id) {
                Some(sender) => {
                    if sender.send(msg.clone()).is_ok() {
                        sent_count += 1;
                    } else {
                        zombies.push(client_id);
                    }
                }
                None => zombies.push(client_id),
            }
        }
        
        for client_id in zombies {
            self.disconnect(&client_id);
        }

        sent_count
    }

    // --- DISCONNECT ---
    fn disconnect(&mut self, client: &ClientId) {
        tracing::debug!("RootActor '{}' disconnecting client {:?}", self.name, client);
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
                .filter(|r| !r.is_expired())
                .map(|retained| payload_to_dashboard_value(&retained.data)),
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

    fn match_recursive(node: &Node, parts: &[String], results: &mut Vec<ClientId>) {
        // "#" matches everything from here
        if let Some(hash_node) = &node.hash_child {
            for client in &hash_node.subscribers {
                results.push(client.clone());
            }
        }

        if parts.is_empty() {
            for client in &node.subscribers {
                results.push(client.clone());
            }
            return;
        }

        let head = &parts[0];
        let tail = &parts[1..];

        if let Some(child) = node.children.get(head) {
            Self::match_recursive(child, tail, results);
        }

        if let Some(plus_node) = &node.plus_child {
            Self::match_recursive(plus_node, tail, results);
        }
    }

    fn collect_retained_for_pattern(node: &Node, pattern: &[String], current_path: &str, results: &mut Vec<(String, Bytes)>) {
        if pattern.is_empty() {
            if let Some(retained) = &node.retained {
                if !retained.is_expired() {
                    results.push((current_path.to_string(), retained.data.clone()));
                }
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
        if let Some(retained) = &node.retained {
            if !retained.is_expired() {
                results.push((current_path.to_string(), retained.data.clone()));
            }
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
    
    // --- PERSISTENCE ---
    
    fn load_retained(&mut self) {
        if !self.retained_file_path.exists() {
            return;
        }
        
        match std::fs::read_to_string(&self.retained_file_path) {
            Ok(json_str) => {
                match serde_json::from_str::<HashMap<String, RetainedMessage>>(&json_str) {
                    Ok(retained_map) => {
                        for (path, mut retained_msg) in retained_map {
                            if let Some(unix_ts) = retained_msg.expires_at_unix {
                                retained_msg = RetainedMessage::from_persisted(retained_msg.data, Some(unix_ts));
                            }
                            
                            if retained_msg.is_expired() {
                                continue;
                            }
                            
                            let parts: Vec<String> = path.split('/').map(|s| s.to_string()).collect();
                            let mut current = &mut self.tree;
                            for part in parts {
                                current = current.children.entry(part).or_insert_with(Node::new);
                            }
                            current.retained = Some(retained_msg);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse retained file for {}: {}", self.name, e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to read retained file for {}: {}", self.name, e);
            }
        }
    }
    
    fn save_retained_async(&self) {
        let path = self.retained_file_path.clone();
        let retained_map = self.collect_retained_for_persistence();
        
        tokio::spawn(async move {
            if let Some(parent) = path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            
            match serde_json::to_string_pretty(&retained_map) {
                Ok(json_str) => {
                    if let Err(e) = tokio::fs::write(&path, json_str).await {
                        eprintln!("Failed to write retained file: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to serialize retained: {}", e);
                }
            }
        });
    }
    
    fn collect_retained_for_persistence(&self) -> HashMap<String, RetainedMessage> {
        let mut result = HashMap::new();
        Self::collect_retained_recursive(&self.tree, String::new(), &mut result);
        result
    }
    
    fn collect_retained_recursive(node: &Node, current_path: String, result: &mut HashMap<String, RetainedMessage>) {
        if let Some(retained) = &node.retained {
            if !retained.is_expired() {
                result.insert(current_path.clone(), retained.clone());
            }
        }
        
        for (key, child) in &node.children {
            let next_path = if current_path.is_empty() {
                key.clone()
            } else {
                format!("{}/{}", current_path, key)
            };
            Self::collect_retained_recursive(child, next_path, result);
        }
    }
    
    fn cleanup_expired_retained(&mut self) {
        let mut cleaned = false;
        Self::cleanup_expired_recursive(&mut self.tree, &mut cleaned);
        
        if cleaned {
            self.save_retained_async();
        }
    }
    
    fn cleanup_expired_recursive(node: &mut Node, cleaned: &mut bool) {
        if let Some(retained) = &node.retained {
            if retained.is_expired() {
                node.retained = None;
                *cleaned = true;
            }
        }
        
        for child in node.children.values_mut() {
            Self::cleanup_expired_recursive(child, cleaned);
        }
        
        if let Some(ref mut plus_child) = node.plus_child {
            Self::cleanup_expired_recursive(plus_child, cleaned);
        }
        
        if let Some(ref mut hash_child) = node.hash_child {
            Self::cleanup_expired_recursive(hash_child, cleaned);
        }
    }
}

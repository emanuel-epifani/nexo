//! PubSub Radix Tree Node: Topic routing data structure

use std::collections::{HashMap, HashSet};
use bytes::Bytes;
use crate::brokers::pub_sub::snapshot::TopicSnapshot;

use super::retained::RetainedMessage;
use super::types::ClientId;

pub(crate) struct Node {
    // Exact match children: "kitchen" -> Node
    pub(crate) children: HashMap<String, Node>,
    
    // Wildcard '+' child: matches any single token at this level
    // Note: Used for routing messages TO subscribers who used '+'
    pub(crate) plus_child: Option<Box<Node>>,
    
    // Wildcard '#' child: matches everything remaining
    // Note: Used for routing messages TO subscribers who used '#'
    pub(crate) hash_child: Option<Box<Node>>,
    /// Only stores ClientIds. Sender resolution happens via shared DashMap at publish time.
    pub(crate) subscribers: HashSet<ClientId>,
    pub(crate) retained: Option<RetainedMessage>,
}

impl Node {
    pub(crate) fn new() -> Self {
        Self {
            children: HashMap::new(),
            plus_child: None,
            hash_child: None,
            subscribers: HashSet::new(),
            retained: None,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
            && self.children.is_empty()
            && self.plus_child.is_none()
            && self.hash_child.is_none()
            && self.retained.is_none()
    }

    pub(crate) fn insert_subscriber(&mut self, parts: &[String], client: &ClientId) {
        let mut current = self;
        for (i, part) in parts.iter().enumerate() {
            if part == "#" {
                if current.hash_child.is_none() {
                    current.hash_child = Some(Box::new(Node::new()));
                }
                current.hash_child.as_mut().unwrap().subscribers.insert(client.clone());
                return;
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
    }

    pub(crate) fn remove_subscriber(&mut self, parts: &[String], client: &ClientId) -> bool {
        if parts.is_empty() {
            self.subscribers.remove(client);
            return self.is_empty();
        }

        let head = &parts[0];
        let tail = &parts[1..];
        let mut should_remove = false;

        if head == "#" {
            if let Some(ref mut hash_node) = self.hash_child {
                hash_node.subscribers.remove(client);
                if hash_node.is_empty() {
                    should_remove = true;
                }
            }
            if should_remove {
                self.hash_child = None;
            }
        } else if head == "+" {
            if let Some(ref mut plus_node) = self.plus_child {
                if plus_node.remove_subscriber(tail, client) {
                    should_remove = true;
                }
            }
            if should_remove {
                self.plus_child = None;
            }
        } else {
            if let Some(child) = self.children.get_mut(head) {
                if child.remove_subscriber(tail, client) {
                    should_remove = true;
                }
            }
            if should_remove {
                self.children.remove(head);
            }
        }
        
        self.is_empty()
    }

    pub(crate) fn match_subscribers(&self, parts: &[String], results: &mut Vec<ClientId>) {
        // "#" matches everything from here
        if let Some(hash_node) = &self.hash_child {
            for client in &hash_node.subscribers {
                results.push(client.clone());
            }
        }

        if parts.is_empty() {
            for client in &self.subscribers {
                results.push(client.clone());
            }
            return;
        }

        let head = &parts[0];
        let tail = &parts[1..];

        if let Some(child) = self.children.get(head) {
            child.match_subscribers(tail, results);
        }

        if let Some(plus_node) = &self.plus_child {
            plus_node.match_subscribers(tail, results);
        }
    }

    pub(crate) fn set_retained(&mut self, parts: &[String], retained: Option<RetainedMessage>) {
        let mut current = self;
        for part in parts {
            current = current.children.entry(part.clone()).or_insert_with(Node::new);
        }
        current.retained = retained;
    }

    pub(crate) fn collect_retained_for_pattern(&self, pattern: &[String], current_path: &str, results: &mut Vec<(String, Bytes)>) {
        if pattern.is_empty() {
            if let Some(retained) = &self.retained {
                if !retained.is_expired() {
                    results.push((current_path.to_string(), retained.data.clone()));
                }
            }
            return;
        }

        let head = &pattern[0];
        let tail = &pattern[1..];

        if head == "+" {
            for (key, child) in &self.children {
                let next_path = if current_path.is_empty() { key.clone() } else { format!("{}/{}", current_path, key) };
                child.collect_retained_for_pattern(tail, &next_path, results);
            }
        } else if head == "#" {
            self.collect_all_retained_for_subscribe(current_path, results);
        } else {
            if let Some(child) = self.children.get(head) {
                let next_path = if current_path.is_empty() { head.clone() } else { format!("{}/{}", current_path, head) };
                child.collect_retained_for_pattern(tail, &next_path, results);
            }
        }
    }

    pub(crate) fn collect_all_retained_for_subscribe(&self, current_path: &str, results: &mut Vec<(String, Bytes)>) {
        if let Some(retained) = &self.retained {
            if !retained.is_expired() {
                results.push((current_path.to_string(), retained.data.clone()));
            }
        }
        for (key, child) in &self.children {
            let next_path = if current_path.is_empty() { key.clone() } else { format!("{}/{}", current_path, key) };
            child.collect_all_retained_for_subscribe(&next_path, results);
        }
    }

    pub(crate) fn collect_all_retained(&self, current_path: &str, results: &mut Vec<(String, Bytes, Option<i64>)>) {
        if let Some(retained) = &self.retained {
            if !retained.is_expired() {
                results.push((current_path.to_string(), retained.data.clone(), retained.expires_at_unix.map(|v| v as i64)));
            }
        }
        for (key, child) in &self.children {
            let next_path = if current_path.is_empty() { key.clone() } else { format!("{}/{}", current_path, key) };
            child.collect_all_retained(&next_path, results);
        }
    }

    pub(crate) fn cleanup_expired_retained(&mut self) -> bool {
        let mut cleaned = false;
        
        if let Some(retained) = &self.retained {
            if retained.is_expired() {
                self.retained = None;
                cleaned = true;
            }
        }
        
        for child in self.children.values_mut() {
            if child.cleanup_expired_retained() {
                cleaned = true;
            }
        }
        
        if let Some(ref mut plus_child) = self.plus_child {
            if plus_child.cleanup_expired_retained() {
                cleaned = true;
            }
        }
        
        if let Some(ref mut hash_child) = self.hash_child {
            if hash_child.cleanup_expired_retained() {
                cleaned = true;
            }
        }
        
        cleaned
    }

    pub(crate) fn collect_filtered_topics(&self, base_path: &str, search: Option<&str>, topics: &mut Vec<TopicSnapshot>) {
        if !base_path.is_empty() {
            let matches = search.map_or(true, |s| base_path.contains(s));
            if matches && (!self.subscribers.is_empty() || self.retained.is_some()) {
                topics.push(TopicSnapshot {
                    full_path: base_path.to_string(),
                    subscribers: self.subscribers.len(),
                    retained_payload: self.retained.as_ref()
                        .filter(|r| !r.is_expired())
                        .map(|retained| retained.data.clone()),
                });
            }
        }

        for (child_name, child_node) in &self.children {
            let full_path = if base_path.is_empty() { child_name.clone() } else { format!("{}/{}", base_path, child_name) };
            child_node.collect_filtered_topics(&full_path, search, topics);
        }

        if let Some(plus_node) = &self.plus_child {
            let full_path = if base_path.is_empty() { "+".to_string() } else { format!("{}/+", base_path) };
            plus_node.collect_filtered_topics(&full_path, search, topics);
        }

        if let Some(hash_node) = &self.hash_child {
            let full_path = if base_path.is_empty() { "#".to_string() } else { format!("{}/#", base_path) };
            hash_node.collect_filtered_topics(&full_path, search, topics);
        }
    }
}

//! PubSub Radix Tree Node: Topic routing data structure

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use bytes::Bytes;

use super::retained::RetainedMessage;

pub(crate) struct Node {
    // Exact match children: "kitchen" -> Node
    pub(crate) children: HashMap<String, Node>,
    
    // Wildcard '+' child: matches any single token at this level
    // Note: Used for routing messages TO subscribers who used '+'
    pub(crate) plus_child: Option<Box<Node>>,
    
    // Wildcard '#' child: matches everything remaining
    // Note: Used for routing messages TO subscribers who used '#'
    pub(crate) hash_child: Option<Box<Node>>,
    /// Only stores client ids. Sender resolution happens via shared DashMap at publish time.
    pub(crate) subscribers: HashSet<Arc<str>>,
    pub(crate) retained: Option<RetainedMessage>,
}

fn join_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{}/{}", parent, child)
    }
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

    pub(crate) fn insert_subscriber(&mut self, parts: &[String], client: &str) {
        let mut current = self;
        for part in parts {
            match part.as_str() {
                "#" => {
                    current.hash_child.get_or_insert_with(|| Box::new(Node::new()))
                        .subscribers.insert(Arc::from(client));
                    return;
                }
                "+" => {
                    current = current.plus_child.get_or_insert_with(|| Box::new(Node::new()));
                }
                _ => {
                    current = current.children.entry(part.clone()).or_insert_with(Node::new);
                }
            }
        }
        current.subscribers.insert(Arc::from(client));
    }

    pub(crate) fn remove_subscriber(&mut self, parts: &[String], client: &str) -> bool {
        let Some((head, tail)) = parts.split_first() else {
            self.subscribers.remove(client);
            return self.is_empty();
        };

        match head.as_str() {
            "#" => {
                if let Some(mut hash_node) = self.hash_child.take() {
                    hash_node.subscribers.remove(client);
                    if !hash_node.is_empty() {
                        self.hash_child = Some(hash_node);
                    }
                }
            }
            "+" => {
                if let Some(mut plus_node) = self.plus_child.take() {
                    if !plus_node.remove_subscriber(tail, client) {
                        self.plus_child = Some(plus_node);
                    }
                }
            }
            _ => {
                if let Some(mut child) = self.children.remove(head) {
                    if !child.remove_subscriber(tail, client) {
                        self.children.insert(head.clone(), child);
                    }
                }
            }
        }

        self.is_empty()
    }

    pub(crate) fn match_subscribers(&self, parts: &[String], results: &mut Vec<Arc<str>>) {
        // "#" matches everything from here
        if let Some(hash_node) = &self.hash_child {
            results.extend(hash_node.subscribers.iter().cloned());
        }

        if parts.is_empty() {
            results.extend(self.subscribers.iter().cloned());
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

        match head.as_str() {
            "+" => {
                for (key, child) in &self.children {
                    let next_path = join_path(current_path, key);
                    child.collect_retained_for_pattern(tail, &next_path, results);
                }
            }
            "#" => {
                self.collect_all_retained_for_subscribe(current_path, results);
            }
            _ => {
                if let Some(child) = self.children.get(head) {
                    let next_path = join_path(current_path, head);
                    child.collect_retained_for_pattern(tail, &next_path, results);
                }
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
            let next_path = join_path(current_path, key);
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
            let next_path = join_path(current_path, key);
            child.collect_all_retained(&next_path, results);
        }
    }

    fn cleanup_child(child: &mut Option<Box<Node>>) -> bool {
        if let Some(node) = child.as_mut() {
            let cleaned = node.cleanup_expired_retained();
            if node.is_empty() {
                *child = None;
            }
            cleaned
        } else {
            false
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

        self.children.retain(|_, child| {
            let child_cleaned = child.cleanup_expired_retained();
            cleaned |= child_cleaned;
            !child.is_empty()
        });

        cleaned |= Self::cleanup_child(&mut self.plus_child);
        cleaned |= Self::cleanup_child(&mut self.hash_child);

        cleaned
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use std::collections::HashSet;

use crate::brokers::pub_sub::config::PubSubConfig;
use crate::brokers::pub_sub::domain::persistence;
use crate::brokers::pub_sub::domain::radix_tree::Node;
use crate::brokers::pub_sub::domain::retained::RetainedMessage;
use crate::brokers::pub_sub::domain::types::{ClientInfo, ClientRegistry, PubSubMessage};

pub struct PubSubManager {
    tree: Arc<RwLock<Node>>,
    clients: ClientRegistry,
    retained_dirty: Arc<AtomicBool>,
    config: Arc<PubSubConfig>,
}

impl PubSubManager {
    pub fn new(config: Arc<PubSubConfig>) -> Self {
        let tree = Arc::new(RwLock::new(Node::new()));
        let retained_dirty = Arc::new(AtomicBool::new(false));
        let clients = Arc::new(DashMap::new());
        let persistence_path = format!("{}/retained.db", config.persistence_path);

        let loaded = match persistence::init_db(&persistence_path) {
            Ok(conn) => match persistence::load_all(&conn) {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::warn!("Failed to load retained topics from SQLite DB: {}", e);
                    Vec::new()
                }
            },
            Err(e) => {
                tracing::warn!("Failed to initialize SQLite for retained at {}: {}", persistence_path, e);
                Vec::new()
            }
        };

        {
            let mut root = tree.write();
            for (path, msg) in loaded {
                let parts: Vec<String> = path.split('/').map(|s| s.to_string()).collect();
                root.set_retained(&parts, Some(msg));
            }
        }

        // Background Flush Task
        let flush_tree = tree.clone();
        let flush_dirty = retained_dirty.clone();
        let flush_path = persistence_path;
        let flush_ms = config.retained_flush_ms;
        
        tokio::spawn(async move {
            if let Ok(mut conn) = persistence::init_db(&flush_path) {
                let mut interval = tokio::time::interval(Duration::from_millis(flush_ms));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if !flush_dirty.swap(false, Ordering::Relaxed) {
                        continue;
                    }

                    let entries = {
                        let root = flush_tree.read();
                        let mut results = Vec::new();
                        root.collect_all_retained("", &mut results);
                        results
                    };

                    if let Err(e) = persistence::flush(&mut conn, &entries) {
                        tracing::error!("Failed to flush retained messages to SQLite: {}", e);
                    }
                }
            }
        });

        // Background Cleanup Task
        let cleanup_tree = tree.clone();
        let cleanup_dirty = retained_dirty.clone();
        let cleanup_secs = config.cleanup_interval_seconds;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(cleanup_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await; // skip first
            loop {
                interval.tick().await;
                let cleaned = {
                    let mut root = cleanup_tree.write();
                    root.cleanup_expired_retained()
                };
                if cleaned {
                    cleanup_dirty.store(true, Ordering::Relaxed);
                }
            }
        });

        Self {
            tree,
            clients,
            retained_dirty,
            config,
        }
    }

    pub fn connect(&self, client_id: &str, sender: mpsc::UnboundedSender<Arc<PubSubMessage>>) {
        self.clients.insert(Arc::from(client_id), ClientInfo {
            sender,
            subscriptions: HashSet::new(),
        });
    }

    pub fn disconnect(&self, client_id: &str) {
        if let Some((_, info)) = self.clients.remove(client_id) {
            let mut root = self.tree.write();
            for sub in info.subscriptions {
                let parts: Vec<String> = sub.split('/').map(|s| s.to_string()).collect();
                root.remove_subscriber(&parts, client_id);
            }
        }
    }

    pub fn validate_subscribe_pattern(pattern: &str) -> Result<(), String> {
        if pattern.is_empty() {
            return Err("Subscribe pattern cannot be empty".into());
        }
        let parts: Vec<&str> = pattern.split('/').collect();
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                return Err("Subscribe pattern contains empty segments".into());
            }
            if *part == "#" && i != parts.len() - 1 {
                return Err("# wildcard must be the last segment".into());
            }
        }
        Ok(())
    }

    pub fn validate_publish_topic(topic: &str) -> Result<(), String> {
        if topic.is_empty() {
            return Err("Publish topic cannot be empty".into());
        }
        for part in topic.split('/') {
            if part.is_empty() {
                return Err("Publish topic contains empty segments".into());
            }
            if part == "+" || part == "#" {
                return Err("Publish topic cannot contain wildcards (+ or #)".into());
            }
        }
        Ok(())
    }

    pub fn subscribe(&self, client_id: &str, pattern: &str) {
        if let Err(e) = Self::validate_subscribe_pattern(pattern) {
            tracing::warn!("Invalid subscribe pattern '{}': {}", pattern, e);
            return;
        }
        let Some(mut info) = self.clients.get_mut(client_id) else { return; };
        info.subscriptions.insert(pattern.to_string());
        let sender = info.sender.clone();
        drop(info);

        let parts: Vec<String> = pattern.split('/').map(|s| s.to_string()).collect();
        let mut root = self.tree.write();
        root.insert_subscriber(&parts, client_id);

        let mut retained = Vec::new();
        root.collect_retained_for_pattern(&parts, "", &mut retained);

        for (p, b) in retained {
            let msg = Arc::new(PubSubMessage::new(p, b));
            let _ = sender.send(msg);
        }
    }

    pub fn unsubscribe(&self, client_id: &str, pattern: &str) {
        let Some(mut info) = self.clients.get_mut(client_id) else { return; };
        info.subscriptions.remove(pattern);
        drop(info);

        let parts: Vec<String> = pattern.split('/').map(|s| s.to_string()).collect();
        let mut root = self.tree.write();
        root.remove_subscriber(&parts, client_id);
    }

    pub fn publish(&self, topic: &str, data: Bytes, retain: bool, clear: bool, ttl_seconds: Option<u32>) -> usize {
        if let Err(e) = Self::validate_publish_topic(topic) {
            tracing::warn!("Invalid publish topic '{}': {}", topic, e);
            return 0;
        }

        let parts: Vec<String> = topic.split('/').map(|s| s.to_string()).collect();

        if clear || retain {
            let retained = if clear {
                None
            } else {
                Some(RetainedMessage::new(data.clone(), Some(ttl_seconds.unwrap_or(self.config.default_retained_ttl_seconds))))
            };
            let mut root = self.tree.write();
            root.set_retained(&parts, retained);
            self.retained_dirty.store(true, Ordering::Relaxed);
        }

        let mut matched = Vec::new();
        {
            let root = self.tree.read();
            root.match_subscribers(&parts, &mut matched);
        }

        let mut seen = HashSet::new();
        matched.retain(|id| seen.insert(id.clone()));

        let msg = Arc::new(PubSubMessage::new(topic.to_string(), data));
        let mut sent_count = 0;
        let mut zombies = Vec::new();

        for client_id in matched {
            if let Some(info) = self.clients.get(client_id.as_ref()) {
                if info.sender.send(msg.clone()).is_ok() {
                    sent_count += 1;
                } else {
                    zombies.push(client_id);
                }
            } else {
                zombies.push(client_id);
            }
        }

        for client_id in zombies {
            self.disconnect(&client_id);
        }

        sent_count
    }

}

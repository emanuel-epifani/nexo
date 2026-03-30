use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use std::collections::HashSet;

use crate::brokers::pub_sub::config::PubSubConfig;
use crate::brokers::pub_sub::radix_tree::Node;
use crate::brokers::pub_sub::types::{ClientId, ClientInfo, ClientRegistry, PubSubMessage};
use crate::brokers::pub_sub::retained;
use crate::dashboard::pubsub::{PubSubBrokerSnapshot, TopicSnapshot, WildcardSubscription, WildcardSubscriptions};

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

        // Load retained from SQLite
        if let Ok(conn) = retained::init_db(&persistence_path) {
            if let Ok(loaded) = retained::load_all(&conn) {
                let mut root = tree.write();
                for (path, msg) in loaded {
                    let parts: Vec<String> = path.split('/').map(|s| s.to_string()).collect();
                    root.set_retained(&parts, Some(msg));
                }
            } else {
                tracing::warn!("Failed to load retained topics from SQLite DB");
            }
        } else {
             tracing::warn!("Failed to initialize SQLite for retained at {}", persistence_path);
        }

        // Background Flush Task
        let flush_tree = tree.clone();
        let flush_dirty = retained_dirty.clone();
        let flush_path = persistence_path.clone();
        let flush_ms = config.retained_flush_ms;
        
        tokio::spawn(async move {
            if let Ok(mut conn) = retained::init_db(&flush_path) {
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

                    if let Err(e) = retained::flush(&mut conn, &entries) {
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

    pub fn connect(&self, client_id: ClientId, sender: mpsc::UnboundedSender<Arc<PubSubMessage>>) {
        self.clients.insert(client_id, ClientInfo {
            sender,
            subscriptions: HashSet::new(),
        });
    }

    pub fn disconnect(&self, client_id: &ClientId) {
        if let Some((_, info)) = self.clients.remove(client_id) {
            let mut root = self.tree.write();
            for sub in info.subscriptions {
                let parts: Vec<String> = sub.split('/').map(|s| s.to_string()).collect();
                root.remove_subscriber(&parts, client_id);
            }
        }
    }

    pub fn subscribe(&self, client_id: &ClientId, pattern: &str) {
        let sender = if let Some(mut info) = self.clients.get_mut(client_id) {
            info.subscriptions.insert(pattern.to_string());
            info.sender.clone()
        } else {
            return;
        };

        let parts: Vec<String> = pattern.split('/').map(|s| s.to_string()).collect();
        let mut root = self.tree.write();
        root.insert_subscriber(&parts, client_id);

        let mut retained = Vec::new();
        root.collect_retained_for_pattern(&parts, "", &mut retained);

        for (p, b) in retained {
            let p = if p.starts_with('/') { p[1..].to_string() } else { p };
            let msg = Arc::new(PubSubMessage::new(p, b));
            let _ = sender.send(msg);
        }
    }

    pub fn unsubscribe(&self, client_id: &ClientId, pattern: &str) {
        if let Some(mut info) = self.clients.get_mut(client_id) {
            info.subscriptions.remove(pattern);
        }
        let parts: Vec<String> = pattern.split('/').map(|s| s.to_string()).collect();
        let mut root = self.tree.write();
        root.remove_subscriber(&parts, client_id);
    }

    pub fn publish(&self, topic: &str, data: Bytes, retain: bool, ttl_seconds: Option<u64>) -> usize {
        let parts: Vec<String> = topic.split('/').map(|s| s.to_string()).collect();
        if parts.is_empty() { return 0; }

        if retain {
            let mut root = self.tree.write();
            if data.is_empty() {
                root.set_retained(&parts, None);
            } else {
                let effective_ttl = ttl_seconds.unwrap_or(self.config.default_retained_ttl_seconds);
                root.set_retained(&parts, Some(retained::RetainedMessage::new(data.clone(), Some(effective_ttl))));
            }
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
            if let Some(info) = self.clients.get(&client_id) {
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

    pub fn scan_topics(&self, limit: usize, offset: usize, search: Option<&str>) -> PubSubBrokerSnapshot {
        let mut all_topics = Vec::new();
        {
            let root = self.tree.read();
            root.collect_filtered_topics("", search, &mut all_topics);
        }

        all_topics.sort_by(|a, b| a.full_path.cmp(&b.full_path));
        let total_topics = all_topics.len();
        
        let end = (offset + limit).min(total_topics);
        let paginated = if offset < total_topics {
            all_topics[offset..end].to_vec()
        } else {
            Vec::new()
        };

        let mut wildcards = WildcardSubscriptions {
            multi_level: Vec::new(),
            single_level: Vec::new(),
        };

        for entry in self.clients.iter() {
            let client_id = entry.key().0.clone();
            for sub in &entry.subscriptions {
                if sub.contains('#') {
                    if search.map_or(true, |s| sub.contains(s)) {
                        wildcards.multi_level.push(WildcardSubscription {
                            pattern: sub.clone(),
                            client_id: client_id.clone(),
                        });
                    }
                } else if sub.contains('+') {
                    if search.map_or(true, |s| sub.contains(s)) {
                        wildcards.single_level.push(WildcardSubscription {
                            pattern: sub.clone(),
                            client_id: client_id.clone(),
                        });
                    }
                }
            }
        }

        PubSubBrokerSnapshot {
            active_clients: self.clients.len(),
            total_topics,
            topics: paginated,
            wildcards,
        }
    }
}

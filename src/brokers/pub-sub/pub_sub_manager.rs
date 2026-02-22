//! PubSub Manager: MQTT-style Publish/Subscribe Router
//! 
//! Architecture: Actor per Root
//! - Each root topic (first segment) has its own actor with a radix tree
//! - Actors process commands sequentially (no internal locking)
//! - Parallelism between different root topics
//! - Global "#" subscribers handled separately
//!
//! Sender Ownership:
//! - PubSubManager.clients (Arc<DashMap>) is the SINGLE SOURCE OF TRUTH for Senders
//! - Radix tree nodes store only ClientId (HashSet<ClientId>)
//! - Actors resolve ClientId → Sender via shared DashMap read at publish time
//! - disconnect() removes from DashMap → all actors see None on next lookup
//!
//! Supports:
//! - Exact matching: "home/kitchen/temp"
//! - Single-level wildcard: "home/+/temp"
//! - Multi-level wildcard: "home/#"
//! - Active Push to connected clients
//! - Retained Messages (Last Value Caching)

use std::sync::Arc;
use std::collections::HashSet;
use tokio::sync::{mpsc, oneshot, RwLock};
use bytes::Bytes;
use dashmap::DashMap;
use crate::brokers::pub_sub::types::{ClientId, PubSubMessage};
use crate::brokers::pub_sub::actor::{RootCommand, RootActor, ClientRegistry};

// ==========================================
// PUBSUB MANAGER
// ==========================================

pub struct PubSubManager {
    actors: DashMap<String, mpsc::Sender<RootCommand>>,
    /// Single source of truth for client Senders. Actors resolve ClientId → Sender via this.
    clients: ClientRegistry,
    client_subscriptions: DashMap<ClientId, HashSet<String>>,
    /// Global "#" subscribers — only stores ClientIds, resolved from `clients` at publish time.
    global_hash_subscribers: RwLock<HashSet<ClientId>>,
    config: crate::config::PubSubConfig,
}

impl PubSubManager {
    pub fn new(config: crate::config::PubSubConfig) -> Self {
        let manager = Self {
            actors: DashMap::new(),
            clients: Arc::new(DashMap::new()),
            client_subscriptions: DashMap::new(),
            global_hash_subscribers: RwLock::new(HashSet::new()),
            config: config.clone(),
        };
        
        let actors = manager.actors.clone();
        let cleanup_interval = std::time::Duration::from_secs(config.cleanup_interval_seconds);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            
            loop {
                interval.tick().await;
                Self::background_cleanup_empty_actors(&actors).await;
            }
        });
        
        manager
    }

    /// Register a new client connection
    pub fn connect(&self, client_id: ClientId, sender: mpsc::UnboundedSender<Arc<PubSubMessage>>) {
        self.clients.insert(client_id.clone(), sender);
        self.client_subscriptions.insert(client_id, HashSet::new());
    }

    /// Subscribe to a topic pattern
    pub async fn subscribe(&self, topic: &str, client: ClientId) {
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
            // Send retained messages to the client
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
        if let Some(mut subs) = self.client_subscriptions.get_mut(client) {
            subs.remove(topic);
        }

        let parts: Vec<&str> = topic.split('/').collect();

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
    pub async fn publish(&self, topic: &str, data: Bytes, retain: bool, ttl_seconds: Option<u64>) -> usize {
        let parts: Vec<&str> = topic.split('/').collect();
        
        if parts.is_empty() {
            return 0;
        }

        let root = parts[0];
        let sub_parts: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
        let mut sent = 0;

        // 1. Dispatch to actor
        if let Some(actor) = self.actors.get(root) {
            let (tx, rx) = oneshot::channel();
            if actor.send(RootCommand::Publish {
                parts: sub_parts.clone(),
                full_topic: topic.to_string(),
                data: data.clone(),
                retain,
                ttl_seconds,
                reply: tx,
            }).await.is_ok() {
                sent += rx.await.unwrap_or_default();
            }
        } else if retain {
            let actor = self.get_or_create_actor(root);
            let (tx, rx) = oneshot::channel();
            if actor.send(RootCommand::Publish {
                parts: sub_parts,
                full_topic: topic.to_string(),
                data: data.clone(),
                retain,
                ttl_seconds,
                reply: tx,
            }).await.is_ok() {
                sent += rx.await.unwrap_or_default();
            }
        }

        // 2. Send to global "#" subscribers (resolve from clients DashMap)
        let global_subs = self.global_hash_subscribers.read().await;
        if !global_subs.is_empty() {
            let msg = Arc::new(PubSubMessage::new(topic.to_string(), data));
            for client_id in global_subs.iter() {
                if let Some(sender) = self.clients.get(client_id) {
                    if sender.send(msg.clone()).is_ok() {
                        sent += 1;
                    }
                }
            }
        }

        sent
    }

    /// Disconnect a client and cleanup subscriptions
    pub async fn disconnect(&self, client_id: &ClientId) {
        // Remove sender from single source of truth — all actors will see None on next lookup
        self.clients.remove(client_id);
        self.global_hash_subscribers.write().await.remove(client_id);

        if let Some((_, topics)) = self.client_subscriptions.remove(client_id) {
            let mut roots: HashSet<String> = HashSet::new();
            for topic in &topics {
                let parts: Vec<&str> = topic.split('/').collect();
                if !parts.is_empty() && parts[0] != "#" {
                    roots.insert(parts[0].to_string());
                }
            }

            let mut waits = Vec::new();
            for root in roots {
                if let Some(actor) = self.actors.get(&root) {
                    let (tx, rx) = oneshot::channel();
                    if actor.send(RootCommand::Disconnect { 
                        client: client_id.clone(),
                        reply: tx 
                    }).await.is_ok() {
                        waits.push(rx);
                    }
                }
            }
            
            for rx in waits {
                let _ = rx.await;
            }
        }
    }

    /// Cleanup expired retained messages in all actors
    pub async fn cleanup_expired_retained(&self) {
        for entry in self.actors.iter() {
            let _ = entry.value().send(RootCommand::CleanupExpired).await;
        }
    }

    /// Get snapshot for dashboard
    pub async fn get_snapshot(&self) -> crate::dashboard::dashboard_pubsub::PubSubBrokerSnapshot {
        use crate::dashboard::dashboard_pubsub::{PubSubBrokerSnapshot, WildcardSubscription, WildcardSubscriptions};

        let mut multi_level = Vec::new();
        let mut single_level = Vec::new();
        for entry in self.client_subscriptions.iter() {
            let client_id = entry.key().0.clone();
            for pattern in entry.value().iter() {
                if pattern.contains('#') {
                    multi_level.push(WildcardSubscription {
                        pattern: pattern.clone(),
                        client_id: client_id.clone(),
                    });
                } else if pattern.contains('+') {
                    single_level.push(WildcardSubscription {
                        pattern: pattern.clone(),
                        client_id: client_id.clone(),
                    });
                }
            }
        }

        let mut all_topics = Vec::new();
        for entry in self.actors.iter() {
            let (tx, rx) = oneshot::channel();
            if entry.value().send(RootCommand::GetFlatSnapshot { reply: tx }).await.is_ok() {
                if let Ok(topics) = rx.await {
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
            let actor = RootActor::new(
                root.to_string(), 
                rx, 
                &self.config.persistence_path,
                self.config.default_retained_ttl_seconds,
                Arc::clone(&self.clients),
            );
            tokio::spawn(actor.run());
            tx
        }).clone()
    }

    /// Background task: cleanup empty actors in parallel
    async fn background_cleanup_empty_actors(actors: &DashMap<String, mpsc::Sender<RootCommand>>) {
        let start = std::time::Instant::now();
        let mut tasks = Vec::new();
        
        for entry in actors.iter() {
            let root = entry.key().clone();
            let actor = entry.value().clone();
            let actors_ref = actors.clone();
            
            let task = tokio::spawn(async move {
                let (tx, rx) = oneshot::channel();
                
                if actor.send(RootCommand::IsEmpty { reply: tx }).await.is_ok() {
                    match tokio::time::timeout(std::time::Duration::from_millis(100), rx).await {
                        Ok(Ok(true)) => {
                            actors_ref.remove(&root);
                            Some(root)
                        }
                        _ => None
                    }
                } else {
                    None
                }
            });
            
            tasks.push(task);
        }
        
        let mut removed = Vec::new();
        for task in tasks {
            if let Ok(Some(root)) = task.await {
                removed.push(root);
            }
        }
        
        if !removed.is_empty() {
            tracing::info!(
                "Background cleanup: removed {} empty actors in {:?}: {:?}",
                removed.len(),
                start.elapsed(),
                removed
            );
        }
    }
}

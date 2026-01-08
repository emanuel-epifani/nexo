//! Topic Actor: Single-threaded message store with MPSC command channel
//! Zero locks, maximum cache locality, Tokio-native

use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, Notify};
use crate::brokers::stream::message::Message;

/// Commands sent to the Topic Actor
pub enum TopicCommand {
    Publish {
        payload: Bytes,
        reply: oneshot::Sender<u64>,
    },
    Read {
        offset: u64,
        limit: usize,
        reply: oneshot::Sender<Vec<Message>>,
    },
    /// Register for new data notifications (long-polling)
    WaitForData {
        offset: u64,
        notify: Arc<Notify>,
    },
    /// Get current high watermark (next_offset)
    GetHighWatermark {
        reply: oneshot::Sender<u64>,
    },
    /// Snapshot for dashboard
    GetStats {
        reply: oneshot::Sender<(u64, u64)>, // (total_messages, high_watermark)
    },
}

/// Handle to communicate with a Topic Actor
#[derive(Clone)]
pub struct TopicHandle {
    pub name: String,
    tx: mpsc::Sender<TopicCommand>,
}

impl TopicHandle {
    pub fn new(name: String, buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);
        
        let actor_name = name.clone();
        tokio::spawn(async move {
            topic_actor(actor_name, rx).await;
        });
        
        Self { name, tx }
    }
    
    pub async fn publish(&self, payload: Bytes) -> Result<u64, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        
        self.tx.send(TopicCommand::Publish { payload, reply: reply_tx })
            .await
            .map_err(|_| "Topic actor closed")?;
        
        reply_rx.await.map_err(|_| "Topic actor dropped reply".to_string())
    }
    
    pub async fn read(&self, offset: u64, limit: usize) -> Vec<Message> {
        let (reply_tx, reply_rx) = oneshot::channel();
        
        if self.tx.send(TopicCommand::Read { offset, limit, reply: reply_tx }).await.is_err() {
            return Vec::new();
        }
        
        reply_rx.await.unwrap_or_default()
    }
    
    pub async fn wait_for_data(&self, offset: u64, notify: Arc<Notify>) {
        let _ = self.tx.send(TopicCommand::WaitForData { offset, notify }).await;
    }
    
    pub async fn get_high_watermark(&self) -> u64 {
        let (reply_tx, reply_rx) = oneshot::channel();
        
        if self.tx.send(TopicCommand::GetHighWatermark { reply: reply_tx }).await.is_err() {
            return 0;
        }
        
        reply_rx.await.unwrap_or(0)
    }
    
    pub async fn get_stats(&self) -> (u64, u64) {
        let (reply_tx, reply_rx) = oneshot::channel();
        
        if self.tx.send(TopicCommand::GetStats { reply: reply_tx }).await.is_err() {
            return (0, 0);
        }
        
        reply_rx.await.unwrap_or((0, 0))
    }
}

/// The actual Topic Actor loop - runs in a single Tokio task
async fn topic_actor(name: String, mut rx: mpsc::Receiver<TopicCommand>) {
    let mut messages: VecDeque<Message> = VecDeque::new();
    let mut next_offset: u64 = 0;
    let start_offset: u64 = 0;
    
    // Waiters: (offset_they_want, weak_notifier)
    // We use Weak references to avoid holding the client connection alive if they disconnect.
    // If the client drops their Arc<Notify>, our Weak pointer becomes invalid, and we cleanup lazily.
    let mut waiters: Vec<(u64, Weak<Notify>)> = Vec::new();
    
    tracing::debug!(topic = %name, "Topic actor started");
    
    while let Some(cmd) = rx.recv().await {
        match cmd {
            TopicCommand::Publish { payload, reply } => {
                let offset = next_offset;
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                
                messages.push_back(Message {
                    offset,
                    timestamp,
                    payload,
                });
                next_offset += 1;
                
                // Notify all waiters that were waiting for this offset or earlier
                waiters.retain(|(wait_offset, weak_notify)| {
                    if *wait_offset <= offset {
                        // Condition met: Try to notify if client is still alive
                        if let Some(notify) = weak_notify.upgrade() {
                            notify.notify_one();
                        }
                        false // Remove from list (task done)
                    } else {
                        // Condition NOT met yet:
                        // Only keep waiting if the client is still alive (upgrade succeeds).
                        // If upgrade fails (None), it means client disconnected -> remove (cleanup).
                        weak_notify.upgrade().is_some()
                    }
                });
                
                let _ = reply.send(offset);
            }
            
            TopicCommand::Read { offset, limit, reply } => {
                let result = if offset < start_offset {
                    // Requested offset is before retention window, start from beginning
                    messages.iter().take(limit).cloned().collect()
                } else {
                    let relative_idx = (offset - start_offset) as usize;
                    if relative_idx >= messages.len() {
                        Vec::new()
                    } else {
                        messages.iter().skip(relative_idx).take(limit).cloned().collect()
                    }
                };
                
                let _ = reply.send(result);
            }
            
            TopicCommand::WaitForData { offset, notify } => {
                // If data already available, notify immediately
                if offset < next_offset {
                    notify.notify_one();
                } else {
                    waiters.push((offset, Arc::downgrade(&notify)));
                }
            }
            
            TopicCommand::GetHighWatermark { reply } => {
                let _ = reply.send(next_offset);
            }
            
            TopicCommand::GetStats { reply } => {
                let total = if next_offset >= start_offset {
                    next_offset - start_offset
                } else {
                    0
                };
                let _ = reply.send((total, next_offset));
            }
        }
    }
    
    tracing::debug!(topic = %name, "Topic actor stopped");
}

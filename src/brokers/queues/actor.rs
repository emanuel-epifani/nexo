//! Queue Actor: Async wrapper around QueueState
//! Handles commands, persistence, and time-based events.

use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant}; // Add Instant
use bytes::Bytes;
use std::collections::VecDeque;
use tracing::{error, info};
use uuid::Uuid;
use std::path::PathBuf;

use crate::brokers::queues::queue::{QueueState, QueueConfig, Message, current_time_ms};
use crate::brokers::queues::persistence::{QueueStore, types::StorageOp};
use crate::brokers::queues::queue_manager::ManagerCommand;
use crate::config::Config;
use crate::dashboard::models::queues::QueueSummary;

// ==========================================
// ACTOR COMMANDS
// ==========================================

pub enum QueueActorCommand {
    Push {
        payload: Bytes,
        priority: u8,
        delay_ms: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    Pop {
        reply: oneshot::Sender<Option<Message>>,
    },
    Ack {
        id: Uuid,
        reply: oneshot::Sender<bool>,
    },
    ConsumeBatch {
        max: usize,
        wait_ms: u64,
        reply: oneshot::Sender<Vec<Message>>,
    },
    GetSnapshot {
        reply: oneshot::Sender<QueueSummary>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
    ProcessExpired,
}

struct WaitingConsumer {
    reply: oneshot::Sender<Vec<Message>>,
    max: usize,
    expires_at: u64, // SystemTime ms
}

pub struct QueueActor {
    name: String,
    state: QueueState,
    config: QueueConfig,
    store: QueueStore,
    rx: mpsc::Receiver<QueueActorCommand>,
    manager_tx: mpsc::Sender<ManagerCommand>,
    waiters: VecDeque<WaitingConsumer>,
}

impl QueueActor {
    pub fn new(
        name: String,
        config: QueueConfig,
        rx: mpsc::Receiver<QueueActorCommand>,
        manager_tx: mpsc::Sender<ManagerCommand>,
    ) -> Self {
        let base_path = PathBuf::from(&Config::global().queue.persistence_path);
        
        if let Err(e) = std::fs::create_dir_all(&base_path) {
            error!("Failed to create queue data directory at {:?}: {}", base_path, e);
        }

        let db_path = base_path.join(format!("{}.db", name));
        let store = QueueStore::new(db_path, config.persistence.clone());

        Self {
            name,
            state: QueueState::new(),
            config,
            store,
            rx,
            manager_tx,
            waiters: VecDeque::new(),
        }
    }

    pub async fn run(mut self) {
        // RECOVERY PHASE
        match self.store.recover() {
            Ok(messages) => {
                let count = messages.len();
                if count > 0 {
                    for msg in messages {
                        self.state.push(msg);
                    }
                    info!("Queue '{}': Recovered {} messages from storage", self.name, count);
                }
            }
            Err(e) => {
                error!("Queue '{}': Persistence recovery failed: {}", self.name, e);
            }
        }

        loop {
            // Determine sleep duration
            let sleep_until = self.next_wakeup_time();

            tokio::select! {
                // 1. Handle Commands
                maybe_cmd = self.rx.recv() => {
                    match maybe_cmd {
                        Some(cmd) => {
                            if !self.handle_command(cmd).await {
                                break;
                            }
                        }
                        None => break, // Channel closed
                    }
                }

                // 2. Handle Timeout
                _ = time::sleep_until(sleep_until) => {
                    self.process_time_events().await;
                }
            }
        }
    }

    fn next_wakeup_time(&self) -> Instant {
        let now_ms = current_time_ms();
        
        let next_msg_ts = self.state.next_timeout();
        let next_waiter_ts = self.waiters.front().map(|w| w.expires_at);

        let next_event_ts = match (next_msg_ts, next_waiter_ts) {
            (Some(t1), Some(t2)) => Some(t1.min(t2)),
            (Some(t), None) => Some(t),
            (None, Some(t)) => Some(t),
            (None, None) => None,
        };

        if let Some(ts) = next_event_ts {
            if ts <= now_ms {
                Instant::now() // Immediately
            } else {
                Instant::now() + std::time::Duration::from_millis(ts - now_ms)
            }
        } else {
            // Sleep "forever"
            Instant::now() + std::time::Duration::from_secs(365 * 24 * 3600)
        }
    }

    async fn handle_command(&mut self, cmd: QueueActorCommand) -> bool {
        match cmd {
            QueueActorCommand::Push { payload, priority, delay_ms, reply } => {
                // ... (existing logic)
                let msg = Message::new(payload, priority, delay_ms);
                if delay_ms.is_none() && !self.waiters.is_empty() {
                    self.state.push(msg.clone());
                    let _ = self.store.execute(StorageOp::Insert(msg)).await;
                    self.process_waiters().await;
                } else {
                    self.state.push(msg.clone());
                    let _ = self.store.execute(StorageOp::Insert(msg)).await;
                }
                let _ = reply.send(());
                true
            }
            
            QueueActorCommand::Pop { reply } => {
                let (msg_opt, _) = self.state.pop(self.config.visibility_timeout_ms);
                if let Some(msg) = &msg_opt {
                    let _ = self.store.execute(StorageOp::UpdateState { 
                        id: msg.id, 
                        visible_at: msg.visible_at, 
                        attempts: msg.attempts 
                    }).await;
                }
                let _ = reply.send(msg_opt);
                true
            }
            
            QueueActorCommand::Ack { id, reply } => {
                let result = self.state.ack(id);
                if result {
                    let _ = self.store.execute(StorageOp::Delete(id)).await;
                }
                let _ = reply.send(result);
                true
            }
            
            QueueActorCommand::ConsumeBatch { max, wait_ms, reply } => {
                let (msgs, _) = self.state.take_batch(max, self.config.visibility_timeout_ms);
                if !msgs.is_empty() {
                    for msg in &msgs {
                        let _ = self.store.execute(StorageOp::UpdateState { 
                            id: msg.id, 
                            visible_at: msg.visible_at, 
                            attempts: msg.attempts 
                        }).await;
                    }
                    let _ = reply.send(msgs);
                } else if wait_ms > 0 {
                    self.waiters.push_back(WaitingConsumer {
                        reply,
                        max,
                        expires_at: current_time_ms() + wait_ms,
                    });
                } else {
                    let _ = reply.send(vec![]);
                }
                true
            }
            
            QueueActorCommand::GetSnapshot { reply } => {
                let snapshot = self.state.get_snapshot(&self.name);
                let _ = reply.send(snapshot);
                true
            }
            
            QueueActorCommand::ProcessExpired => {
                self.process_time_events().await;
                true
            }

            QueueActorCommand::Stop { reply } => {
                let _ = reply.send(());
                false // Signal to break the loop
            }
        }
    }

    async fn process_time_events(&mut self) {
        let now = current_time_ms();

        // 1. Process Message Expirations (Scheduled -> Ready, InFlight -> Retry/DLQ)
        if let Some(next_msg_ts) = self.state.next_timeout() {
            if next_msg_ts <= now {
                let (requeued, dlq_msgs) = self.state.process_expired(self.config.max_retries);
                
                for msg in requeued {
                    let _ = self.store.execute(StorageOp::UpdateState {
                        id: msg.id,
                        visible_at: 0,
                        attempts: msg.attempts
                    }).await;
                }

                for msg in dlq_msgs {
                    let _ = self.store.execute(StorageOp::Delete(msg.id)).await;
                    let dlq_name = format!("{}_dlq", self.name);
                    let _ = self.manager_tx.send(ManagerCommand::MoveToDLQ {
                        queue_name: dlq_name,
                        payload: msg.payload,
                        priority: msg.priority,
                    }).await;
                }
                
                // Since messages might have become Ready, check waiters
                self.process_waiters().await;
            }
        }

        // 2. Process Waiter Expirations
        // Waiters are ordered by arrival? Not necessarily expiration. 
        // We should iterate and remove expired ones.
        // Or if we use VecDeque, we assume FIFO... but expiration might differ if variable wait_ms.
        // Simple scan:
        let mut i = 0;
        while i < self.waiters.len() {
            if self.waiters[i].expires_at <= now {
                if let Some(waiter) = self.waiters.remove(i) {
                    // Reply empty
                    let _ = waiter.reply.send(vec![]);
                }
            } else {
                i += 1;
            }
        }
    }

    async fn process_waiters(&mut self) {
        // While we have ready messages AND waiters...
        while !self.waiters.is_empty() && self.state.has_ready_messages() {
            if let Some(waiter) = self.waiters.pop_front() {
                // Try to fulfill this waiter
                let (msgs, _) = self.state.take_batch(waiter.max, self.config.visibility_timeout_ms);
                
                if !msgs.is_empty() {
                    for msg in &msgs {
                        let _ = self.store.execute(StorageOp::UpdateState { 
                            id: msg.id, 
                            visible_at: msg.visible_at, 
                            attempts: msg.attempts 
                        }).await;
                    }
                    if waiter.reply.send(msgs).is_err() {
                        // Waiter disconnected? Messages are already InFlight in memory + DB.
                        // We rely on VisibilityTimeout to recover them. Correct.
                    }
                } else {
                    // Should not happen if has_ready_messages is true, unless race/logic error.
                    // Put waiter back?
                    self.waiters.push_front(waiter);
                    break;
                }
            }
        }
    }
}

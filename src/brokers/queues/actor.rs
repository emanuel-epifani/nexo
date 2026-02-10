//! Queue Actor: Async wrapper around QueueState
//! Handles commands, persistence, and time-based events.

use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant}; // Add Instant
use bytes::Bytes;
use std::collections::VecDeque;
use tracing::{error, info};
use uuid::Uuid;
use std::path::PathBuf;
use crate::brokers::queues::MessageState;
use crate::brokers::queues::queue::{QueueState, QueueConfig, Message, current_time_ms};
use crate::brokers::queues::dlq::{DlqState, DlqMessage};
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
    Nack {
        id: Uuid,
        reason: String,
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
    
    // DLQ Commands
    PeekDLQ {
        limit: usize,
        offset: usize,
        reply: oneshot::Sender<(usize, Vec<DlqMessage>)>,
    },
    MoveToQueue {
        message_id: Uuid,
        reply: oneshot::Sender<bool>,
    },
    DeleteDLQ {
        message_id: Uuid,
        reply: oneshot::Sender<bool>,
    },
    PurgeDLQ {
        reply: oneshot::Sender<usize>,
    },
}

struct WaitingConsumer {
    reply: oneshot::Sender<Vec<Message>>,
    max: usize,
    expires_at: u64, // SystemTime ms
}

pub struct QueueActor {
    name: String,
    main_state: QueueState,
    dlq_state: DlqState,
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
        persistence_path: PathBuf,
        rx: mpsc::Receiver<QueueActorCommand>,
        manager_tx: mpsc::Sender<ManagerCommand>,
    ) -> Self {
        
        if let Err(e) = std::fs::create_dir_all(&persistence_path) {
            error!("Failed to create queue data directory at {:?}: {}", persistence_path, e);
        }

        let db_path = persistence_path.join(format!("{}.db", name));
        let store = QueueStore::new(
            db_path, 
            config.persistence.clone(),
            config.writer_channel_capacity,
            config.writer_batch_size,
        );

        Self {
            name,
            main_state: QueueState::new(),
            dlq_state: DlqState::new(),
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
            Ok((main_messages, dlq_messages)) => {
                let main_count = main_messages.len();
                let dlq_count = dlq_messages.len();
                
                for msg in main_messages {
                    self.main_state.push(msg);
                }
                
                for msg in dlq_messages {
                    self.dlq_state.push(msg);
                }
                
                if main_count > 0 || dlq_count > 0 {
                    info!("Queue '{}': Recovered {} main + {} DLQ messages from storage", 
                          self.name, main_count, dlq_count);
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
        
        let next_msg_ts = self.main_state.next_timeout();
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
                let msg = Message::new(payload, priority, delay_ms);
                if delay_ms.is_none() && !self.waiters.is_empty() {
                    self.main_state.push(msg.clone());
                    let _ = self.store.execute(StorageOp::Insert(msg)).await;
                    self.process_waiters().await;
                } else {
                    self.main_state.push(msg.clone());
                    let _ = self.store.execute(StorageOp::Insert(msg)).await;
                }
                let _ = reply.send(());
                true
            }
            
            QueueActorCommand::Pop { reply } => {
                let (msg_opt, _) = self.main_state.pop(self.config.visibility_timeout_ms);
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
                let result = self.main_state.ack(id);
                if result {
                    let _ = self.store.execute(StorageOp::Delete(id)).await;
                }
                let _ = reply.send(result);
                true
            }

            QueueActorCommand::Nack { id, reason, reply } => {
                let (requeued, dlq_msg) = self.main_state.nack(id, reason.clone(), self.config.max_retries);
                
                if let Some(msg) = requeued {
                    // Requeued (Ready)
                    let _ = self.store.execute(StorageOp::UpdateState { 
                        id: msg.id, 
                        visible_at: msg.visible_at, 
                        attempts: msg.attempts 
                    }).await;
                    // Check waiters since it became Ready
                    self.process_waiters().await;
                    let _ = reply.send(true);
                } else if let Some(dlq_message) = dlq_msg {
                    // Moved to DLQ - Already converted
                    self.dlq_state.push(dlq_message.clone());
                    
                    let _ = self.store.execute(StorageOp::MoveToDLQ {
                        id: dlq_message.id,
                        msg: dlq_message,
                    }).await;
                    let _ = reply.send(true);
                } else {
                    // Not found
                    let _ = reply.send(false);
                }
                true
            }
            
            QueueActorCommand::ConsumeBatch { max, wait_ms, reply } => {
                let (msgs, _) = self.main_state.take_batch(max, self.config.visibility_timeout_ms);
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
                let snapshot = self.main_state.get_snapshot(&self.name);
                let _ = reply.send(snapshot);
                true
            }

            QueueActorCommand::Stop { reply } => {
                let _ = reply.send(());
                false // Signal to break the loop
            }
            
            // DLQ Command Handlers
            QueueActorCommand::PeekDLQ { limit, offset, reply } => {
                let (total, messages) = self.dlq_state.peek(offset, limit);
                let _ = reply.send((total, messages));
                true
            }
            
            QueueActorCommand::MoveToQueue { message_id, reply } => {
                // Remove from DLQ and add to main queue
                if let Some(dlq_msg) = self.dlq_state.remove(&message_id) {
                    // Create new message for main queue (reset attempts)
                    let new_msg = dlq_msg.clone().to_message();
                    
                    // Add to main queue
                    self.main_state.push(new_msg.clone());
                    
                    // Persist atomically
                    let _ = self.store.execute(StorageOp::MoveToMain {
                        id: message_id,
                        msg: new_msg,
                    }).await;
                    
                    // Check if waiters can be fulfilled
                    self.process_waiters().await;
                    
                    let _ = reply.send(true);
                } else {
                    let _ = reply.send(false);
                }
                true
            }
            
            QueueActorCommand::DeleteDLQ { message_id, reply } => {
                // Remove from DLQ
                if self.dlq_state.remove(&message_id).is_some() {
                    // Persist deletion
                    let _ = self.store.execute(StorageOp::DeleteDLQ(message_id)).await;
                    let _ = reply.send(true);
                } else {
                    let _ = reply.send(false);
                }
                true
            }
            
            QueueActorCommand::PurgeDLQ { reply } => {
                // Count messages before purging
                let count = self.dlq_state.len();
                
                // Clear all DLQ state
                self.dlq_state.clear();
                
                // Persist purge
                let _ = self.store.execute(StorageOp::PurgeDLQ).await;
                
                let _ = reply.send(count);
                true
            }
        }
    }

    async fn process_time_events(&mut self) {
        let now = current_time_ms();

        // 1. Process Message Expirations (Scheduled -> Ready, InFlight -> Retry/DLQ)
        if let Some(next_msg_ts) = self.main_state.next_timeout() {
            if next_msg_ts <= now {
                let (requeued, dlq_msgs) = self.main_state.process_expired(self.config.max_retries);
                
                for msg in requeued {
                    let _ = self.store.execute(StorageOp::UpdateState {
                        id: msg.id,
                        visible_at: 0,
                        attempts: msg.attempts
                    }).await;
                }

                for dlq_msg in dlq_msgs {
                    self.dlq_state.push(dlq_msg.clone());
                    
                    let _ = self.store.execute(StorageOp::MoveToDLQ {
                        id: dlq_msg.id,
                        msg: dlq_msg,
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
        while !self.waiters.is_empty() && self.main_state.has_ready_messages() {
            if let Some(waiter) = self.waiters.pop_front() {
                // Skip stale waiters from disconnected clients
                if waiter.reply.is_closed() {
                    continue;
                }

                let (msgs, _) = self.main_state.take_batch(waiter.max, self.config.visibility_timeout_ms);
                
                if !msgs.is_empty() {
                    for msg in &msgs {
                        let _ = self.store.execute(StorageOp::UpdateState { 
                            id: msg.id, 
                            visible_at: msg.visible_at, 
                            attempts: msg.attempts 
                        }).await;
                    }
                    // Safety net: if send fails despite is_closed check (race between
                    // check and send), re-queue messages immediately as Ready.
                    if let Err(returned_msgs) = waiter.reply.send(msgs) {
                        for msg in &returned_msgs {
                            let reverted_attempts = msg.attempts.saturating_sub(1);
                            self.main_state.requeue_inflight(msg.id);
                            let _ = self.store.execute(StorageOp::UpdateState {
                                id: msg.id,
                                visible_at: 0,
                                attempts: reverted_attempts,
                            }).await;
                        }
                    }
                } else {
                    self.waiters.push_front(waiter);
                    break;
                }
            }
        }
    }
}

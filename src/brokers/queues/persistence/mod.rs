pub mod types;
mod sqlite;
mod writer;

use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use types::{PersistenceMode, StorageOp, StoreCommand};
use writer::run_writer;

use crate::brokers::queues::queue::Message;
use crate::brokers::queues::dlq::DlqMessage;
use sqlite::{load_all_messages, load_dlq_messages};
use rusqlite::Connection;

use crate::brokers::queues::persistence::sqlite::init_db; // Import init_db

#[derive(Clone)]
pub struct QueueStore {
    sender: Option<mpsc::Sender<StoreCommand>>,
    mode: PersistenceMode,
    db_path: PathBuf,
}

impl QueueStore {
    pub fn new(
        db_path: PathBuf, 
        mode: PersistenceMode, 
        writer_channel_capacity: usize,
        batch_size: usize
    ) -> Self {
        // 1. SYNCHRONOUS INIT: Ensure DB schema exists before anything else
        // This prevents race conditions where recover() runs before Writer creates tables.
        if let Ok(conn) = Connection::open(&db_path) {
            if let Err(e) = init_db(&conn, &mode) {
                tracing::error!("FATAL: Failed to initialize Queue DB at {:?}: {}", db_path, e);
                // We proceed, but the actor will likely fail later.
            }
        } else {
             tracing::error!("FATAL: Failed to open Queue DB for initialization at {:?}", db_path);
        }

        let (tx, rx) = mpsc::channel(writer_channel_capacity);
        
        let mode_clone = mode.clone();
        let path_clone = db_path.clone();
        // Spawn Writer Thread
        tokio::spawn(async move {
            run_writer(rx, path_clone, mode_clone, batch_size).await;
        });

        Self {
            sender: Some(tx),
            mode,
            db_path,
        }
    }

    /// Recover all messages from DB (Read-Only connection)
    /// Returns (main_messages, dlq_messages)
    pub fn recover(&self) -> Result<(Vec<Message>, Vec<DlqMessage>), String> {
        let conn = Connection::open(&self.db_path)
            .map_err(|e| format!("Failed to open DB for recovery: {}", e))?;

        let main_messages = load_all_messages(&conn)
            .map_err(|e| format!("Failed to load main messages: {}", e))?;
        
        let dlq_messages = load_dlq_messages(&conn)
            .map_err(|e| format!("Failed to load DLQ messages: {}", e))?;

        Ok((main_messages, dlq_messages))
    }

    pub async fn execute(&self, op: StorageOp) -> Result<(), String> {
        let sender = self.sender.as_ref().ok_or("Store uninitialized")?;

        match self.mode {
            PersistenceMode::Sync => {
                let (tx, rx) = oneshot::channel();
                sender
                    .send(StoreCommand { op, sync_channel: Some(tx) })
                    .await
                    .map_err(|_| "Writer channel closed".to_string())?;
                
                rx.await.map_err(|_| "Writer dropped reply".to_string())?
            }
            PersistenceMode::Async { .. } => {
                sender
                    .send(StoreCommand { op, sync_channel: None })
                    .await
                    .map_err(|_| "Writer channel closed".to_string())?;
                Ok(())
            }
        }
    }
}

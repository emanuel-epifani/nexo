pub mod types;
mod sqlite;
mod writer;

use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use types::{PersistenceMode, StorageOp, StoreCommand};
use writer::run_writer;

use crate::brokers::queues::queue::Message;
use sqlite::load_all_messages;
use rusqlite::Connection;

#[derive(Clone)]
pub struct QueueStore {
    sender: Option<mpsc::Sender<StoreCommand>>,
    mode: PersistenceMode,
    db_path: PathBuf,
}

impl QueueStore {
    pub fn new(db_path: PathBuf, mode: PersistenceMode) -> Self {
        if let PersistenceMode::Memory = mode {
            return Self {
                sender: None,
                mode,
                db_path,
            };
        }

        let (tx, rx) = mpsc::channel(10000); // Channel buffer
        
        let mode_clone = mode.clone();
        let path_clone = db_path.clone();
        // Spawn Writer Thread
        tokio::spawn(async move {
            run_writer(rx, path_clone, mode_clone).await;
        });

        Self {
            sender: Some(tx),
            mode,
            db_path,
        }
    }

    /// Recover all messages from DB (Read-Only connection)
    pub fn recover(&self) -> Result<Vec<Message>, String> {
        if let PersistenceMode::Memory = self.mode {
            return Ok(Vec::new());
        }

        let conn = Connection::open_with_flags(
            &self.db_path, 
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
        ).map_err(|e| format!("Failed to open DB for recovery: {}", e))?;

        load_all_messages(&conn).map_err(|e| format!("Failed to load messages: {}", e))
    }

    pub async fn execute(&self, op: StorageOp) -> Result<(), String> {
        // If Memory mode, just return success immediately (no-op)
        if let PersistenceMode::Memory = self.mode {
            return Ok(());
        }

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
            PersistenceMode::Memory => Ok(()), // Should be unreachable due to first check, but safe
        }
    }
}

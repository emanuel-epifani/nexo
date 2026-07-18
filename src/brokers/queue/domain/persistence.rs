use std::path::PathBuf;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use rusqlite::{params, types::Type, Connection, Result};
use tracing::{error, info};
use uuid::Uuid;

use crate::brokers::queue::domain::queue::{Message, MessageState, current_time_ms};
use crate::brokers::queue::domain::dlq::DlqMessage;

// ==========================================
// STORAGE OPERATIONS
// ==========================================

/// Atomic operations that storage can execute
#[derive(Debug)]
pub enum StorageOp {
    /// Insert a new message (Push)
    Insert(Message),
    /// Insert multiple messages in one operation (PushBatch)
    InsertBatch(Vec<Message>),
    /// Remove a message (Ack)
    Delete(Uuid),
    /// Update visibility and attempts (Nack / Timeout / In-flight)
    UpdateState {
        id: Uuid,
        visible_at: u64,
        attempts: u32,
        failure_reason: Option<String>,
    },
    /// Update visibility and attempts for multiple messages in one operation
    UpdateStateBatch(Vec<Message>),
    
    // DLQ Operations
    /// Delete a message from DLQ
    DeleteDLQ(Uuid),
    /// Move message from main queue to DLQ (atomic)
    MoveToDLQ(DlqMessage),
    /// Move message from DLQ to main queue (atomic)
    MoveToMain(Message),
    /// Purge all messages from DLQ
    PurgeDLQ,
}

// ==========================================
// QUEUE STORE (Public API)
// ==========================================

pub struct QueueStore {
    sender: Mutex<Option<mpsc::UnboundedSender<StorageOp>>>,
    writer_handle: Mutex<Option<JoinHandle<()>>>,
    db_path: PathBuf,
}

impl QueueStore {
    pub fn new(
        db_path: PathBuf, 
        flush_ms: u64,
        batch_size: usize,
    ) -> Self {
        // SYNCHRONOUS INIT: Ensure DB schema exists before anything else
        // This prevents race conditions where recover() runs before Writer creates tables.
        if let Ok(conn) = Connection::open(&db_path) {
            if let Err(e) = init_db(&conn) {
                error!("FATAL: Failed to initialize Queue DB at {:?}: {}", db_path, e);
            }
        } else {
            error!("FATAL: Failed to open Queue DB for initialization at {:?}", db_path);
        }

        let (tx, rx) = mpsc::unbounded_channel();
        
        let path_clone = db_path.clone();
        let handle = tokio::spawn(async move {
            run_writer(rx, path_clone, flush_ms, batch_size).await;
        });

        Self {
            sender: Mutex::new(Some(tx)),
            writer_handle: Mutex::new(Some(handle)),
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

    /// Send a storage op to the background writer (sync, never blocks)
    #[inline]
    pub fn execute(&self, op: StorageOp) {
        if let Some(sender) = self.sender.lock().as_ref() {
            if let Err(e) = sender.send(op) {
                error!("Writer channel closed, op lost: {:?}", e.0);
            }
        }
    }

    /// Graceful shutdown: drop sender so writer drains remaining ops, then wait for it to exit
    pub async fn shutdown(&self) {
        self.sender.lock().take(); // drop sender → writer recv() returns None after draining
        let handle = self.writer_handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

// ==========================================
// BACKGROUND WRITER
// ==========================================

async fn run_writer(
    mut rx: mpsc::UnboundedReceiver<StorageOp>,
    db_path: PathBuf,
    flush_ms: u64,
    batch_size: usize,
) {
    let mut conn = match Connection::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            error!("FATAL: Cannot open queue DB at {:?}: {}", db_path, e);
            return;
        }
    };
    if let Err(e) = conn.execute_batch(
        // Writer connection pragmas for high-throughput batch operations
        // synchronous=OFF for performance (data is flushed periodically via timer)
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = OFF;
         PRAGMA cache_size = -64000;
         PRAGMA temp_store = MEMORY;
         PRAGMA mmap_size = 268435456;"
    ) {
        error!("Failed to set writer pragmas: {}", e);
    }

    info!("Queue Persistence Writer started for {:?}", db_path);

    let mut flush_timer = tokio::time::interval(Duration::from_millis(flush_ms));
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut batch = Vec::with_capacity(batch_size);

    loop {
        tokio::select! {
            recv_result = rx.recv() => {
                match recv_result {
                    Some(op) => {
                        batch.push(op);

                        // Drain everything currently available in the channel
                        while batch.len() < batch_size {
                            match rx.try_recv() {
                                Ok(op) => batch.push(op),
                                Err(_) => break,
                            }
                        }

                        if batch.len() >= batch_size {
                            flush_batch(&mut conn, &mut batch);
                        }
                    }
                    None => {
                        // Sender dropped — flush remaining and exit
                        if !batch.is_empty() {
                            flush_batch(&mut conn, &mut batch);
                        }
                        if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                            error!("Failed to checkpoint WAL on shutdown: {}", e);
                        }
                        info!("Queue Persistence Writer stopped for {:?}", db_path);
                        return;
                    }
                }
            }
            
            _ = flush_timer.tick() => {
                if !batch.is_empty() {
                    flush_batch(&mut conn, &mut batch);
                }
            }
        }
    }
}

fn flush_batch(conn: &mut Connection, batch: &mut Vec<StorageOp>) {
    let tx = match conn.transaction() {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to start transaction: {}", e);
            return;
        }
    };

    for op in batch.iter() {
        if let Err(e) = exec_op(&tx, op) {
            error!("Failed to exec op {:?}: {}", op, e);
        }
    }

    if let Err(e) = tx.commit() {
        error!("Failed to commit batch ({} ops retained for retry): {}", batch.len(), e);
        return;
    }

    batch.clear();
}

// ==========================================
// SQLITE OPERATIONS
// ==========================================

fn uuid_from_blob(id_blob: Vec<u8>) -> Result<Uuid> {
    Uuid::from_slice(&id_blob).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(id_blob.len(), Type::Blob, Box::new(e))
    })
}

fn init_db(conn: &Connection) -> Result<()> {
    // Set pragmas for schema initialization connection
    // synchronous=NORMAL for safety during table creation
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA cache_size = -64000;
         PRAGMA temp_store = MEMORY;
         "
    )?;

    // Main Queue Table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS queue (
            id BLOB PRIMARY KEY,
            payload BLOB NOT NULL,
            priority INTEGER NOT NULL,
            visible_at INTEGER NOT NULL,
            attempts INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            error TEXT
        )",
        [],
    )?;

    // DLQ Table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dlq_messages (
            id BLOB PRIMARY KEY,
            payload BLOB NOT NULL,
            priority INTEGER NOT NULL,
            attempts INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            failed_at INTEGER NOT NULL,
            error TEXT
        )",
        [],
    )?;

    Ok(())
}

fn load_all_messages(conn: &Connection) -> Result<Vec<Message>> {
    let mut stmt = conn.prepare(
        "SELECT id, payload, priority, visible_at, attempts, created_at, error FROM queue"
    )?;

    let message_iter = stmt.query_map([], |row| {
        let id_blob: Vec<u8> = row.get(0)?;
        let id = uuid_from_blob(id_blob)?;
        let payload: Vec<u8> = row.get(1)?;
        let priority: u8 = row.get(2)?;
        let visible_at = row.get::<_, i64>(3)? as u64;
        let attempts: u32 = row.get(4)?;
        let created_at = row.get::<_, i64>(5)? as u64;
        let error: Option<String> = row.get(6)?;

         let now = current_time_ms();
         
         // Reconstruct State
         let state = if visible_at > now && attempts > 0 {
             MessageState::InFlight
         } else {
             MessageState::Ready
         };

         Ok(Message {
             id,
             payload: bytes::Bytes::from(payload),
             priority,
             attempts,
             created_at,
             visible_at,
             failure_reason: error,
             state,
         })
    })?;

    let mut messages = Vec::new();
    for msg in message_iter {
        messages.push(msg?);
    }
    Ok(messages)
}

fn load_dlq_messages(conn: &Connection) -> Result<Vec<DlqMessage>> {
    let mut stmt = conn.prepare(
        "SELECT id, payload, priority, attempts, created_at, failed_at, error FROM dlq_messages"
    )?;

    let message_iter = stmt.query_map([], |row| {
        let id_blob: Vec<u8> = row.get(0)?;
        let id = uuid_from_blob(id_blob)?;
        let payload: Vec<u8> = row.get(1)?;
        let priority: u8 = row.get(2)?;
        let attempts: u32 = row.get(3)?;
        let created_at = row.get::<_, i64>(4)? as u64;
        let failed_at = row.get::<_, i64>(5)? as u64;
        let error: Option<String> = row.get(6)?;

        Ok(DlqMessage {
            id,
            payload: bytes::Bytes::from(payload),
            priority,
            attempts,
            created_at,
            failed_at,
            failure_reason: error.unwrap_or_default(),
        })
    })?;

    let mut messages = Vec::new();
    for msg in message_iter {
        messages.push(msg?);
    }
    Ok(messages)
}

fn exec_op(tx: &rusqlite::Transaction, op: &StorageOp) -> Result<()> {
    match op {
        StorageOp::Insert(msg) => {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO queue (id, payload, priority, visible_at, attempts, created_at, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;
            stmt.execute(params![
                msg.id.as_bytes(),
                msg.payload.as_ref(),
                msg.priority,
                msg.visible_at as i64,
                msg.attempts,
                msg.created_at as i64,
                msg.failure_reason.as_deref()
            ])?;
        }
        StorageOp::InsertBatch(msgs) => {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO queue (id, payload, priority, visible_at, attempts, created_at, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;
            for msg in msgs {
                stmt.execute(params![
                    msg.id.as_bytes(),
                    msg.payload.as_ref(),
                    msg.priority,
                    msg.visible_at as i64,
                    msg.attempts,
                    msg.created_at as i64,
                    msg.failure_reason.as_deref()
                ])?;
            }
        }
        StorageOp::Delete(id) => {
            let mut stmt = tx.prepare_cached("DELETE FROM queue WHERE id = ?1")?;
            stmt.execute(params![id.as_bytes()])?;
        }
        StorageOp::UpdateState { id, visible_at, attempts, failure_reason } => {
            let mut stmt = tx.prepare_cached(
                "UPDATE queue SET visible_at = ?1, attempts = ?2, error = ?3 WHERE id = ?4"
            )?;
            stmt.execute(params![*visible_at as i64, *attempts, failure_reason.as_deref(), id.as_bytes()])?;
        }
        StorageOp::UpdateStateBatch(msgs) => {
            let mut stmt = tx.prepare_cached(
                "UPDATE queue SET visible_at = ?1, attempts = ?2, error = ?3 WHERE id = ?4"
            )?;
            for msg in msgs {
                stmt.execute(params![
                    msg.visible_at as i64,
                    msg.attempts,
                    msg.failure_reason.as_deref(),
                    msg.id.as_bytes()
                ])?;
            }
        }
        
        // DLQ Operations
        StorageOp::DeleteDLQ(id) => {
            let mut stmt = tx.prepare_cached("DELETE FROM dlq_messages WHERE id = ?1")?;
            stmt.execute(params![id.as_bytes()])?;
        }
        StorageOp::MoveToDLQ(msg) => {
            // Atomic: delete from queue, insert into DLQ
            let mut stmt = tx.prepare_cached("DELETE FROM queue WHERE id = ?1")?;
            stmt.execute(params![msg.id.as_bytes()])?;
            
            let mut stmt = tx.prepare_cached(
                "INSERT INTO dlq_messages (id, payload, priority, attempts, created_at, failed_at, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;
            stmt.execute(params![
                msg.id.as_bytes(),
                msg.payload.as_ref(),
                msg.priority,
                msg.attempts,
                msg.created_at as i64,
                msg.failed_at as i64,
                msg.failure_reason
            ])?;
        }
        StorageOp::MoveToMain(msg) => {
            // Atomic: delete from DLQ, insert into queue
            let mut stmt = tx.prepare_cached("DELETE FROM dlq_messages WHERE id = ?1")?;
            stmt.execute(params![msg.id.as_bytes()])?;
            
            let mut stmt = tx.prepare_cached(
                "INSERT INTO queue (id, payload, priority, visible_at, attempts, created_at, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;
            stmt.execute(params![
                msg.id.as_bytes(),
                msg.payload.as_ref(),
                msg.priority,
                0i64, // visible_at = 0 (ready immediately)
                0u32, // reset attempts
                msg.created_at as i64,
                msg.failure_reason.as_deref()
            ])?;
        }
        StorageOp::PurgeDLQ => {
            tx.execute("DELETE FROM dlq_messages", [])?;
        }
    }
    Ok(())
}

use rusqlite::{params, Connection, Result};
use super::types::{StorageOp, PersistenceMode};

use crate::brokers::queue::queue::{Message, MessageState, current_time_ms};
use crate::brokers::queue::dlq::DlqMessage;
use uuid::Uuid;

pub fn init_db(conn: &Connection, mode: &PersistenceMode) -> Result<()> {
    let sync_pragma = match mode {
        PersistenceMode::Sync => "FULL",
        _ => "NORMAL",
    };

    conn.execute_batch(
        &format!(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = {};
         PRAGMA foreign_keys = ON;
         PRAGMA cache_size = -64000;
         PRAGMA temp_store = MEMORY;
         ", // Tuned for high throughput
         sync_pragma
        )
    )?;

    // Main Queue Table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS queue (
            id TEXT PRIMARY KEY,
            payload BLOB NOT NULL,
            priority INTEGER NOT NULL,
            visible_at INTEGER NOT NULL,
            attempts INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )",
        [],
    )?;

    // DLQ Table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dlq_messages (
            id TEXT PRIMARY KEY,
            payload BLOB NOT NULL,
            priority INTEGER NOT NULL,
            attempts INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            failed_at INTEGER NOT NULL,
            error TEXT
        )",
        [],
    )?;

    // Crucial index for fast POP
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pop 
         ON queue (visible_at ASC, priority DESC, created_at ASC)",
        [],
    )?;

    // Index for DLQ queries
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dlq_failed 
         ON dlq_messages (failed_at DESC)",
        [],
    )?;

    Ok(())
}

pub fn load_all_messages(conn: &Connection) -> Result<Vec<Message>> {
    let mut stmt = conn.prepare(
        "SELECT id, payload, priority, visible_at, attempts, created_at FROM queue"
    )?;

    let message_iter = stmt.query_map([], |row| {
        let id_str: String = row.get(0)?;
        let payload: Vec<u8> = row.get(1)?;
        let priority: u8 = row.get(2)?;
        let visible_at = row.get::<_, i64>(3)? as u64;
        let attempts: u32 = row.get(4)?;
        let created_at = row.get::<_, i64>(5)? as u64;

        let now = current_time_ms();
        
        // Reconstruct State
        let state = if visible_at <= now {
            MessageState::Ready
        } else if attempts > 0 {
            MessageState::InFlight(visible_at)
        } else {
            MessageState::Scheduled(visible_at)
        };

        Ok(Message {
            id: Uuid::parse_str(&id_str).unwrap_or_default(),
            payload: bytes::Bytes::from(payload),
            priority,
            attempts,
            created_at,
            visible_at,
            delayed_until: None, // Lost persistence of original delay, but functional equivalent
            failure_reason: None, // Not persisted in main queue yet
            state,
        })
    })?;

    let mut messages = Vec::new();
    for msg in message_iter {
        messages.push(msg?);
    }
    Ok(messages)
}

pub fn load_dlq_messages(conn: &Connection) -> Result<Vec<DlqMessage>> {
    let mut stmt = conn.prepare(
        "SELECT id, payload, priority, attempts, created_at, failed_at, error FROM dlq_messages"
    )?;

    let message_iter = stmt.query_map([], |row| {
        let id_str: String = row.get(0)?;
        let payload: Vec<u8> = row.get(1)?;
        let priority: u8 = row.get(2)?;
        let attempts: u32 = row.get(3)?;
        let created_at = row.get::<_, i64>(4)? as u64;
        let failed_at = row.get::<_, i64>(5)? as u64;
        let error: Option<String> = row.get(6)?;

        Ok(DlqMessage {
            id: Uuid::parse_str(&id_str).unwrap_or_default(),
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

pub fn exec_op(tx: &rusqlite::Transaction, op: &StorageOp) -> Result<()> {
    match op {
        StorageOp::Insert(msg) => {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO queue (id, payload, priority, visible_at, attempts, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            )?;
            stmt.execute(params![
                msg.id.to_string(),
                msg.payload.as_ref(), // Bytes -> &[u8]
                msg.priority,
                msg.visible_at as i64,
                msg.attempts,
                msg.created_at as i64
            ])?;
        }
        StorageOp::Delete(id) => {
            let mut stmt = tx.prepare_cached("DELETE FROM queue WHERE id = ?1")?;
            stmt.execute(params![id.to_string()])?;
        }
        StorageOp::UpdateState { id, visible_at, attempts } => {
            let mut stmt = tx.prepare_cached(
                "UPDATE queue SET visible_at = ?1, attempts = ?2 WHERE id = ?3"
            )?;
            stmt.execute(params![*visible_at as i64, *attempts, id.to_string()])?;
        }
        
        // DLQ Operations
        StorageOp::InsertDLQ(msg) => {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO dlq_messages (id, payload, priority, attempts, created_at, failed_at, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;
            stmt.execute(params![
                msg.id.to_string(),
                msg.payload.as_ref(),
                msg.priority,
                msg.attempts,
                msg.created_at as i64,
                msg.failed_at as i64,
                msg.failure_reason
            ])?;
        }
        StorageOp::DeleteDLQ(id) => {
            let mut stmt = tx.prepare_cached("DELETE FROM dlq_messages WHERE id = ?1")?;
            stmt.execute(params![id.to_string()])?;
        }
        StorageOp::MoveToDLQ { id, msg } => {
            // Atomic: delete from queue, insert into DLQ
            let mut stmt = tx.prepare_cached("DELETE FROM queue WHERE id = ?1")?;
            stmt.execute(params![id.to_string()])?;
            
            let mut stmt = tx.prepare_cached(
                "INSERT INTO dlq_messages (id, payload, priority, attempts, created_at, failed_at, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;
            stmt.execute(params![
                msg.id.to_string(),
                msg.payload.as_ref(),
                msg.priority,
                msg.attempts,
                msg.created_at as i64,
                msg.failed_at as i64,
                msg.failure_reason
            ])?;
        }
        StorageOp::MoveToMain { id, msg } => {
            // Atomic: delete from DLQ, insert into queue
            let mut stmt = tx.prepare_cached("DELETE FROM dlq_messages WHERE id = ?1")?;
            stmt.execute(params![id.to_string()])?;
            
            let mut stmt = tx.prepare_cached(
                "INSERT INTO queue (id, payload, priority, visible_at, attempts, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            )?;
            stmt.execute(params![
                msg.id.to_string(),
                msg.payload.as_ref(),
                msg.priority,
                0i64, // visible_at = 0 (ready immediately)
                0u32, // reset attempts
                msg.created_at as i64
            ])?;
            // failure_reason is lost when moving back to main because table doesn't support it yet
            // and we are resetting the message anyway.
        }
        StorageOp::PurgeDLQ => {
            tx.execute("DELETE FROM dlq_messages", [])?;
        }
    }
    Ok(())
}

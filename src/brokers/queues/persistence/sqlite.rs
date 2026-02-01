use rusqlite::{params, Connection, Result};
use super::types::{StorageOp, PersistenceMode};

use crate::brokers::queues::queue::{Message, MessageState, current_time_ms}; // Add current_time_ms import
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
         PRAGMA foreign_keys = ON;", // Tuned for high throughput
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

    // Dead Letter Queue Table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dlq (
            id TEXT PRIMARY KEY,
            payload BLOB NOT NULL,
            reason TEXT,
            moved_at INTEGER NOT NULL
        )",
        [],
    )?;

    // Crucial index for fast POP
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pop 
         ON queue (visible_at ASC, priority DESC, created_at ASC)",
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
            state,
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
        StorageOp::MoveToDlq { msg, reason } => {
            // Atomic: Delete from queue and insert into DLQ
            let mut delete_stmt = tx.prepare_cached("DELETE FROM queue WHERE id = ?1")?;
            delete_stmt.execute(params![msg.id.to_string()])?;
            
            let mut insert_dlq = tx.prepare_cached(
                "INSERT INTO dlq (id, payload, reason, moved_at)
                 VALUES (?1, ?2, ?3, ?4)"
            )?;
            insert_dlq.execute(params![
                msg.id.to_string(),
                msg.payload.as_ref(),
                reason,
                current_time_ms() as i64
            ])?;
        }
    }
    Ok(())
}

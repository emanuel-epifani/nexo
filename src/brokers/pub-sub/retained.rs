//! PubSub Retained Message: Last Value Caching with TTL support

use std::time::{Instant, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use serde::{Serialize, Deserialize};
use rusqlite::{params, Connection, Result};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RetainedMessage {
    pub(crate) data: Bytes,
    #[serde(skip)]
    pub(crate) expires_at: Option<Instant>,
    pub(crate) expires_at_unix: Option<u64>,
}

impl RetainedMessage {
    pub(crate) fn new(data: Bytes, ttl_seconds: Option<u64>) -> Self {
        let expires_at = ttl_seconds.map(|secs| Instant::now() + std::time::Duration::from_secs(secs));
        let expires_at_unix = ttl_seconds.map(|secs| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + secs
        });
        Self { data, expires_at, expires_at_unix }
    }
    
    pub(crate) fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |exp| Instant::now() >= exp)
    }
    
    pub(crate) fn from_persisted(data: Bytes, expires_at_unix: Option<u64>) -> Self {
        let expires_at = expires_at_unix.and_then(|unix_ts| {
            let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if unix_ts > now_unix {
                let remaining = unix_ts - now_unix;
                Some(Instant::now() + std::time::Duration::from_secs(remaining))
            } else {
                None
            }
        });
        Self { data, expires_at, expires_at_unix }
    }
}

pub(crate) fn init_db(path: &str) -> std::result::Result<Connection, rusqlite::Error> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent).unwrap_or(());
    }
    
    let conn = Connection::open(path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS retained (
            path TEXT PRIMARY KEY,
            data BLOB NOT NULL,
            expires_at INTEGER
        )",
        [],
    )?;
    Ok(conn)
}

pub(crate) fn load_all(conn: &Connection) -> std::result::Result<Vec<(String, RetainedMessage)>, rusqlite::Error> {
    let mut stmt = conn.prepare("SELECT path, data, expires_at FROM retained")?;
    let entries = stmt.query_map([], |row| {
        let path: String = row.get(0)?;
        let data: Vec<u8> = row.get(1)?;
        let expires_at_unix: Option<i64> = row.get(2)?;
        Ok((path, Bytes::from(data), expires_at_unix.map(|v| v as u64)))
    })?;

    let mut results = Vec::new();
    for entry in entries {
        if let Ok((path, data, expires)) = entry {
            let msg = RetainedMessage::from_persisted(data, expires);
            if !msg.is_expired() {
                results.push((path, msg));
            }
        }
    }
    Ok(results)
}

pub(crate) fn flush(conn: &mut Connection, entries: &[(String, Bytes, Option<i64>)]) -> std::result::Result<(), rusqlite::Error> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM retained", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO retained (path, data, expires_at) VALUES (?, ?, ?)"
        )?;
        for (path, data, expires) in entries {
            stmt.execute(params![path, data.as_ref(), expires])?;
        }
    }
    tx.commit()?;
    Ok(())
}

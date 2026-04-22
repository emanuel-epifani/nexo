//! SQLite persistence for retained messages.

use bytes::Bytes;
use rusqlite::{params, Connection};

use super::retained::RetainedMessage;

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

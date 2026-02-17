use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use rusqlite::Connection;
use tracing::{error, info};

use super::types::{StoreCommand, PersistenceMode};
use super::sqlite::{init_db, exec_op};

pub async fn run_writer(
    mut rx: mpsc::Receiver<StoreCommand>,
    db_path: PathBuf,
    mode: PersistenceMode,
    batch_size: usize,
) {
    let mut conn = match Connection::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            error!("FATAL: Cannot open queue DB at {:?}: {}", db_path, e);
            return;
        }
    };

    info!("Queue Persistence Writer started for {:?}", db_path);

    // Flush Configuration
    let mut flush_timer = match mode {
        PersistenceMode::Async { flush_ms } => {
            let mut t = tokio::time::interval(Duration::from_millis(flush_ms));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            Some(t)
        }
        PersistenceMode::Sync => None,
    };

    let mut batch = Vec::new();
    let mut waiters = Vec::new();

    loop {
        tokio::select! {
            Some(cmd) = rx.recv() => {
                let is_sync_req = cmd.sync_channel.is_some();
                
                if let Some(ch) = cmd.sync_channel {
                    waiters.push(ch);
                }
                batch.push(cmd.op);

                // If Sync or buffer too full, flush immediately
                if is_sync_req || batch.len() >= batch_size {
                    flush_batch(&mut conn, &mut batch, &mut waiters);
                }
            }
            
            _ = async {
                if let Some(timer) = &mut flush_timer {
                    timer.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                // Timer tick (only Async mode)
                if !batch.is_empty() {
                    flush_batch(&mut conn, &mut batch, &mut waiters);
                }
            }
        }
    }
}

fn flush_batch(
    conn: &mut Connection, 
    batch: &mut Vec<super::types::StorageOp>, 
    waiters: &mut Vec<tokio::sync::oneshot::Sender<Result<(), String>>>
) {
    let tx = match conn.transaction() {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to start transaction: {}", e);
            notify_all(waiters, Err(e.to_string()));
            return;
        }
    };

    for op in batch.iter() {
        if let Err(e) = exec_op(&tx, op) {
            error!("Failed to exec op {:?}: {}", op, e);
            // In production you might want to abort or ignore the single op
        }
    }

    match tx.commit() {
        Ok(_) => notify_all(waiters, Ok(())),
        Err(e) => {
            error!("Failed to commit batch: {}", e);
            notify_all(waiters, Err(e.to_string()));
        }
    }
    
    batch.clear();
}

fn notify_all(
    waiters: &mut Vec<tokio::sync::oneshot::Sender<Result<(), String>>>, 
    result: Result<(), String>
) {
    for ch in waiters.drain(..) {
        let _ = ch.send(result.clone());
    }
}

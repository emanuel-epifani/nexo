use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use rusqlite::Connection;
use tracing::{error, info};

use super::types::StorageOp;
use super::sqlite::{init_db, exec_op};

pub async fn run_writer(
    mut rx: mpsc::Receiver<StorageOp>,
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

    info!("Queue Persistence Writer started for {:?}", db_path);

    let mut flush_timer = tokio::time::interval(Duration::from_millis(flush_ms));
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut batch = Vec::with_capacity(batch_size);

    loop {
        tokio::select! {
            Some(op) = rx.recv() => {
                batch.push(op);

                // If buffer too full, flush immediately
                if batch.len() >= batch_size {
                    flush_batch(&mut conn, &mut batch);
                }
            }
            
            _ = flush_timer.tick() => {
                // Timer tick
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
            // In production you might want to abort or ignore the single op
        }
    }

    if let Err(e) = tx.commit() {
        error!("Failed to commit batch: {}", e);
    }
    
    batch.clear();
}


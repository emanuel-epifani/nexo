use std::fs::{self, File, OpenOptions};
use std::io::{Write, BufWriter};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};
use bytes::{Bytes, BufMut};
use crc32fast::Hasher;

use super::types::{WriterCommand, StreamStorageOp, PersistenceMode};
use crate::config::Config;

pub struct StreamWriter {
    topic_name: String,
    mode: PersistenceMode,
    base_path: PathBuf,
    partitions_count: u32,
    
    // Eager handles
    partition_files: Vec<Option<BufWriter<File>>>,
    commit_file: Option<BufWriter<File>>,

    // Async Batching
    batch: Vec<WriterCommand>,
    rx: mpsc::Receiver<WriterCommand>,
}

impl StreamWriter {
    pub fn new(
        topic_name: String,
        partitions_count: u32,
        mode: PersistenceMode,
        rx: mpsc::Receiver<WriterCommand>,
    ) -> Self {
        let base_path = PathBuf::from(&Config::global().stream.persistence_path).join(&topic_name);
        
        Self {
            topic_name,
            mode,
            base_path,
            partitions_count,
            partition_files: Vec::new(),
            commit_file: None,
            batch: Vec::new(),
            rx,
        }
    }

    pub async fn run(mut self) {
        if let PersistenceMode::Memory = self.mode {
            // In Memory mode, we just drain the channel to avoid blocking actors
            while let Some(cmd) = self.rx.recv().await {
                if let Some(reply) = cmd.reply {
                    let _ = reply.send(Ok(()));
                }
            }
            return;
        }

        // 1. Init Files (Eager)
        if let Err(e) = self.init_files() {
            error!("FATAL: Failed to init persistence for topic '{}': {}", self.topic_name, e);
            // Drain channel with errors
            while let Some(cmd) = self.rx.recv().await {
                if let Some(reply) = cmd.reply {
                    let _ = reply.send(Err(format!("Persistence Init Failed: {}", e)));
                }
            }
            return;
        }

        info!("Stream Persistence Writer started for '{}' ({:?})", self.topic_name, self.mode);

        let flush_interval = match self.mode {
            PersistenceMode::Async { flush_ms } => Duration::from_millis(flush_ms),
            PersistenceMode::Sync | PersistenceMode::Memory => Duration::from_secs(3600),
        };

        let mut timer = tokio::time::interval(flush_interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(cmd) = self.rx.recv() => {
                    let is_sync = match self.mode {
                        PersistenceMode::Sync => true,
                        _ => cmd.reply.is_some() // Force sync if reply channel exists? Or just append to batch? 
                        // Actually, if we use Sync mode, we flush immediately.
                        // If Async, we batch. BUT if the command *has* a reply channel, 
                        // the caller is waiting. We should probably flush immediately or 
                        // at least flush this batch. 
                        // For simplicity in Async: we add to batch. The caller waits until flush.
                    };

                    self.batch.push(cmd);

                    let should_flush = match self.mode {
                        PersistenceMode::Sync => true,
                        PersistenceMode::Async { .. } => self.batch.len() >= 1000, // Cap batch size
                        PersistenceMode::Memory => false,
                    };

                    if should_flush {
                        self.flush_batch();
                    }
                }
                
                _ = timer.tick() => {
                    if !self.batch.is_empty() {
                        self.flush_batch();
                    }
                }
            }
        }
    }

    fn init_files(&mut self) -> std::io::Result<()> {
        fs::create_dir_all(&self.base_path)?;

        // Open Partition Logs
        self.partition_files.resize_with(self.partitions_count as usize, || None);
        
        for i in 0..self.partitions_count {
            let path = self.base_path.join(format!("{}.log", i));
            let file = OpenOptions::new().create(true).append(true).open(path)?;
            self.partition_files[i as usize] = Some(BufWriter::new(file));
        }

        // Open Commit Log
        let commit_path = self.base_path.join("commits.log");
        let commit_file = OpenOptions::new().create(true).append(true).open(commit_path)?;
        self.commit_file = Some(BufWriter::new(commit_file));

        Ok(())
    }

    fn flush_batch(&mut self) {
        if self.batch.is_empty() {
            return;
        }

        // Move batch out to satisfy borrow checker
        let current_batch: Vec<WriterCommand> = self.batch.drain(..).collect();

        let mut failed = false;
        let mut error_msg = String::new();

        // 1. Write everything to buffers
        for cmd in &current_batch {
            if let Err(e) = self.write_op(&cmd.op) {
                error!("Failed to write op: {}", e);
                failed = true;
                error_msg = e.to_string();
            }
        }

        // 2. Flush to disk
        // Flush partitions
        for writer in &mut self.partition_files {
            if let Some(w) = writer {
                if let Err(e) = w.flush() {
                    error!("Failed to flush partition file: {}", e);
                    failed = true;
                }
            }
        }
        // Flush commits
        if let Some(w) = &mut self.commit_file {
            if let Err(e) = w.flush() {
                error!("Failed to flush commit file: {}", e);
                failed = true;
            }
        }

        // 3. Reply to waiters
        // Use into_iter to consume the local batch
        for cmd in current_batch {
            if let Some(reply) = cmd.reply {
                let res = if failed {
                    Err(format!("IO Error: {}", error_msg))
                } else {
                    Ok(())
                };
                let _ = reply.send(res);
            }
        }
    }

    fn write_op(&mut self, op: &StreamStorageOp) -> std::io::Result<()> {
        match op {
            StreamStorageOp::Append { partition, offset, timestamp, payload } => {
                if let Some(Some(writer)) = self.partition_files.get_mut(*partition as usize) {
                    // [Len: u32][CRC32: u32][Offset: u64][Timestamp: u64][Payload: Bytes]
                    let len = 8 + 8 + payload.len() as u32; // Offset + Ts + Payload
                    
                    let mut hasher = Hasher::new();
                    hasher.update(&offset.to_be_bytes());
                    hasher.update(&timestamp.to_be_bytes());
                    hasher.update(payload);
                    let crc = hasher.finalize();

                    writer.write_all(&len.to_be_bytes())?;
                    writer.write_all(&crc.to_be_bytes())?;
                    writer.write_all(&offset.to_be_bytes())?;
                    writer.write_all(&timestamp.to_be_bytes())?;
                    writer.write_all(payload)?;
                }
            }
            StreamStorageOp::Commit { group, partition, offset, generation_id } => {
                if let Some(writer) = &mut self.commit_file {
                    // [Len: u32][CRC32: u32][GenID: u64][Partition: u32][Offset: u64][GroupLen: u16][GroupBytes]
                    let group_bytes = group.as_bytes();
                    let group_len = group_bytes.len() as u16;
                    
                    let len = 8 + 4 + 8 + 2 + group_len as u32; // Gen + Part + Off + GLen + GBytes
                    
                    let mut hasher = Hasher::new();
                    hasher.update(&generation_id.to_be_bytes());
                    hasher.update(&partition.to_be_bytes());
                    hasher.update(&offset.to_be_bytes());
                    hasher.update(&group_len.to_be_bytes());
                    hasher.update(group_bytes);
                    let crc = hasher.finalize();

                    writer.write_all(&len.to_be_bytes())?;
                    writer.write_all(&crc.to_be_bytes())?;
                    writer.write_all(&generation_id.to_be_bytes())?;
                    writer.write_all(&partition.to_be_bytes())?;
                    writer.write_all(&offset.to_be_bytes())?;
                    writer.write_all(&group_len.to_be_bytes())?;
                    writer.write_all(group_bytes)?;
                }
            }
        }
        Ok(())
    }
}

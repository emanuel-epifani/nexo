use std::fs::{self, File, OpenOptions};
use std::io::{Write, BufWriter, Read, BufReader};
use std::path::PathBuf;
use std::time::Duration;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use bytes::{Bytes, BufMut};
use crc32fast::Hasher;

use super::types::{WriterCommand, StreamStorageOp, PersistenceMode};
use crate::brokers::stream::message::Message;
use crate::config::Config;

// --- RECOVERY STRUCTS ---

#[derive(Default)]
pub struct RecoveredState {
    // Partition ID -> Messages
    pub partitions_data: HashMap<u32, VecDeque<Message>>,
    // Group ID -> (GenerationID, Committed Offsets)
    pub groups_data: HashMap<String, (u64, HashMap<u32, u64>)>,
}

// --- PUBLIC API ---

pub fn recover_topic(topic_name: &str, partitions_count: u32, base_path: PathBuf) -> RecoveredState {
    let base_path = base_path.join(topic_name);
    let mut state = RecoveredState::default();

    if !base_path.exists() {
        return state;
    }

    // 1. Load Partitions (0.log, 1.log, ...)
    for i in 0..partitions_count {
        let path = base_path.join(format!("{}.log", i));
        if path.exists() {
            let msgs = load_partition_file(&path);
            info!("Topic '{}' P{}: recovered {} messages", topic_name, i, msgs.len());
            state.partitions_data.insert(i, msgs);
        }
    }

    // 2. Load Commits (commits.log)
    let commits_path = base_path.join("commits.log");
    if commits_path.exists() {
        match load_commits_file(&commits_path) {
            Ok(groups) => {
                info!("Topic '{}': recovered {} consumer groups", topic_name, groups.len());
                state.groups_data = groups;
            }
            Err(e) => {
                error!("Topic '{}': failed to load commits: {}", topic_name, e);
            }
        }
    }

    state
}

// --- STREAM WRITER ---

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

    // Compaction
    ops_since_last_compaction: u64,
    compaction_threshold: u64,
}

impl StreamWriter {
    pub fn new(
        topic_name: String,
        partitions_count: u32,
        mode: PersistenceMode,
        rx: mpsc::Receiver<WriterCommand>,
        base_path: PathBuf,
        compaction_threshold: u64,
    ) -> Self {
        let base_path = base_path.join(&topic_name);
        
        Self {
            topic_name,
            mode,
            base_path,
            partitions_count,
            partition_files: Vec::new(),
            commit_file: None,
            batch: Vec::new(),
            rx,
            ops_since_last_compaction: 0,
            compaction_threshold,
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

            // Sync Check for Compaction
            if self.ops_since_last_compaction >= self.compaction_threshold {
                self.ops_since_last_compaction = 0;
                if let Err(e) = self.compact_commits_log() {
                    error!("Compaction failed for topic '{}': {}", self.topic_name, e);
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
        self.reopen_commit_file()?;

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

            // Track commits for compaction
            if let StreamStorageOp::Commit { .. } = cmd.op {
                self.ops_since_last_compaction += 1;
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
                    write_commit_entry(writer, group, *partition, *offset, *generation_id)?;
                }
            }
        }
        Ok(())
    }

    fn compact_commits_log(&mut self) -> std::io::Result<()> {
        info!("Compacting commits.log for topic '{}'...", self.topic_name);
        let start = std::time::Instant::now();
        let path = self.base_path.join("commits.log");

        // 1. Ensure flush and release lock
        if let Some(mut w) = self.commit_file.take() {
             let _ = w.flush();
        }
        // self.commit_file is now None.

        // 2. Read current state (Last write wins logic is in loader)
        let groups_map = match load_commits_file(&path) {
            Ok(map) => map,
            Err(e) => {
                warn!("Failed to read commits log during compaction: {}. Aborting compaction.", e);
                self.reopen_commit_file()?;
                return Err(e);
            }
        };

        // 3. Write to temporary file
        let tmp_path = self.base_path.join("commits.log.tmp");
        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);

            for (group_id, (gen_id, offsets)) in groups_map {
                for (partition, offset) in offsets {
                     write_commit_entry(&mut writer, &group_id, partition, offset, gen_id)?;
                }
            }
            writer.flush()?;
        }

        // 4. Atomic Swap
        fs::rename(&tmp_path, &path)?;

        // 5. Reopen
        self.reopen_commit_file()?;

        info!("Compaction completed for '{}' in {:?}", self.topic_name, start.elapsed());
        Ok(())
    }

    fn reopen_commit_file(&mut self) -> std::io::Result<()> {
        let commit_path = self.base_path.join("commits.log");
        let commit_file = OpenOptions::new().create(true).append(true).open(commit_path)?;
        self.commit_file = Some(BufWriter::new(commit_file));
        Ok(())
    }
}

// --- FILE IO HELPERS (Private) ---

fn write_commit_entry<W: Write>(
    writer: &mut W,
    group: &str,
    partition: u32,
    offset: u64,
    generation_id: u64
) -> std::io::Result<()> {
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

    Ok(())
}

fn load_partition_file(path: &PathBuf) -> VecDeque<Message> {
    let mut msgs = VecDeque::new();
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open {:?}: {}", path, e);
            return msgs;
        }
    };

    let mut reader = BufReader::new(file);
    
    loop {
        // [Len: u32]
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).is_err() {
            break; // EOF
        }
        let len = u32::from_be_bytes(len_buf);

        // [CRC32: u32]
        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).is_err() {
            warn!("Unexpected EOF reading CRC at {:?}", path);
            break; 
        }
        let stored_crc = u32::from_be_bytes(crc_buf);

        // Read Content (Offset + Timestamp + Payload)
        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).is_err() {
             warn!("Unexpected EOF reading Content at {:?}", path);
             break;
        }

        // Verify CRC
        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        let calc_crc = hasher.finalize();

        if calc_crc != stored_crc {
            error!("CRC Mismatch in {:?}! Data corrupt. Stopping read.", path);
            break;
        }

        // Parse Content
        // [Offset: u64][Timestamp: u64][Payload: Bytes]
        if content_buf.len() < 16 {
            error!("Content too short in {:?}", path);
            break;
        }

        let (offset_bytes, rest) = content_buf.split_at(8);
        let (ts_bytes, payload_bytes) = rest.split_at(8);

        let offset = u64::from_be_bytes(offset_bytes.try_into().unwrap());
        let timestamp = u64::from_be_bytes(ts_bytes.try_into().unwrap());
        let payload = Bytes::copy_from_slice(payload_bytes);

        msgs.push_back(Message {
            offset,
            timestamp,
            payload,
        });
    }

    msgs
}

fn load_commits_file(path: &PathBuf) -> Result<HashMap<String, (u64, HashMap<u32, u64>)>, std::io::Error> {
    let mut groups: HashMap<String, (u64, HashMap<u32, u64>)> = HashMap::new();
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    loop {
        // [Len: u32]
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf);

        // [CRC32: u32]
        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).is_err() {
             break;
        }
        let stored_crc = u32::from_be_bytes(crc_buf);

        // Read Content
        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).is_err() {
             break;
        }

        // Verify CRC
        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        if hasher.finalize() != stored_crc {
            error!("CRC Mismatch in commits.log. Stopping.");
            break;
        }

        // Parse
        // [GenID: u64][Partition: u32][Offset: u64][GroupLen: u16][GroupBytes]
        let mut cursor = std::io::Cursor::new(content_buf);
        
        let mut buf8 = [0u8; 8];
        let mut buf4 = [0u8; 4];
        let mut buf2 = [0u8; 2];

        if cursor.read_exact(&mut buf8).is_err() { continue; }
        let gen_id = u64::from_be_bytes(buf8);
        
        if cursor.read_exact(&mut buf4).is_err() { continue; }
        let partition = u32::from_be_bytes(buf4);

        if cursor.read_exact(&mut buf8).is_err() { continue; }
        let offset = u64::from_be_bytes(buf8);

        if cursor.read_exact(&mut buf2).is_err() { continue; }
        let group_len = u16::from_be_bytes(buf2);

        let mut group_bytes = vec![0u8; group_len as usize];
        if cursor.read_exact(&mut group_bytes).is_err() { continue; }
        let group_id = String::from_utf8_lossy(&group_bytes).to_string();

        // Update Memory State (Last write wins)
        let entry = groups.entry(group_id).or_insert((0, HashMap::new()));
        
        // Update Generation ID (max wins)
        if gen_id > entry.0 {
            entry.0 = gen_id;
        }
        
        // Update Offset
        entry.1.insert(partition, offset);
    }

    Ok(groups)
}

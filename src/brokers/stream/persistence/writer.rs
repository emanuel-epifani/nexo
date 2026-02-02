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
use crate::brokers::stream::commands::RetentionOptions;
use crate::brokers::stream::message::Message;
use crate::config::Config;

// --- RECOVERY STRUCTS ---

#[derive(Default)]
pub struct RecoveredState {
    // Partition ID -> (Active Messages, Segments Index)
    pub partitions_data: HashMap<u32, (VecDeque<Message>, Vec<Segment>)>,
    // Group ID -> (GenerationID, Committed Offsets)
    pub groups_data: HashMap<String, (u64, HashMap<u32, u64>)>,
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub path: PathBuf,
    pub start_offset: u64,
}

// --- PUBLIC API ---

pub fn read_log_segment(path: &PathBuf, start_offset: u64, limit: usize) -> Vec<Message> {
    let mut msgs = Vec::new();
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            error!("Cold Read: Failed to open {:?}: {}", path, e);
            return msgs;
        }
    };

    let mut reader = BufReader::new(file);
    let mut count = 0;

    // TODO Optimization: Binary Search or Index file to skip directly to offset.
    // Current implementation: Scan linearly until we find >= offset.
    
    loop {
        // [Len: u32]
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).is_err() { break; }
        let len = u32::from_be_bytes(len_buf);

        // [CRC32: u32]
        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).is_err() { break; }
        let stored_crc = u32::from_be_bytes(crc_buf);

        // Content
        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).is_err() { break; }

        // Skip CRC check on read for speed? Or check? Let's check for safety.
        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        if hasher.finalize() != stored_crc {
            error!("CRC Mismatch in Cold Read {:?}. Skipping.", path);
            continue;
        }

        // Parse Offset [Offset: u64]...
        if content_buf.len() < 8 { continue; }
        let offset = u64::from_be_bytes(content_buf[0..8].try_into().unwrap());

        if offset >= start_offset {
            // Found it! Parse full message
            if content_buf.len() < 16 { continue; }
            let timestamp = u64::from_be_bytes(content_buf[8..16].try_into().unwrap());
            let payload = Bytes::copy_from_slice(&content_buf[16..]);

            msgs.push(Message { offset, timestamp, payload });
            count += 1;
            
            if count >= limit {
                break;
            }
        }
    }

    msgs
}

pub fn recover_topic(topic_name: &str, partitions_count: u32, base_path: PathBuf) -> RecoveredState {
    let base_path = base_path.join(topic_name);
    let mut state = RecoveredState::default();

    if !base_path.exists() {
        return state;
    }

    // 1. Load Partitions (Segments)
    for i in 0..partitions_count {
        if let Ok(segments) = find_segments(&base_path, i) {
            let mut active_msgs = VecDeque::new();
            
            // LAZY LOADING: Carica solo l'ultimo segmento
            if let Some(last_segment) = segments.last() {
                active_msgs = load_partition_file(&last_segment.path);
                info!("Topic '{}' P{} Segment {}: LAZY LOADED {} messages (Active)", topic_name, i, last_segment.start_offset, active_msgs.len());
            } else {
                info!("Topic '{}' P{}: No segments found.", topic_name, i);
            }

            state.partitions_data.insert(i, (active_msgs, segments));
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

// --- HELPERS ---

fn find_segments(base_path: &PathBuf, partition: u32) -> std::io::Result<Vec<Segment>> {
    let mut segments = Vec::new();
    let prefix = format!("{}_", partition);
    let old_style = format!("{}.log", partition);

    for entry in std::fs::read_dir(base_path)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() { continue; }
        
        let fname = entry.file_name().to_string_lossy().to_string();
        
        if fname == old_style {
            segments.push(Segment { path, start_offset: 0 });
        } else if fname.starts_with(&prefix) && fname.ends_with(".log") {
            // Parse offset: "0_100.log" -> 100
            let part_offset = &fname[..fname.len()-4]; // remove .log
            if let Some(offset_str) = part_offset.split('_').nth(1) {
                if let Ok(offset) = offset_str.parse::<u64>() {
                    segments.push(Segment { path, start_offset: offset });
                }
            }
        }
    }
    
    segments.sort_by_key(|s| s.start_offset);
    Ok(segments)
}

fn find_active_segment(base_path: &PathBuf, partition: u32) -> std::io::Result<Segment> {
    let segments = find_segments(base_path, partition)?;
    if let Some(last) = segments.into_iter().last() {
        Ok(last)
    } else {
        // Nessun segmento, creiamo il primo: 0_0.log
        let path = base_path.join(format!("{}_0.log", partition));
        Ok(Segment { path, start_offset: 0 })
    }
}


// --- STREAM WRITER ---

pub struct StreamWriter {
    topic_name: String,
    mode: PersistenceMode,
    base_path: PathBuf,
    partitions_count: u32,
    
    // Eager handles
    // Mappa Partizione -> (File Handle, Bytes Scritti Attuali)
    writers: HashMap<u32, (BufWriter<File>, u64)>, 
    commit_file: Option<BufWriter<File>>,

    // Async Batching
    batch: Vec<WriterCommand>,
    rx: mpsc::Receiver<WriterCommand>,

    // Compaction & Segmentation & Retention
    ops_since_last_compaction: u64,
    compaction_threshold: u64,
    max_segment_size: u64,
    retention: RetentionOptions,
    retention_check_interval_ms: u64,
    batch_size: usize,
    
    ack_tx: mpsc::Sender<super::super::stream_manager::TopicCommand>,
}

impl StreamWriter {
    pub fn new(
        topic_name: String,
        partitions_count: u32,
        mode: PersistenceMode,
        rx: mpsc::Receiver<WriterCommand>,
        base_path: PathBuf,
        compaction_threshold: u64,
        max_segment_size: u64,
        retention: RetentionOptions,
        retention_check_interval_ms: u64,
        batch_size: usize,
        ack_tx: mpsc::Sender<super::super::stream_manager::TopicCommand>,
    ) -> Self {
        let base_path = base_path.join(&topic_name);
        
        Self {
            topic_name,
            mode,
            base_path,
            partitions_count,
            writers: HashMap::new(),
            commit_file: None,
            batch: Vec::new(),
            rx,
            ops_since_last_compaction: 0,
            compaction_threshold,
            max_segment_size,
            retention,
            retention_check_interval_ms,
            batch_size,
            ack_tx,
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

        let mut retention_timer = tokio::time::interval(Duration::from_millis(self.retention_check_interval_ms));
        retention_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(cmd) = self.rx.recv() => {
                    self.batch.push(cmd);

                    let should_flush = match self.mode {
                        PersistenceMode::Sync => true,
                        PersistenceMode::Async { .. } => self.batch.len() >= self.batch_size, // Cap batch size
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

                _ = retention_timer.tick() => {
                    if let Err(e) = self.check_retention() {
                        error!("Retention check failed for topic '{}': {}", self.topic_name, e);
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

    fn check_retention(&mut self) -> std::io::Result<()> {
        // Se non c'è nessuna policy configurata, usciamo subito
        if self.retention.max_age_ms.is_none() && self.retention.max_bytes.is_none() {
            return Ok(());
        }

        info!("Starting retention check for topic '{}'", self.topic_name);

        for p in 0..self.partitions_count {
            let mut segments = match find_segments(&self.base_path, p) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Retention: Failed to list segments for P{}: {}", p, e);
                    continue;
                }
            };

            // ESCLUDI IL SEGMENTO ATTIVO (l'ultimo)
            // Non cancelliamo mai il segmento su cui stiamo scrivendo.
            if segments.len() <= 1 {
                continue; 
            }
            let _active_segment = segments.pop(); 

            // 1. TIME RETENTION
            if let Some(max_age) = self.retention.max_age_ms {
                let limit = std::time::SystemTime::now() - Duration::from_millis(max_age);
                
                // filter_map o retain non permettono facilmente IO fallibile con early exit, usiamo loop manuale
                let mut survivors = Vec::new();
                for seg in segments {
                    let mut deleted = false;
                    if let Ok(metadata) = fs::metadata(&seg.path) {
                        if let Ok(modified) = metadata.modified() {
                            if modified < limit {
                                info!("Retention (Time): Deleting segment {:?}", seg.path);
                                if let Err(e) = fs::remove_file(&seg.path) {
                                    error!("Failed to delete {:?}: {}", seg.path, e);
                                    // If delete fails, we treat it as not deleted, so it stays in survivors
                                } else {
                                    deleted = true;
                                }
                            }
                        }
                    }
                    if !deleted {
                        survivors.push(seg);
                    }
                }
                segments = survivors;
            }

            // 2. SIZE RETENTION
            if let Some(max_bytes) = self.retention.max_bytes {
                // Calcola dimensione totale ATTUALE (incluso il segmento attivo che abbiamo poppato)
                let active_size = self.writers.get(&p).map(|(_, size)| *size).unwrap_or(0);
                
                let mut current_total_size: u64 = active_size;
                // Aggiungi size dei segmenti chiusi
                for seg in &segments {
                    if let Ok(meta) = fs::metadata(&seg.path) {
                        current_total_size += meta.len();
                    }
                }

                // Cancella i più vecchi (primi della lista) finché Total > Max
                let mut i = 0;
                while current_total_size > max_bytes && i < segments.len() {
                    let seg = &segments[i];
                    if let Ok(meta) = fs::metadata(&seg.path) {
                        let size = meta.len();
                        info!("Retention (Size): Deleting {:?} (Total {} > Max {})", seg.path, current_total_size, max_bytes);
                        
                        if let Err(e) = fs::remove_file(&seg.path) {
                             error!("Failed to delete {:?}: {}", seg.path, e);
                        } else {
                             current_total_size = current_total_size.saturating_sub(size);
                        }
                    }
                    i += 1;
                }
            }
        }
        Ok(())
    }

    fn init_files(&mut self) -> std::io::Result<()> {
        fs::create_dir_all(&self.base_path)?;

        // Open Partition Logs
        // Per ogni partizione, cerchiamo l'ultimo segmento attivo
        for i in 0..self.partitions_count {
            // Cerchiamo file del tipo: {i}_{offset}.log
            // Se non ne troviamo, creiamo {i}_0.log
            let active_segment = find_active_segment(&self.base_path, i)?;
            let path = active_segment.path.clone();
            
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            let size = file.metadata()?.len();
            
            self.writers.insert(i, (BufWriter::new(file), size));
            
            // Notifica TopicActor del segmento iniziale
            let _ = self.ack_tx.try_send(super::super::stream_manager::TopicCommand::SegmentRotated {
                partition: i,
                segment: active_segment,
            });
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
        let mut last_offsets: HashMap<u32, u64> = HashMap::new();

        // 1. Write everything to buffers
        for cmd in &current_batch {
            if let Err(e) = self.write_op(&cmd.op) {
                error!("Failed to write op: {}", e);
                failed = true;
                error_msg = e.to_string();
            }

            // Track last offset per partition for ACK
            if let StreamStorageOp::Append { partition, offset, .. } = cmd.op {
                last_offsets.entry(partition)
                    .and_modify(|o| *o = (*o).max(offset))
                    .or_insert(offset);
            }

            // Track commits for compaction
            if let StreamStorageOp::Commit { .. } = cmd.op {
                self.ops_since_last_compaction += 1;
            }
        }

        // 2. Flush to disk
        // Flush partitions
        for (_, (w, _)) in self.writers.iter_mut() {
            if let Err(e) = w.flush() {
                error!("Failed to flush partition file: {}", e);
                failed = true;
            }
            // Explicit SYNC for Sync Mode
            if let PersistenceMode::Sync = self.mode {
                if let Err(e) = w.get_ref().sync_all() {
                    error!("Failed to sync_all partition file: {}", e);
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
            // Explicit SYNC for Sync Mode
            if let PersistenceMode::Sync = self.mode {
                if let Err(e) = w.get_ref().sync_all() {
                    error!("Failed to sync_all commit file: {}", e);
                    failed = true;
                }
            }
        }

        // 3. Send ACK to TopicActor (non-blocking)
        if !failed {
            for (partition, last_offset) in last_offsets {
                let _ = self.ack_tx.try_send(super::super::stream_manager::TopicCommand::PersistenceAck {
                    partition,
                    last_persisted_offset: last_offset,
                });
            }
        }

        // 4. Reply to waiters
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
                if let Some((writer, current_size)) = self.writers.get_mut(partition) {
                    // [Len: u32][CRC32: u32][Offset: u64][Timestamp: u64][Payload: Bytes]
                    let len = 8 + 8 + payload.len() as u32; // Offset + Ts + Payload
                    let entry_size = 4 + 4 + len as u64; // Len + CRC + Content

                    // Check Segmentation
                    if *current_size + entry_size > self.max_segment_size {
                        writer.flush()?;
                        
                        // Ruota: Nuovo file {partition}_{offset}.log
                        let new_path = self.base_path.join(format!("{}_{}.log", partition, offset));
                        let new_file = File::create(&new_path)?;
                        *writer = BufWriter::new(new_file);
                        *current_size = 0;
                        info!("Rotated log for partition {} to offset {}", partition, offset);
                        
                        // Notifica TopicActor del nuovo segmento
                        let segment = Segment {
                            path: new_path.clone(),
                            start_offset: *offset,
                        };
                        let _ = self.ack_tx.try_send(super::super::stream_manager::TopicCommand::SegmentRotated {
                            partition: *partition,
                            segment,
                        });
                    }

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
                    
                    *current_size += entry_size;
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

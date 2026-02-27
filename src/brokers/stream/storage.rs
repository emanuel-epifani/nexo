//! Storage Manager: Global actor handling all file I/O for stream topics.
//! 
//! Responsibilities:
//! - Offloads disk I/O from individual topic actors.
//! - Batches writes automatically via `BufWriter` for high throughput.
//! - Manages an LRU Cache of file descriptors to prevent OS limits exhaustion.
//! - Executes a global periodic flush to sync bytes to disk and notify topic actors.

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lru::LruCache;
use bytes::Bytes;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter, AsyncReadExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, debug, warn};
use crc32fast::Hasher;

use crate::brokers::stream::commands::RetentionOptions;
use crate::brokers::stream::message::Message;

// ==========================================
// DATA STRUCTURES
// ==========================================

#[derive(Default)]
pub struct RecoveredState {
    /// Active messages from the last segment
    pub messages: VecDeque<Message>,
    /// All segment paths in order
    pub segments: Vec<Segment>,
    /// Group ID -> ack_floor
    pub groups_data: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub path: PathBuf,
    pub start_seq: u64,
}

#[derive(Debug)]
pub struct MessageToAppend {
    pub seq: u64,
    pub timestamp: u64,
    pub payload: Bytes,
}

// ==========================================
// COMMANDS
// ==========================================

pub enum StorageCommand {
    /// Append messages to a topic's active log file.
    Append {
        topic_name: String,
        messages: Vec<MessageToAppend>,
        ack_sender: mpsc::Sender<u64>,
    },
    
    ColdRead {
        topic_name: String,
        from_seq: u64,
        limit: usize,
        reply: oneshot::Sender<Vec<Message>>,
    },

    SaveGroups {
        topic_name: String,
        groups_data: HashMap<String, u64>,
    },

    ApplyRetention {
        topic_name: String,
        retention: RetentionOptions,
        max_segment_size: u64,
    },

    DropTopic {
        topic_name: String,
        reply: oneshot::Sender<()>,
    }
}

pub struct TopicContext {
    active_path: PathBuf,
    ack_sender: Option<mpsc::Sender<u64>>,
    highest_pending_seq: u64,
    current_file_size: u64,
}

// ==========================================
// STORAGE MANAGER ACTOR
// ==========================================

pub struct StorageManager {
    base_path: PathBuf,
    rx: mpsc::Receiver<StorageCommand>,
    open_files: LruCache<PathBuf, BufWriter<File>>,
    topics: HashMap<String, TopicContext>,
    flush_interval: Duration,
    max_segment_size: u64,
    dirty_topics: HashSet<String>,
}

impl StorageManager {
    pub fn new(
        base_path: String,
        rx: mpsc::Receiver<StorageCommand>,
        max_open_files: usize,
        flush_interval_ms: u64,
        max_segment_size: u64,
    ) -> Self {
        Self {
            base_path: PathBuf::from(base_path),
            rx,
            open_files: LruCache::new(NonZeroUsize::new(max_open_files).unwrap()),
            topics: HashMap::new(),
            flush_interval: Duration::from_millis(flush_interval_ms),
            max_segment_size,
            dirty_topics: HashSet::new(),
        }
    }

    pub async fn run(mut self) {
        info!("StorageManager started");
        let mut flush_timer = tokio::time::interval(self.flush_interval);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                cmd_res = self.rx.recv() => {
                    match cmd_res {
                        Some(cmd) => {
                            self.handle_command(cmd).await;
                            while let Ok(next) = self.rx.try_recv() {
                                self.handle_command(next).await;
                            }
                        }
                        None => break,
                    }
                }
                _ = flush_timer.tick() => {
                    self.flush_all().await;
                }
            }
        }
        
        self.flush_all().await;
        info!("StorageManager stopped");
    }

    async fn handle_command(&mut self, cmd: StorageCommand) {
        match cmd {
            StorageCommand::Append { topic_name, messages, ack_sender } => {
                self.handle_append(topic_name, messages, ack_sender).await;
            }
            StorageCommand::ColdRead { topic_name, from_seq, limit, reply } => {
                let msgs = self.cold_read(&topic_name, from_seq, limit).await;
                let _ = reply.send(msgs);
            }
            StorageCommand::SaveGroups { topic_name, groups_data } => {
                let base_path = self.base_path.join(&topic_name);
                if let Err(e) = save_groups_file(&base_path, &groups_data).await {
                    error!("Failed to save groups for {}: {}", topic_name, e);
                }
            }
            StorageCommand::ApplyRetention { topic_name, retention, max_segment_size: _ } => {
                let base_path = self.base_path.join(&topic_name);
                self.apply_retention(&topic_name, &base_path, &retention).await;
            }
            StorageCommand::DropTopic { topic_name, reply } => {
                if let Some(ctx) = self.topics.remove(&topic_name) {
                    self.open_files.pop(&ctx.active_path);
                }
                let topic_path = self.base_path.join(&topic_name);
                if topic_path.exists() {
                    let _ = std::fs::remove_dir_all(&topic_path);
                }
                let _ = reply.send(());
            }
        }
    }

    async fn handle_append(
        &mut self,
        topic_name: String,
        messages: Vec<MessageToAppend>,
        ack_sender: mpsc::Sender<u64>,
    ) {
        if messages.is_empty() { return; }

        let highest_seq = messages.last().unwrap().seq;
        let base_topic_path = self.base_path.join(&topic_name);

        if !self.topics.contains_key(&topic_name) {
            if !base_topic_path.exists() {
                if let Err(e) = tokio::fs::create_dir_all(&base_topic_path).await {
                    error!("FATAL: Failed to create topic dir {:?}: {}", base_topic_path, e);
                    return;
                }
            }
            
            let segments = find_segments(&base_topic_path).await.unwrap_or_default();
            let (active_path, file_size) = if let Some(last) = segments.last() {
                let size = tokio::fs::metadata(&last.path).await.map(|m| m.len()).unwrap_or(0);
                (last.path.clone(), size)
            } else {
                (base_topic_path.join("1.log"), 0)
            };

            self.topics.insert(topic_name.clone(), TopicContext {
                active_path,
                ack_sender: Some(ack_sender.clone()),
                highest_pending_seq: 0,
                current_file_size: file_size,
            });
        } else {
            self.topics.get_mut(&topic_name).unwrap().ack_sender = Some(ack_sender);
        }

        let mut buffer = Vec::new();
        for msg in &messages {
            serialize_message(&mut buffer, msg.seq, msg.timestamp, &msg.payload);
        }
        let bytes_len = buffer.len() as u64;

        let path = {
            let ctx = self.topics.get_mut(&topic_name).unwrap();
            if ctx.current_file_size + bytes_len > self.max_segment_size && ctx.current_file_size > 0 {
                if let Some(mut old_writer) = self.open_files.pop(&ctx.active_path) {
                    let _ = old_writer.flush().await;
                }
                let first_seq = messages.first().unwrap().seq;
                ctx.active_path = base_topic_path.join(format!("{}.log", first_seq));
                ctx.current_file_size = 0;
            }
            ctx.active_path.clone()
        };

        match self.get_or_open_writer(&path).await {
            Ok(writer) => {
                if let Err(e) = writer.write_all(&buffer).await {
                    error!("StorageManager: Failed to write to {:?}: {}", path, e);
                    self.open_files.pop(&path);
                    return;
                }
                let ctx = self.topics.get_mut(&topic_name).unwrap();
                ctx.current_file_size += bytes_len;
                ctx.highest_pending_seq = highest_seq;
                self.dirty_topics.insert(topic_name.clone());
            }
            Err(e) => error!("StorageManager: Failed to open file {:?}: {}", path, e),
        }
    }

    async fn get_or_open_writer(&mut self, path: &PathBuf) -> Result<&mut BufWriter<File>, std::io::Error> {
        if !self.open_files.contains(path) {
            if self.open_files.len() == self.open_files.cap().get() {
                if let Some((_, mut evicted_writer)) = self.open_files.pop_lru() {
                    let _ = evicted_writer.flush().await;
                }
            }
            let file = OpenOptions::new().create(true).append(true).open(path).await?;
            self.open_files.put(path.clone(), BufWriter::new(file));
        }
        Ok(self.open_files.get_mut(path).unwrap())
    }

    async fn flush_all(&mut self) {
        for (_, writer) in self.open_files.iter_mut() {
            let _ = writer.flush().await;
        }

        if !self.dirty_topics.is_empty() {
            let topics_to_flush: Vec<String> = self.dirty_topics.drain().collect();
            for name in topics_to_flush {
                if let Some(ctx) = self.topics.get_mut(&name) {
                    if ctx.highest_pending_seq > 0 {
                        if let Some(ref sender) = ctx.ack_sender {
                            if let Ok(_) = sender.try_send(ctx.highest_pending_seq) {
                                ctx.highest_pending_seq = 0;
                            } else {
                                self.dirty_topics.insert(name);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn cold_read(&self, topic_name: &str, from_seq: u64, limit: usize) -> Vec<Message> {
        let base_path = self.base_path.join(topic_name);
        let segments = find_segments(&base_path).await.unwrap_or_default();
        let mut all_msgs = Vec::new();
        let mut current_from_seq = from_seq;
        let mut remaining_limit = limit;

        if let Some(idx) = segments.iter().rposition(|s| s.start_seq <= from_seq) {
            for segment in segments.iter().skip(idx) {
                if remaining_limit == 0 { break; }
                let msgs = read_log_segment(&segment.path, current_from_seq, remaining_limit).await;
                if !msgs.is_empty() {
                    current_from_seq = msgs.last().unwrap().seq + 1;
                    remaining_limit = remaining_limit.saturating_sub(msgs.len());
                    all_msgs.extend(msgs);
                }
            }
        }
        all_msgs
    }

    async fn apply_retention(&mut self, _topic_name: &str, base_path: &PathBuf, retention: &RetentionOptions) {
        if retention.max_age_ms.is_none() && retention.max_bytes.is_none() { return; }
        let mut segments = find_segments(base_path).await.unwrap_or_default();
        if segments.len() <= 1 { return; }

        if let Some(max_age) = retention.max_age_ms {
            let limit = std::time::SystemTime::now() - Duration::from_millis(max_age);
            let mut survivors = Vec::new();
            for seg in segments {
                let mut deleted = false;
                if let Ok(metadata) = tokio::fs::metadata(&seg.path).await {
                    if let Ok(modified) = metadata.modified() {
                        if modified < limit {
                            self.open_files.pop(&seg.path);
                            let _ = tokio::fs::remove_file(&seg.path).await;
                            deleted = true;
                        }
                    }
                }
                if !deleted { survivors.push(seg); }
            }
            segments = survivors;
        }

        if let Some(max_bytes) = retention.max_bytes {
            let mut current_total: u64 = 0;
            for seg in &segments {
                current_total += tokio::fs::metadata(&seg.path).await.map(|m| m.len()).unwrap_or(0);
            }
            let mut i = 0;
            while current_total > max_bytes && i < segments.len().saturating_sub(1) {
                let seg = &segments[i];
                let size = tokio::fs::metadata(&seg.path).await.map(|m| m.len()).unwrap_or(0);
                self.open_files.pop(&seg.path);
                let _ = tokio::fs::remove_file(&seg.path).await;
                current_total = current_total.saturating_sub(size);
                i += 1;
            }
        }
    }
}

// ==========================================
// HELPERS (Formerly in writer.rs)
// ==========================================

/// Serialize a message into a buffer (does NOT write to disk).
pub fn serialize_message(buf: &mut Vec<u8>, seq: u64, timestamp: u64, payload: &[u8]) {
    let len = 8 + 8 + payload.len() as u32;
    let mut hasher = Hasher::new();
    hasher.update(&seq.to_be_bytes());
    hasher.update(&timestamp.to_be_bytes());
    hasher.update(payload);
    let crc = hasher.finalize();

    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&crc.to_be_bytes());
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&timestamp.to_be_bytes());
    buf.extend_from_slice(payload);
}

/// Read messages from a log segment file starting at a given seq.
pub async fn read_log_segment(path: &PathBuf, start_seq: u64, limit: usize) -> Vec<Message> {
    let mut msgs = Vec::new();
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(_) => return msgs,
    };
    let mut reader = BufReader::new(file);

    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() { break; }
        let len = u32::from_be_bytes(len_buf);

        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).await.is_err() { break; }
        let stored_crc = u32::from_be_bytes(crc_buf);

        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).await.is_err() { break; }

        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        if hasher.finalize() != stored_crc { continue; }

        if content_buf.len() < 16 { continue; }
        let seq = u64::from_be_bytes(content_buf[0..8].try_into().unwrap());
        let timestamp = u64::from_be_bytes(content_buf[8..16].try_into().unwrap());
        let payload = Bytes::copy_from_slice(&content_buf[16..]);

        if seq >= start_seq {
            msgs.push(Message { seq, timestamp, payload });
            if msgs.len() >= limit { break; }
        }
    }
    msgs
}

/// Recover topic state from filesystem.
pub async fn recover_topic(topic_name: &str, base_path: PathBuf) -> RecoveredState {
    let base_path = base_path.join(topic_name);
    let mut state = RecoveredState::default();
    if !base_path.exists() { return state; }

    if let Ok(segments) = find_segments(&base_path).await {
        if let Some(last_segment) = segments.last() {
            state.messages = load_segment_file(&last_segment.path).await;
        }
        state.segments = segments;
    }

    let groups_path = base_path.join("groups.log");
    if groups_path.exists() {
        if let Ok(groups) = load_groups_file(&groups_path).await {
            state.groups_data = groups;
        }
    }
    state
}

/// Find all segment files for a topic, sorted by start_seq.
pub async fn find_segments(base_path: &Path) -> std::io::Result<Vec<Segment>> {
    let mut segments = Vec::new();
    if !base_path.exists() { return Ok(segments); }

    let mut entries = tokio::fs::read_dir(base_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_file() { continue; }
        let fname = entry.file_name().to_string_lossy().to_string();
        if fname.ends_with(".log") && fname != "groups.log" {
            let name_part = &fname[..fname.len() - 4];
            if let Ok(start_seq) = name_part.parse::<u64>() {
                segments.push(Segment { path, start_seq });
            }
        }
    }
    segments.sort_by_key(|s| s.start_seq);
    Ok(segments)
}

/// Write the groups.log file (ack_floor for each group). Atomic write via temp file + rename.
pub async fn save_groups_file(base_path: &Path, groups: &HashMap<String, u64>) -> std::io::Result<()> {
    let tmp_path = base_path.join("groups.log.tmp");
    let final_path = base_path.join("groups.log");

    {
        let file = File::create(&tmp_path).await?;
        let mut writer = BufWriter::new(file);
        for (group_id, ack_floor) in groups {
            write_group_entry(&mut writer, group_id, *ack_floor).await?;
        }
        writer.flush().await?;
    }

    tokio::fs::rename(&tmp_path, &final_path).await?;
    Ok(())
}

async fn write_group_entry<W: tokio::io::AsyncWrite + std::marker::Unpin>(writer: &mut W, group_id: &str, ack_floor: u64) -> std::io::Result<()> {
    let group_bytes = group_id.as_bytes();
    let group_len = group_bytes.len() as u16;
    let len = 8 + 2 + group_len as u32;
    
    let mut hasher = Hasher::new();
    hasher.update(&ack_floor.to_be_bytes());
    hasher.update(&group_len.to_be_bytes());
    hasher.update(group_bytes);
    let crc = hasher.finalize();

    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&crc.to_be_bytes()).await?;
    writer.write_all(&ack_floor.to_be_bytes()).await?;
    writer.write_all(&group_len.to_be_bytes()).await?;
    writer.write_all(group_bytes).await?;
    Ok(())
}

async fn load_segment_file(path: &PathBuf) -> VecDeque<Message> {
    let mut msgs = VecDeque::new();
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(_) => return msgs,
    };

    let mut reader = BufReader::new(file);
    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() { break; }
        let len = u32::from_be_bytes(len_buf);

        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).await.is_err() { break; }
        let stored_crc = u32::from_be_bytes(crc_buf);

        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).await.is_err() { break; }

        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        if hasher.finalize() != stored_crc { break; }

        if content_buf.len() < 16 { break; }
        let seq = u64::from_be_bytes(content_buf[0..8].try_into().unwrap());
        let timestamp = u64::from_be_bytes(content_buf[8..16].try_into().unwrap());
        let payload = Bytes::copy_from_slice(&content_buf[16..]);

        msgs.push_back(Message { seq, timestamp, payload });
    }
    msgs
}

async fn load_groups_file(path: &PathBuf) -> Result<HashMap<String, u64>, std::io::Error> {
    let mut groups = HashMap::new();
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);

    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() { break; }
        let len = u32::from_be_bytes(len_buf);

        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).await.is_err() { break; }
        let stored_crc = u32::from_be_bytes(crc_buf);

        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).await.is_err() { break; }

        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        if hasher.finalize() != stored_crc { break; }

        let mut cursor = std::io::Cursor::new(content_buf);
        use std::io::Read;
        
        let mut buf8 = [0u8; 8];
        let mut buf2 = [0u8; 2];

        if Read::read_exact(&mut cursor, &mut buf8).is_err() { continue; }
        let ack_floor = u64::from_be_bytes(buf8);

        if Read::read_exact(&mut cursor, &mut buf2).is_err() { continue; }
        let group_len = u16::from_be_bytes(buf2);

        let mut group_bytes = vec![0u8; group_len as usize];
        if Read::read_exact(&mut cursor, &mut group_bytes).is_err() { continue; }
        let group_id = String::from_utf8_lossy(&group_bytes).to_string();

        groups.insert(group_id, ack_floor);
    }
    Ok(groups)
}

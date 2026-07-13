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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use lru::LruCache;
use bytes::Bytes;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter, AsyncReadExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};
use crc32fast::Hasher;

use crate::brokers::stream::options::RetentionOptions;
use crate::brokers::stream::domain::message::Message;
use crate::brokers::stream::domain::group::DltEntry;

// ==========================================
// DATA STRUCTURES
// ==========================================

/// Per-group persistent state (ack_floor + DLT entries + parked_keys)
#[derive(Default, Clone)]
pub struct GroupPersistentState {
    pub ack_floor: u64,
    pub dlt_entries: HashMap<u64, DltEntry>,
    pub parked_keys: HashSet<Bytes>,
}

#[derive(Default)]
pub struct RecoveredState {
    /// Active messages from the last segment
    pub messages: VecDeque<Message>,
    /// All segment paths in order
    pub segments: Vec<Segment>,
    /// Group ID -> GroupPersistentState
    pub groups_data: HashMap<String, GroupPersistentState>,
    /// First retained sequence on disk
    pub head_seq: u64,
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
    pub key: Option<Bytes>,
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
        persisted_seq: Arc<AtomicU64>,
    },
    
    ColdRead {
        topic_name: String,
        from_seq: u64,
        limit: usize,
        reply: oneshot::Sender<Vec<Message>>,
    },

    SaveState {
        topic_name: String,
        groups: HashMap<String, GroupPersistentState>,
    },

    ApplyRetention {
        topic_name: String,
        retention: RetentionOptions,
        reply: oneshot::Sender<u64>,
    },

    DropTopic {
        topic_name: String,
        reply: oneshot::Sender<()>,
    }
}

pub struct TopicContext {
    active_path: PathBuf,
    persisted_seq: Arc<AtomicU64>,
    highest_pending_seq: u64,
    current_file_size: u64,
}

// ==========================================
// STORAGE MANAGER ACTOR
// ==========================================

pub struct StorageManager {
    base_path: PathBuf,
    rx: mpsc::UnboundedReceiver<StorageCommand>,
    open_files: LruCache<PathBuf, BufWriter<File>>,
    topics: HashMap<String, TopicContext>,
    flush_interval: Duration,
    max_segment_size: u64,
    dirty_topics: HashSet<String>,
}

impl StorageManager {
    pub fn new(
        base_path: String,
        rx: mpsc::UnboundedReceiver<StorageCommand>,
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
            StorageCommand::Append { topic_name, messages, persisted_seq } => {
                self.handle_append(topic_name, messages, persisted_seq).await;
            }
            StorageCommand::ColdRead { topic_name, from_seq, limit, reply } => {
                let msgs = self.cold_read(&topic_name, from_seq, limit).await;
                let _ = reply.send(msgs);
            }
            StorageCommand::SaveState { topic_name, groups } => {
                let base_path = self.base_path.join(&topic_name);
                if let Err(e) = save_state_file(&base_path, &groups).await {
                    error!("Failed to save state for {}: {}", topic_name, e);
                }
            }
            StorageCommand::ApplyRetention { topic_name, retention, reply } => {
                let base_path = self.base_path.join(&topic_name);
                let outcome = self.apply_retention(&topic_name, &base_path, &retention).await;
                let _ = reply.send(outcome);
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
        persisted_seq: Arc<AtomicU64>,
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
                persisted_seq: persisted_seq.clone(),
                highest_pending_seq: 0,
                current_file_size: file_size,
            });
        } else {
            self.topics.get_mut(&topic_name).unwrap().persisted_seq = persisted_seq;
        }

        let mut buffer = Vec::new();
        for msg in &messages {
            serialize_message(&mut buffer, msg.seq, msg.timestamp, msg.key.as_deref(), &msg.payload);
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
                        ctx.persisted_seq.store(ctx.highest_pending_seq, Ordering::Release);
                        ctx.highest_pending_seq = 0;
                    }
                }
            }
        }
    }

    async fn cold_read(&self, topic_name: &str, from_seq: u64, limit: usize) -> Vec<Message> {
        let base_path = self.base_path.join(topic_name);
        let segments = find_segments(&base_path).await.unwrap_or_default();
        let mut all_msgs = Vec::new();
        let Some(first_segment) = segments.first() else {
            return all_msgs;
        };

        let mut current_from_seq = from_seq.max(first_segment.start_seq);
        let mut remaining_limit = limit;

        if let Some(idx) = segments.iter().rposition(|s| s.start_seq <= current_from_seq) {
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

    async fn apply_retention(&mut self, _topic_name: &str, base_path: &PathBuf, retention: &RetentionOptions) -> u64 {
        if retention.max_age_ms.is_none() && retention.max_bytes.is_none() {
            return find_segments(base_path).await.unwrap_or_default().first().map(|s| s.start_seq).unwrap_or(1);
        }
        let mut segments = find_segments(base_path).await.unwrap_or_default();
        if segments.len() <= 1 {
            return segments.first().map(|s| s.start_seq).unwrap_or(1);
        }

        if let Some(max_age) = retention.max_age_ms {
            let limit = std::time::SystemTime::now() - Duration::from_millis(max_age);
            let mut survivors = Vec::new();
            let last_start_seq = segments.last().map(|seg| seg.start_seq);
            for seg in segments {
                let mut deleted = false;
                if Some(seg.start_seq) != last_start_seq {
                    if let Ok(metadata) = tokio::fs::metadata(&seg.path).await {
                        if let Ok(modified) = metadata.modified() {
                            if modified < limit {
                                self.open_files.pop(&seg.path);
                                let _ = tokio::fs::remove_file(&seg.path).await;
                                deleted = true;
                            }
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

        find_segments(base_path).await.unwrap_or_default().first().map(|s| s.start_seq).unwrap_or(1)
    }
}

// ==========================================
// HELPERS (Formerly in writer.rs)
// ==========================================

/// Serialize a message into a buffer (does NOT write to disk).
pub fn serialize_message(buf: &mut Vec<u8>, seq: u64, timestamp: u64, key: Option<&[u8]>, payload: &[u8]) {
    use bytes::BufMut;
    let key_len = key.map_or(0, |k| k.len()) as u16;
    let len = 8 + 8 + 2 + key_len as u32 + payload.len() as u32;
    let mut hasher = Hasher::new();
    hasher.update(&seq.to_be_bytes());
    hasher.update(&timestamp.to_be_bytes());
    hasher.update(&key_len.to_be_bytes());
    if let Some(k) = key { hasher.update(k); }
    hasher.update(payload);
    let crc = hasher.finalize();

    buf.put_u32(len);
    buf.put_u32(crc);
    buf.put_u64(seq);
    buf.put_u64(timestamp);
    buf.put_u16(key_len);
    if let Some(k) = key { buf.put_slice(k); }
    buf.put_slice(payload);
}

enum ReadOutcome {
    Record(Vec<u8>),
    Corrupted,
    Eof,
}

/// Read the next record from a framed file.
/// Format: [len: u32 BE][crc: u32 BE][content: len bytes]
/// Returns Corrupted on CRC mismatch, Eof at end of file or read error.
async fn read_record(reader: &mut BufReader<File>) -> ReadOutcome {
    let mut len_buf = [0u8; 4];
    if reader.read_exact(&mut len_buf).await.is_err() { return ReadOutcome::Eof; }
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut crc_buf = [0u8; 4];
    if reader.read_exact(&mut crc_buf).await.is_err() { return ReadOutcome::Eof; }
    let stored_crc = u32::from_be_bytes(crc_buf);

    let mut content_buf = vec![0u8; len];
    if reader.read_exact(&mut content_buf).await.is_err() { return ReadOutcome::Eof; }

    let mut hasher = Hasher::new();
    hasher.update(&content_buf);
    if hasher.finalize() != stored_crc { return ReadOutcome::Corrupted; }

    ReadOutcome::Record(content_buf)
}

/// Read messages from a log segment file starting at a given seq.
pub async fn read_log_segment(path: &PathBuf, start_seq: u64, limit: usize) -> Vec<Message> {
    use bytes::Buf;
    let mut msgs = Vec::new();
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(_) => return msgs,
    };
    let mut reader = BufReader::new(file);

    loop {
        match read_record(&mut reader).await {
            ReadOutcome::Record(content_buf) => {
                if content_buf.len() < 18 { continue; }

                let mut cursor = std::io::Cursor::new(content_buf);
                let seq = cursor.get_u64();
                let timestamp = cursor.get_u64();
                let key_len = cursor.get_u16();
                let key = if key_len > 0 {
                    let key_bytes = cursor.copy_to_bytes(key_len as usize);
                    Some(Bytes::copy_from_slice(&key_bytes))
                } else {
                    None
                };
                let payload_len = cursor.remaining();
                let payload = Bytes::copy_from_slice(&cursor.copy_to_bytes(payload_len));

                if seq >= start_seq {
                    msgs.push(Message { seq, timestamp, key, payload });
                    if msgs.len() >= limit { break; }
                }
            }
            ReadOutcome::Corrupted | ReadOutcome::Eof => break,
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
        state.head_seq = segments.first().map(|seg| seg.start_seq).unwrap_or(1);
        if let Some(last_segment) = segments.last() {
            state.messages = load_segment_file(&last_segment.path).await;
        }
        state.segments = segments;
    }

    let state_path = base_path.join("state.log");
    if state_path.exists() {
        if let Ok(groups) = load_state_file(&state_path).await {
            state.groups_data = groups;
        }
    } else {
        // Fallback: try old groups.log for backward compat
        let groups_path = base_path.join("groups.log");
        if groups_path.exists() {
            if let Ok(groups) = load_groups_file(&groups_path).await {
                state.groups_data = groups.into_iter().map(|(id, ack_floor)| {
                    (id, GroupPersistentState { ack_floor, dlt_entries: HashMap::new(), parked_keys: HashSet::new() })
                }).collect();
            }
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
        if fname.ends_with(".log") && fname != "groups.log" && fname != "state.log" {
            let name_part = &fname[..fname.len() - 4];
            if let Ok(start_seq) = name_part.parse::<u64>() {
                segments.push(Segment { path, start_seq });
            }
        }
    }
    segments.sort_by_key(|s| s.start_seq);
    Ok(segments)
}

/// Write the state.log file (ack_floor + DLT entries + parked_keys per group). Atomic write via temp file + rename.
pub async fn save_state_file(base_path: &Path, groups: &HashMap<String, GroupPersistentState>) -> std::io::Result<()> {
    let tmp_path = base_path.join("state.log.tmp");
    let final_path = base_path.join("state.log");

    {
        let file = File::create(&tmp_path).await?;
        let mut writer = BufWriter::new(file);
        for (group_id, state) in groups {
            write_state_entry(&mut writer, group_id, state).await?;
        }
        writer.flush().await?;
    }

    tokio::fs::rename(&tmp_path, &final_path).await?;
    Ok(())
}

async fn write_state_entry<W: tokio::io::AsyncWrite + std::marker::Unpin>(writer: &mut W, group_id: &str, state: &GroupPersistentState) -> std::io::Result<()> {
    use bytes::{BufMut, BytesMut};
    let group_bytes = group_id.as_bytes();
    let group_len = group_bytes.len() as u16;
    let parked_keys_count = state.parked_keys.len() as u32;
    let dlt_count = state.dlt_entries.len() as u32;

    // Calculate total content length
    let mut content_len = 8 + 2 + group_len as u32 + 4 + 4; // ack_floor + group_len + group + parked_keys_count + dlt_count
    for key in &state.parked_keys {
        content_len += 2 + key.len() as u32;
    }
    for (_seq, entry) in &state.dlt_entries {
        let reason_bytes = entry.reason.as_bytes();
        content_len += 8 + 2 + entry.key.as_ref().map_or(0, |k| k.len()) as u32 + 2 + reason_bytes.len() as u32 + 4;
    }

    // CRC over content
    let mut content_buf = BytesMut::with_capacity(content_len as usize);
    content_buf.put_u64(state.ack_floor);
    content_buf.put_u16(group_len);
    content_buf.put_slice(group_bytes);
    content_buf.put_u32(parked_keys_count);
    for key in &state.parked_keys {
        content_buf.put_u16(key.len() as u16);
        content_buf.put_slice(key);
    }
    content_buf.put_u32(dlt_count);
    for (seq, entry) in &state.dlt_entries {
        content_buf.put_u64(*seq);
        let key_len = entry.key.as_ref().map_or(0, |k| k.len()) as u16;
        content_buf.put_u16(key_len);
        if let Some(k) = &entry.key { content_buf.put_slice(k); }
        let reason_bytes = entry.reason.as_bytes();
        content_buf.put_u16(reason_bytes.len() as u16);
        content_buf.put_slice(reason_bytes);
        content_buf.put_u32(entry.attempts);
    }

    let mut hasher = Hasher::new();
    hasher.update(&content_buf);
    let crc = hasher.finalize();

    let mut buf = BytesMut::with_capacity(4 + 4 + content_len as usize);
    buf.put_u32(content_len);
    buf.put_u32(crc);
    buf.extend_from_slice(&content_buf);

    writer.write_all(&buf).await?;
    Ok(())
}

async fn load_state_file(path: &PathBuf) -> Result<HashMap<String, GroupPersistentState>, std::io::Error> {
    use bytes::Buf;
    let mut groups: HashMap<String, GroupPersistentState> = HashMap::new();
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);

    loop {
        match read_record(&mut reader).await {
            ReadOutcome::Record(content_buf) => {
                let mut cursor = std::io::Cursor::new(content_buf);
                if cursor.remaining() < 10 { continue; }

                let ack_floor = cursor.get_u64();
                let group_len = cursor.get_u16();
                if cursor.remaining() < group_len as usize { continue; }
                let group_bytes = cursor.copy_to_bytes(group_len as usize);
                let group_id = String::from_utf8_lossy(&group_bytes).to_string();

                let mut state = GroupPersistentState { ack_floor, dlt_entries: HashMap::new(), parked_keys: HashSet::new() };

                if cursor.remaining() < 4 { continue; }
                let parked_keys_count = cursor.get_u32();
                for _ in 0..parked_keys_count {
                    if cursor.remaining() < 2 { break; }
                    let key_len = cursor.get_u16();
                    if cursor.remaining() < key_len as usize { break; }
                    let key = Bytes::copy_from_slice(&cursor.copy_to_bytes(key_len as usize));
                    state.parked_keys.insert(key);
                }

                if cursor.remaining() < 4 { continue; }
                let dlt_count = cursor.get_u32();
                for _ in 0..dlt_count {
                    if cursor.remaining() < 8 { break; }
                    let seq = cursor.get_u64();
                    if cursor.remaining() < 2 { break; }
                    let key_len = cursor.get_u16();
                    let key = if key_len > 0 {
                        if cursor.remaining() < key_len as usize { break; }
                        Some(Bytes::copy_from_slice(&cursor.copy_to_bytes(key_len as usize)))
                    } else { None };
                    if cursor.remaining() < 2 { break; }
                    let reason_len = cursor.get_u16();
                    if cursor.remaining() < reason_len as usize { break; }
                    let reason = String::from_utf8_lossy(&cursor.copy_to_bytes(reason_len as usize)).to_string();
                    if cursor.remaining() < 4 { break; }
                    let attempts = cursor.get_u32();
                    state.dlt_entries.insert(seq, DltEntry { reason, attempts, key });
                }

                groups.insert(group_id, state);
            }
            ReadOutcome::Corrupted | ReadOutcome::Eof => break,
        }
    }
    Ok(groups)
}

async fn load_segment_file(path: &PathBuf) -> VecDeque<Message> {
    use bytes::Buf;
    let mut msgs = VecDeque::new();
    let file = match OpenOptions::new().read(true).write(true).open(path).await {
        Ok(f) => f,
        Err(_) => return msgs,
    };

    let mut reader = BufReader::new(file);
    let mut valid_bytes: u64 = 0;
    loop {
        match read_record(&mut reader).await {
            ReadOutcome::Record(content_buf) => {
                valid_bytes += 4 + 4 + content_buf.len() as u64;
                if content_buf.len() < 18 { continue; }

                let mut cursor = std::io::Cursor::new(content_buf);
                let seq = cursor.get_u64();
                let timestamp = cursor.get_u64();
                let key_len = cursor.get_u16();
                let key = if key_len > 0 {
                    let key_bytes = cursor.copy_to_bytes(key_len as usize);
                    Some(Bytes::copy_from_slice(&key_bytes))
                } else {
                    None
                };
                let payload_len = cursor.remaining();
                let payload = Bytes::copy_from_slice(&cursor.copy_to_bytes(payload_len));

                msgs.push_back(Message { seq, timestamp, key, payload });
            }
            ReadOutcome::Corrupted => {
                error!("Corrupted record at byte {} in {:?}, truncating segment", valid_bytes, path);
                if let Err(e) = reader.get_ref().set_len(valid_bytes).await {
                    error!("Failed to truncate segment {:?}: {}", path, e);
                }
                break;
            }
            ReadOutcome::Eof => break,
        }
    }
    msgs
}

async fn load_groups_file(path: &PathBuf) -> Result<HashMap<String, u64>, std::io::Error> {
    use bytes::Buf;
    let mut groups = HashMap::new();
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);

    loop {
        match read_record(&mut reader).await {
            ReadOutcome::Record(content_buf) => {
                let mut cursor = std::io::Cursor::new(content_buf);
                if cursor.remaining() < 10 { continue; }
                let ack_floor = cursor.get_u64();
                let group_len = cursor.get_u16();

                if cursor.remaining() < group_len as usize { continue; }
                let group_bytes = cursor.copy_to_bytes(group_len as usize);
                let group_id = String::from_utf8_lossy(&group_bytes).to_string();

                groups.insert(group_id, ack_floor);
            }
            ReadOutcome::Corrupted | ReadOutcome::Eof => break,
        }
    }
    Ok(groups)
}

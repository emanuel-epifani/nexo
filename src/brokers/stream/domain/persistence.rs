//! Storage Manager: handles all file I/O for stream topics (append, read, retention, state).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lru::LruCache;
use bytes::Bytes;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncWriteExt, AsyncReadExt, AsyncSeekExt, BufReader};
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
    pub dlt_entries: BTreeMap<u64, DltEntry>,
    pub parked_keys: HashSet<Bytes>,
}

#[derive(Default)]
pub struct RecoveredState {
    /// seq → byte offset index (rebuilt from segment files)
    pub index: BTreeMap<u64, u64>,
    /// Next seq to assign (max seq + 1)
    pub next_seq: u64,
    /// All segment paths in order
    pub segments: Vec<Segment>,
    /// Group ID -> GroupPersistentState
    pub groups_data: BTreeMap<String, GroupPersistentState>,
    /// First retained sequence on disk
    pub head_seq: u64,
    /// Size of the last (active) segment in bytes
    pub last_segment_size: u64,
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub path: PathBuf,
    pub start_seq: u64,
}

// ==========================================
// COMMANDS
// ==========================================

pub enum StorageCommand {
    /// Append messages to a topic's log file. Fire-and-forget.
    /// Manager computes the file_path and offsets.
    Append {
        topic_name: String,
        file_path: PathBuf,
        messages: Vec<Message>,
    },
    
    /// Read messages from a topic by reading specific byte offsets from segment files.
    ReadRange {
        topic_name: String,
        /// (seq, byte_offset) pairs to read
        offsets: Vec<(u64, u64)>,
        reply: oneshot::Sender<Vec<Message>>,
    },

    SaveState {
        topic_name: String,
        groups: BTreeMap<String, GroupPersistentState>,
    },

    ApplyRetention {
        topic_name: String,
        retention: RetentionOptions,
        reply: oneshot::Sender<u64>,
    },

    DropTopic {
        topic_name: String,
        reply: oneshot::Sender<()>,
    },

    Shutdown {
        reply: oneshot::Sender<()>,
    }
}

pub struct StorageManager {
    base_path: PathBuf,
    rx: mpsc::UnboundedReceiver<StorageCommand>,
    open_files: LruCache<PathBuf, File>,
}

impl StorageManager {
    pub fn new(
        base_path: String,
        rx: mpsc::UnboundedReceiver<StorageCommand>,
        max_open_files: usize,
    ) -> Self {
        Self {
            base_path: PathBuf::from(base_path),
            rx,
            open_files: LruCache::new(NonZeroUsize::new(max_open_files).unwrap()),
        }
    }

    pub async fn run(mut self) {
        info!("StorageManager started");

        loop {
            match self.rx.recv().await {
                Some(StorageCommand::Shutdown { reply }) => {
                    while let Ok(cmd) = self.rx.try_recv() {
                        self.handle_command(cmd).await;
                    }
                    let _ = reply.send(());
                    break;
                }
                Some(cmd) => {
                    self.handle_command(cmd).await;
                    while let Ok(next) = self.rx.try_recv() {
                        self.handle_command(next).await;
                    }
                }
                None => break,
            }
        }

        info!("StorageManager stopped");
    }

    async fn handle_command(&mut self, cmd: StorageCommand) {
        match cmd {
            StorageCommand::Append { topic_name, file_path, messages } => {
                self.handle_append(&topic_name, file_path, messages).await;
            }
            StorageCommand::ReadRange { topic_name, offsets, reply } => {
                let base_path = self.base_path.join(&topic_name);
                tokio::spawn(async move {
                    let msgs = read_range(&base_path, offsets).await;
                    let _ = reply.send(msgs);
                });
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
                let topic_path = self.base_path.join(&topic_name);
                let to_remove: Vec<PathBuf> = self.open_files.iter()
                    .filter(|(p, _)| p.starts_with(&topic_path))
                    .map(|(p, _)| p.clone())
                    .collect();
                for p in to_remove {
                    self.open_files.pop(&p);
                }
                if topic_path.exists() {
                    let _ = std::fs::remove_dir_all(&topic_path);
                }
                let _ = reply.send(());
            }
            StorageCommand::Shutdown { .. } => {
                // Handled in run() loop directly
            }
        }
    }

    async fn handle_append(
        &mut self,
        _topic_name: &str,
        file_path: PathBuf,
        messages: Vec<Message>,
    ) {
        if messages.is_empty() { return; }

        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    error!("FATAL: Failed to create topic dir {:?}: {}", parent, e);
                    return;
                }
            }
        }

        let mut buffer = Vec::new();
        for msg in &messages {
            serialize_message(&mut buffer, msg.seq, msg.timestamp, msg.key.as_deref(), &msg.payload);
        }

        match self.get_or_open_file(&file_path).await {
            Ok(writer) => {
                if let Err(e) = writer.write_all(&buffer).await {
                    error!("StorageManager: Failed to write to {:?}: {}", file_path, e);
                    self.open_files.pop(&file_path);
                }
            }
            Err(e) => {
                error!("StorageManager: Failed to open file {:?}: {}", file_path, e);
            }
        }
    }

    async fn get_or_open_file(&mut self, path: &PathBuf) -> Result<&mut File, std::io::Error> {
        if !self.open_files.contains(path) {
            if self.open_files.len() == self.open_files.cap().get() {
                let _ = self.open_files.pop_lru();
            }
            let file = OpenOptions::new().read(true).write(true).create(true).append(true).open(path).await?;
            self.open_files.put(path.clone(), file);
        }
        Ok(self.open_files.get_mut(path).unwrap())
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

/// Read messages from segment files at specific byte offsets.
/// Called from spawned tasks — must not access StorageManager state.
async fn read_range(base_path: &Path, offsets: Vec<(u64, u64)>) -> Vec<Message> {
    if offsets.is_empty() { return Vec::new(); }

    let segments = find_segments(base_path).await.unwrap_or_default();
    if segments.is_empty() { return Vec::new(); }

    // Group offsets by segment to open each file once
    let mut by_segment: HashMap<usize, Vec<(u64, u64)>> = HashMap::new();
    for (seq, byte_offset) in offsets {
        if let Some(idx) = segments.iter().rposition(|s| s.start_seq <= seq) {
            by_segment.entry(idx).or_default().push((seq, byte_offset));
        }
    }

    let mut result = Vec::new();
    for (idx, mut seg_offsets) in by_segment {
        seg_offsets.sort_by_key(|(_, off)| *off);
        let seg = &segments[idx];
        match File::open(&seg.path).await {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                for (seq, byte_offset) in seg_offsets {
                    if reader.seek(std::io::SeekFrom::Start(byte_offset)).await.is_ok() {
                        match read_record(&mut reader).await {
                            ReadOutcome::Record(content_buf) => {
                                if let Some(msg) = parse_message(&content_buf) {
                                    if msg.seq == seq {
                                        result.push(msg);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            Err(e) => error!("StorageManager: Failed to open segment {:?}: {}", seg.path, e),
        }
    }
    result.sort_by_key(|m| m.seq);
    result
}

// ==========================================
// HELPERS (Formerly in writer.rs)
// ==========================================

/// Total on-disk size of a serialized message record: [len: u32][crc: u32][content].
pub fn record_len(key: Option<&[u8]>, payload: &[u8]) -> u64 {
    let key_len = key.map_or(0, |k| k.len()) as u64;
    4 + 4 + 8 + 8 + 2 + key_len + payload.len() as u64
}

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
    UnexpectedEof,
}

/// Read the next record from a framed file.
/// Format: [len: u32 BE][crc: u32 BE][content: len bytes]
/// Returns Corrupted on CRC mismatch, Eof at clean end of file, UnexpectedEof on partial read.
async fn read_record<R: AsyncRead + Unpin>(reader: &mut R) -> ReadOutcome {
    let mut len_buf = [0u8; 4];
    if reader.read_exact(&mut len_buf).await.is_err() { return ReadOutcome::Eof; }
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut crc_buf = [0u8; 4];
    if reader.read_exact(&mut crc_buf).await.is_err() { return ReadOutcome::UnexpectedEof; }
    let stored_crc = u32::from_be_bytes(crc_buf);

    let mut content_buf = vec![0u8; len];
    if reader.read_exact(&mut content_buf).await.is_err() { return ReadOutcome::UnexpectedEof; }

    let mut hasher = Hasher::new();
    hasher.update(&content_buf);
    if hasher.finalize() != stored_crc { return ReadOutcome::Corrupted; }

    ReadOutcome::Record(content_buf)
}

/// Parse a message from a framed record content buffer.
fn parse_message(content: &[u8]) -> Option<Message> {
    use bytes::Buf;
    if content.len() < 18 { return None; }
    let mut cursor = std::io::Cursor::new(content);
    let seq = cursor.get_u64();
    let timestamp = cursor.get_u64();
    let key_len = cursor.get_u16();
    let key = if key_len > 0 {
        if cursor.remaining() < key_len as usize { return None; }
        let key_bytes = cursor.copy_to_bytes(key_len as usize);
        Some(Bytes::copy_from_slice(&key_bytes))
    } else {
        None
    };
    let payload_len = cursor.remaining();
    let payload = Bytes::copy_from_slice(&cursor.copy_to_bytes(payload_len));
    Some(Message { seq, timestamp, key, payload })
}

/// Build a seq→byte_offset index by scanning a segment file.
async fn build_segment_index(path: &PathBuf) -> std::io::Result<BTreeMap<u64, u64>> {
    let mut index = BTreeMap::new();
    let file = OpenOptions::new().read(true).write(true).open(path).await?;
    let mut reader = BufReader::new(file);
    let mut current_offset: u64 = 0;
    let mut valid_bytes: u64 = 0;

    loop {
        match read_record(&mut reader).await {
            ReadOutcome::Record(content_buf) => {
                let len = 4 + 4 + content_buf.len() as u64;
                if let Some(msg) = parse_message(&content_buf) {
                    index.insert(msg.seq, current_offset);
                    current_offset += len;
                    valid_bytes += len;
                } else {
                    // parse_message failed: record is corrupted, truncate
                    if valid_bytes > 0 {
                        if let Err(e) = reader.get_ref().set_len(valid_bytes).await {
                            error!("Failed to truncate segment {:?}: {}", path, e);
                        }
                    }
                    break;
                }
            }
            ReadOutcome::Corrupted | ReadOutcome::UnexpectedEof => {
                if valid_bytes > 0 {
                    if let Err(e) = reader.get_ref().set_len(valid_bytes).await {
                        error!("Failed to truncate segment {:?}: {}", path, e);
                    }
                }
                break;
            }
            ReadOutcome::Eof => break,
        }
    }
    Ok(index)
}

/// Recover topic state from filesystem.
/// Rebuilds the seq→byte_offset index by scanning all segment files.
pub async fn recover_topic(topic_name: &str, base_path: PathBuf) -> RecoveredState {
    let base_path = base_path.join(topic_name);
    let mut state = RecoveredState::default();
    if !base_path.exists() { return state; }

    if let Ok(segments) = find_segments(&base_path).await {
        state.head_seq = segments.first().map(|seg| seg.start_seq).unwrap_or(1);
        state.segments = segments.clone();

        // Rebuild index by scanning all segment files
        let mut max_seq = 0u64;
        for seg in &segments {
            if let Ok(index) = build_segment_index(&seg.path).await {
                for (seq, offset) in index {
                    if seq > max_seq { max_seq = seq; }
                    state.index.insert(seq, offset);
                }
            }
        }
        state.next_seq = if max_seq > 0 { max_seq + 1 } else { state.head_seq.max(1) };
        state.last_segment_size = segments.last()
            .and_then(|seg| std::fs::metadata(&seg.path).ok())
            .map(|m| m.len())
            .unwrap_or(0);
    }

    let state_path = base_path.join("state.log");
    if state_path.exists() {
        if let Ok(groups) = load_state_file(&state_path).await {
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
        if fname.ends_with(".log") && fname != "state.log" {
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
pub async fn save_state_file(base_path: &Path, groups: &BTreeMap<String, GroupPersistentState>) -> std::io::Result<()> {
    let tmp_path = base_path.join("state.log.tmp");
    let final_path = base_path.join("state.log");

    {
        let mut writer = File::create(&tmp_path).await?;
        for (group_id, state) in groups {
            write_state_entry(&mut writer, group_id, state).await?;
        }
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

async fn load_state_file(path: &PathBuf) -> Result<BTreeMap<String, GroupPersistentState>, std::io::Error> {
    use bytes::Buf;
    let mut groups: BTreeMap<String, GroupPersistentState> = BTreeMap::new();
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

                let mut state = GroupPersistentState { ack_floor, dlt_entries: BTreeMap::new(), parked_keys: HashSet::new() };

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
            ReadOutcome::Corrupted | ReadOutcome::Eof | ReadOutcome::UnexpectedEof => break,
        }
    }
    Ok(groups)
}

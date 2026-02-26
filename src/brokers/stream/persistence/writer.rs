//! Stream Persistence: Recovery and Segment I/O
//!
//! Contains functions for reading/writing segment files and recovering topic state from disk.
//! The StreamWriter actor has been removed — write logic is embedded in the TopicActor.

use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter, AsyncReadExt, BufReader};
use std::collections::{HashMap, VecDeque};
use tracing::{error, info, warn};
use bytes::Bytes;
use crc32fast::Hasher;

use crate::brokers::stream::message::Message;

// --- STRUCTS ---

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

// --- PUBLIC API ---

/// Read messages from a log segment file starting at a given seq.
pub async fn read_log_segment(path: &PathBuf, start_seq: u64, limit: usize) -> Vec<Message> {
    let mut msgs = Vec::new();
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(e) => {
            error!("Cold Read: Failed to open {:?}: {}", path, e);
            return msgs;
        }
    };

    let mut reader = BufReader::new(file);

    loop {
        // [Len: u32]
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() { break; }
        let len = u32::from_be_bytes(len_buf);

        // [CRC32: u32]
        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).await.is_err() { break; }
        let stored_crc = u32::from_be_bytes(crc_buf);

        // Content
        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).await.is_err() { break; }

        // CRC check
        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        if hasher.finalize() != stored_crc {
            error!("CRC Mismatch in Cold Read {:?}. Skipping.", path);
            continue;
        }

        // Parse: [Seq: u64][Timestamp: u64][Payload]
        if content_buf.len() < 16 { continue; }
        let seq = u64::from_be_bytes(content_buf[0..8].try_into().unwrap());
        let timestamp = u64::from_be_bytes(content_buf[8..16].try_into().unwrap());
        let payload = Bytes::copy_from_slice(&content_buf[16..]);

        if seq >= start_seq {
            msgs.push(Message { seq, timestamp, payload });
            if msgs.len() >= limit {
                break;
            }
        }
    }

    msgs
}

/// Recover topic state from filesystem.
pub async fn recover_topic(topic_name: &str, base_path: PathBuf) -> RecoveredState {
    let base_path = base_path.join(topic_name);
    let mut state = RecoveredState::default();

    if !base_path.exists() {
        return state;
    }

    // 1. Load Log Segments
    if let Ok(segments) = find_segments(&base_path).await {
        // Lazy load: only the last segment into RAM
        if let Some(last_segment) = segments.last() {
            state.messages = load_segment_file(&last_segment.path).await;
            info!("Topic '{}' Segment {}: LOADED {} messages", topic_name, last_segment.start_seq, state.messages.len());
        }
        state.segments = segments;
    }

    // 2. Load Groups (groups.log)
    let groups_path = base_path.join("groups.log");
    if groups_path.exists() {
        match load_groups_file(&groups_path).await {
            Ok(groups) => {
                info!("Topic '{}': recovered {} consumer groups", topic_name, groups.len());
                state.groups_data = groups;
            }
            Err(e) => {
                error!("Topic '{}': failed to load groups: {}", topic_name, e);
            }
        }
    }

    state
}

/// Find all segment files for a topic, sorted by start_seq.
pub async fn find_segments(base_path: &PathBuf) -> std::io::Result<Vec<Segment>> {
    let mut segments = Vec::new();

    if !base_path.exists() {
        return Ok(segments);
    }

    let mut entries = tokio::fs::read_dir(base_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_file() { continue; }
        
        let fname = entry.file_name().to_string_lossy().to_string();
        
        // Segment files are named: {start_seq}.log
        if fname.ends_with(".log") && fname != "groups.log" {
            let name_part = &fname[..fname.len() - 4]; // remove .log
            if let Ok(start_seq) = name_part.parse::<u64>() {
                segments.push(Segment { path, start_seq });
            }
        }
    }
    
    segments.sort_by_key(|s| s.start_seq);
    Ok(segments)
}

// --- WRITE HELPERS (used by TopicActor inline) ---

/// Serialize a message into a buffer (does NOT write to disk).
pub fn serialize_message(buf: &mut Vec<u8>, seq: u64, timestamp: u64, payload: &[u8]) {
    let len = 8 + 8 + payload.len() as u32; // Seq + Timestamp + Payload

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

/// Write the groups.log file (ack_floor for each group). Atomic write via temp file + rename.
pub async fn save_groups_file(base_path: &PathBuf, groups: &HashMap<String, u64>) -> std::io::Result<()> {
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

/// Append a single group entry to an existing groups.log file.
pub async fn append_group_entry(writer: &mut BufWriter<File>, group_id: &str, ack_floor: u64) -> std::io::Result<()> {
    write_group_entry(writer, group_id, ack_floor).await?;
    writer.flush().await?;
    Ok(())
}

// --- INTERNAL HELPERS ---

async fn write_group_entry<W: tokio::io::AsyncWrite + std::marker::Unpin>(writer: &mut W, group_id: &str, ack_floor: u64) -> std::io::Result<()> {
    // [Len: u32][CRC32: u32][AckFloor: u64][GroupLen: u16][GroupBytes]
    let group_bytes = group_id.as_bytes();
    let group_len = group_bytes.len() as u16;
    
    let len = 8 + 2 + group_len as u32; // AckFloor + GroupLen + GroupBytes
    
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
        Err(e) => {
            error!("Failed to open {:?}: {}", path, e);
            return msgs;
        }
    };

    let mut reader = BufReader::new(file);
    
    loop {
        // [Len: u32]
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() { break; }
        let len = u32::from_be_bytes(len_buf);

        // [CRC32: u32]
        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).await.is_err() {
            warn!("Unexpected EOF reading CRC at {:?}", path);
            break;
        }
        let stored_crc = u32::from_be_bytes(crc_buf);

        // Content
        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).await.is_err() {
            warn!("Unexpected EOF reading Content at {:?}", path);
            break;
        }

        // CRC check
        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        if hasher.finalize() != stored_crc {
            error!("CRC Mismatch in {:?}! Data corrupt. Stopping read.", path);
            break;
        }

        // Parse: [Seq: u64][Timestamp: u64][Payload]
        if content_buf.len() < 16 {
            error!("Content too short in {:?}", path);
            break;
        }

        let seq = u64::from_be_bytes(content_buf[0..8].try_into().unwrap());
        let timestamp = u64::from_be_bytes(content_buf[8..16].try_into().unwrap());
        let payload = Bytes::copy_from_slice(&content_buf[16..]);

        msgs.push_back(Message { seq, timestamp, payload });
    }

    msgs
}

async fn load_groups_file(path: &PathBuf) -> Result<HashMap<String, u64>, std::io::Error> {
    let mut groups: HashMap<String, u64> = HashMap::new();
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);

    loop {
        // [Len: u32]
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() { break; }
        let len = u32::from_be_bytes(len_buf);

        // [CRC32: u32]
        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).await.is_err() { break; }
        let stored_crc = u32::from_be_bytes(crc_buf);

        // Content
        let mut content_buf = vec![0u8; len as usize];
        if reader.read_exact(&mut content_buf).await.is_err() { break; }

        // CRC check
        let mut hasher = Hasher::new();
        hasher.update(&content_buf);
        if hasher.finalize() != stored_crc {
            error!("CRC Mismatch in groups.log. Stopping.");
            break;
        }

        // Parse: [AckFloor: u64][GroupLen: u16][GroupBytes]
        let mut cursor = std::io::Cursor::new(content_buf);
        use std::io::Read; // for the in-memory cursor
        
        let mut buf8 = [0u8; 8];
        let mut buf2 = [0u8; 2];

        if Read::read_exact(&mut cursor, &mut buf8).is_err() { continue; }
        let ack_floor = u64::from_be_bytes(buf8);

        if Read::read_exact(&mut cursor, &mut buf2).is_err() { continue; }
        let group_len = u16::from_be_bytes(buf2);

        let mut group_bytes = vec![0u8; group_len as usize];
        if Read::read_exact(&mut cursor, &mut group_bytes).is_err() { continue; }
        let group_id = String::from_utf8_lossy(&group_bytes).to_string();

        // Last write wins
        groups.insert(group_id, ack_floor);
    }

    Ok(groups)
}

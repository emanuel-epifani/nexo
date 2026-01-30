use std::fs::{self, File};
use std::io::{Read, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::collections::{HashMap, VecDeque};
use bytes::Bytes;
use crc32fast::Hasher;
use tracing::{error, info, warn};

use crate::brokers::stream::message::Message;
use crate::config::Config;

#[derive(Default)]
pub struct RecoveredState {
    // Partition ID -> Messages
    pub partitions_data: HashMap<u32, VecDeque<Message>>,
    // Group ID -> (GenerationID, Committed Offsets)
    pub groups_data: HashMap<String, (u64, HashMap<u32, u64>)>,
}

pub fn recover_topic(topic_name: &str, partitions_count: u32) -> RecoveredState {
    let base_path = PathBuf::from(&Config::global().stream.persistence_path).join(topic_name);
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

//! Storage Manager: Global actor handling all file I/O for stream topics.
//! 
//! Responsibilities:
//! - Offloads disk I/O from individual topic actors.
//! - Batches writes automatically via `BufWriter` for high throughput.
//! - Manages an LRU Cache of file descriptors to prevent OS limits exhaustion.
//! - Executes a global periodic flush to sync bytes to disk and notify topic actors.

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use lru::LruCache;
use bytes::Bytes;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, debug};

use crate::brokers::stream::commands::RetentionOptions;
use crate::brokers::stream::persistence::writer::serialize_message;

// ==========================================
// COMMANDS
// ==========================================

#[derive(Debug)]
pub struct MessageToAppend {
    pub seq: u64,
    pub timestamp: u64,
    pub payload: Bytes,
}

pub enum StorageCommand {
    /// Append messages to a topic's active log file.
    /// The manager will buffer this in RAM. 
    /// The `reply` channel will be triggered after the next global `flush()`.
    Append {
        topic_name: String,
        messages: Vec<MessageToAppend>,
        ack_sender: mpsc::Sender<u64>,
    },
    
    ColdRead {
        topic_name: String,
        from_seq: u64,
        limit: usize,
        reply: oneshot::Sender<Vec<crate::brokers::stream::message::Message>>,
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
    /// Active file path
    active_path: PathBuf,
    /// The last sender we received for this topic
    ack_sender: Option<mpsc::Sender<u64>>,
    /// Highest sequence received but not yet flushed
    highest_pending_seq: u64,
    /// We keep a reference to know when to rotate
    current_file_size: u64,
}

pub struct StorageManager {
    base_path: PathBuf,
    rx: mpsc::Receiver<StorageCommand>,
    
    /// LRU Cache of Open File Descriptors (BufWriter)
    /// Key: PathBuf (the exact .log file path)
    open_files: LruCache<PathBuf, BufWriter<File>>,
    
    /// Context around each topic (keeps track of where to append and who to notify)
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
                            
                            // DRAIN: Process as many waiting commands as possible
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
        
        // Final flush on shutdown
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
                if let Err(e) = crate::brokers::stream::persistence::writer::save_groups_file(&base_path, &groups_data).await {
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
                    info!("StorageManager: Dropping context for '{}'", topic_name);
                }
                // Physical deletion of the topic directory
                let topic_path = self.base_path.join(&topic_name);
                if topic_path.exists() {
                    if let Err(e) = std::fs::remove_dir_all(&topic_path) {
                        error!("StorageManager: Failed to physically delete topic dir {:?}: {}", topic_path, e);
                    } else {
                        info!("StorageManager: Physically deleted topic dir {:?}", topic_path);
                    }
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
        if messages.is_empty() {
            return;
        }

        let highest_seq = messages.last().unwrap().seq;
        let base_topic_path = self.base_path.join(&topic_name);

        // Ensure topic context exists
        if !self.topics.contains_key(&topic_name) {
            // Lazy initialization of topic directory
            if !base_topic_path.exists() {
                if let Err(e) = tokio::fs::create_dir_all(&base_topic_path).await {
                    error!("FATAL: Failed to create topic dir {:?}: {}", base_topic_path, e);
                    return;
                }
            }
            
            // For now, we naively append to the last segment or create 1.log
            // TODO: In a full refactor, the StorageManager should fetch the active segment from the filesystem on init
            let active_path = base_topic_path.join("1.log"); 
            let file_size = tokio::fs::metadata(&active_path).await.map(|m| m.len()).unwrap_or(0);

            self.topics.insert(topic_name.clone(), TopicContext {
                active_path: base_topic_path.join("1.log"),
                ack_sender: Some(ack_sender.clone()),
                highest_pending_seq: 0,
                current_file_size: file_size,
            });
        } else {
            // Update the sender in case it changed (e.g. actor restart)
            self.topics.get_mut(&topic_name).unwrap().ack_sender = Some(ack_sender);
        }

        // Serialize all messages into a temporary buffer
        let mut buffer = Vec::new();
        for msg in &messages {
            serialize_message(&mut buffer, msg.seq, msg.timestamp, &msg.payload);
        }
        let bytes_len = buffer.len() as u64;

        let path = {
            let ctx = self.topics.get_mut(&topic_name).unwrap();
            ctx.highest_pending_seq = highest_seq;
            self.dirty_topics.insert(topic_name.clone());

            // Check if we need to rotate segment BEFORE writing
            // Include bytes_len to see if this append pushes us over the limit
            if ctx.current_file_size + bytes_len > self.max_segment_size && ctx.current_file_size > 0 {
                // FLUSH AND REMOVE old file from cache
                if let Some(mut old_writer) = self.open_files.pop(&ctx.active_path) {
                    if let Err(e) = old_writer.flush().await {
                        error!("StorageManager: Failed to flush on rotation: {}", e);
                    }
                }
                
                // New path based on the *first* message we are about to write
                let first_seq = messages.first().unwrap().seq;
                ctx.active_path = base_topic_path.join(format!("{}.log", first_seq));
                ctx.current_file_size = 0;
                info!("StorageManager: Rotated log for '{}' to {}", topic_name, ctx.active_path.display());
            }
            
            ctx.active_path.clone()
        };

        // Get or open the writer via LRU Cache
        let writer_res = self.get_or_open_writer(&path).await;
        match writer_res {
            Ok(writer) => {
                if let Err(e) = writer.write_all(&buffer).await {
                    error!("StorageManager: Failed to write to {:?}: {}", path, e);
                    // Could pop the writer so it retries opening next time
                    self.open_files.pop(&path);
                    return;
                }
                
                let ctx = self.topics.get_mut(&topic_name).unwrap();
                ctx.current_file_size += bytes_len;
                ctx.highest_pending_seq = highest_seq;
            }
            Err(e) => {
                error!("StorageManager: Failed to open file {:?}: {}", path, e);
            }
        }
    }

    /// Fetches an active `BufWriter` from the LRU Cache.
    /// If it's not present, opens it (which might evict and *flush+close* an old file).
    async fn get_or_open_writer(&mut self, path: &PathBuf) -> Result<&mut BufWriter<File>, std::io::Error> {
        if !self.open_files.contains(path) {
            if self.open_files.len() == self.open_files.cap().get() {
                if let Some((_k, mut evicted_writer)) = self.open_files.pop_lru() {
                    let _ = evicted_writer.flush().await;
                }
            }

            // We need to open it.
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?;
            
            let writer = BufWriter::new(file);
            self.open_files.put(path.clone(), writer);
            debug!("StorageManager: Opened new FD for {:?}", path);
        }
        
        // Return mutable reference
        Ok(self.open_files.get_mut(path).unwrap())
    }

    /// The Global Tick: Flushes all currently tracked dirty writers to disk,
    /// and resolves all promises for TopicActors.
    async fn flush_all(&mut self) {
        // 1. Flush only active file writers
        for (_, writer) in self.open_files.iter_mut() {
            if let Err(e) = writer.flush().await {
                error!("StorageManager: Flush error: {}", e);
            }
        }

        // 2. Notify ACKs only for topics that received messages (O(active) instead of O(total))
        if !self.dirty_topics.is_empty() {
            let topics_to_flush: Vec<String> = self.dirty_topics.drain().collect();
            for name in topics_to_flush {
                if let Some(ctx) = self.topics.get_mut(&name) {
                    if ctx.highest_pending_seq > 0 {
                        if let Some(ref sender) = ctx.ack_sender {
                            if let Ok(_) = sender.try_send(ctx.highest_pending_seq) {
                                ctx.highest_pending_seq = 0;
                            } else {
                                // If send fails, keep it dirty for next flush
                                self.dirty_topics.insert(name);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn cold_read(&self, topic_name: &str, from_seq: u64, limit: usize) -> Vec<crate::brokers::stream::message::Message> {
        let base_path = self.base_path.join(topic_name);
        let segments = crate::brokers::stream::persistence::find_segments(&base_path).await.unwrap_or_default();
        
        let mut all_msgs = Vec::new();
        let mut current_from_seq = from_seq;
        let mut remaining_limit = limit;

        // Find the index of the first segment to read from
        let first_seg_idx = segments.iter().rposition(|s| s.start_seq <= from_seq);

        if let Some(idx) = first_seg_idx {
            for segment in segments.iter().skip(idx) {
                if remaining_limit == 0 { break; }
                let msgs = crate::brokers::stream::persistence::read_log_segment(&segment.path, current_from_seq, remaining_limit).await;
                if !msgs.is_empty() {
                    let last_seq = msgs.last().unwrap().seq;
                    current_from_seq = last_seq + 1;
                    remaining_limit = remaining_limit.saturating_sub(msgs.len());
                    all_msgs.extend(msgs);
                }
            }
        }
        all_msgs
    }

    async fn apply_retention(&mut self, _topic_name: &str, base_path: &PathBuf, retention: &RetentionOptions) {
        if retention.max_age_ms.is_none() && retention.max_bytes.is_none() {
            return;
        }

        let mut segments = match crate::brokers::stream::persistence::find_segments(base_path).await {
            Ok(s) => s,
            Err(_) => return,
        };

        if segments.len() <= 1 {
            return;
        }

        // Time retention
        if let Some(max_age) = retention.max_age_ms {
            let limit = std::time::SystemTime::now() - Duration::from_millis(max_age);
            let mut survivors = Vec::new();
            for seg in segments {
                let mut deleted = false;
                if let Ok(metadata) = tokio::fs::metadata(&seg.path).await {
                    if let Ok(modified) = metadata.modified() {
                        if modified < limit {
                            self.open_files.pop(&seg.path);
                            if let Err(e) = tokio::fs::remove_file(&seg.path).await {
                                error!("Failed to delete {:?}: {}", seg.path, e);
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

        // Size retention
        if let Some(max_bytes) = retention.max_bytes {
            let mut active_size = 0;
            if let Some(active) = segments.last() {
                if let Ok(meta) = tokio::fs::metadata(&active.path).await {
                    active_size = meta.len();
                }
            }
            
            let mut current_total: u64 = active_size;
            for seg in &segments {
                if let Ok(meta) = tokio::fs::metadata(&seg.path).await {
                    current_total += meta.len();
                }
            }

            let mut i = 0;
            // Never delete the last segment (the active one)
            while current_total > max_bytes && i < segments.len().saturating_sub(1) {
                let seg = &segments[i];
                if let Ok(meta) = tokio::fs::metadata(&seg.path).await {
                    let size = meta.len();
                    self.open_files.pop(&seg.path);
                    if let Err(e) = tokio::fs::remove_file(&seg.path).await {
                        error!("Failed to delete {:?}: {}", seg.path, e);
                    } else {
                        current_total = current_total.saturating_sub(size);
                    }
                }
                i += 1;
            }
        }
    }
}

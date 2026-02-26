//! Stream Topic Actor: Async actor owning a single topic's state
//!
//! Single-log model (no partitions). Writer is embedded (no separate actor).
//! Handles: Publish, Fetch, Ack, Nack, Seek, Join, Leave, Eviction, Flush, Retention.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use tracing::{error, info, warn};

use crate::brokers::stream::topic::{TopicState, TopicConfig};
use crate::brokers::stream::message::Message;
use crate::brokers::stream::group::ConsumerGroup;
use crate::brokers::stream::commands::SeekTarget;
use crate::brokers::stream::persistence::{
    recover_topic, read_log_segment, find_segments, Segment,
    writer::{serialize_message, save_groups_file},
};

// ==========================================
// COMMANDS (The Internal Protocol)
// ==========================================

pub enum TopicCommand {
    Publish {
        payload: Bytes,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    Fetch {
        group_id: String,
        client_id: String,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<Message>, String>>,
    },
    Ack {
        group_id: String,
        seq: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Nack {
        group_id: String,
        seq: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Seek {
        group_id: String,
        target: SeekTarget,
        reply: oneshot::Sender<Result<(), String>>,
    },
    JoinGroup {
        group_id: String,
        client_id: String,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    LeaveGroup {
        client_id: String,
        reply: oneshot::Sender<()>,
    },
    Read {
        from_seq: u64,
        limit: usize,
        reply: oneshot::Sender<Vec<Message>>,
    },
    GetSnapshot {
        reply: oneshot::Sender<crate::dashboard::stream::TopicSummary>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
}

// ==========================================
// TOPIC ACTOR
// ==========================================

pub(crate) struct TopicActor {
    state: TopicState,
    groups: HashMap<String, ConsumerGroup>,
    client_map: HashMap<String, Vec<String>>,
    rx: mpsc::Receiver<TopicCommand>,
    // Embedded writer
    write_buffer: Vec<u8>,
    log_file: Option<BufWriter<File>>,
    log_file_size: u64,
    groups_dirty: bool,
    base_path: PathBuf,
    segments: Vec<Segment>,
    // Config
    max_segment_size: u64,
    retention: crate::brokers::stream::commands::RetentionOptions,
    max_ack_pending: usize,
    ack_wait: Duration,
}

impl TopicActor {
    pub(crate) async fn new(
        name: String,
        rx: mpsc::Receiver<TopicCommand>,
        config: TopicConfig,
    ) -> Self {
        let base_path = PathBuf::from(&config.persistence_path).join(&name);

        // Create directory
        if let Err(e) = fs::create_dir_all(&base_path) {
            error!("FATAL: Failed to create topic directory at {:?}: {}", base_path, e);
        }

        // Recovery
        let recovered = recover_topic(&name, PathBuf::from(config.persistence_path.clone())).await;
        let mut state = TopicState::restore(
            name.clone(),
            config.ram_soft_limit,
            recovered.messages,
        );

        // Restore segments index
        let segments = recovered.segments;
        if let Some(first) = segments.first() {
            state.start_seq = first.start_seq;
        }

        // Restore groups
        let mut groups = HashMap::new();
        for (gid, ack_floor) in recovered.groups_data {
            groups.insert(
                gid.clone(),
                ConsumerGroup::restore(gid, ack_floor, config.max_ack_pending, Duration::from_millis(config.ack_wait_ms)),
            );
        }

        // Open active segment file
        let mut segments = segments;
        let (log_file, log_file_size) = Self::open_active_segment(&base_path, &segments).await;

        // Ensure initial segment is in the index (for cold reads on fresh topics)
        if segments.is_empty() {
            segments.push(Segment {
                path: base_path.join("1.log"),
                start_seq: 1,
            });
        }

        Self {
            state,
            groups,
            client_map: HashMap::new(),
            rx,
            write_buffer: Vec::with_capacity(64 * 1024),
            log_file,
            log_file_size,
            groups_dirty: false,
            base_path,
            segments,
            max_segment_size: config.max_segment_size,
            retention: config.retention,
            max_ack_pending: config.max_ack_pending,
            ack_wait: Duration::from_millis(config.ack_wait_ms),
        }
    }

    pub(crate) async fn run(mut self, config: TopicConfig) {
        info!("TopicActor '{}' started", self.state.name);

        let flush_interval = Duration::from_millis(config.default_flush_ms);
        let mut flush_timer = tokio::time::interval(flush_interval);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Eviction timer
        let mut eviction_timer = tokio::time::interval(Duration::from_millis(config.eviction_interval_ms));
        eviction_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Retention timer
        let mut retention_timer = tokio::time::interval(Duration::from_millis(config.retention_check_ms));
        retention_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Redelivery check timer
        let mut redeliver_timer = tokio::time::interval(Duration::from_secs(5));
        redeliver_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            if self.handle_command(cmd).await {
                                break; // Stop command received
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                _ = flush_timer.tick() => {
                    self.flush_to_disk().await;
                    // Periodically save groups if dirty (not strictly tied to message buffer flush)
                    if self.groups_dirty {
                        self.save_groups().await;
                        self.groups_dirty = false;
                    }
                }
                _ = eviction_timer.tick() => {
                    self.state.evict();
                }
                _ = retention_timer.tick() => {
                    if let Err(e) = self.check_retention().await {
                        error!("Retention check failed for '{}': {}", self.state.name, e);
                    }
                }
                _ = redeliver_timer.tick() => {
                    for group in self.groups.values_mut() {
                        group.check_redelivery();
                    }
                }
            }
        }

        // Final flush on shutdown
        self.flush_to_disk().await;
        if self.groups_dirty {
            self.save_groups().await;
        }
        info!("TopicActor '{}' stopped", self.state.name);
    }

    /// Returns true if the actor should stop.
    async fn handle_command(&mut self, cmd: TopicCommand) -> bool {
        match cmd {
            TopicCommand::Publish { payload, reply } => {
                let (seq, timestamp) = self.state.append(payload.clone());

                // Buffer for disk
                serialize_message(&mut self.write_buffer, seq, timestamp, &payload);

                let _ = reply.send(Ok(seq));
            }

            TopicCommand::Fetch { group_id, client_id, limit, reply } => {
                let group = self.groups.entry(group_id.clone())
                    .or_insert_with(|| ConsumerGroup::new(group_id.clone(), self.max_ack_pending, self.ack_wait));

                if !group.is_member(&client_id) {
                    let _ = reply.send(Err("NOT_MEMBER".to_string()));
                    return false;
                }

                let msgs = group.fetch(&client_id, limit, &self.state.log, self.state.ram_start_seq);
                let _ = reply.send(Ok(msgs));
            }

            TopicCommand::Ack { group_id, seq, reply } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    match group.ack(seq) {
                        Ok(()) => {
                            self.groups_dirty = true;
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                        }
                    }
                } else {
                    let _ = reply.send(Err("Group not found".to_string()));
                }
            }

            TopicCommand::Nack { group_id, seq, reply } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    match group.nack(seq) {
                        Ok(()) => { let _ = reply.send(Ok(())); }
                        Err(e) => { let _ = reply.send(Err(e)); }
                    }
                } else {
                    let _ = reply.send(Err("Group not found".to_string()));
                }
            }

            TopicCommand::Seek { group_id, target, reply } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    match target {
                        SeekTarget::Beginning => group.seek_beginning(),
                        SeekTarget::End => group.seek_end(self.state.next_seq.saturating_sub(1)),
                    }
                    self.groups_dirty = true;
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err("Group not found".to_string()));
                }
            }

            TopicCommand::JoinGroup { group_id, client_id, reply } => {
                let group = self.groups.entry(group_id.clone())
                    .or_insert_with(|| ConsumerGroup::new(group_id.clone(), self.max_ack_pending, self.ack_wait));

                group.add_member(client_id.clone());

                let g_list = self.client_map.entry(client_id).or_default();
                if !g_list.contains(&group_id) {
                    g_list.push(group_id);
                }

                let _ = reply.send(Ok(group.ack_floor));
            }

            TopicCommand::LeaveGroup { client_id, reply } => {
                if let Some(g_ids) = self.client_map.remove(&client_id) {
                    for gid in g_ids {
                        if let Some(g) = self.groups.get_mut(&gid) {
                            g.remove_member(&client_id);
                        }
                    }
                }
                let _ = reply.send(());
            }

            TopicCommand::Read { from_seq, limit, reply } => {
                let mut msgs = self.state.read(from_seq, limit);
                // If hot read returned nothing, try cold read from segments
                if msgs.is_empty() {
                    msgs = self.cold_read(from_seq, limit).await;
                }
                let _ = reply.send(msgs);
            }

            TopicCommand::GetSnapshot { reply } => {
                let groups_info: Vec<_> = self.groups.values().map(|g| {
                    crate::dashboard::stream::ConsumerGroupSummary {
                        id: g.id.clone(),
                        ack_floor: g.ack_floor,
                        pending_count: g.pending.len(),
                    }
                }).collect();

                let summary = crate::dashboard::stream::TopicSummary {
                    name: self.state.name.clone(),
                    last_seq: self.state.next_seq.saturating_sub(1),
                    groups: groups_info,
                };
                let _ = reply.send(summary);
            }

            TopicCommand::Stop { reply } => {
                let _ = reply.send(());
                return true;
            }
        }

        false
    }

    // ==========================================
    // EMBEDDED WRITER
    // ==========================================

    async fn flush_to_disk(&mut self) {
        if self.write_buffer.is_empty() {
            return;
        }

        // Ensure file is open
        if self.log_file.is_none() {
            let (f, s) = Self::open_active_segment(&self.base_path, &self.segments).await;
            self.log_file = f;
            self.log_file_size = s;
        }

        if let Some(ref mut writer) = self.log_file {
            // Check segmentation BEFORE writing
            let buf_len = self.write_buffer.len() as u64;
            if self.log_file_size + buf_len > self.max_segment_size && self.log_file_size > 0 {
                // Rotate segment
                let _ = writer.flush().await;

                let new_seq = self.state.next_seq.saturating_sub(1); // approximate
                let new_path = self.base_path.join(format!("{}.log", new_seq));

                match File::create(&new_path).await {
                    Ok(new_file) => {
                        *writer = BufWriter::new(new_file);
                        self.log_file_size = 0;
                        let segment = Segment { path: new_path, start_seq: new_seq };
                        self.segments.push(segment);
                        info!("Rotated log for '{}' at seq {}", self.state.name, new_seq);
                    }
                    Err(e) => {
                        error!("Failed to create new segment: {}", e);
                    }
                }
            }

            if let Err(e) = writer.write_all(&self.write_buffer).await {
                error!("Failed to write to log: {}", e);
            } else {
                self.log_file_size += buf_len;
                self.state.persisted_seq = self.state.next_seq.saturating_sub(1);
            }

            if let Err(e) = writer.flush().await {
                error!("Failed to flush log: {}", e);
            }
        }

        self.write_buffer.clear();


    }

    async fn save_groups(&self) {
        let groups_map: HashMap<String, u64> = self.groups.iter()
            .map(|(id, g)| (id.clone(), g.ack_floor))
            .collect();

        if let Err(e) = save_groups_file(&self.base_path, &groups_map).await {
            error!("Failed to save groups for '{}': {}", self.state.name, e);
        }
    }

    async fn cold_read(&self, from_seq: u64, limit: usize) -> Vec<Message> {
        let segments = self.segments.clone();
        
        let mut all_msgs = Vec::new();
        let mut current_from_seq = from_seq;
        let mut remaining_limit = limit;

        // Find the index of the first segment to read from
        let first_seg_idx = segments.iter()
            .rposition(|s| s.start_seq <= from_seq);

        if let Some(idx) = first_seg_idx {
            for segment in segments.iter().skip(idx) {
                if remaining_limit == 0 { break; }
                let msgs = read_log_segment(&segment.path, current_from_seq, remaining_limit).await;
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

    async fn check_retention(&mut self) -> std::io::Result<()> {
        if self.retention.max_age_ms.is_none() && self.retention.max_bytes.is_none() {
            return Ok(());
        }

        let mut segments = match find_segments(&self.base_path).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Retention: Failed to list segments: {}", e);
                return Err(e);
            }
        };

        // Never delete the active segment (last one)
        if segments.len() <= 1 {
            return Ok(());
        }
        let _active = segments.pop();

        // Time retention
        if let Some(max_age) = self.retention.max_age_ms {
            let limit = std::time::SystemTime::now() - Duration::from_millis(max_age);
            let mut survivors = Vec::new();
            for seg in segments {
                let mut deleted = false;
                if let Ok(metadata) = tokio::fs::metadata(&seg.path).await {
                    if let Ok(modified) = metadata.modified() {
                        if modified < limit {
                            info!("Retention (Time): Deleting {:?}", seg.path);
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
        if let Some(max_bytes) = self.retention.max_bytes {
            let active_size = self.log_file_size;
            let mut current_total: u64 = active_size;
            for seg in &segments {
                if let Ok(meta) = tokio::fs::metadata(&seg.path).await {
                    current_total += meta.len();
                }
            }

            let mut i = 0;
            while current_total > max_bytes && i < segments.len() {
                let seg = &segments[i];
                if let Ok(meta) = tokio::fs::metadata(&seg.path).await {
                    let size = meta.len();
                    info!("Retention (Size): Deleting {:?} (Total {} > Max {})", seg.path, current_total, max_bytes);
                    if let Err(e) = tokio::fs::remove_file(&seg.path).await {
                        error!("Failed to delete {:?}: {}", seg.path, e);
                    } else {
                        current_total = current_total.saturating_sub(size);
                    }
                }
                i += 1;
            }
        }

        // Refresh segments index
        if let Ok(new_segments) = find_segments(&self.base_path).await {
            self.segments = new_segments;
            if let Some(first) = self.segments.first() {
                self.state.start_seq = first.start_seq;
            }
        }

        Ok(())
    }

    async fn open_active_segment(base_path: &PathBuf, segments: &[Segment]) -> (Option<BufWriter<File>>, u64) {
        let path = if let Some(last) = segments.last() {
            last.path.clone()
        } else {
            base_path.join("1.log") // first segment starts at seq 1
        };

        match OpenOptions::new().create(true).append(true).open(&path).await {
            Ok(file) => {
                let size = file.metadata().await.map(|m| m.len()).unwrap_or(0);
                (Some(BufWriter::new(file)), size)
            }
            Err(e) => {
                error!("Failed to open log file {:?}: {}", path, e);
                (None, 0)
            }
        }
    }
}

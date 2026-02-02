//! Topic: Pure Logic Struct (No Actors, No Channels)
//! Contains N partitions.
//! Now prepared for Actor usage (sync methods, pass-through logic).

use crate::brokers::stream::message::Message;
use std::collections::{VecDeque, BTreeMap};
use std::sync::{Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use tokio::sync::Notify;

use crate::brokers::stream::persistence::writer::{Segment, read_log_segment};
use crate::brokers::stream::persistence::types::PersistenceMode;
use crate::brokers::stream::commands::{StreamCreateOptions, RetentionOptions, PersistenceOptions};
use crate::config::SystemStreamConfig;

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub partitions: u32,
    pub persistence_mode: PersistenceMode,
    pub persistence_path: String,
    pub compaction_threshold: u64,
    pub max_segment_size: u64,
    pub retention: RetentionOptions,
    pub retention_check_ms: u64,
    pub max_ram_messages: usize,
    pub writer_channel_capacity: usize,
    pub writer_batch_size: usize,
    pub eviction_interval_ms: u64,
    pub eviction_batch_size: usize,
    pub ram_soft_limit: usize,
    pub ram_hard_limit: usize,
}

impl TopicConfig {
    pub fn from_options(opts: StreamCreateOptions, sys: &SystemStreamConfig) -> Self {
         let persistence_mode = match opts.persistence {
            Some(PersistenceOptions::Memory) => PersistenceMode::Memory,
            Some(PersistenceOptions::FileSync) => PersistenceMode::Sync,
            Some(PersistenceOptions::FileAsync) => PersistenceMode::Async {
                flush_ms: sys.default_flush_ms,
            },
            None => PersistenceMode::Async { flush_ms: sys.default_flush_ms },
        };
        
        let retention = match opts.retention {
             Some(r) => RetentionOptions {
                max_age_ms: r.max_age_ms.map_or(
                    Some(sys.default_retention_age_ms),
                    |v| if v == 0 { None } else { Some(v) }
                ),
                max_bytes: r.max_bytes.map_or(
                    Some(sys.default_retention_bytes),
                    |v| if v == 0 { None } else { Some(v) }
                ),
             },
             None => RetentionOptions {
                max_age_ms: Some(sys.default_retention_age_ms),
                max_bytes: Some(sys.default_retention_bytes),
             }
        };

        Self {
            partitions: opts.partitions.unwrap_or(sys.default_partitions),
            persistence_mode,
            persistence_path: sys.persistence_path.clone(),
            compaction_threshold: sys.compaction_threshold,
            max_segment_size: sys.max_segment_size,
            retention,
            retention_check_ms: sys.retention_check_interval_ms,
            max_ram_messages: sys.max_ram_messages,
            writer_channel_capacity: sys.writer_channel_capacity,
            writer_batch_size: sys.writer_batch_size,
            eviction_interval_ms: sys.eviction_interval_ms,
            eviction_batch_size: sys.eviction_batch_size,
            ram_soft_limit: sys.ram_soft_limit,
            ram_hard_limit: sys.ram_hard_limit,
        }
    }
}

pub struct TopicState {
    pub name: String,
    pub partitions: Vec<PartitionState>,
}

impl TopicState {
    pub fn new(name: String, partitions_count: u32, max_ram_messages: usize, soft_limit: usize, hard_limit: usize) -> Self {
        let partitions = (0..partitions_count)
            .map(|id| PartitionState::new(id, max_ram_messages, soft_limit, hard_limit))
            .collect();
        
        Self { name, partitions }
    }
    
    pub fn restore(name: String, partitions_count: u32, max_ram_messages: usize, soft_limit: usize, hard_limit: usize, mut data: std::collections::HashMap<u32, (VecDeque<Message>, Vec<Segment>)>) -> Self {
        let partitions = (0..partitions_count)
            .map(|id| {
                let mut p = PartitionState::new(id, max_ram_messages, soft_limit, hard_limit);
                if let Some((msgs, segments)) = data.remove(&id) {
                    p.segments = segments;
                    
                    // Recover Start Offset (Global) from segments
                    if let Some(first_segment) = p.segments.first() {
                        p.start_offset = first_segment.start_offset;
                    }

                    // Recover Next Offset
                    // If we have RAM messages, next_offset is last_ram_msg.offset + 1
                    // If we don't (empty active segment), next_offset is active_segment.start_offset
                    if let Some(last) = msgs.back() {
                        p.next_offset = last.offset + 1;
                        // RAM start offset depends on where RAM messages start
                        if let Some(first) = msgs.front() {
                            p.ram_start_offset = first.offset;
                        }
                    } else {
                        // No messages in RAM, maybe new segment or just rotated?
                        // Fallback to last segment start offset if exists
                        if let Some(last_seg) = p.segments.last() {
                            p.next_offset = last_seg.start_offset;
                            p.ram_start_offset = last_seg.start_offset;
                        }
                    }
                    
                    p.log = msgs;
                }
                p
            })
            .collect();
        
        Self { name, partitions }
    }

    pub fn publish(&mut self, partition_id: u32, payload: Bytes) -> (u64, u64) {
        if let Some(p) = self.partitions.get_mut(partition_id as usize) {
            p.append(payload)
        } else {
            (0, 0)
        }
    }

    pub fn read(&self, partition_id: u32, offset: u64, limit: usize) -> Vec<Message> {
        if let Some(p) = self.partitions.get(partition_id as usize) {
            p.read(offset, limit)
        } else {
            Vec::new()
        }
    }

    pub fn get_partitions_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn get_high_watermark(&self, partition_id: u32) -> u64 {
        if let Some(p) = self.partitions.get(partition_id as usize) {
            p.next_offset
        } else {
            0
        }
    }
}

pub struct PartitionState {
    pub id: u32,
    pub log: VecDeque<Message>,
    pub segments: Vec<Segment>, 
    pub next_offset: u64,       
    pub start_offset: u64,      
    pub ram_start_offset: u64, 
    pub max_ram_messages: usize,
    // for eviction log in RAM
    pub soft_limit: usize, // for hot read
    pub hard_limit: usize, // for burst
    pub persisted_offset: u64, //last log can clea
    
    waiters: BTreeMap<u64, Vec<Weak<Notify>>>,
}

impl PartitionState {
    pub fn new(id: u32, max_ram_messages: usize, soft_limit: usize, hard_limit: usize) -> Self {
        Self {
            id,
            log: VecDeque::new(),
            segments: Vec::new(),
            next_offset: 0,
            start_offset: 0,
            ram_start_offset: 0,
            max_ram_messages,
            soft_limit,
            hard_limit,
            persisted_offset: 0,
            waiters: BTreeMap::new(),
        }
    }

    pub fn append(&mut self, payload: Bytes) -> (u64, u64) {
        let offset = self.next_offset;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if self.log.is_empty() {
            self.ram_start_offset = offset;
        }

        self.log.push_back(Message {
            offset,
            timestamp,
            payload,
        });
        self.next_offset += 1;

        // Wake waiters logic
        let future_waiters = self.waiters.split_off(&(offset + 1));
        
        for (_, batch) in self.waiters.iter() {
            for weak in batch {
                if let Some(notify) = weak.upgrade() {
                    notify.notify_one();
                }
            }
        }
        
        self.waiters = future_waiters;
        
        (offset, timestamp)
    }

    pub fn read(&self, offset: u64, limit: usize) -> Vec<Message> {
        // 1. Check RAM
        if offset >= self.ram_start_offset && !self.log.is_empty() {
            let idx = (offset - self.ram_start_offset) as usize;
            if idx < self.log.len() {
                // HOT READ
                return self.log.iter().skip(idx).take(limit).cloned().collect();
            }
        }

        // 2. COLD READ (Fallback)
        // Scan segments index to find where `offset` lives
        for segment in &self.segments {
            // Un segmento copre da start_offset a... non sappiamo l'end esatto senza guardare il prossimo
            // Ma sappiamo che se offset >= segment.start_offset, POTREBBE essere qui.
            // Poiché segments è ordinato, cerchiamo il candidato migliore.
            // La logica semplice: se offset >= segment.start_offset, controlla se è < next_segment.start_offset
            
            if offset >= segment.start_offset {
                // Questo segmento è un candidato.
                // Controlliamo se siamo "oltre" questo segmento guardando il prossimo?
                // Oppure proviamo a leggere.
                // Optimization: Se abbiamo next_segment, e offset >= next.start, allora NON è in questo.
                
                // Per ora implementazione "Naive robusta":
                // Proviamo a leggere dal segmento che ha start_offset <= offset.
                // Se segments è ordinato ASC, iteriamo reverse o forward?
                // Iteriamo forward e teniamo l'ultimo valido.
            }
        }

        // Better Logic: Find the segment with max(start_offset) such that start_offset <= offset
        if let Some(segment) = self.segments.iter()
            .filter(|s| s.start_offset <= offset)
            .last() 
        {
            // Trovato il file che dovrebbe contenere l'offset.
            // Eseguiamo la lettura sincrona.
            let msgs = read_log_segment(&segment.path, offset, limit);
            if !msgs.is_empty() {
                return msgs;
            }
        }

        Vec::new()
    }
}

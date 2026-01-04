use std::collections::VecDeque;
use crate::brokers::stream::message::Message;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use tokio::sync::Notify;

pub struct Partition {
    pub id: u32,
    pub messages: VecDeque<Message>,
    pub start_offset: u64,
    pub next_offset: u64,
    pub new_data_notifier: Arc<Notify>,
}

impl Partition {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            messages: VecDeque::new(),
            start_offset: 0,
            next_offset: 0,
            new_data_notifier: Arc::new(Notify::new()),
        }
    }

    pub fn append(&mut self, payload: Bytes, key: Option<String>) -> u64 {
        let offset = self.next_offset;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let msg = Message {
            offset,
            timestamp,
            payload,
            key,
        };

        // TODO: Persistence Layer (AOL - Append Only Log)
        // Before pushing to memory, we should write to disk (or buffer it).
        // if let Err(e) = self.wal_writer.append(&msg) { return error; }

        self.messages.push_back(msg);
        self.next_offset += 1;
        
        // Notify waiters that new data is available
        self.new_data_notifier.notify_waiters();
        
        offset
    }

    pub fn read(&self, offset: u64, limit: usize) -> Vec<Message> {
        if offset < self.start_offset {
            // If requested data is too old (retention), start from the beginning of available data
            return self.read(self.start_offset, limit);
        }

        let relative_idx = if offset >= self.start_offset {
            (offset - self.start_offset) as usize
        } else {
            0
        };
        
        if relative_idx >= self.messages.len() {
            return Vec::new();
        }

        self.messages.range(relative_idx..).take(limit).cloned().collect()
    }
    
    pub fn high_watermark(&self) -> u64 {
        self.next_offset
    }
}


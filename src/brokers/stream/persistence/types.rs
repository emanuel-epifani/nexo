use tokio::sync::oneshot;
use bytes::Bytes;
use crate::brokers::stream::commands::PersistenceOptions;
use crate::config::Config;

#[derive(Debug, Clone)]
pub enum PersistenceMode {
    Memory,
    Sync,
    Async { flush_ms: u64 },
}

impl From<Option<PersistenceOptions>> for PersistenceMode {
    fn from(opts: Option<PersistenceOptions>) -> Self {
        match opts {
            Some(PersistenceOptions::Memory) => PersistenceMode::Memory,
            Some(PersistenceOptions::FileSync) => PersistenceMode::Sync,
            Some(PersistenceOptions::FileAsync) => PersistenceMode::Async {
                flush_ms: Config::global().stream.default_flush_ms,
            },
            None => PersistenceMode::Async {
                flush_ms: Config::global().stream.default_flush_ms,
            },
        }
    }
}

#[derive(Debug)]
pub enum StreamStorageOp {
    Append {
        partition: u32,
        offset: u64,
        timestamp: u64,
        payload: Bytes,
    },
    Commit {
        group: String,
        partition: u32,
        offset: u64,
        generation_id: u64,
    },
}

pub struct WriterCommand {
    pub op: StreamStorageOp,
    pub reply: Option<oneshot::Sender<Result<(), String>>>,
}

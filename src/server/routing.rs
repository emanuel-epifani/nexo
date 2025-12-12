//! Routing Layer: Command parsing + dispatching to managers

use crate::features::kv::KvManager;
use crate::server::protocol::{Args, Response};

// ========================================
// COMMAND ENUM
// ========================================

/// Represents all commands supported by Nexo
#[derive(Debug, Clone)]
pub enum Command {
    // Meta commands
    Ping,

    // KV commands
    KvSet {
        key: String,
        value: Vec<u8>,
        ttl: Option<u64>,
    },
    KvGet {
        key: String,
    },
    KvDel {
        key: String,
    },

    // Topic commands (not implemented yet)
    TopicSubscribe {
        topics: Vec<String>,
    },
    TopicPublish {
        topic: String,
        message: Vec<u8>,
    },
    TopicUnsubscribe {
        topics: Vec<String>,
    },

    // Queue commands (not implemented yet)
    QueuePush {
        queue: String,
        data: Vec<u8>,
    },
    QueuePop {
        queue: String,
    },
}

// ========================================
// COMMAND PARSING
// ========================================

impl Command {
    /// Parse command from RESP args (validates arguments).
    /// Binary-safe: payload arguments remain raw bytes.
    fn from_args(args: Args) -> Result<Self, String> {
        let cmd = args.cmd.as_slice();

        // Handle PING (no namespace)
        if eq_ignore_ascii_case(cmd, b"PING") {
            return Ok(Command::Ping);
        }

        // ===== KV Commands =====
        if eq_ignore_ascii_case(cmd, b"KV.SET") {
            if args.rest.len() < 2 {
                return Err("KV.SET requires at least 2 arguments: key value [ttl]".to_string());
            }
            let key = bytes_to_utf8_string(&args.rest[0], "key")?;
            let value = args.rest[1].clone();
            let ttl = if args.rest.len() >= 3 {
                Some(parse_u64_ascii(&args.rest[2], "ttl")?)
            } else {
                None
            };
            return Ok(Command::KvSet { key, value, ttl });
        }

        if eq_ignore_ascii_case(cmd, b"KV.GET") {
            if args.rest.len() != 1 {
                return Err("KV.GET requires exactly 1 argument: key".to_string());
            }
            let key = bytes_to_utf8_string(&args.rest[0], "key")?;
            return Ok(Command::KvGet { key });
        }

        if eq_ignore_ascii_case(cmd, b"KV.DEL") {
            if args.rest.len() != 1 {
                return Err("KV.DEL requires exactly 1 argument: key".to_string());
            }
            let key = bytes_to_utf8_string(&args.rest[0], "key")?;
            return Ok(Command::KvDel { key });
        }

        // ===== Topic Commands (not implemented) =====
        if eq_ignore_ascii_case(cmd, b"TOPIC.SUBSCRIBE") {
            if args.rest.is_empty() {
                return Err("TOPIC.SUBSCRIBE requires at least 1 topic".to_string());
            }
            let mut topics = Vec::with_capacity(args.rest.len());
            for t in args.rest {
                topics.push(bytes_to_utf8_string(&t, "topic")?);
            }
            return Ok(Command::TopicSubscribe { topics });
        }

        if eq_ignore_ascii_case(cmd, b"TOPIC.PUBLISH") {
            if args.rest.len() != 2 {
                return Err("TOPIC.PUBLISH requires 2 arguments: topic message".to_string());
            }
            let topic = bytes_to_utf8_string(&args.rest[0], "topic")?;
            let message = args.rest[1].clone();
            return Ok(Command::TopicPublish { topic, message });
        }

        if eq_ignore_ascii_case(cmd, b"TOPIC.UNSUBSCRIBE") {
            let mut topics = Vec::with_capacity(args.rest.len());
            for t in args.rest {
                topics.push(bytes_to_utf8_string(&t, "topic")?);
            }
            return Ok(Command::TopicUnsubscribe { topics });
        }

        // ===== Queue Commands (not implemented) =====
        if eq_ignore_ascii_case(cmd, b"QUEUE.PUSH") {
            if args.rest.len() != 2 {
                return Err("QUEUE.PUSH requires 2 arguments: queue data".to_string());
            }
            let queue = bytes_to_utf8_string(&args.rest[0], "queue")?;
            let data = args.rest[1].clone();
            return Ok(Command::QueuePush { queue, data });
        }

        if eq_ignore_ascii_case(cmd, b"QUEUE.POP") {
            if args.rest.len() != 1 {
                return Err("QUEUE.POP requires 1 argument: queue".to_string());
            }
            let queue = bytes_to_utf8_string(&args.rest[0], "queue")?;
            return Ok(Command::QueuePop { queue });
        }

        Err(format!("Unknown command: {}", String::from_utf8_lossy(cmd)))
    }
}

// ========================================
// DISPATCHER
// ========================================

/// Route parsed command to appropriate manager
pub fn route(args: Args, kv_manager: &KvManager) -> Result<Response, String> {
    // Parse and validate command
    let command = Command::from_args(args)?;

    // Route to appropriate handler
    match command {
        // ===== Meta Commands =====
        Command::Ping => Ok(Response::BulkString(b"PONG".to_vec())),

        // ===== KV Commands =====
        Command::KvSet { key, value, ttl } => {
            kv_manager.set(key, value, ttl)?;
            Ok(Response::Ok)
        }

        Command::KvGet { key } => match kv_manager.get(&key)? {
            Some(value) => Ok(Response::BulkString(value)),
            None => Ok(Response::Null),
        },

        Command::KvDel { key } => {
            let deleted = kv_manager.del(&key)?;
            Ok(Response::Integer(if deleted { 1 } else { 0 }))
        }

        // ===== Topic Commands =====
        Command::TopicSubscribe { .. } => Err("TOPIC manager not implemented yet".to_string()),

        Command::TopicPublish { .. } => Err("TOPIC manager not implemented yet".to_string()),

        Command::TopicUnsubscribe { .. } => Err("TOPIC manager not implemented yet".to_string()),

        // ===== Queue Commands  =====
        Command::QueuePush { .. } => Err("QUEUE manager not implemented yet".to_string()),

        Command::QueuePop { .. } => Err("QUEUE manager not implemented yet".to_string()),
    }
}

fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.to_ascii_uppercase() == y.to_ascii_uppercase())
}

fn bytes_to_utf8_string(bytes: &[u8], field: &str) -> Result<String, String> {
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|e| format!("Invalid UTF-8 for {}: {}", field, e))
}

fn parse_u64_ascii(bytes: &[u8], field: &str) -> Result<u64, String> {
    let s = std::str::from_utf8(bytes).map_err(|e| format!("Invalid UTF-8 for {}: {}", field, e))?;
    s.parse::<u64>().map_err(|e| format!("Invalid {}: {}", field, e))
}


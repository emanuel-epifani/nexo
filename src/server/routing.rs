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
        // Convert command to uppercase string for efficient matching
        // (Commands must be valid UTF-8, payloads can be arbitrary bytes)
        let cmd_str = std::str::from_utf8(&args.cmd)
            .map_err(|_| "Invalid command encoding")?
            .to_uppercase();

        // Use iterator to consume arguments without cloning
        let mut args_iter = args.rest.into_iter();

        match cmd_str.as_str() {
            // ===== Meta Commands =====
            "PING" => Ok(Command::Ping),

            // ===== KV Commands =====
            "KV.SET" => {
                if args_iter.len() < 2 {
                    return Err("KV.SET requires at least 2 arguments: key value [ttl]".to_string());
                }
                
                // Unwrapping is safe here because we checked len()
                let key_bytes = args_iter.next().unwrap();
                let value = args_iter.next().unwrap(); // Zero-copy move
                
                let key = bytes_to_utf8_string(&key_bytes, "key")?;

                let ttl = if let Some(ttl_bytes) = args_iter.next() {
                    Some(parse_u64_ascii(&ttl_bytes, "ttl")?)
                } else {
                    None
                };

                Ok(Command::KvSet { key, value, ttl })
            }

            "KV.GET" => {
                if args_iter.len() != 1 {
                    return Err("KV.GET requires exactly 1 argument: key".to_string());
                }
                let key_bytes = args_iter.next().unwrap();
                let key = bytes_to_utf8_string(&key_bytes, "key")?;
                Ok(Command::KvGet { key })
            }

            "KV.DEL" => {
                if args_iter.len() != 1 {
                    return Err("KV.DEL requires exactly 1 argument: key".to_string());
                }
                let key_bytes = args_iter.next().unwrap();
                let key = bytes_to_utf8_string(&key_bytes, "key")?;
                Ok(Command::KvDel { key })
            }

            // ===== Topic Commands (not implemented) =====
            "TOPIC.SUBSCRIBE" => {
                if args_iter.len() < 1 {
                    return Err("TOPIC.SUBSCRIBE requires at least 1 topic".to_string());
                }
                let mut topics = Vec::with_capacity(args_iter.len());
                for t in args_iter {
                    topics.push(bytes_to_utf8_string(&t, "topic")?);
                }
                Ok(Command::TopicSubscribe { topics })
            }

            "TOPIC.PUBLISH" => {
                if args_iter.len() != 2 {
                    return Err("TOPIC.PUBLISH requires 2 arguments: topic message".to_string());
                }
                let topic_bytes = args_iter.next().unwrap();
                let topic = bytes_to_utf8_string(&topic_bytes, "topic")?;
                let message = args_iter.next().unwrap();
                Ok(Command::TopicPublish { topic, message })
            }

            "TOPIC.UNSUBSCRIBE" => {
                let mut topics = Vec::with_capacity(args_iter.len());
                for t in args_iter {
                    topics.push(bytes_to_utf8_string(&t, "topic")?);
                }
                Ok(Command::TopicUnsubscribe { topics })
            }

            // ===== Queue Commands (not implemented) =====
            "QUEUE.PUSH" => {
                if args_iter.len() != 2 {
                    return Err("QUEUE.PUSH requires 2 arguments: queue data".to_string());
                }
                let queue_bytes = args_iter.next().unwrap();
                let queue = bytes_to_utf8_string(&queue_bytes, "queue")?;
                let data = args_iter.next().unwrap();
                Ok(Command::QueuePush { queue, data })
            }

            "QUEUE.POP" => {
                if args_iter.len() != 1 {
                    return Err("QUEUE.POP requires 1 argument: queue".to_string());
                }
                let queue_bytes = args_iter.next().unwrap();
                let queue = bytes_to_utf8_string(&queue_bytes, "queue")?;
                Ok(Command::QueuePop { queue })
            }

            _ => Err(format!("Unknown command: {}", cmd_str)),
        }
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

fn bytes_to_utf8_string(bytes: &[u8], field: &str) -> Result<String, String> {
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|e| format!("Invalid UTF-8 for {}: {}", field, e))
}

fn parse_u64_ascii(bytes: &[u8], field: &str) -> Result<u64, String> {
    let s = std::str::from_utf8(bytes).map_err(|e| format!("Invalid UTF-8 for {}: {}", field, e))?;
    s.parse::<u64>().map_err(|e| format!("Invalid {}: {}", field, e))
}


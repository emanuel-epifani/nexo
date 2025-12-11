//! Routing Layer: Command parsing + dispatching to managers

use crate::features::kv::KvManager;
use crate::server::protocol::Response;

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
    /// Parse command from RESP args (validates arguments)
    fn from_args(args: Vec<String>) -> Result<Self, String> {
        if args.is_empty() {
            return Err("Empty command".to_string());
        }

        let command = args[0].to_uppercase();

        // Handle PING (no namespace)
        if command == "PING" {
            return Ok(Command::Ping);
        }

        // Split namespace from action
        let parts: Vec<&str> = command.splitn(2, '.').collect();

        if parts.len() != 2 {
            return Err(format!("Invalid command format: {}", command));
        }

        let namespace = parts[0];
        let action = parts[1];

        match (namespace, action) {
            // ===== KV Commands =====
            ("KV", "SET") => {
                if args.len() < 3 {
                    return Err(
                        "KV.SET requires at least 2 arguments: key value [ttl]".to_string()
                    );
                }

                let key = args[1].clone();
                let value = args[2].as_bytes().to_vec();

                // Parse optional TTL (3rd argument)
                let ttl = if args.len() >= 4 {
                    Some(
                        args[3]
                            .parse::<u64>()
                            .map_err(|e| format!("Invalid TTL: {}", e))?,
                    )
                } else {
                    None
                };

                Ok(Command::KvSet { key, value, ttl })
            }

            ("KV", "GET") => {
                if args.len() != 2 {
                    return Err("KV.GET requires exactly 1 argument: key".to_string());
                }

                Ok(Command::KvGet {
                    key: args[1].clone(),
                })
            }

            ("KV", "DEL") => {
                if args.len() != 2 {
                    return Err("KV.DEL requires exactly 1 argument: key".to_string());
                }

                Ok(Command::KvDel {
                    key: args[1].clone(),
                })
            }

            // ===== Topic Commands (not implemented) =====
            ("TOPIC", "SUBSCRIBE") => {
                if args.len() < 2 {
                    return Err("TOPIC.SUBSCRIBE requires at least 1 topic".to_string());
                }
                Ok(Command::TopicSubscribe {
                    topics: args[1..].to_vec(),
                })
            }

            ("TOPIC", "PUBLISH") => {
                if args.len() != 3 {
                    return Err("TOPIC.PUBLISH requires 2 arguments: topic message".to_string());
                }
                Ok(Command::TopicPublish {
                    topic: args[1].clone(),
                    message: args[2].as_bytes().to_vec(),
                })
            }

            ("TOPIC", "UNSUBSCRIBE") => Ok(Command::TopicUnsubscribe {
                topics: if args.len() > 1 {
                    args[1..].to_vec()
                } else {
                    vec![]
                },
            }),

            // ===== Queue Commands (not implemented) =====
            ("QUEUE", "PUSH") => {
                if args.len() != 3 {
                    return Err("QUEUE.PUSH requires 2 arguments: queue data".to_string());
                }
                Ok(Command::QueuePush {
                    queue: args[1].clone(),
                    data: args[2].as_bytes().to_vec(),
                })
            }

            ("QUEUE", "POP") => {
                if args.len() != 2 {
                    return Err("QUEUE.POP requires 1 argument: queue".to_string());
                }
                Ok(Command::QueuePop {
                    queue: args[1].clone(),
                })
            }

            _ => Err(format!("Unknown command: {}.{}", namespace, action)),
        }
    }
}

// ========================================
// DISPATCHER
// ========================================

/// Route parsed command to appropriate manager
pub fn route(args: Vec<String>, kv_manager: &KvManager) -> Result<Response, String> {
    // Parse and validate command
    let command = Command::from_args(args)?;

    // Route to appropriate handler
    match command {
        // ===== Meta Commands =====
        Command::Ping => Ok(Response::Ok),

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

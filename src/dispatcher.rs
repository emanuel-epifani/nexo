//! Dispatcher: Routes commands to appropriate managers

use crate::command::Command;
use crate::kv::KvManager;
use crate::protocol::Response;

/// Dispatch parsed command to appropriate manager
pub fn dispatch(args: Vec<String>, kv_manager: &KvManager) -> Result<Response, String> {
    // Parse and validate command
    let command = Command::from_args(args)?;

    // Route to appropriate handler (LOG QUI per ogni case)
    match command {
        // ===== Meta Commands =====
        Command::Ping => {
            println!("[Dispatcher] Routing → PING handler (direct)");
            Ok(Response::Ok)
        }

        // ===== KV Commands =====
        Command::KvSet { key, value, ttl } => {
            println!("[Dispatcher] Routing → KV Manager (SET) - key={}, value_len={}, ttl={:?}",
                     key, value.len(), ttl);
            kv_manager.set(key, value, ttl)?;
            Ok(Response::Ok)
        }

        Command::KvGet { key } => {
            println!("[Dispatcher] Routing → KV Manager (GET) - key={}", key);
            match kv_manager.get(&key)? {
                Some(value) => Ok(Response::BulkString(value)),
                None => Ok(Response::Null),
            }
        }

        Command::KvDel { key } => {
            println!("[Dispatcher] Routing → KV Manager (DEL) - key={}", key);
            let deleted = kv_manager.del(&key)?;
            Ok(Response::Integer(if deleted { 1 } else { 0 }))
        }

        // ===== Topic Commands =====
        Command::TopicSubscribe { topics } => {
            println!("[Dispatcher] Routing → Topic Manager (SUBSCRIBE) - topics={:?}", topics);
            Err("TOPIC manager not implemented yet".to_string())
        }

        Command::TopicPublish { topic, message } => {
            println!("[Dispatcher] Routing → Topic Manager (PUBLISH) - topic={}, msg_len={}",
                     topic, message.len());
            Err("TOPIC manager not implemented yet".to_string())
        }

        Command::TopicUnsubscribe { topics } => {
            println!("[Dispatcher] Routing → Topic Manager (UNSUBSCRIBE) - topics={:?}", topics);
            Err("TOPIC manager not implemented yet".to_string())
        }

        // ===== Queue Commands  =====
        Command::QueuePush { queue, data } => {
            println!("[Dispatcher] Routing → Queue Manager (PUSH) - queue={}, data_len={}",
                     queue, data.len());
            Err("QUEUE manager not implemented yet".to_string())
        }

        Command::QueuePop { queue } => {
            println!("[Dispatcher] Routing → Queue Manager (POP) - queue={}", queue);
            Err("QUEUE manager not implemented yet".to_string())
        }
    }
}
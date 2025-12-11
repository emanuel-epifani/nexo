//! Dispatcher: Routes commands to appropriate managers

use crate::kv::KvManager;
use crate::protocol::Response;

pub fn dispatch(args: Vec<String>, kv_manager: &KvManager) -> Result<Response, String> {
    if args.is_empty() {
        return Err("Empty command".to_string());
    }

    let command = args[0].to_uppercase();

    // Handle PING (no namespace)
    if command == "PING" {
        return Ok(Response::Ok); // Will encode to +PONG via special case
    }

    // Split namespace from action
    let parts: Vec<&str> = command.splitn(2, '.').collect();

    if parts.len() != 2 {
        return Err(format!("Invalid command format: {}", command));
    }

    let namespace = parts[0];
    let action = parts[1];

    match namespace {
        "KV" => {
            println!("[Dispatcher] Command routing → KV Manager, action: {}", action);
            route_kv(action, &args[1..], kv_manager)
        }
        "TOPIC" => {
            println!("[Dispatcher] Command routing → Topic Manager, action: {}", action);
            //TODO: implement TOPIC manager
            Err("TOPIC manager not implemented yet".to_string())
        }
        "QUEUE" => {
            println!("[Dispatcher] Command routing → Queue Manager, action: {}", action);
            //TODO: implement QUEUE manager
            Err("QUEUE manager not implemented yet".to_string())
        }
        _ => Err(format!("Unknown namespace: {}", namespace)),
    }
}

fn route_kv(action: &str, args: &[String], kv_manager: &KvManager) -> Result<Response, String> {
    match action {
        "SET" => {
            if args.len() < 2 {
                return Err("KV.SET requires at least 2 arguments: key value [ttl]".to_string());
            }

            let key = args[0].clone();
            let value = args[1].as_bytes().to_vec();

            // Parse optional TTL (3rd argument)
            let ttl = if args.len() >= 3 {
                Some(
                    args[2]
                        .parse::<u64>()
                        .map_err(|e| format!("Invalid TTL: {}", e))?,
                )
            } else {
                None
            };

            kv_manager.set(key, value, ttl)?;
            Ok(Response::Ok)
        }

        "GET" => {
            if args.len() != 1 {
                return Err("KV.GET requires exactly 1 argument: key".to_string());
            }

            let key = &args[0];
            match kv_manager.get(key)? {
                Some(value) => Ok(Response::BulkString(value)),
                None => Ok(Response::Null),
            }
        }

        "DEL" => {
            if args.len() != 1 {
                return Err("KV.DEL requires exactly 1 argument: key".to_string());
            }

            let key = &args[0];
            let deleted = kv_manager.del(key)?;
            Ok(Response::Integer(if deleted { 1 } else { 0 }))
        }

        _ => Err(format!("Unknown KV action: {}", action)),
    }
}

use serde_json::Value;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionOutcome {
    Created,
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionResult<T> {
    pub outcome: ProvisionOutcome,
    pub definition: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerErrorKind {
    Internal,
    InvalidArgument,
    ResourceNotFound,
    ResourceConfigConflict,
    NotAuthorized,
    Fenced,
    NotMember,
    SlowConsumer,
    Storage,
}

#[derive(Debug, PartialEq)]
pub struct BrokerError {
    pub kind: BrokerErrorKind,
    pub message: String,
    pub details: Option<Value>,
}

impl BrokerError {
    pub fn new(kind: BrokerErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(BrokerErrorKind::InvalidArgument, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(BrokerErrorKind::ResourceNotFound, message)
    }

    pub fn config_conflict(message: impl Into<String>, details: Value) -> Self {
        Self::new(BrokerErrorKind::ResourceConfigConflict, message).with_details(details)
    }

    pub fn storage(message: impl Into<String>) -> Self {
        Self::new(BrokerErrorKind::Storage, message)
    }

    pub fn fenced() -> Self {
        Self::new(BrokerErrorKind::Fenced, "Consumer generation is fenced")
    }

    pub fn not_member() -> Self {
        Self::new(BrokerErrorKind::NotMember, "Consumer is not a group member")
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BrokerError {}

impl From<String> for BrokerError {
    fn from(message: String) -> Self {
        Self::new(BrokerErrorKind::Internal, message)
    }
}

impl From<&str> for BrokerError {
    fn from(message: &str) -> Self {
        Self::new(BrokerErrorKind::Internal, message)
    }
}

const MAX_RESOURCE_NAME_BYTES: usize = 255;

// Resource names map to filesystem paths, so '.', '..' and non-ASCII
// characters outside [A-Za-z0-9._-] are rejected for every broker.
pub(crate) fn validate_resource_name(kind: &str, name: &str) -> Result<(), BrokerError> {
    if name.is_empty() || name.len() > MAX_RESOURCE_NAME_BYTES {
        return Err(BrokerError::invalid_argument(format!(
            "Invalid {kind} name: length must be between 1 and {MAX_RESOURCE_NAME_BYTES} bytes"
        )));
    }
    if name == "."
        || name == ".."
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(BrokerError::invalid_argument(format!(
            "Invalid {kind} name: only ASCII letters, digits, '.', '_' and '-' are allowed"
        )));
    }
    Ok(())
}

fn collect_config_differences(path: &str, requested: &Value, actual: &Value, out: &mut Vec<Value>) {
    if let (Value::Object(requested_map), Value::Object(actual_map)) = (requested, actual) {
        for (key, requested_value) in requested_map {
            let child_path = format!("{path}.{key}");
            match actual_map.get(key) {
                Some(actual_value) => {
                    collect_config_differences(&child_path, requested_value, actual_value, out)
                }
                None => out.push(serde_json::json!({
                    "path": child_path,
                    "requested": requested_value,
                    "actual": Value::Null,
                })),
            }
        }
        for (key, actual_value) in actual_map {
            if !requested_map.contains_key(key) {
                out.push(serde_json::json!({
                    "path": format!("{path}.{key}"),
                    "requested": Value::Null,
                    "actual": actual_value,
                }));
            }
        }
    } else if requested != actual {
        out.push(serde_json::json!({
            "path": path,
            "requested": requested,
            "actual": actual,
        }));
    }
}

// `differences` is derived from the two wire-shape config JSONs so it cannot
// drift from `requested`/`actual`; each broker owns its own `config_json`.
pub(crate) fn config_conflict_error(
    kind: &str,
    name: &str,
    requested: Value,
    actual: Value,
) -> BrokerError {
    let mut differences = Vec::new();
    collect_config_differences("config", &requested, &actual, &mut differences);
    let display_kind = match kind.chars().next() {
        Some(first) => first.to_uppercase().collect::<String>() + &kind[1..],
        None => kind.to_string(),
    };
    BrokerError::config_conflict(
        format!("{display_kind} '{name}' already exists with different configuration"),
        serde_json::json!({
            "resourceKind": kind,
            "resourceName": name,
            "requested": requested,
            "actual": actual,
            "differences": differences,
        }),
    )
}

#[path = "pub-sub/mod.rs"]
pub mod pub_sub;
pub mod queue;
pub mod store;
pub mod stream;

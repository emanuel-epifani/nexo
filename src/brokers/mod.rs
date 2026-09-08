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

#[path = "pub-sub/mod.rs"]
pub mod pub_sub;
pub mod queue;
pub mod store;
pub mod stream;

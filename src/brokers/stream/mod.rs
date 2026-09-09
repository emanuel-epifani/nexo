pub mod config;
mod domain;
pub mod manager;
pub mod options;
pub mod tcp;

pub use domain::message::Message;
pub use domain::definition::{StreamConfig, StreamDefinition};
pub use domain::persistence::{recover_stream, serialize_message};
pub use manager::StreamManager;

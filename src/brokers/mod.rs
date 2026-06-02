pub mod store;
pub mod queue;
#[path = "pub-sub/mod.rs"]
pub mod pub_sub;
pub mod stream;

/// Neutral per-connection session identity, shared by all brokers.
/// Lives here (not inside a single broker) so no broker depends on another
/// just to reference the client/session that issued a command.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

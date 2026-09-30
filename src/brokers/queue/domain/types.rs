//! Queue domain primitives: on-disk scalar encodings and state constants.
//!
//! All counters/seq/timestamps are INTEGER columns: values are monotonic
//! counters starting at 1 or epoch milliseconds, so the signed 64-bit domain
//! is never exhausted in practice and byte-order encoding is unnecessary.

/// Single shared database file inside the queue persistence root.
pub const DB_FILE_NAME: &str = "queues.sqlite3";
/// Advisory process-ownership lock kept next to the database.
pub const LOCK_FILE_NAME: &str = "queues.lock";
/// On-disk schema version persisted in `schema_meta`.
pub const SCHEMA_VERSION: i64 = 1;

/// `messages.state`: deliverable (ordered by ready_idx).
pub const MSG_READY: i64 = 0;
/// `messages.state`: leased to a consumer (visible_at/delivery_token set).
pub const MSG_INFLIGHT: i64 = 1;

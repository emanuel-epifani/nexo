# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-02-10

### ⚠️ Breaking Changes
- **Protocol Upgrade**: Updated binary protocol structure. Requires Nexo Server v0.2.0+.

### 🚀 Features
- **Dead Letter Queue (DLQ) Management**:
    - Added `queue.dlq.peek(limit)`: Inspect failed messages.
    - Added `queue.dlq.moveToQueue(id)`: Replay failed messages back to the main queue.
    - Added `queue.dlq.delete(id)`: Permanently remove a message from DLQ.
    - Added `queue.dlq.purge()`: Clear all messages from DLQ.
- **Robust Reconnection**:
    - Implemented deterministic reconnection logic for Queue and Stream consumers.
    - Fixed edge cases where "stale waiters" could steal messages after a client crash.
    - Removed non-deterministic jitter in favor of robust server-side cleanup.

### 🐛 Bug Fixes
- Fixed race conditions in `reconnection.test.ts`.
- Improved error handling during network instability.


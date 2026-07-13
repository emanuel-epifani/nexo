# Queue

**Durable FIFO buffer with acknowledgments.** Essential for load leveling and reliable background processing — video transcoding, email sending, order processing.

## Basic Usage

```typescript
// Create queue
const mailQ = await client.queue<MailJob>("emails").create();

// Push message
await mailQ.push({ to: "test@test.com" });

// Subscribe (auto-ACK on success)
await mailQ.subscribe((msg) => console.log(msg));

// Delete queue
await mailQ.delete();
```

## Persistence

All queues are **persisted to disk** by default using a Write-Ahead Log (WAL) backed by SQLite. To maximize throughput and performance, Nexo uses an **asynchronous flush strategy** for all queues. Writes are buffered in memory and flushed to disk periodically.

By default, the server flushes data to disk every **100ms**. This interval is globally configurable via the `QUEUE_DEFAULT_FLUSH_MS` environment variable (see [Configuration](#configuration) below).

## Advanced Creation

Configure reliability and timeout settings:

```typescript
const criticalQueue = await client.queue<CriticalTask>('critical-tasks').create({
  // RELIABILITY
  visibilityTimeoutMs: 10000,  // Retry if not ACKed within 10s (default: 30s)
  maxRetries: 5,               // Move to DLQ after 5 failures (default: 5)
});
```

## Priority

```typescript
// PRIORITY: Higher value = delivered first (0-255)
await criticalQueue.push({ type: 'urgent' }, { priority: 255 });
```

## Consumer Tuning

Queues are **pull-based**: the SDK continuously polls the server for new messages in a loop, processes them, and polls again. The server never pushes messages to the client. Three parameters control this behavior:

### `batchSize` (default: 50)

How many messages the SDK fetches from the server **in a single network request**. Higher values reduce round-trips but increase memory usage per cycle.

### `waitMs` (default: 20000)

When the queue is **empty**, the server holds the connection open for up to `waitMs` milliseconds before responding with an empty result (long-polling). This avoids the client hammering the server with tight empty loops. If a message arrives during the wait, the server responds immediately.

### `concurrency` (default: 5)

How many messages are processed **in parallel** within a single batch. This is useful when your callback involves I/O (HTTP calls, DB writes) — Node.js is single-threaded for CPU, but can run multiple async I/O operations concurrently.

::: tip FIFO Ordering
With `concurrency: 1`, messages are processed **strictly in order** (true FIFO). With `concurrency > 1`, messages are still *fetched* in FIFO order, but since each callback may take a different amount of time, the **completion order is not guaranteed**. Use `concurrency: 1` when ordering matters.
:::

```typescript
await criticalQueue.subscribe(
  async (task) => { await processTask(task); },
  {
    batchSize: 100,    // Fetch 100 messages per network request
    concurrency: 10,   // Process 10 messages concurrently (I/O-bound tasks)
    waitMs: 5000       // If empty, wait 5s (server-side) before responding
  }
);
```

## Dead Letter Queue (DLQ)

Every queue automatically has a **dedicated DLQ**. When a message exceeds `maxRetries` (default: 5), it's moved to the DLQ automatically — no setup needed.

Since DLQs are created alongside their parent queue, you can inspect failed messages at any time via `queue.dlq`.

### Inspect Failed Messages

```typescript
const failedMessages = await criticalQueue.dlq.peek(10);
console.log(`Found ${failedMessages.total} failed messages`);

for (const msg of failedMessages.items) {
  console.log(`Message ${msg.id}: attempts=${msg.attempts}, reason=${msg.failureReason}`);
  console.log(`Payload:`, msg.data);
}
```

### Replay or Discard

```typescript
// Replay: move back to main queue (resets attempts to 0)
const moved = await criticalQueue.dlq.moveToQueue(msg.id);

// Discard: permanently delete from DLQ
const deleted = await criticalQueue.dlq.delete(msg.id);

// Purge: clear all DLQ messages
const purgedCount = await criticalQueue.dlq.purge();
```

### API Reference

| Method | Description | Returns |
|:---|:---|:---|
| `peek(limit, offset)` | Inspect messages without removing them | `{ total, items[] }` |
| `moveToQueue(messageId)` | Replay message to main queue (resets attempts) | `boolean` |
| `delete(messageId)` | Permanently remove a single message | `boolean` |
| `purge()` | Remove all messages from DLQ | `number` (count) |

## Configuration

### How it works

1. Server starts → reads env vars (global defaults)
2. Queue created → server snapshots defaults into `config.json` (per-queue)
3. SDK can override `visibilityTimeoutMs` and `maxRetries` at creation — everything else uses system defaults
4. On restart → each queue reads its own `config.json` (ignores current env vars)

> **Existing queues are not affected by env var changes.** Only new queues pick up new defaults.

### Environment Variables

Global, set at server startup.

| Variable | Default | Description |
|:---|:---|:---|
| `QUEUE_ROOT_PERSISTENCE_PATH` | `./data/queues` | Base directory for all queue SQLite DBs |
| `QUEUE_VISIBILITY_MS` | `30000` (30s) | Default visibility timeout — how long before an unacked message is redelivered |
| `QUEUE_MAX_RETRIES` | `5` | Default max delivery attempts before moving to DLQ |
| `QUEUE_DEFAULT_BATCH_SIZE` | `10` | Default batch size for server-side consume |
| `QUEUE_DEFAULT_WAIT_MS` | `0` | Default long-polling wait (ms) when queue is empty |
| `QUEUE_DEFAULT_FLUSH_MS` | `100` | Max durability window (ms) — how often writes are flushed to disk |
| `QUEUE_WRITER_BATCH_SIZE` | `50000` | SQLite writer batch size (internal tuning) |

### Per-Queue (`config.json`)

Persisted at queue creation, read on restart.

| Field | From | SDK override? |
|:---|:---|:---|
| `visibility_timeout_ms` | SDK or system default | **Yes** |
| `max_retries` | SDK or system default | **Yes** |
| `default_batch_size` | System default | No |
| `default_wait_ms` | System default | No |
| `default_flush_ms` | System default | No |
| `writer_batch_size` | System default | No |

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

All queues are **persisted to disk** by default using a Write-Ahead Log (WAL) backed by SQLite. Two modes are available:

| Mode | Behavior | Trade-off |
|:---|:---|:---|
| `file_async` **(default)** | Buffers writes and flushes every **200ms** or every **50k messages** (whichever comes first) | Fast & durable — best for most workloads |
| `file_sync` | Flushes to disk on **every single message** | Safest, but slower — use for critical data where zero loss is required |

In `file_async` mode, if the server crashes between flush intervals, you may lose up to 200ms of messages. For most use cases this is an acceptable trade-off given the significant throughput improvement.

## Advanced Creation

Configure reliability, persistence, and timeout settings:

```typescript
const criticalQueue = await client.queue<CriticalTask>('critical-tasks').create({
  // RELIABILITY
  visibilityTimeoutMs: 10000,  // Retry if not ACKed within 10s (default: 30s)
  maxRetries: 5,               // Move to DLQ after 5 failures (default: 5)
  ttlMs: 60000,                // Expires if not consumed in 60s (default: 7 days)

  // PERSISTENCE (see above)
  persistence: 'file_sync',
});
```

## Priority & Scheduling

```typescript
// PRIORITY: Higher value = delivered first (0-255)
await criticalQueue.push({ type: 'urgent' }, { priority: 255 });

// SCHEDULING: Delay visibility by 1 hour
await criticalQueue.push({ type: 'scheduled' }, { delayMs: 3600000 });
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
    waitMs: 5000       // If empty, wait 5s before retrying
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

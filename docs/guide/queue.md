# Queue

**Durable FIFO buffer with acknowledgments.** Essential for load leveling and reliable background processing — video transcoding, email sending, order processing.

## Basic Usage

Provision the durable queue before deploying application workers:

::: code-group

```typescript
const result = await client.queue.create('emails');
console.log(result.status, result.definition.config);
```

```python
result = await client.queue.create("emails")
print(result.status, result.definition.config)
```

:::

Application code retrieves the existing queue and fails fast when provisioning is missing:

::: code-group

```typescript
const mailQueue = await client.queue.get<MailJob>('emails');
await mailQueue.push({ to: 'test@test.com' });
const subscription = await mailQueue.subscribe((message) => console.log(message));
await subscription.stop();
```

```python
mail_queue: NexoQueue[MailJob] = await client.queue.get("emails")
await mail_queue.push({"to": "test@test.com"})

async def handle_email(message: MailJob) -> None:
    print(message)

subscription = await mail_queue.subscribe(handle_email)
await subscription.stop()
```

:::

Delete the resource from an administrative process with `client.queue.delete("emails")`.

## Persistence

All queues are **persisted to disk** by default using a Write-Ahead Log (WAL) backed by SQLite. To maximize throughput and performance, Nexo uses an **asynchronous flush strategy** for all queues. Writes are buffered in memory and flushed to disk periodically.

By default, the server flushes data to disk every **100ms**. This interval is globally configurable via the `QUEUE_DEFAULT_FLUSH_MS` environment variable (see [Configuration](#configuration) below).

## Advanced Creation

Configure reliability and timeout settings:

::: code-group

```typescript
const result = await client.queue.create('critical-tasks', {
  // RELIABILITY
  visibilityTimeoutMs: 10000,  // Retry if not ACKed within 10s (default: 30s)
  maxDeliveries: 5,            // Move to DLQ after 5 failed deliveries (default: 5)
});
console.log(result.status, result.definition.config);
```

```python
result = await client.queue.create(
    "critical-tasks",
    visibility_timeout_ms=10000,  # Retry if not ACKed within 10s (default: 30s)
    max_deliveries=5,             # Move to DLQ after 5 failed deliveries (default: 5)
)
print(result.status, result.definition.config)
```

:::

`create()` returns `status: "created" | "unchanged"` plus the complete effective configuration. Repeating it with an equivalent configuration is safe; a different configuration raises `ResourceConfigurationConflictError`. Use `client.queue.describe(name)` to inspect the authoritative configuration without provisioning.

## Priority

::: code-group

```typescript
const criticalQueue = await client.queue.get<CriticalTask>('critical-tasks');

// PRIORITY: Higher value = delivered first (0-255)
await criticalQueue.push({ type: 'urgent' }, { priority: 255 });
```

```python
critical_queue: NexoQueue[CriticalTask] = await client.queue.get("critical-tasks")

# PRIORITY: Higher value = delivered first (0-255)
await critical_queue.push({"type": "urgent"}, priority=255)
```

:::

## Batch Push

Push multiple messages in a single network request. Reduces round-trip overhead and improves throughput when producing bursts of messages.

::: code-group

```typescript
await mailQ.pushBatch([
  { data: { to: 'user1@example.com' } },
  { data: { to: 'user2@example.com' } },
  { data: { to: 'user3@example.com' }, options: { priority: 10 } },
]);
```

```python
await mail_q.push_batch([
    {"data": {"to": "user1@example.com"}},
    {"data": {"to": "user2@example.com"}},
    {"data": {"to": "user3@example.com"}, "priority": 10},
])
```

:::

Each item can have its own `priority`. The server processes all items atomically under a single lock, then notifies consumers once.

## Consumer Tuning

Queues are **pull-based**: the SDK continuously polls the server for new messages in a loop, processes them, and polls again. The server never pushes messages to the client. Three parameters control this behavior:

### `batchSize` (default: 50)

How many messages the SDK fetches from the server **in a single network request**. Higher values reduce round-trips but increase memory usage per cycle.

::: tip Choosing batchSize vs concurrency
The SDK fetches `batchSize` messages per request and processes them with `concurrency` parallel workers. If `batchSize` is much larger than `concurrency`, some messages may sit in the client's local buffer waiting for a worker. The server marks each delivered message with a visibility timeout (set at queue creation via `visibilityTimeoutMs`). If a message waits too long in the buffer, its timeout expires and the server redelivers it to another consumer — resulting in duplicate processing.

This is safe (Nexo guarantees at-least-once delivery), but wasteful. As a rule of thumb: if your callbacks are fast, `batchSize` can be much larger than `concurrency`. If your callbacks are slow or your visibility timeout is short, keep `batchSize` close to `concurrency`.
:::

### `waitMs` (default: 20000)

When the queue is **empty**, the server holds the connection open for up to `waitMs` milliseconds before responding with an empty result (long-polling). This avoids the client hammering the server with tight empty loops. If a message arrives during the wait, the server responds immediately.

### `concurrency` (default: 5)

How many messages are processed **in parallel** within a single batch. This is useful when your callback involves I/O (HTTP calls, DB writes) — Node.js is single-threaded for CPU, but can run multiple async I/O operations concurrently.

::: tip FIFO Ordering
With `concurrency: 1`, messages are processed **strictly in order** (true FIFO). With `concurrency > 1`, messages are still *fetched* in FIFO order, but since each callback may take a different amount of time, the **completion order is not guaranteed**. Use `concurrency: 1` when ordering matters.
:::

::: code-group

```typescript
await criticalQueue.subscribe(
  async (task) => { await processTask(task); },
  {
    batchSize: 100,    // Request up to 100 messages per network request
    concurrency: 10,   // Process 10 messages concurrently (I/O-bound tasks)
    waitMs: 5000       // If empty, wait 5s (server-side) before responding
  }
);
```

```python
async def handle_task(task: CriticalTask) -> None:
    print(task)

await critical_queue.subscribe(
    handle_task,
    {
        "batch_size": 100,    # Request up to 100 messages per network request
        "concurrency": 10,    # Process 10 messages concurrently (I/O-bound tasks)
        "wait_ms": 5000       # If empty, wait 5s (server-side) before responding
    }
)
```

:::

## Dead Letter Queue (DLQ)

Every queue automatically has a **dedicated DLQ**. When a message exceeds `maxDeliveries` (default: 5), it's moved to the DLQ automatically — no setup needed.

Since DLQs are created alongside their parent queue, you can inspect failed messages at any time via `queue.dlq`.

### Inspect Failed Messages

::: code-group

```typescript
const failedMessages = await criticalQueue.dlq.peek(10);
console.log(`Found ${failedMessages.total} failed messages`);

for (const msg of failedMessages.items) {
  console.log(`Message ${msg.id}: attempts=${msg.attempts}, reason=${msg.failureReason}`);
  console.log(`Payload:`, msg.data);
}
```

```python
failed_messages = await critical_queue.dlq.peek(10)
print(f"Found {failed_messages['total']} failed messages")

for msg in failed_messages["items"]:
    print(f"Message {msg['id']}: attempts={msg['attempts']}, reason={msg['failure_reason']}")
    print(f"Payload: {msg['data']}")
```

:::

### Replay or Discard

::: code-group

```typescript
// Replay: move back to main queue (resets attempts to 0)
const moved = await criticalQueue.dlq.replay(msg.id);

// Discard: permanently delete from DLQ
const deleted = await criticalQueue.dlq.delete(msg.id);

// Purge: clear all DLQ messages
const purgedCount = await criticalQueue.dlq.purge();
```

```python
# Replay: move back to main queue (resets attempts to 0)
moved = await critical_queue.dlq.replay(msg["id"])

# Discard: permanently delete from DLQ
deleted = await critical_queue.dlq.delete(msg["id"])

# Purge: clear all DLQ messages
purged_count = await critical_queue.dlq.purge()
```

:::

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
3. SDK can override `visibilityTimeoutMs` and `maxDeliveries` at creation — everything else uses system defaults
4. On restart → each queue reads its own `config.json` (ignores current env vars)

> **Existing queues are not affected by env var changes.** Only new queues pick up new defaults.

### Environment Variables

Global, set at server startup.

| Variable | Default | Description |
|:---|:---|:---|
| `QUEUE_ROOT_PERSISTENCE_PATH` | `./data/queues` | Base directory for all queue SQLite DBs |
| `QUEUE_VISIBILITY_MS` | `30000` (30s) | Default visibility timeout — how long before an unacked message is redelivered |
| `QUEUE_MAX_DELIVERIES` | `5` | Default max delivery attempts before moving to DLQ |
| `QUEUE_DEFAULT_BATCH_SIZE` | `10` | Default batch size for server-side consume |
| `QUEUE_DEFAULT_WAIT_MS` | `0` | Default long-polling wait (ms) when queue is empty |
| `QUEUE_DEFAULT_FLUSH_MS` | `100` | Max durability window (ms) — how often writes are flushed to disk |
| `QUEUE_WRITER_BATCH_SIZE` | `50000` | SQLite writer batch size (internal tuning) |

### SDK Overrides

Fields settable at `create()` time. If omitted, system defaults apply.

| Field | SDK option | System default (env var) |
|:---|:---|:---|
| Visibility timeout | `visibilityTimeoutMs` | `30000` (`QUEUE_VISIBILITY_MS`) |
| Max deliveries | `maxDeliveries` | `5` (`QUEUE_MAX_DELIVERIES`) |

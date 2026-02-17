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

## Advanced Creation

Configure reliability, persistence, and timeout settings:

```typescript
const criticalQueue = await client.queue<CriticalTask>('critical-tasks').create({
  // RELIABILITY
  visibilityTimeoutMs: 10000,  // Retry if not ACKed within 10s (default: 30s)
  maxRetries: 5,               // Move to DLQ after 5 failures (default: 5)
  ttlMs: 60000,                // Expires if not consumed in 60s (default: 7 days)

  // PERSISTENCE
  // 'memory'     → Volatile (Fastest, lost on restart)
  // 'file_sync'  → Save every message (Safest, Slowest)
  // 'file_async' → Flush periodically (Fast & Durable) — DEFAULT
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

```typescript
await criticalQueue.subscribe(
  async (task) => { await processTask(task); },
  {
    batchSize: 100,    // Fetch 100 messages per network request
    concurrency: 10,   // Process 10 messages concurrently
    waitMs: 5000       // If empty, wait 5s before retrying
  }
);
```

## Performance

**160k ops/sec** with 3µs latency for persisted PUSH operations.

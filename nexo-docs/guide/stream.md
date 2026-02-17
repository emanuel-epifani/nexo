# Stream

**Append-only immutable log with offset tracking.** The source of truth for your system's history — Event Sourcing, audit trails, replaying historical data.

## Basic Usage

```typescript
// Create stream
const stream = await client.stream<UserEvent>('user-events').create();

// Publish event
await stream.publish({ type: 'login', userId: 'u1' });

// Subscribe with consumer group
await stream.subscribe('analytics', (msg) => { console.log(`User ${msg.userId} performed ${msg.type}`); });

// Delete stream
await stream.delete();
```

## Advanced Creation

```typescript
const orders = await client.stream<Order>('orders').create({
  // SCALING
  partitions: 4,  // Max concurrent consumers per group (default: 8)

  // PERSISTENCE
  persistence: 'file_sync',  // 'memory' | 'file_sync' | 'file_async'

  // RETENTION (Cleanup Policy)
  retention: {
    maxAgeMs: 86400000,    // 1 Day (Default: 7 Days)
    maxBytes: 536870912    // 512 MB (Default: 1 GB)
  },
});
```

## Consumer Patterns

### Scaling (Load Balancing)

Same group name → automatic load balancing across consumers.

```typescript
// K8s pods / microservice replicas
await orders.subscribe('workers', (order) => process(order));
await orders.subscribe('workers', (order) => process(order));
```

### Broadcast (Independent Domains)

Different groups → each gets a full copy of the stream.

```typescript
await orders.subscribe('analytics', (order) => trackMetrics(order));
await orders.subscribe('audit-log', (order) => saveAudit(order));
```

## Features

- **Immutable History:** Events are strictly appended and never modified.
- **Consumer Groups:** Maintains separate read cursors for different consumers.
- **Replayability:** Consumers can rewind their offset to re-process from any point.

## Performance

**650k ops/sec** with 1µs latency for persisted PUBLISH operations.

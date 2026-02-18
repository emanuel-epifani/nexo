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

## Persistence

All streams are **persisted to disk** by default using an append-only segment file format. Two modes are available:

| Mode | Behavior | Trade-off |
|:---|:---|:---|
| `file_async` **(default)** | Buffers writes and flushes every **50ms** or every **5k messages** (whichever comes first) | Fast & durable — best for most workloads |
| `file_sync` | Flushes to disk on **every single message** | Safest, but slower — use for critical data where zero loss is required |

## Retention

Streams grow indefinitely by default (up to the retention limits). When **either** limit is reached, old segments are deleted:

| Setting | Default | Description |
|:---|:---|:---|
| `maxAgeMs` | **7 days** (604800000ms) | Delete segments older than this |
| `maxBytes` | **1 GB** (1073741824 bytes) | Delete oldest segments when total size exceeds this |

Retention is checked periodically (every 10 minutes by default). Both limits can be set independently — whichever is reached first triggers cleanup.

## Partitions

When you create a stream, it's divided into **partitions** (default: 4). Think of them as independent lanes inside the stream — each partition holds a portion of the messages and maintains its own order.

**Why do you need them?** Unlike a Queue (where each message is consumed and removed), Stream messages are **immutable** — they stay in the log. Without partitions, every consumer in a group would read the exact same messages:

```
  ❌ Without partitions — all consumers read the same data:

  Stream ──▶ [msg1, msg2, msg3, msg4, msg5, ...]  ──▶ Consumer A reads ALL
                                                  ──▶ Consumer B reads ALL  (duplicate work!)
                                                  ──▶ Consumer C reads ALL  (duplicate work!)
```

Partitions fix this. The server assigns each partition to **one consumer only**, so the work is split with no duplicates:

```
  ✅ With 4 partitions — work is split automatically:

  Stream    ┌─ P0 [msg1, msg5, ...] ──▶ Consumer A
            ├─ P1 [msg2, msg6, ...] ──▶ Consumer B
            ├─ P2 [msg3, msg7, ...] ──▶ Consumer C
            └─ P3 [msg4, msg8, ...] ──▶ Consumer A  (gets 2 partitions)
```

When a new consumer joins or one disconnects, the server **automatically rebalances** — redistributing partitions among the remaining consumers. No manual work needed.

### Partition Keys

By default, messages are spread across partitions in round-robin. This means two events for the same user might end up in different partitions, processed by different consumers, potentially **out of order**.

If ordering matters for a specific entity, use a **partition key**:

```typescript
// Without key: round-robin (no ordering guarantee)
await stream.publish({ type: 'login', userId: 'u1' });

// With key: all events for 'u1' always go to the same partition
await stream.publish({ type: 'login', userId: 'u1' }, { key: 'u1' });
await stream.publish({ type: 'purchase', userId: 'u1' }, { key: 'u1' });
// ↑ These two are guaranteed to be processed in this order
```

### Choosing the Right Count

The partition count equals the **maximum number of consumers** that can work in parallel within a group. If you have 4 partitions and 6 consumers, 2 will sit idle with nothing to process.

::: warning Fixed at creation
Partitions cannot be changed after a stream is created. To change the count, you must delete and recreate the stream.
:::

## Advanced Creation

```typescript
const orders = await client.stream<Order>('orders').create({
  partitions: 4,             // Max concurrent consumers per group (default: 4)
  persistence: 'file_sync',  // See Persistence section above
  retention: {
    maxAgeMs: 86400000,      // 1 Day
    maxBytes: 536870912      // 512 MB
  },
});
```

## Consumer Groups

Every consumer subscribes to a stream through a **group name**. The group name determines how messages are distributed:

- **Same group** = work is split across consumers (each message processed once)
- **Different groups** = each group reads all messages independently

### Scaling: Same Group

You have an order processing service and a single instance can't keep up. You deploy 3 replicas — all subscribe with the **same group name**. The server splits the partitions between them, so each order is processed exactly once.

```typescript
// Pod-1, Pod-2, Pod-3 — same group, work is split
await orders.subscribe('workers', (order) => process(order));
await orders.subscribe('workers', (order) => process(order));
await orders.subscribe('workers', (order) => process(order));
```

### Broadcast: Different Groups

You need analytics **and** an audit log for the same orders. These are two independent services that both need to see **every** order. Each subscribes with a **different group name**, so each gets a full copy of the stream.

```typescript
// Two independent services, each reads ALL orders
await orders.subscribe('analytics', (order) => trackMetrics(order));
await orders.subscribe('audit-log', (order) => saveAudit(order));
```

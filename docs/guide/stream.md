# Stream

**Append-only immutable log with per-message acknowledgment.** The source of truth for your system's history — Event Sourcing, audit trails, replaying historical data.

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
| `file_async` **(default)** | Buffers writes and flushes every **50ms** | Fast & durable — best for most workloads |
| `file_sync` | Flushes to disk on **every single message** | Safest, but slower — use for critical data where zero loss is required |

## Retention

Streams grow indefinitely by default (up to the retention limits). When **either** limit is reached, old segments are deleted:

| Setting | Default | Description |
|:---|:---|:---|
| `maxAgeMs` | **7 days** (604800000ms) | Delete segments older than this |
| `maxBytes` | **1 GB** (1073741824 bytes) | Delete oldest segments when total size exceeds this |

Retention is checked periodically (every 10 minutes by default). Both limits can be set independently — whichever is reached first triggers cleanup.

## Acknowledgments

Each message delivered to a consumer must be **acknowledged** (acked) or **negative-acknowledged** (nacked):

- **Ack** → message is confirmed as processed. The consumer group's `ack_floor` advances past contiguous acked messages.
- **Nack** → message is put back in the redelivery queue and will be delivered again on the next fetch.
- **Timeout** → if a message is not acked within the `ack_wait` period (default: 30 seconds), it is automatically redelivered.

Acks are handled automatically by the SDK's `subscribe()` method:
- If your callback completes successfully → **ack**
- If your callback throws an error → **nack** (message will be redelivered)

```
  Message Lifecycle:
  
  Published → Pending (delivered to consumer) → Acked ✓
                                              → Nacked → Redelivered
                                              → Timeout → Redelivered
```

## Seek

You can reset a consumer group's position in the stream:

```typescript
// Replay all messages from the beginning
await stream.seek('my-group', 'beginning');

// Skip to the end (ignore all existing messages)
await stream.seek('my-group', 'end');
```

## Advanced Creation

```typescript
const orders = await client.stream<Order>('orders').create({
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

You have an order processing service and a single instance can't keep up. You deploy 3 replicas — all subscribe with the **same group name**. The server delivers different messages to each consumer, so each order is processed exactly once.

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

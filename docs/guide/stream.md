# Stream

**Append-only immutable log with per-message acknowledgment.** The source of truth for your system's history — Event Sourcing, audit trails, replaying historical data.

Stream consumers are **pull-based with long-polling**: the SDK polls the server in a loop, and when there are no new messages the server holds the connection open until a message arrives or the timeout expires. This means latency is near-zero when messages are available, with no tight busy-loop overhead when the stream is idle.

## Basic Usage

```typescript
// Create stream
const stream = await client.stream<UserEvent>('user-events').create();

// Publish event
await stream.publish({ type: 'login', userId: 'u1' });

// Subscribe with consumer group
await stream.subscribe('analytics', (msg) => { console.log(`User ${msg.userId} performed ${msg.type}`); });
```

---

## The Scaling Model

Nexo abandons the traditional "Kafka-style" partitioning model in favor of a **Virtual Distributed Queue**. 

### The Problem with Partitions
In Kafka, concurrency is tied to the number of partitions. If you have 3 partitions, you can only have 3 active consumers in a group. Adding a 4th consumer does nothing; it stays idle.

### The Nexo Way: Dynamic Fan-Out
Nexo streams are single, unified logs. The broker dynamically coordinates message delivery to any number of consumers in a group. You can scale from 1 to 100 consumers at runtime without repartitioning or restarting.

```text
KAFKA (Static)                      NEXO (Dynamic)
┌──────────────────────────┐        ┌──────────────────────────┐
│ Topic: [P0] [P1] [P2]    │        │ Topic: [ Unified Log ]   │
└────┬─────┬─────┬─────────┘        └────┬─────┬─────┬─────┬───┘
     │     │     │                       │     │     │     │
   [C1]  [C2]  [C3]  [C4:Idle]        [C1]  [C2]  [C3]  [C4]  [C5...]
```

*   **Zero Rebalancing**: No heavy rebalancing protocols when consumers join or leave.
*   **True Elasticity**: Scale your worker pods up or down instantly based on the actual load.

---


## Persistence

Nexo uses an **Asynchronous Draining Pattern** to balance high-speed ingestion and durability.

*   **Continuous Batching**: Messages are automatically accumulated in memory buffers and written to disk in optimized batches for maximum throughput.
*   **Bounded Flush**: `STREAM_DEFAULT_FLUSH_MS` (default: 50ms) defines your maximum durability window - data is synced to disk at least every 50ms, regardless of traffic.

[//]: # ()
### High-Cardinality: Treat Streams like Keys

In Nexo, creating a stream is as cheap and safe as writing a key in a database. You can generate thousands of streams dynamically at runtime (e.g., `ai_chat_{id}` or `sensor_{id}`) without worrying about server stability.

*   **FD Management via LRU**: An open file handle is faster — writes are plain appends with no overhead. Opening a file, on the other hand, costs. With thousands of streams, keeping them all open simultaneously hits OS limits and memory pressure. Nexo uses a **Global FD Cache** that keeps only the `N` most recently used file handles open, automatically flushing and closing the least-recently-used ones when the cap is reached.
*   **Controlled by `STREAM_MAX_OPEN_FILES`** (Default: 256): only the most active streams hold an open handle at any given moment.

::: tip BEST PERFORMANCE
Set `STREAM_MAX_OPEN_FILES` to match your average number of *concurrently active* topics to limit unnecessary rotation overhead.
::: 

```text
    [ Topic 1 ] [ Topic 2 ] [ Topic 3 ] ... [ Topic 999 ]
          \          |           /                /
           \         |          /                /
         ┌──────────────────────────────────────────┐
         │        Global FD Manager (LRU)           │
         │  (Only keeps N files open at a time)     │
         └──────────────────┬───────────────────────┘
                            ▼
                    [ FILE SYSTEM ]
```

## Retention

When your stream reaches its limits, old data is automatically purged.

| Setting | Default | Description |
|:---|:---|:---|
| `maxAgeMs` | **7 days** | Delete data older than this |
| `maxBytes` | **1 GB** | Delete oldest data when total size exceeds this |

## Acknowledgments & Lifecycle

Nexo guarantees that every message is processed.

*   **Ack**: Successful processing. Move forward.
*   **Nack**: Explicit failure. Redeliver immediately.
*   **Timeout**: If a worker crashes, the message is automatically redelivered after 30s.

```text
Published ──▶ Delivered ──▶ [ Processing ] ──┬──▶ Ack (Done)
                               ▲             │
                               └─────────────┴──▶ Nack/Timeout (Retry)
```

## Consumer Groups

Every consumer subscribes through a **group name**. This determines how messages are distributed:

*   **Same group** = Work is split (Load Balancing).
*   **Different groups** = Each group gets everything (Broadcast).

### Scaling Service (Same Group)
To scale horizontally, run multiple instances of your worker using the same group name. Nexo will automatically distribute messages across them.

```typescript
// Process 'orders' stream using 3 parallel workers
// Run this code in 3 different instances/pods:
await orders.subscribe('worker-group', (order) => {
  console.log(`Processing order ${order.id}`);
});
```

### Multiple Services (Different Groups)
If you have independent services (e.g., Audit and Metrics), give them different group names.
Each group gets a full copy of every message.

```typescript
// Instance A: Audit Service
await orders.subscribe('audit-service', (order) => saveToDb(order));

// Instance B: Metrics Service
await orders.subscribe('metrics-service', (order) => updateGrafana(order));
```


## Consumer Tuning

Stream consumers are **pull-based with long-polling**: the SDK polls the server in a loop and, when no messages are available, the server holds the connection open until a message arrives or the timeout expires — no busy-loop, no wasted round-trips.

Two parameters control this behavior:

### `batchSize` (default: 100)

How many messages the SDK fetches from the server **in a single network request**. Higher values reduce round-trips when the stream has a backlog, at the cost of more memory per cycle. Messages within a batch are always processed **in order**, one at a time.

### `waitMs` (default: 20000)

When the stream is **caught up**, the server holds the connection open for up to `waitMs` milliseconds waiting for new messages. If a message arrives during the wait, the server responds immediately. Lowering this reduces max latency for new messages at the cost of more idle round-trips.

```typescript
await stream.subscribe('my-group', (event) => process(event), {
  batchSize: 200,   // Fetch 200 messages per network request
  waitMs: 5000,     // If empty, wait 5s (server-side) before responding
});
```

## Seek & Replay

By default, new consumer groups start reading from the **beginning** of the stream history. You can use `seek` to reset the group's cursor on the server.

> [!NOTE]
> `seek` impacts the **Consumer Group state**. If you have active subscribers (via `.subscribe()`), they will immediately start receiving messages from the new position on their next fetch.

### Typical Patterns

#### 1. Replay from Beginning
Use this when you update your processing logic and need to re-scan the entire history.
```typescript
// 1. Reset the group position
await stream.seek('analytics-v2', 'beginning');

// 2. Start (or resume) processing
await stream.subscribe('analytics-v2', (msg) => { ... });
```

#### 2. Skip to End
Best for real-time dashboards or monitors that don't need historical data.
```typescript
// 1. Skip all existing history
await stream.seek('live-dashboard', 'end');

// 2. Process only future messages
await stream.subscribe('live-dashboard', (msg) => { ... });
```
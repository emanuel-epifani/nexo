# Stream

**Append-only immutable log with per-message acknowledgment.** The source of truth for your system's history — Event Sourcing, audit trails, replaying historical data.

Stream consumers are **pull-based with long-polling**: the SDK polls the server in a loop, and when there are no new messages the server holds the connection open until a message arrives or the timeout expires. This means latency is near-zero when messages are available, with no tight busy-loop overhead when the stream is idle.

## Basic Usage

```typescript
// Create stream
const stream = await client.stream<UserEvent>('user-events').create();

// Publish event (with key for per-key ordering)
await stream.publish({ type: 'login', userId: 'u1' }, { key: 'u1' });

// Publish event (no key — no ordering constraint)
await stream.publish({ type: 'heartbeat' });

// Subscribe with consumer group
await stream.subscribe('analytics', (msg, meta) => {
  console.log(`seq=${meta.seq} key=${meta.key} — User ${msg.userId} performed ${msg.type}`);
});
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
*   **Timeout**: If a worker crashes or does not respond, the message is automatically redelivered after `ack_wait` (default 30s).
*   **Max Deliveries**: After exceeding the configured retry limit, the message is moved to a **parked** state and requires manual intervention.

```text
Published ──▶ Delivered ──▶ [ Processing ] ──┬──▶ Ack (Done)
                               ▲             │
                               └─────────────┴──▶ Timeout (Retry)
                                                     │
                                                     ▼ (after max retries)
                                                  Parked (Manual)
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
await orders.subscribe('worker-group', (order, meta) => {
  console.log(`Processing order ${order.id} [seq=${meta.seq}]`);
});
```

### Multiple Services (Different Groups)
If you have independent services (e.g., Audit and Metrics), give them different group names.
Each group gets a full copy of every message.

```typescript
// Instance A: Audit Service
await orders.subscribe('audit-service', (order, meta) => saveToDb(order));

// Instance B: Metrics Service
await orders.subscribe('metrics-service', (order, meta) => updateGrafana(order));
```


## Consumer Tuning

Stream consumers are **pull-based with long-polling**: the SDK polls the server in a loop and, when no messages are available, the server holds the connection open until a message arrives or the timeout expires — no busy-loop, no wasted round-trips.

Two parameters control this behavior:

### `batchSize` (default: 100)

How many messages the SDK fetches from the server **in a single network request**. Higher values reduce round-trips when the stream has a backlog, at the cost of more memory per cycle. By default, messages within a batch are processed **in order**, one at a time (see [Concurrency](#concurrency-default-1) below).

### `waitMs` (default: 20000)

When the stream is **caught up**, the server holds the connection open for up to `waitMs` milliseconds waiting for new messages. If a message arrives during the wait, the server responds immediately. Lowering this reduces max latency for new messages at the cost of more idle round-trips.

```typescript
await stream.subscribe('my-group', (event) => process(event), {
  batchSize: 200,   // Fetch 200 messages per network request
  waitMs: 5000,     // If empty, wait 5s (server-side) before responding
});
```

## Per-Key Ordering

Nexo provides **broker-side per-key delivery ordering** — a feature that traditional partition-based brokers (Kafka, Pulsar) cannot offer without client-side complexity.

### The Problem: Head-of-Line Blocking in Partitions

In Kafka, ordering is guaranteed only **within a partition**. To scale parallelism, you partition by key — but this creates a rigid mapping:

- If `user-A` maps to partition 0, **all** messages for `user-A` go to partition 0.
- If `user-A` has a slow or failing message, it blocks **every subsequent message** in partition 0 — including messages for `user-B`, `user-C`, and any other key that happens to hash to the same partition.
- This is called **head-of-line blocking**: one bad message paralyzes unrelated keys.

The only workaround is more partitions, but that increases overhead, complicates operations, and doesn't solve the fundamental issue — two different keys can always collide on the same partition.

### The Nexo Way: Key-Level Locking

Nexo takes a fundamentally different approach. There are no partitions. The stream is a single unified log, and the broker enforces ordering **at the key level**:

- Messages **with the same key** are delivered **one at a time, in order**. The broker holds back `msg-2` for key `K` until `msg-1` for key `K` is acknowledged.
- Messages **with different keys** are delivered **in parallel**, with no blocking between them.
- Messages **with no key** (`key` omitted at publish time) are delivered **without any ordering constraint** — full parallelism, exactly as before.

```text
Stream log:  [msg-1 key=A] [msg-2 key=B] [msg-3 key=A] [msg-4 key=C] [msg-5 key=A]

Broker delivers:
  Consumer 1 ← msg-1 (key=A locked)
  Consumer 2 ← msg-2 (key=B locked)
  Consumer 3 ← msg-4 (key=C locked)

  msg-3 (key=A) → BLOCKED, waiting for msg-1 ack
  msg-5 (key=A) → BLOCKED, waiting for msg-3 ack

After Consumer 1 acks msg-1:
  Consumer 1 ← msg-3 (key=A re-locked)
  msg-5 still blocked, waiting for msg-3 ack
```

This means:
- **No head-of-line blocking across keys**: a slow message for `user-A` does not delay messages for `user-B`.
- **No partitioning to manage**: you don't choose a partition count. The broker handles parallelism dynamically.
- **Ordering is exact**: messages with the same key are delivered in the exact order they were published.

### Publishing with a Key

```typescript
// With key — ordered delivery for same key
await stream.publish({ action: 'update', userId: 'u1' }, { key: 'u1' });
await stream.publish({ action: 'view', userId: 'u1' }, { key: 'u1' });
await stream.publish({ action: 'update', userId: 'u2' }, { key: 'u2' });

// Without key — no ordering constraint, full parallelism
await stream.publish({ action: 'heartbeat' });
```

The `key` is an opaque byte string (string or `Uint8Array`). The server treats it as a deduplication lock — it does not interpret or hash it. Keys can be user IDs, order IDs, entity IDs, or any natural partitioning key in your domain.

### Subscribing: How the Callback Works

When you subscribe, your callback receives both the message data and metadata:

```typescript
await stream.subscribe('order-processor', (data, meta) => {
  console.log(`seq=${meta.seq}, key=${meta.key}`);
  // data is your published payload
  // meta.key is the key as Uint8Array (or undefined if no key was set)
  // meta.seq is the message sequence number
});
```

### Interaction with `concurrency`

The `concurrency` option controls how many callbacks run in parallel **within a single batch**. Per-key ordering is enforced by the **broker** before messages are sent to the SDK — it is not a client-side filter.

This means:

- **`concurrency: 1` (default)**: Messages are processed one at a time. Per-key ordering is naturally preserved. This is the safest option.
- **`concurrency > 1`**: The broker still guarantees that no two messages with the **same key** are in the same batch. You can safely use `concurrency: 10` — messages with different keys will be processed in parallel, and the broker will never send two messages with the same key in a single batch.

```text
Broker guarantees per batch:
  [msg-1 key=A] [msg-2 key=B] [msg-4 key=C]   ← safe to parallelize
  NOT:
  [msg-1 key=A] [msg-3 key=A] [msg-2 key=B]  ← never happens
```

**When to use `concurrency > 1`**: Your handler is I/O-bound (HTTP calls, DB writes) and order-independent across keys. For example, sending webhooks for different users — each webhook is independent, but webhooks for the same user must be ordered.

**When to keep `concurrency: 1`**: Your handler has implicit ordering dependencies, or you're using event sourcing where the full sequence matters.

### At-Least-Once & Idempotency

Nexo provides **at-least-once delivery**. This means:

- Every message **will** be delivered at least once.
- If a consumer crashes, restarts, or is too slow, the message is **redelivered** after `ack_wait` (default 30s).
- Redelivery means the **same message** may arrive more than once.

**You must design your consumers to be idempotent.** This is not optional — it is a fundamental requirement of at-least-once systems.

#### How to Design Idempotent Consumers

1. **Use the `seq` number as a deduplication key.** Store the highest processed `seq` per key (or globally) and skip messages you've already seen:

   ```typescript
   await stream.subscribe('orders', async (order, meta) => {
     const alreadyProcessed = await db.setIfAbsent(`processed:${meta.seq}`, '1');
     if (!alreadyProcessed) return; // skip duplicate
     await processOrder(order);
   });
   ```

2. **Use natural idempotency keys from your domain.** If your message contains an `orderId`, use it:

   ```typescript
   await stream.subscribe('payments', async (event, meta) => {
     // INSERT ... ON CONFLICT DO NOTHING — safe to call twice
     await db.query('INSERT INTO processed_payments (id, status) VALUES ($1, $2) ON CONFLICT DO NOTHING', 
                    [event.paymentId, event.status]);
   });
   ```

3. **Make operations commutative.** If your side effect is "set balance to X" rather than "add X to balance", redelivery is harmless.

#### Why Not Exactly-Once?

Exactly-once delivery requires distributed consensus (two-phase commit or transactional messaging), which fundamentally limits throughput and increases latency. At-least-once with client-side idempotency is the industry standard (used by Kafka, NATS JetStream, GCP Pub/Sub) because it achieves the same end result — exactly-once **processing** — with far better performance.

### Poison Messages: Park-All

When a message with a key exceeds `max_deliveries` (default: 5), it is **parked** — removed from the delivery cycle and flagged for manual intervention.

Nexo uses a **park-all** strategy for keys: when a message with key `K` is parked, **all subsequent messages with key `K` are also parked immediately**. This prevents a poison message from blocking the key forever while new messages pile up behind it.

```text
msg-1 (key=A) → delivered 5 times → PARKED
msg-2 (key=A) → auto-parked (same key is poisoned)
msg-3 (key=A) → auto-parked (same key is poisoned)
msg-4 (key=B) → delivered normally (different key, unaffected)
```

Parked messages stay in the stream log but are never redelivered. To resume processing, you must:
1. Fix the underlying issue (bad data, downstream service down, etc.)
2. Use `seek` to reset the consumer group position, which clears all parked state.

> [!WARNING]
> `seek` clears **all** parked messages and in-flight state for the group. It is a full reset. Use it only when you're ready to reprocess from the chosen position.

### When to Use Per-Key Ordering

| Use Case | Use Keys? | Why |
|:---|:---|:---|
| **Order processing** | Yes (`orderId`) | Each order's events must be processed in sequence |
| **User activity feed** | Yes (`userId`) | A user's actions must be ordered; different users are independent |
| **Banking transactions** | Yes (`accountId`) | Account balance changes must be sequential per account |
| **IoT sensor data** | Yes (`deviceId`) | Each device's readings should be ordered |
| **Webhook fan-out** | No | Webhooks are independent; ordering adds unnecessary serialization |
| **Metrics/telemetry** | No | Data points are aggregated, not sequenced |
| **Event sourcing** | No (use `concurrency: 1`) | The full log order matters, not per-key subsets |

### `concurrency` (default: 1)

Streams are an **ordered, append-only history**. The server delivers messages in sequence and the SDK, by default, invokes your callback **one message at a time** to preserve that ordering — this is the right default for event sourcing, audit logs, and any logic where the order of events matters.

For workloads where the order **does not matter** at the consumer (independent events, idempotent handlers, I/O-bound processing where most time is spent waiting), the SDK lets you process messages of the same batch in parallel via the `concurrency` option:

```typescript
await stream.subscribe('webhooks', (event, meta) => callExternalApi(event), {
  batchSize: 100,
  concurrency: 10,  // Up to 10 callbacks in flight at the same time
});
```

**How it works**

*   The SDK fetches a batch of `batchSize` messages.
*   Up to `concurrency` callbacks run in parallel within that batch.
*   The next fetch is issued only when the entire batch has been processed.
*   `ack` is sent per-message as soon as that message's callback resolves; the server natively handles out-of-order acks.

**Trade-offs**

*   With `concurrency > 1`, callback invocations within the same batch are **not ordered**. Use this only when your handler is order-independent.
*   When using **per-key ordering** (see [Per-Key Ordering](#per-key-ordering)), the broker guarantees that no two messages with the same key appear in the same batch — so `concurrency > 1` is safe even with keys.
*   For ordered scaling, run **multiple consumers in the same group** instead — Nexo distributes messages dynamically across them while preserving per-message at-least-once semantics.

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
await stream.subscribe('analytics-v2', (msg, meta) => { ... });
```

#### 2. Skip to End
Best for real-time dashboards or monitors that don't need historical data.
```typescript
// 1. Skip all existing history
await stream.seek('live-dashboard', 'end');

// 2. Process only future messages
await stream.subscribe('live-dashboard', (msg, meta) => { ... });
```
# Stream

**Append-only immutable log with per-message acknowledgment.** The source of truth for your system's history — Event Sourcing, audit trails, replaying historical data.

Stream consumers are **pull-based with long-polling**: the SDK polls the server in a loop, and when there are no new messages the server holds the connection open until a message arrives or the timeout expires. This means latency is near-zero when messages are available, with no tight busy-loop overhead when the stream is idle.

## Basic Usage

::: code-group

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

```python
# Create stream
stream: NexoStream[UserEvent] = await client.stream("user-events").create()

# Publish event (with key for per-key ordering)
await stream.publish({"type": "login", "userId": "u1"}, {"key": "u1"})

# Publish event (no key — no ordering constraint)
await stream.publish({"type": "heartbeat"})

# Subscribe with consumer group
async def on_event(msg: UserEvent, meta: StreamMessageMeta) -> None:
    print(f"seq={meta['seq']} key={meta['key']} — User {msg['userId']} performed {msg['type']}")

await stream.subscribe("analytics", on_event)
```

:::

## Batch Publish

Publish multiple events in a single network request. Returns the sequence numbers assigned to each event.

::: code-group

```typescript
const seqs = await stream.publishBatch([
  { data: { type: 'login', userId: 'u1' }, key: 'u1' },
  { data: { type: 'login', userId: 'u2' }, key: 'u2' },
  { data: { type: 'heartbeat' } },
]);

// seqs = [1n, 2n, 3n]
```

```python
seqs = await stream.publish_batch([
    {"data": {"type": "login", "userId": "u1"}, "key": "u1"},
    {"data": {"type": "login", "userId": "u2"}, "key": "u2"},
    {"data": {"type": "heartbeat"}},
])

# seqs = [1, 2, 3]
```

:::

Each item can have its own `key` for per-key ordering. The server appends all events atomically under a single lock, then notifies consumers once.

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
- Messages **with no key** (`key` omitted at publish time) are delivered **without any ordering constraint** — full parallelism.

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

::: code-group

```typescript
// With key — ordered delivery for same key
await stream.publish({ action: 'update', userId: 'u1' }, { key: 'u1' });
await stream.publish({ action: 'view', userId: 'u1' }, { key: 'u1' });
await stream.publish({ action: 'update', userId: 'u2' }, { key: 'u2' });

// Without key — no ordering constraint, full parallelism
await stream.publish({ action: 'heartbeat' });
```

```python
# With key — ordered delivery for same key
await stream.publish({"action": "update", "userId": "u1"}, {"key": "u1"})
await stream.publish({"action": "view", "userId": "u1"}, {"key": "u1"})
await stream.publish({"action": "update", "userId": "u2"}, {"key": "u2"})

# Without key — no ordering constraint, full parallelism
await stream.publish({"action": "heartbeat"})
```

:::

The `key` is a non-empty opaque byte string (string or `Uint8Array`, at most 65,535 bytes). The server treats it as an ordering lock — it does not interpret or hash it. Keys can be user IDs, order IDs, entity IDs, or any natural partitioning key in your domain. Omit the key for unordered delivery; empty keys are rejected because the wire format reserves length zero for "no key".

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

## Consumer Groups

Every consumer subscribes through a **group name**. This determines how messages are distributed:

*   **Same group** = Work is split (Load Balancing).
*   **Different groups** = Each group gets everything (Broadcast).

### Scaling Service (Same Group)
To scale horizontally, run multiple instances of your worker using the same group name. Nexo will automatically distribute messages across them.

::: code-group

```typescript
// Process 'orders' stream using 3 parallel workers
// Run this code in 3 different instances/pods:
await orders.subscribe('worker-group', (order, meta) => {
  console.log(`Processing order ${order.id} [seq=${meta.seq}]`);
});
```

```python
# Process 'orders' stream using 3 parallel workers
# Run this code in 3 different instances/pods:
orders: NexoStream[Order] = await client.stream("orders").create()

async def on_order(order: Order, meta: StreamMessageMeta) -> None:
    print(f"Processing order {order['id']} [seq={meta['seq']}]")

await orders.subscribe("worker-group", on_order)
```

:::

### Multiple Services (Different Groups)
If you have independent services (e.g., Audit and Metrics), give them different group names.
Each group gets a full copy of every message.

::: code-group

```typescript
// Instance A: Audit Service
await orders.subscribe('audit-service', (order, meta) => saveToDb(order));

// Instance B: Metrics Service
await orders.subscribe('metrics-service', (order, meta) => updateGrafana(order));
```

```python
# Instance A: Audit Service
async def save_to_db(order: Order, meta: StreamMessageMeta) -> None:
    await persist_order(order)

await orders.subscribe("audit-service", save_to_db)

# Instance B: Metrics Service
async def update_metrics(order: Order, meta: StreamMessageMeta) -> None:
    await update_grafana(order)

await orders.subscribe("metrics-service", update_metrics)
```

:::

### Subscribing: How the Callback Works

When you subscribe, your callback receives both the message data and metadata:

::: code-group

```typescript
await stream.subscribe('order-processor', (data, meta) => {
  console.log(`seq=${meta.seq}, key=${meta.key}`);
  // data is your published payload
  // meta.key is the key as Uint8Array (or undefined if no key was set)
  // meta.seq is the message sequence number
});
```

```python
async def on_message(data: Order, meta: StreamMessageMeta) -> None:
    print(f"seq={meta['seq']}, key={meta['key']}")
    # data is your published payload
    # meta['key'] is the key as bytes (or None if no key was set)
    # meta['seq'] is the message sequence number

await stream.subscribe("order-processor", on_message)
```

:::

## Consumer Tuning

Three parameters control fetch and processing behavior:

### `batchSize` (default: 100)

How many messages the SDK fetches from the server **in a single network request**. Higher values reduce round-trips when the stream has a backlog, at the cost of more memory per cycle.

The value must be an integer between 1 and 65,536.

### `waitMs` (default: 20000)

When the stream is **caught up**, the server holds the connection open for up to `waitMs` milliseconds waiting for new messages. If a message arrives during the wait, the server responds immediately. Lowering this reduces max latency for new messages at the cost of more idle round-trips.

The value must be a positive integer. Zero is rejected to prevent a tight polling loop.

### `concurrency` (default: 1)

Controls how many callbacks run in parallel **within a single batch**.

### `stopTimeoutMs` / `stop_timeout_ms` (default: 30000)

Maximum time `stop()` waits for callbacks that have already started and for their ACK batch to be confirmed. On timeout, `stop()` fails visibly instead of pretending the group committed successfully.

::: code-group

```typescript
await stream.subscribe('webhooks', (event, meta) => callExternalApi(event), {
  batchSize: 200,     // Fetch 200 messages per network request
  waitMs: 5000,       // If empty, wait 5s (server-side) before responding
  concurrency: 10,    // Up to 10 callbacks in flight at the same time
});
```

```python
stream: NexoStream[WebhookEvent] = await client.stream("webhooks").create()

async def call_api(event: WebhookEvent, meta: StreamMessageMeta) -> None:
    await call_external_api(event)

await stream.subscribe("webhooks", call_api, {
    "batch_size": 200,   # Fetch 200 messages per network request
    "wait_ms": 5000,     # If empty, wait 5s (server-side) before responding
    "concurrency": 10,   # Up to 10 callbacks in flight at the same time
})
```

:::

**How it works**

*   The SDK fetches a batch of `batchSize` messages.
*   Up to `concurrency` callbacks run in parallel within that batch.
*   The next fetch is issued only when the entire batch has been processed.
*   Each successful callback sends and awaits its own `ACK` request. Failed callbacks remain eligible for timeout-based redelivery.
*   With `concurrency > 1`, callbacks and their ACK round-trips remain parallel. A fast callback frees its pending slot and key without waiting for slower callbacks in the same fetch batch.
*   If an ACK fails, the SDK stops starting new callbacks from that fetched batch, reports every concurrent ACK failure, and rejoins the group. Uncommitted messages are then redelivered.
*   `stop()` cancels an idle long-poll immediately. If callbacks have already started, it waits for their ACK responses and only then leaves the consumer group.

**Trade-offs**

*   With `concurrency: 1` (default), messages are processed one at a time. This is the right default for event sourcing, audit logs, and any logic where order matters.
*   With `concurrency: 1`, each message adds one network round-trip for its confirmed ACK. Increase `concurrency` for order-independent workloads to overlap ACK latency and recover throughput.
*   With `concurrency > 1`, callback invocations within the same batch are **not ordered**. Use this only when your handler is order-independent.
*   When using **per-key ordering**, the broker guarantees that no two messages with the same key appear in the same batch — so `concurrency > 1` is safe even with keys.
*   For ordered scaling, run **multiple consumers in the same group** instead — Nexo distributes messages dynamically across them.

## Seek & Replay

By default, new consumer groups start reading from the **beginning** of the stream history. You can use `seek` to reset the group's cursor on the server.

> [!NOTE]
> `seek` impacts the **Consumer Group state**. If you have active subscribers (via `.subscribe()`), they will immediately start receiving messages from the new position on their next fetch.

### Typical Patterns

#### 1. Replay from Beginning
Use this when you update your processing logic and need to re-scan the entire history.

::: code-group

```typescript
// 1. Reset the group position
await stream.seek('analytics-v2', 'beginning');

// 2. Start (or resume) processing
await stream.subscribe('analytics-v2', (msg, meta) => { ... });
```

```python
# 1. Reset the group position
await stream.seek("analytics-v2", "beginning")

# 2. Start (or resume) processing
async def on_message(msg: Order, meta: StreamMessageMeta) -> None:
    pass

await stream.subscribe("analytics-v2", on_message)
```

:::

#### 2. Skip to End
Best for real-time dashboards or monitors that don't need historical data.

::: code-group

```typescript
// 1. Skip all existing history
await stream.seek('live-dashboard', 'end');

// 2. Process only future messages
await stream.subscribe('live-dashboard', (msg, meta) => { ... });
```

```python
# 1. Skip all existing history
await stream.seek("live-dashboard", "end")

# 2. Process only future messages
async def on_message(msg: Order, meta: StreamMessageMeta) -> None:
    pass

await stream.subscribe("live-dashboard", on_message)
```

:::

## Acknowledgments & Lifecycle

Nexo provides **at-least-once delivery**. Every message **will** be delivered at least once. If a consumer crashes, restarts, or is too slow, the message is **redelivered** after `ack_wait` (default 30s). **You must design your consumers to be idempotent** — this is a fundamental requirement of at-least-once systems.

*   **Ack**: Successful processing. Move forward.
*   **Timeout**: If a worker crashes or does not respond, the message is automatically redelivered after `ack_wait` (default 30s).
*   **Max Deliveries**: After exceeding the configured retry limit, the message is moved to a **Dead Letter Topic (DLT)** and requires manual intervention.

```text
Published ──▶ Delivered ──▶ [ Processing ] ──┬──▶ Ack (Done)
                               ▲             │
                               └─────────────┴──▶ Timeout (Retry)
                                                     │
                                                     ▼ (after max retries)
                                                  DLT (Manual)
```

### Poison Messages: Dead Letter Topic (DLT)

When a message exceeds `max_deliveries` (default: 5), it is moved to the **Dead Letter Topic (DLT)** — removed from the delivery cycle and stored for inspection and manual recovery.

Nexo uses a **park-all** strategy for keys: when a message with key `K` is moved to the DLT, **all subsequent messages with key `K` are also moved to the DLT immediately**. This prevents a poison message from blocking the key forever while new messages pile up behind it.

```text
msg-1 (key=A) → delivered 5 times → DLT
msg-2 (key=A) → auto-parked in DLT (same key is poisoned)
msg-3 (key=A) → auto-parked in DLT (same key is poisoned)
msg-4 (key=B) → delivered normally (different key, unaffected)
```

Moving a message to the DLT also **acks** it — the `ack_floor` advances past it, so it never blocks the consumer group's progress.

#### DLT API

The DLT is internal to each consumer group. You can inspect and manage it with four operations:

::: code-group

```typescript
// List entries in the DLT (paginated)
const entries = await stream.peekDlt(group, limit?, offset?);
// → [{ seq: 1n, reason: "max_deliveries exceeded (5)", attempts: 5, key: Uint8Array }]

// Move a message back to the stream for redelivery
await stream.moveToStream(group, seq);

// Delete a message from the DLT permanently
await stream.deleteDlt(group, seq);

// Purge all entries from the DLT
const count = await stream.purgeDlt(group);
```

```python
# List entries in the DLT (paginated)
entries = await stream.peek_dlt(group, limit=100, offset=0)
# -> [{"seq": 1, "reason": "max_deliveries exceeded (5)", "attempts": 5, "key": b"..."}]

# Move a message back to the stream for redelivery
await stream.move_to_stream(group, seq)

# Delete a message from the DLT permanently
await stream.delete_dlt(group, seq)

# Purge all entries from the DLT
count = await stream.purge_dlt(group)
```

:::

#### Auto-Unblock

A key is automatically **unparked** only when the **last DLT entry** for that key is removed (via `moveToStream` or `deleteDlt`). This ensures that all poison messages for a key are resolved before new messages with that key can be delivered.

```text
DLT contains: msg-1 (key=A), msg-2 (key=A), msg-3 (key=A)
→ delete msg-1: key A still parked (msg-2, msg-3 remain)
→ delete msg-2: key A still parked (msg-3 remains)
→ delete msg-3: key A unblocked! New messages with key A can be delivered
```

#### Persistence

DLT state, redelivery entries, and parked keys are persisted in `state.log` alongside the group's `ack_floor`. They survive broker restarts, ensuring that poisoned keys remain blocked and `moveToStream` redrives remain deliverable after a crash or planned downtime.

> [!NOTE]
> `seek` clears all DLT entries and parked keys for the group, in addition to resetting the consumer position. It is a full reset.

## Persistence

Nexo uses a single ordered storage writer backed by the operating system page cache.

*   **Publish acknowledgment**: `publish` and `publishBatch` return only after `write_all` succeeds. At that point the message is accepted by the OS page cache and visible to consumers. Nexo does not run `fsync` per message, so recently acknowledged data may still be lost after an OS crash or power loss.
*   **Automatic backpressure**: Storage commands use a bounded queue. When it fills, publish requests wait for capacity; no message is dropped and no overload retry policy is exposed to the SDK.
*   **Batching**: `publishBatch` writes multiple messages as one storage operation and is the preferred API for high-throughput ingestion.
*   **Limits**: A publish batch may contain at most 65,536 messages and each encoded record may be at most 64 MiB. Limits are checked before allocation.
*   **Recovery**: Nexo recovers only a contiguous sequence prefix. Partial or invalid tails are truncated; segment files after a sequence gap are renamed with `.corrupt` so they remain available for diagnosis but cannot be appended again.
*   **Group State Persistence**: `STREAM_DEFAULT_FLUSH_MS` (default: 50ms) controls how often consumer group state (ack_floor, DLT entries, parked keys) is saved to disk. Message data itself relies on OS-level page cache flushing.

### High-Cardinality: Treat Streams like Keys

In Nexo, creating a stream is as cheap and safe as writing a key in a database. You can generate thousands of streams dynamically at runtime (e.g., `ai_chat_{id}` or `sensor_{id}`) without worrying about server stability.

Stream names are 1–255 ASCII bytes and may contain letters, digits, `.`, `_`, and `-`. Path separators, whitespace, `.` and `..` are rejected.

*   **FD Management via LRU**: An open file handle is faster — writes are plain appends with no overhead. Opening a file, on the other hand, costs. With thousands of streams, keeping them all open simultaneously hits OS limits and memory pressure. Nexo uses a **Global FD Cache** that keeps only the `N` most recently used writer handles open, evicting and closing the least-recently-used ones when the cap is reached.
*   **Controlled by `STREAM_MAX_OPEN_FILES`** (Default: 256): only the most active streams hold an open handle at any given moment.
*   **Reads**: Readers use independent temporary handles so concurrent seeks cannot interfere with the append cursor. Segment locations come from the in-memory topic catalog; read handles close when the request completes.

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

## Configuration

### How it works

1. Server starts → reads env vars (global defaults)
2. Topic created → server snapshots defaults into `config.json` (per-topic)
3. SDK can override `retention` at creation — everything else uses system defaults
4. On restart → each topic reads its own `config.json` (ignores current env vars)

> **Existing topics are not affected by env var changes.** Only new topics pick up new defaults.

### Environment Variables

Global, set at server startup.

| Variable | Default | Description |
|:---|:---|:---|
| `STREAM_ROOT_PERSISTENCE_PATH` | `./data/streams` | Base directory for all stream data |
| `STREAM_DEFAULT_FLUSH_MS` | `50` | Group state save interval multiplier (×10 = actual ms) |
| `STREAM_STORAGE_QUEUE_CAPACITY` | `16384` | Pending storage commands before publishers wait for capacity |
| `STREAM_MAX_SEGMENT_SIZE` | `104857600` (100MB) | Max segment file size before rollover |
| `STREAM_RETENTION_CHECK_MS` | `600000` (10min) | Retention task interval |
| `STREAM_DEFAULT_RETENTION_BYTES` | `1073741824` (1GB) | Default `maxBytes` if SDK omits it |
| `STREAM_DEFAULT_RETENTION_AGE_MS` | `604800000` (7 days) | Default `maxAgeMs` if SDK omits it |
| `STREAM_MAX_ACK_PENDING` | `10000` | Max unacked messages per consumer group |
| `STREAM_MAX_OPEN_FILES` | `256` | Max open file handles (LRU cache) |
| `STREAM_ACK_WAIT_MS` | `30000` (30s) | Ack timeout before redelivery |
| `STREAM_MAX_DELIVERIES` | `5` | Max delivery attempts before DLT |

### SDK Overrides

Fields settable at `create()` time. If omitted, system defaults apply.

| Field | SDK option | System default (env var) |
|:---|:---|:---|
| Retention | `retention: { maxAgeMs, maxBytes }` | 7 days, 1GB (`STREAM_DEFAULT_RETENTION_AGE_MS`, `STREAM_DEFAULT_RETENTION_BYTES`) |

::: code-group

```typescript
await client.stream('my-topic').create({
  retention: { maxAgeMs: 3_600_000, maxBytes: 100_000_000 }  // 1h, 100MB
});
```

```python
stream: NexoStream[MyEvent] = await client.stream("my-topic").create({
    "retention": {"max_age_ms": 3_600_000, "max_bytes": 100_000_000}  # 1h, 100MB
})
```

:::

| Setting | Default | Description |
|:---|:---|:---|
| `maxAgeMs` | 7 days | Delete data older than this |
| `maxBytes` | 1 GB | Delete oldest data when total exceeds this |


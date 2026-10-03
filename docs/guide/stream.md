# Stream

**Persistent event log with independent consumer groups and managed per-key delivery.** Events remain available to other groups and replay until retention removes them; confirming consumption does not delete the log.

Stream consumers are **pull-based with long-polling**: the SDK polls the server in a loop, and when there are no new messages the server holds the connection open until a message arrives or the timeout expires. This means latency is near-zero when messages are available, with no tight busy-loop overhead when the stream is idle.

## Basic Usage

Provision the durable stream before deploying producers and consumers:

::: code-group

```typescript
const result = await client.stream.create('user-events');
console.log(result.status, result.definition.config);
```

```python
result = await client.stream.create("user-events")
print(result.status, result.definition.config)
```

:::

Application code retrieves the existing stream. Consumer-group state is created on first use and is addressed through a group handle.

::: code-group

```typescript
const stream = await client.stream.get<UserEvent>('user-events');
await stream.publish({ type: 'login', userId: 'u1' }, { key: 'u1' });
await stream.publish({ type: 'heartbeat' });

const analytics = stream.group('analytics');
await analytics.subscribe((message, meta) => {
  console.log(`seq=${meta.seq} key=${meta.key} — User ${message.userId} performed ${message.type}`);
});
```

```python
stream: NexoStream[UserEvent] = await client.stream.get("user-events")
await stream.publish({"type": "login", "userId": "u1"}, key="u1")
await stream.publish({"type": "heartbeat"})

analytics = stream.group("analytics")

async def on_event(message: UserEvent, meta: StreamMessageMeta) -> None:
    print(f"seq={meta['seq']} key={meta['key']} — User {message['userId']} performed {message['type']}")

await analytics.subscribe(on_event)
```

:::

`create()` returns `status: "created" | "unchanged"` plus the complete effective configuration. Repeating it with an equivalent configuration is safe; a different configuration raises `ResourceConfigurationConflictError`. Use `client.stream.describe(name)` to inspect the authoritative configuration without provisioning.

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

Each item can have its own key. A publish batch is appended atomically in a SQLite transaction; its boundaries do not determine consumer delivery batches.

---

## No User-Managed Partitions

Nexo abandons the traditional "Kafka-style" partitioning model. There are no partitions for you to choose, and no user-managed rebalancing protocol when consumers join or leave — the broker coordinates delivery continuously.

### The Problem with Partitions

In Kafka, concurrency is tied to the number of partitions. If you have 3 partitions, you can only have 3 active consumers in a group. Adding a 4th consumer does nothing; it stays idle.

### The Nexo Way: Dynamic Fan-Out

Nexo streams are single, unified logs, and additional group members join without repartitioning or restarting. Actual parallelism is bounded by the ready keys and keyless work available, each consumer's callback capacity, and server-side budgets — extra consumers are optional capacity scaling, not a guarantee of more throughput.

```text
KAFKA (Static)                      NEXO (Dynamic)
┌──────────────────────────┐        ┌──────────────────────────┐
│ Topic: [P0] [P1] [P2]    │        │ Stream: [ Unified Log ]  │
└────┬─────┬─────┬─────────┘        └────┬─────┬─────┬─────┬───┘
     │     │     │                       │     │     │     │
   [C1]  [C2]  [C3]  [C4:Idle]        [C1]  [C2]  [C3]  [C4]  [C5...]
```

- **No user-managed rebalance protocol**: the broker hands out leased batches as members join and leave.
- **Bounded elasticity**: scaling is limited to single-node vertical scope — ready independent keys, callback slots, and configured budgets.

---

## Delivery Models: No-Key vs Per-Key

A key is optional, and both models may coexist in one stream. Groups consume independently.

| Workload | Model |
|---|---|
| Entity events or aggregate projections | Use the entity/aggregate ID as key |
| Independent webhooks or telemetry retained as history | Omit the key |
| One globally serialized event sequence | Use one shared key; this deliberately sacrifices parallelism |

### No Key

Each event is an independent singleton assignment. The SDK aggregates confirmations over the network, but one event's failure does not impose an ordering dependency on unrelated events. There is no global processing-order guarantee, even with a single subscriber when retries occur.

### Per-Key Ordering

The server grants at most one **valid batch assignment per `(stream, group, key)`**. A batch may contain several consecutive events of that key. The official SDK runs their callbacks sequentially; different keys use separate execution slots and refill independently.

```text
A: [A1 -> A2 -> A3] -> confirm A's progress
B: [B1 -> B2 -> B3] -> confirm B's progress
```

B can finish and receive its next batch while A is slow. The next batch for A is not granted until the current assignment settles. A batch-size setting is a limit, not an instruction to wait for more events.

Ordering follows append order accepted by the server, not causal order between concurrent producers. Retries can repeat an unconfirmed prefix, and explicitly requested replay is distinct from normal forward progress.

### Confirmations, Failure and Capacity

A confirmation reports the completed prefix of one assigned batch. Completing A1 and A2 then failing on A3 saves that prefix and schedules retry from A3, without running A4 first. The retry gets a new assignment receipt. A crash before a confirmation commits can repeat already processed events; external effects must tolerate duplicates.

Keyless confirmations resolve individual events. Both modes use aggregated update frames and shared SQLite transactions. An acknowledgment is not a separate disk commit per event.

`maxAckPending` counts unconfirmed **events**, not batches. `maxAckPendingBytes` bounds their logical storage bytes. SDK message/byte buffers and callback concurrency add local limits. Isolation means no artificial dependency between keys; it does not promise progress when all execution slots or capacity are occupied.

`ack_floor` is the group's contiguous **resolved frontier** for original events, derived from durable consumption state. Parking/DLS operations, retention and seek can also move that frontier. It is not proof that every earlier callback succeeded; inspect DLS separately.

## Per-Key Ordering: How It Works

The server owns durable cursors, exclusive assignments, retry timing and fencing. Each assignment has a fresh receipt; the group has a separate opaque epoch that changes on seek/reset. Updates from an expired receipt or superseded epoch cannot advance current work. RPC replies may arrive out of order: a new receipt for the same key supersedes the older assignment even when its completion response is still in flight.

The SDK owns sequential callback execution within a valid keyed assignment, bounded parallelism across assignments, renewal, cumulative confirmations and continuous refill. Applications do not need client-side key locks or extra consumers to work around a page barrier.

These guarantees apply to valid assignments. The broker cannot undo external effects or forcibly stop a disconnected/expired callback. TypeScript abort signals and Python task cancellation are cooperative, not exactly-once execution. Within one subscription, a timed-out callback that ignores cancellation keeps its execution slot, buffer credits and local key gate until it actually settles. A newly assigned batch for that key waits behind the gate; other keys may continue when capacity is available. Stopping the subscription releases its unstarted assignments but cannot kill the old user callback or undo external effects.

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
await stream.publish({"action": "update", "userId": "u1"}, key="u1")
await stream.publish({"action": "view", "userId": "u1"}, key="u1")
await stream.publish({"action": "update", "userId": "u2"}, key="u2")

# Without key — no ordering constraint, full parallelism
await stream.publish({"action": "heartbeat"})
```

:::

The `key` is a non-empty opaque byte string (string or `Uint8Array`, at most 65,535 bytes). The server treats it as an opaque identifier for ordering, not a routing partition. Keys can be user IDs, order IDs, entity IDs, or any natural identifier in your domain. Omit the key for unordered delivery; empty keys are rejected because the wire format reserves length zero for "no key".

---

## Consumer Groups

Every consumer subscribes through a **group name**. This determines how messages are distributed:

- **Same group** = Work is split (Load Balancing).
- **Different groups** = Each group gets everything (Broadcast).

### Scaling Service (Same Group)

To scale horizontally, run multiple instances of your worker using the same group name. Nexo will automatically distribute messages across them.

::: code-group

```typescript
// Process 'orders' stream using 3 parallel workers
// Run this code in 3 different instances/pods:
await orders.group('worker-group').subscribe((order, meta) => {
  console.log(`Processing order ${order.id} [seq=${meta.seq}]`);
});
```

```python
# Process 'orders' stream using 3 parallel workers
# Run this code in 3 different instances/pods:
orders: NexoStream[Order] = await client.stream.get("orders")

async def on_order(order: Order, meta: StreamMessageMeta) -> None:
    print(f"Processing order {order['id']} [seq={meta['seq']}]")

await orders.group("worker-group").subscribe(on_order)
```

:::

### Multiple Services (Different Groups)

If you have independent services (e.g., Audit and Metrics), give them different group names.
Each group gets a full copy of every message.

::: code-group

```typescript
// Instance A: Audit Service
await orders.group('audit-service').subscribe((order, meta) => saveToDb(order));

// Instance B: Metrics Service
await orders.group('metrics-service').subscribe((order, meta) => updateGrafana(order));
```

```python
# Instance A: Audit Service
async def save_to_db(order: Order, meta: StreamMessageMeta) -> None:
    await persist_order(order)

await orders.group("audit-service").subscribe(save_to_db)

# Instance B: Metrics Service
async def update_metrics(order: Order, meta: StreamMessageMeta) -> None:
    await update_grafana(order)

await orders.group("metrics-service").subscribe(update_metrics)
```

:::

### Subscribing: How the Callback Works

When you subscribe, your callback receives both the message data and metadata:

::: code-group

```typescript
await stream.group('order-processor').subscribe((data, meta) => {
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

await stream.group("order-processor").subscribe(on_message)
```

:::

---

## Consumer Tuning

### `batchSize` (default: 100)

How many messages the SDK asks the server for **in a single fetch request**. Higher values reduce round-trips when the stream has a backlog, at the cost of more buffered memory.

The value must be an integer between 1 and 65,536.

### `keyBatchSize` / `key_batch_size` (default: 32)

Maximum messages the server may include in **one keyed batch** (all messages of a batch share the same key). Caps how much same-key work a single lease carries.

The value must be an integer between 1 and 1,024.

### `waitMs` (default: 20000)

When the stream is **caught up**, the server holds the connection open for up to `waitMs` milliseconds waiting for new messages. If a message arrives during the wait, the server responds immediately. While the subscription still has active work, the SDK automatically short-polls (at most 5 ms) so a stale in-flight request can never starve freed capacity.

The value must be a positive integer. Zero is rejected to prevent a tight polling loop.

### `concurrency` (default: 16)

Maximum callbacks in flight at once. It also bounds how many keyed batches the subscription may hold at one time: the server guarantees **at most one in-flight batch per key**, and the SDK guarantees the callback for a given key runs **sequentially** — different keys still process in parallel.

### `callbackTimeoutMs` / `callback_timeout_ms` (default: 30000)

Maximum time **one callback invocation** may run before the SDK requests cancellation. On timeout the TypeScript callback's `meta.signal` aborts and the Python task receives `asyncio.CancelledError`.

The SDK stops waiting after the per-callback timeout and requests cooperative cancellation. It confirms only the completed prefix, releases the failed batch assignment, and schedules its remaining suffix for retry after the configured retry wait. The next assignment uses a new receipt. The callback timeout is independent of the lease duration; leases are renewed while legitimate work is active, not after a terminal retry or release.

Cancellation is **cooperative**. A callback that ignores the abort/cancel keeps occupying its slot and its buffer credits until it actually settles — synchronous CPU-blocking work cannot be preempted by an async SDK. If every callback slot is held by such non-cooperative invocations and nothing can make progress, the subscription fails visibly (`callback capacity exhausted`) instead of leaking memory forever.

### `maxBufferedMessages` (default: `2 × batchSize`)

Cap on received-but-unsettled messages held in memory (queued + running + in-flight fetch reservation). Bounds memory when callbacks are slow.

### `maxBufferedBytes` (default: 20 MiB)

Byte cap on the same buffered work, counted from encoded payload sizes. May be smaller than needed for `batchSize` messages — the SDK simply fetches less. It must still accommodate the largest event you need to consume, including its key and fetch framing; a smaller budget cannot deliver that event. This is an encoded-data budget, not a process RSS limit.

### `stopTimeoutMs` / `stop_timeout_ms` (default: 30000)

Maximum time `stop()` waits for callbacks that have already started and for their batch updates to be confirmed. On timeout, `stop()` fails visibly instead of pretending the group committed successfully.

::: code-group

```typescript
await stream.group('webhooks').subscribe((event, meta) => callExternalApi(event), {
  batchSize: 200,     // Ask for up to 200 messages per fetch request
  waitMs: 5000,       // If empty, wait 5s (server-side) before responding
  concurrency: 10,    // Up to 10 callbacks in flight at the same time
  callbackTimeoutMs: 60_000,
});
```

```python
stream: NexoStream[WebhookEvent] = await client.stream.get("webhooks")

async def call_api(event: WebhookEvent, meta: StreamMessageMeta) -> None:
    await call_external_api(event)

await stream.group("webhooks").subscribe(
    call_api,
    batch_size=200,   # Ask for up to 200 messages per fetch request
    wait_ms=5000,     # If empty, wait 5s (server-side) before responding
    concurrency=10,   # Up to 10 callbacks in flight at the same time
    callback_timeout_ms=60_000,
)
```

:::

**How it works**

- The SDK continuously refills: it keeps at most one fetch in flight and issues a new one whenever buffer credits (messages, bytes, key slots) allow — it never waits for a whole page to finish before fetching more.
- The server leases **batches**; every keyed batch owns its key exclusively until it settles. Up to `concurrency` batch runners and callbacks run in parallel; messages within a batch — and therefore all same-key messages — are delivered to your callback **sequentially**.
- Progress is reported cumulatively per batch: a confirmed prefix stays confirmed even if a later callback in the same batch fails or the subscription stops.
- A callback timeout or throw retries the batch's unconfirmed suffix. A finished prefix counts as finished only after the server commits the acknowledgement — a crash or network loss in between can still cause at-least-once redelivery.
- `stop()` cancels an idle long-poll immediately. If callbacks have already started, it waits for them within `stopTimeoutMs`, then releases any unstarted work and only then leaves the consumer group.

**Trade-offs**

- With `concurrency: 1`, callbacks run one at a time, but this does **not** guarantee global ordering across keys or retries — use one shared key when you need a globally serialized sequence.
- With `concurrency > 1`, callbacks for **different keys** run in parallel with no ordering between them. Use this only when your handler is order-independent across keys.
- Per-key ordering is preserved at every `concurrency`: the SDK runs each keyed batch's callbacks sequentially, and the server never grants a second valid assignment for the same key while one is in flight.
- Running **multiple consumers in the same group** is optional capacity scaling. Additional consumers are not required merely to keep independent keys progressing within one subscription.

---

## Seek & Replay

By default, new consumer groups start reading from the **beginning** of the stream history. You can use `seek` to reset the group's cursor on the server.

> [!NOTE]
> `seek` impacts the **Consumer Group state**. Active subscribers detect the new opaque epoch on their next RPC: work not yet started under the old epoch is discarded and running callbacks are cooperatively aborted. External effects already performed cannot be undone. Seek also clears the group's DLS entries and parked keys — it is a full reset.

### Typical Patterns

#### 1. Replay from Beginning

Use this when you update your processing logic and need to re-scan the entire history.

::: code-group

```typescript
const analytics = stream.group('analytics-v2');

// 1. Reset the group position
await analytics.seek('beginning');

// 2. Start (or resume) processing
await analytics.subscribe((msg, meta) => { ... });
```

```python
analytics = stream.group("analytics-v2")

# 1. Reset the group position
await analytics.seek("beginning")

# 2. Start (or resume) processing
async def on_message(msg: Order, meta: StreamMessageMeta) -> None:
    pass

await analytics.subscribe(on_message)
```

:::

#### 2. Skip to End

Best for real-time dashboards or monitors that don't need historical data.

::: code-group

```typescript
const dashboard = stream.group('live-dashboard');

// 1. Skip all existing history
await dashboard.seek('end');

// 2. Process only future messages
await dashboard.subscribe((msg, meta) => { ... });
```

```python
dashboard = stream.group("live-dashboard")

# 1. Skip all existing history
await dashboard.seek("end")

# 2. Process only future messages
async def on_message(msg: Order, meta: StreamMessageMeta) -> None:
    pass

await dashboard.subscribe(on_message)
```

:::

---

## Acknowledgments & Lifecycle

Nexo provides **at-least-once delivery**. Redelivery applies while events remain retained and have not been explicitly skipped or parked. Lease expiry recovers a crashed or disconnected consumer's unconfirmed work; a known callback failure confirms its successful prefix and retries the remaining suffix after `ack_wait` (default 30s); a clean `stop()` returns unstarted work without treating it as failure. **You must design your consumers to be idempotent** — this is a fundamental requirement of at-least-once systems.

- **Ack**: Successful processing. Move forward.
- **Timeout**: If a worker crashes or does not respond, the message is automatically redelivered after `ack_wait` (default 30s).
- **Max Deliveries**: After exceeding the configured retry limit, the message is moved to a **Dead Letter Stream (DLS)** and requires manual intervention.

```text
Published ──▶ Delivered ──▶ [ Processing ] ──┬──▶ Ack (Done)
                               ▲             │
                               └─────────────┴──▶ Timeout (Retry)
                                                     │
                                                     ▼ (after max retries)
                                                  DLS (Manual)
```

### Poison Messages: Dead Letter Stream (DLS)

When a message exceeds `max_deliveries` (default: 5), it is moved to the **Dead Letter Stream (DLS)** — removed from the delivery cycle and stored for inspection and manual recovery.

Nexo uses a **park-all** strategy for keys: when a message with key `K` is moved to the DLS, **all subsequent messages with key `K` are also parked immediately**. The poison key stays parked until its DLS entries are resolved; different keys are unaffected.

```text
msg-1 (key=A) → delivered 5 times → DLS
msg-2 (key=A) → auto-parked in DLS (same key is poisoned)
msg-3 (key=A) → auto-parked in DLS (same key is poisoned)
msg-4 (key=B) → delivered normally (different key, unaffected)
```

Moving a message to the DLS resolves its normal delivery obligation — the group's resolved frontier advances past it — but it does not claim business success. The event payload remains in the log under retention; the DLS stores ranges referencing it, not duplicate payloads.

#### DLS API

The DLS is internal to each consumer group. You can inspect and manage it with four operations:

::: code-group

```typescript
const consumerGroup = stream.group('analytics');

// List entries in the DLS (paginated)
const entries = await consumerGroup.dls.peek({ limit: 100, offset: 0 });
// → [{ seq: 1n, reason: "max_deliveries exceeded (5)", attempts: 5, key: Uint8Array }]

// Move a message back to the stream for redelivery
await consumerGroup.dls.replay(seq);

// Delete a message from the DLS permanently
await consumerGroup.dls.delete(seq);

// Purge all entries from the DLS
const count = await consumerGroup.dls.purge();
```

```python
consumer_group = stream.group("analytics")

# List entries in the DLS (paginated)
entries = await consumer_group.dls.peek(limit=100, offset=0)
# -> [{"seq": 1, "reason": "max_deliveries exceeded (5)", "attempts": 5, "key": b"..."}]

# Move a message back to the stream for redelivery
await consumer_group.dls.replay(seq)

# Delete a message from the DLS permanently
await consumer_group.dls.delete(seq)

# Purge all entries from the DLS
count = await consumer_group.dls.purge()
```

:::

#### Auto-Unblock

A key is automatically **unparked** only when the **last DLS entry** for that key is removed (via `dls.replay()` or `dls.delete()`). This ensures that all poison messages for a key are resolved before new messages with that key can be delivered.

```text
DLS contains: msg-1 (key=A), msg-2 (key=A), msg-3 (key=A)
→ delete msg-1: key A still parked (msg-2, msg-3 remain)
→ delete msg-2: key A still parked (msg-3 remains)
→ delete msg-3: key A unblocked! New messages with key A can be delivered
```

#### Persistence

DLS ranges, replay intents, parked keys, durable cursors, and leases are persisted in `streams.sqlite3`. They survive broker restarts, ensuring that poisoned keys remain blocked and `dls.replay()` redrives remain deliverable after a crash or planned downtime.

> [!NOTE]
> `seek` clears all DLS entries and parked keys for the group, in addition to resetting the consumer position. It is a full reset.

---

## Persistence

Nexo stores every stream — messages, keys, consumer groups, leases, and the dead-letter store — in a **single embedded SQLite database** (`streams.sqlite3` under the persistence directory, WAL mode). A dedicated writer thread owns the database; all commands execute inside SQLite transactions, so message data and consumer-group state are committed atomically.

- **Publish acknowledgment**: `publish` and `publishBatch` return only after their transaction commits. With `synchronous = NORMAL` the WAL is checkpointed periodically; a process crash never loses acknowledged writes, while an OS crash or power loss may lose the last unsynced commit.
- **Automatic backpressure**: Storage commands use a bounded queue (count and bytes). When it fills, publish requests wait for capacity; no message is dropped and no overload retry policy is exposed to the SDK.
- **Batching**: `publishBatch` commits multiple messages as one transaction and is the preferred API for high-throughput ingestion.
- **Limits**: A publish batch may contain at most 65,536 messages and each encoded record may be at most 64 MiB. A record that can never fit inside a FETCH response is rejected at publish time. Limits are checked before allocation.
- **Recovery**: On startup Nexo opens the database, wipes stale membership rows, and reconciles every outstanding lease — messages leased-but-unacked at shutdown are re-offered or parked by `maxDeliveries`. Consumer group progress, DLS entries, and parked keys are all durable.
- **Fail-closed startup**: The persistence directory must contain only the SQLite layout (`streams.sqlite3`, its WAL files, and the lock file). Foreign files — including data written by previous versions — abort startup instead of being silently imported or ignored.

### High-Cardinality: Treat Streams like Keys

In Nexo, creating a stream is as cheap and safe as writing a key in a database. You can generate thousands of streams dynamically at runtime (e.g., `ai_chat_{id}` or `sensor_{id}`) without worrying about server stability.

Stream names are 1–255 ASCII bytes and may contain letters, digits, `.`, `_`, and `-`. Path separators, whitespace, `.` and `..` are rejected.

- **One row per stream**: All streams share a single SQLite database, so creating a stream is a small transaction — no files to open, no per-stream handles to cache. A process-level lock file prevents two Nexo instances from opening the same database.
- **Reads**: `fetch` runs as a bounded read+write transaction inside the writer thread's working set — it claims a lease while reading; SQLite's own page cache keeps hot ranges warm.
- **Key metadata GC**: unreferenced key metadata (internal key IDs and empty per-group lanes) is reclaimed incrementally, only after all retained payloads and obligations for that key are gone. Key IDs and global sequence numbers are never reused or reset, and parked/replay state is preserved — this is internal bookkeeping and does not affect the SDK-visible model.

---

## Configuration

### How it works

1. Server starts → reads env vars (global defaults)
2. Stream created → server snapshots defaults into the stream's row (per-stream)
3. SDK can override `retention` at creation — everything else uses system defaults
4. On restart → each stream reads its persisted config (ignores current env vars)

> **Existing streams are not affected by env var changes.** Only new streams pick up new defaults.

### Environment Variables

Global, set at server startup.

| Variable | Default | Description |
|:---|:---|:---|
| `STREAM_ROOT_PERSISTENCE_PATH` | `./data/streams` | Directory containing `streams.sqlite3` and its lock file |
| `STREAM_STORAGE_QUEUE_CAPACITY` | `16384` | Pending storage commands before publishers wait for capacity |
| `STREAM_STORAGE_QUEUE_MAX_BYTES` | `268435456` (256MB) | Byte bound on queued command payloads |
| `STREAM_FETCH_RESPONSE_BYTES` | `10485760` (10MB) | Max encoded size of one FETCH response |
| `STREAM_RETENTION_CHECK_MS` | `600000` (10min) | Retention task interval |
| `STREAM_DEFAULT_RETENTION_BYTES` | `1073741824` (1GB) | Default `maxBytes` if SDK omits it |
| `STREAM_DEFAULT_RETENTION_AGE_MS` | `604800000` (7 days) | Default `maxAgeMs` if SDK omits it |
| `STREAM_MAX_ACK_PENDING` | `10000` | Max unconfirmed events per consumer group |
| `STREAM_MAX_ACK_PENDING_BYTES` | `67108864` (64 MiB) | Logical bytes of unconfirmed events per consumer group |
| `STREAM_ACK_WAIT_MS` | `30000` (30s) | Batch lease validity and the known-failure retry delay — distinct from the SDK callback timeout (also 30s by default) |
| `STREAM_MAX_DELIVERIES` | `5` | Max delivery attempts before DLS |

### SDK Overrides

Fields settable at `create()` time. If omitted, system defaults apply.

| Field | SDK option | System default (env var) |
|:---|:---|:---|
| Retention | `retention: { maxAgeMs, maxBytes }` | 7 days, 1GB (`STREAM_DEFAULT_RETENTION_AGE_MS`, `STREAM_DEFAULT_RETENTION_BYTES`) |

::: code-group

```typescript
const result = await client.stream.create('my-stream', {
  retention: { maxAgeMs: 3_600_000, maxBytes: 100_000_000 }  // 1h, 100MB
});
console.log(result.definition.config);
```

```python
result = await client.stream.create(
    "my-stream",
    max_age_ms=3_600_000,
    max_bytes=100_000_000,
)
print(result.definition.config)
```

:::

| Setting | Default | Description |
|:---|:---|:---|
| `maxAgeMs` | 7 days | Delete data older than this |
| `maxBytes` | 1 GB | Delete oldest data when logical event bytes exceed this: payload, key and fixed per-event accounting overhead. This is not a quota on physical database/WAL size, other metadata or process RSS. |

Retention is not a success signal and does not guarantee forever history: it can remove unprocessed or currently leased events, invalidating their old receipts.

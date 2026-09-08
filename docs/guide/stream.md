# Stream

**Append-only immutable log with per-message acknowledgment.** The source of truth for your system's history — Event Sourcing, audit trails, replaying historical data.

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

Each item can have its own `key` for per-key ordering. The server appends all events atomically under a single lock, then notifies consumers once.

---

## No Partitions: Dynamic Consumer Scaling

Nexo abandons the traditional "Kafka-style" partitioning model. There are no partitions to choose, no rebalancing protocols, and no idle consumers.

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

- **Zero Rebalancing**: No heavy rebalancing protocols when consumers join or leave.
- **True Elasticity**: Scale your worker pods up or down instantly based on the actual load.

---

## Delivery Models: No-Key vs Per-Key

Every message you publish can optionally carry a **key**. The presence or absence of a key determines the delivery model the broker uses for that message. You can mix keyed and keyless messages in the same stream — the broker handles each according to its own model.

### When to Use Each Model

| Use Case | Model | Why |
|:---|:---|:---|
| **Order processing** | Per-key (`orderId`) | Each order's events must be processed in sequence |
| **User activity feed** | Per-key (`userId`) | A user's actions must be ordered; different users are independent |
| **Banking transactions** | Per-key (`accountId`) | Account balance changes must be sequential per account |
| **IoT sensor data** | Per-key (`deviceId`) | Each device's readings should be ordered |
| **Webhook fan-out** | No key | Webhooks are independent; ordering adds unnecessary serialization |
| **Metrics/telemetry** | No key | Data points are aggregated, not sequenced |
| **Event sourcing** | No key + `concurrency: 1` | The full log order matters, not per-key subsets |
| **Task queue on log** | No key | Tasks are independent; need at-least-once + DLT, not ordering |

> [!TIP]
> If you need ordering, use per-key. If you need durability and reliability (at-least-once, DLT, redelivery) but not ordering, use no-key. If you need neither, consider [PubSub](./pubsub.md) — it's fire-and-forget with no persistence overhead.

### Model 1: No Key — Full Parallelism

When you publish without a key, there is **no ordering constraint**. The broker delivers messages to any available consumer in the group, up to `max_ack_pending` in flight simultaneously. Each message is ACKed independently.

**Guarantees:**
- Messages are delivered **at least once**.
- No ordering between messages — consumer B may process seq 5 before consumer A finishes seq 3.
- Backpressure is global: `max_ack_pending` limits total unacked messages across the group.
- A slow or failing message does **not** block other messages.

```text
Producer:  pub(msg-1)  pub(msg-2)  pub(msg-3)  pub(msg-4)  pub(msg-5)
              │           │           │           │           │
              ▼           ▼           ▼           ▼           ▼
Log:       [seq=1]    [seq=2]    [seq=3]    [seq=4]    [seq=5]
              │                       │           │       │
              ▼                       ▼           ▼       ▼
Broker ──▶ Consumer A: seq=1   Consumer B: seq=3   Consumer A: seq=4
              │                       │           │       │
              ▼                       ▼           │       ▼
           process                  process       │    process
              │                       │           │       │
              ▼                       ▼           │       ▼
            ACK(1)                  ACK(3)        │    ACK(4)
              │                       │           │
              ▼                       ▼           ▼
Broker ◀── ack_floor advances to max contiguous acked seq
              │
              ▼
         Consumer B: seq=2  (was fetched but callback slower)
              │
              ▼
           process → ACK(2)
```

**What happens on failure:**
- If consumer A crashes while processing seq 1, the broker redelivers seq 1 after `ack_wait` (30s default).
- seq 2, 3, 4, 5 are unaffected — they can be delivered and acked independently.

### Model 2: Per-Key Ordering — Key-Level Locking

When you publish with a key, the broker guarantees that messages with the **same key** are delivered **one at a time, in publication order**. Messages with **different keys** are delivered in parallel.

**Guarantees:**
- Messages with the same key are delivered in **exact publication order**: seq 1 (key=A) before seq 3 (key=A), always.
- The broker holds back seq 3 (key=A) until seq 1 (key=A) is ACKed.
- Messages with different keys (key=B, key=C) are **not blocked** — they flow in parallel.
- `max_ack_pending` limits total unacked messages across the group (not per key).
- A slow message for key=A blocks **only** subsequent messages with key=A. Key=B, key=C, and keyless messages continue flowing.

```text
Producer:  pub(msg-1, key=A)  pub(msg-2, key=B)  pub(msg-3, key=A)  pub(msg-4, key=C)  pub(msg-5, key=A)
                 │                  │                  │                  │                  │
                 ▼                  ▼                  ▼                  ▼                  ▼
Log:       [seq=1 key=A]     [seq=2 key=B]     [seq=3 key=A]     [seq=4 key=C]     [seq=5 key=A]
                 │                  │                  │                  │                  │
                 ▼                  ▼                  ▼                  ▼                  ▼
Broker:    key=A: LOCK          key=B: LOCK        key=A: BLOCKED     key=C: LOCK        key=A: BLOCKED
           deliver seq=1        deliver seq=2      (waiting for        deliver seq=4      (waiting for
           to Consumer A        to Consumer B       seq=1 ACK)         to Consumer C       seq=3 ACK)
                 │                  │                                     │
                 ▼                  ▼                                     ▼
           process             process                              process
                 │                  │                                     │
                 ▼                  ▼                                     ▼
              ACK(1)              ACK(2)                               ACK(4)
                 │
                 ▼
Broker:    key=A: UNLOCK → seq=3 becomes deliverable
                 │
                 ▼
           deliver seq=3 to Consumer A (or any free consumer)
                 │
                 ▼
           process → ACK(3) → key=A: UNLOCK → seq=5 becomes deliverable
```

**What happens on failure:**
- If consumer A crashes while processing seq 1 (key=A), the broker redelivers seq 1 after `ack_wait`.
- seq 3 and seq 5 (key=A) remain **blocked** until seq 1 is successfully acked.
- seq 2 (key=B) and seq 4 (key=C) are **unaffected** — they continue flowing normally.

### Side-by-Side Comparison

```text
                  No Key                          Per-Key Ordering
  ┌─────────────────────────────┐    ┌─────────────────────────────────┐
  │  Log: [1][2][3][4][5]       │    │  Log: [1,A][2,B][3,A][4,C][5,A] │
  │                              │    │                                  │
  │  All delivered in parallel   │    │  key=A: 1 → (wait ACK) → 3 → 5  │
  │  up to max_ack_pending       │    │  key=B: 2 (independent)          │
  │                              │    │  key=C: 4 (independent)          │
  │  ACK advances ack_floor      │    │  ACK advances floor + unblocks   │
  │  (contiguous seq only)       │    │  next message for that key       │
  │                              │    │                                  │
  │  Failure: redeliver that msg │    │  Failure: redeliver that msg,    │
  │  Others continue             │    │  same-key successors stay blocked│
  └─────────────────────────────┘    └─────────────────────────────────┘
```

### How ACK Works in Each Model

The ACK mechanism is the same in both models — each message is ACKed individually. The difference is what the broker does **after** the ACK:

```text
No Key:
  ACK(seq) → remove from pending → try advance ack_floor → done

Per-Key:
  ACK(seq) → remove from pending → unlock key →
    if blocked seqs exist for that key:
      move next blocked seq to redeliver → it becomes deliverable
    → try advance ack_floor → done
```

`ack_floor` is the highest sequence number such that **all** sequences 1..=ack_floor are acked. It advances identically in both models — the difference is that per-key ACK also unblocks the next message for that key.

### What This Means for Consumers

| Aspect | No Key | Per-Key Ordering |
|:---|:---|:---|
| **Delivery order** | Arbitrary (any consumer, any order) | Strict per-key, parallel across keys |
| **Parallelism** | Up to `max_ack_pending` total | Up to `max_ack_pending` total, but 1 per key |
| **Slow message impact** | Blocks nothing | Blocks only same-key successors |
| **Crash recovery** | Only the crashed message is redelivered | Crashed message redelivered, same-key successors wait |
| **ACK semantics** | Per-message, advances `ack_floor` | Per-message, advances `ack_floor` + unblocks key |
| **Idempotency required** | Yes (at-least-once) | Yes (at-least-once) |
| **`concurrency > 1` safe?** | Yes (no order to break) | Yes (broker never sends 2 same-key msgs in one batch) |

> [!IMPORTANT]
> In both models, you **must** design your consumers to be idempotent. At-least-once delivery means any message may be redelivered — after a crash, timeout, or network issue. This is not a bug; it is the core reliability guarantee.

---

## Per-Key Ordering: How It Works

Per-key ordering is the feature that sets Nexo apart from partition-based brokers. The previous section explained the two delivery models from a consumer's perspective. This section explains **why** per-key ordering works and how it compares to the Kafka approach.

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
- Messages **with no key** are delivered **without any ordering constraint** — full parallelism.

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
await stream.publish({"action": "update", "userId": "u1"}, key="u1")
await stream.publish({"action": "view", "userId": "u1"}, key="u1")
await stream.publish({"action": "update", "userId": "u2"}, key="u2")

# Without key — no ordering constraint, full parallelism
await stream.publish({"action": "heartbeat"})
```

:::

The `key` is a non-empty opaque byte string (string or `Uint8Array`, at most 65,535 bytes). The server treats it as an ordering lock — it does not interpret or hash it. Keys can be user IDs, order IDs, entity IDs, or any natural partitioning key in your domain. Omit the key for unordered delivery; empty keys are rejected because the wire format reserves length zero for "no key".

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
await stream.group('webhooks').subscribe((event, meta) => callExternalApi(event), {
  batchSize: 200,     // Fetch 200 messages per network request
  waitMs: 5000,       // If empty, wait 5s (server-side) before responding
  concurrency: 10,    // Up to 10 callbacks in flight at the same time
});
```

```python
stream: NexoStream[WebhookEvent] = await client.stream.get("webhooks")

async def call_api(event: WebhookEvent, meta: StreamMessageMeta) -> None:
    await call_external_api(event)

await stream.group("webhooks").subscribe(
    call_api,
    batch_size=200,   # Fetch 200 messages per network request
    wait_ms=5000,     # If empty, wait 5s (server-side) before responding
    concurrency=10,   # Up to 10 callbacks in flight at the same time
)
```

:::

**How it works**

- The SDK fetches a batch of `batchSize` messages.
- Up to `concurrency` callbacks run in parallel within that batch.
- The next fetch is issued only when the entire batch has been processed.
- Each successful callback sends and awaits its own `ACK` request. Failed callbacks remain eligible for timeout-based redelivery.
- With `concurrency > 1`, callbacks and their ACK round-trips remain parallel. A fast callback frees its pending slot and key without waiting for slower callbacks in the same fetch batch.
- If an ACK fails, the SDK stops starting new callbacks from that fetched batch, reports every concurrent ACK failure, and rejoins the group. Uncommitted messages are then redelivered.
- `stop()` cancels an idle long-poll immediately. If callbacks have already started, it waits for their ACK responses and only then leaves the consumer group.

**Trade-offs**

- With `concurrency: 1` (default), messages are processed one at a time. This is the right default for event sourcing, audit logs, and any logic where order matters.
- With `concurrency: 1`, each message adds one network round-trip for its confirmed ACK. Increase `concurrency` for order-independent workloads to overlap ACK latency and recover throughput.
- With `concurrency > 1`, callback invocations within the same batch are **not ordered**. Use this only when your handler is order-independent.
- When using **per-key ordering**, the broker guarantees that no two messages with the same key appear in the same batch — so `concurrency > 1` is safe even with keys.
- For ordered scaling, run **multiple consumers in the same group** instead — Nexo distributes messages dynamically across them.

---

## Seek & Replay

By default, new consumer groups start reading from the **beginning** of the stream history. You can use `seek` to reset the group's cursor on the server.

> [!NOTE]
> `seek` impacts the **Consumer Group state**. If you have active subscribers (via `.subscribe()`), they will immediately start receiving messages from the new position on their next fetch.

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

Nexo provides **at-least-once delivery**. Every message **will** be delivered at least once. If a consumer crashes, restarts, or is too slow, the message is **redelivered** after `ack_wait` (default 30s). **You must design your consumers to be idempotent** — this is a fundamental requirement of at-least-once systems.

- **Ack**: Successful processing. Move forward.
- **Timeout**: If a worker crashes or does not respond, the message is automatically redelivered after `ack_wait` (default 30s).
- **Max Deliveries**: After exceeding the configured retry limit, the message is moved to a **Dead Letter Topic (DLT)** and requires manual intervention.

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
const consumerGroup = stream.group('analytics');

// List entries in the DLT (paginated)
const entries = await consumerGroup.dlt.peek(limit?, offset?);
// → [{ seq: 1n, reason: "max_deliveries exceeded (5)", attempts: 5, key: Uint8Array }]

// Move a message back to the stream for redelivery
await consumerGroup.dlt.replay(seq);

// Delete a message from the DLT permanently
await consumerGroup.dlt.delete(seq);

// Purge all entries from the DLT
const count = await consumerGroup.dlt.purge();
```

```python
consumer_group = stream.group("analytics")

# List entries in the DLT (paginated)
entries = await consumer_group.dlt.peek(limit=100, offset=0)
# -> [{"seq": 1, "reason": "max_deliveries exceeded (5)", "attempts": 5, "key": b"..."}]

# Move a message back to the stream for redelivery
await consumer_group.dlt.replay(seq)

# Delete a message from the DLT permanently
await consumer_group.dlt.delete(seq)

# Purge all entries from the DLT
count = await consumer_group.dlt.purge()
```

:::

#### Auto-Unblock

A key is automatically **unparked** only when the **last DLT entry** for that key is removed (via `dlt.replay()` or `dlt.delete()`). This ensures that all poison messages for a key are resolved before new messages with that key can be delivered.

```text
DLT contains: msg-1 (key=A), msg-2 (key=A), msg-3 (key=A)
→ delete msg-1: key A still parked (msg-2, msg-3 remain)
→ delete msg-2: key A still parked (msg-3 remains)
→ delete msg-3: key A unblocked! New messages with key A can be delivered
```

#### Persistence

DLT state, redelivery entries, and parked keys are persisted in `state.log` alongside the group's `ack_floor`. They survive broker restarts, ensuring that poisoned keys remain blocked and `dlt.replay()` redrives remain deliverable after a crash or planned downtime.

> [!NOTE]
> `seek` clears all DLT entries and parked keys for the group, in addition to resetting the consumer position. It is a full reset.

---

## Persistence

Nexo uses a single ordered storage writer backed by the operating system page cache.

- **Publish acknowledgment**: `publish` and `publishBatch` return only after `write_all` succeeds. At that point the message is accepted by the OS page cache and visible to consumers. Nexo does not run `fsync` per message, so recently acknowledged data may still be lost after an OS crash or power loss.
- **Automatic backpressure**: Storage commands use a bounded queue. When it fills, publish requests wait for capacity; no message is dropped and no overload retry policy is exposed to the SDK.
- **Batching**: `publishBatch` writes multiple messages as one storage operation and is the preferred API for high-throughput ingestion.
- **Limits**: A publish batch may contain at most 65,536 messages and each encoded record may be at most 64 MiB. Limits are checked before allocation.
- **Recovery**: Nexo recovers only a contiguous sequence prefix. Partial or invalid tails are truncated; segment files after a sequence gap are renamed with `.corrupt` so they remain available for diagnosis but cannot be appended again.
- **Group State Persistence**: `STREAM_DEFAULT_FLUSH_MS` (default: 50ms) controls how often consumer group state (ack_floor, DLT entries, parked keys) is saved to disk. Message data itself relies on OS-level page cache flushing.

### High-Cardinality: Treat Streams like Keys

In Nexo, creating a stream is as cheap and safe as writing a key in a database. You can generate thousands of streams dynamically at runtime (e.g., `ai_chat_{id}` or `sensor_{id}`) without worrying about server stability.

Stream names are 1–255 ASCII bytes and may contain letters, digits, `.`, `_`, and `-`. Path separators, whitespace, `.` and `..` are rejected.

- **FD Management via LRU**: An open file handle is faster — writes are plain appends with no overhead. Opening a file, on the other hand, costs. With thousands of streams, keeping them all open simultaneously hits OS limits and memory pressure. Nexo uses a **Global FD Cache** that keeps only the `N` most recently used writer handles open, evicting and closing the least-recently-used ones when the cap is reached.
- **Controlled by `STREAM_MAX_OPEN_FILES`** (Default: 256): only the most active streams hold an open handle at any given moment.
- **Reads**: Readers use independent temporary handles so concurrent seeks cannot interfere with the append cursor. Segment locations come from the in-memory topic catalog; read handles close when the request completes.

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

---

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
const result = await client.stream.create('my-topic', {
  retention: { maxAgeMs: 3_600_000, maxBytes: 100_000_000 }  // 1h, 100MB
});
console.log(result.definition.config);
```

```python
result = await client.stream.create(
    "my-topic",
    max_age_ms=3_600_000,
    max_bytes=100_000_000,
)
print(result.definition.config)
```

:::

| Setting | Default | Description |
|:---|:---|:---|
| `maxAgeMs` | 7 days | Delete data older than this |
| `maxBytes` | 1 GB | Delete oldest data when total exceeds this |

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

*   **Optimized Batching**: Under load, Nexo automatically "drains" the message queue into optimized batches for maximum disk throughput.
*   **Flush Interval**: `STREAM_DEFAULT_FLUSH_MS` (default: 50ms) ensures data is synced even during low traffic, defining your maximum durability window.

[//]: # ()
### High-Cardinality: Treat Streams like Keys

In Nexo, creating a stream is as cheap and safe as writing a key in a database. You can generate thousands of streams dynamically at runtime (e.g., `ai_chat_{id}` or `sensor_{id}`) without worrying about server stability.

*   **Safety via LRU**: Traditional brokers crash when topic counts grow (hitting "Too Many Open Files" limits). Nexo uses a **Global FD Cache** to rotate handles automatically.
*   **Zero Overhead**: Only the most active streams stay open in memory, controlled by `STREAM_MAX_OPEN_FILES` (Default: 256).
*   **Performance Tip**: 
::: tip BEST PERFORMANCE
Set `STREAM_MAX_OPEN_FILES` to match your average *concurrently active* topics to eliminate rotation overhead.
::: 

```text
    [ Topic A ] [ Topic B ] [ Topic C ] ... [ Topic ZZZZZ ]
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
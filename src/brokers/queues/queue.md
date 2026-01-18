# Queue Broker

## Architecture: Mutex + State Machine

```
┌─────────────────────────────────────────────────────────┐
│                    QueueManager                         │
│         DashMap<name, Arc<Queue>>                       │
│              ↓ parallelism across queues                │
├─────────────────────────────────────────────────────────┤
│                      Queue                              │
│              Mutex<InternalState>                       │
│                                                         │
│   ┌─────────┐    ┌──────────┐    ┌─────────┐           │
│   │ Ready   │───▶│ InFlight │───▶│ Deleted │  (ack)    │
│   │ (FIFO)  │    │ (timeout)│    └─────────┘           │
│   └────▲────┘    └────┬─────┘                          │
│        │              │ timeout                         │
│        └──────────────┘ (retry or DLQ)                 │
│                                                         │
│   ┌───────────┐                                        │
│   │ Scheduled │──────▶ Ready  (delay elapsed)          │
│   └───────────┘                                        │
└─────────────────────────────────────────────────────────┘
```

## Design Rationale

**Why Mutex instead of Actor?**
- Queue operations are fast O(1) state transitions (insert/remove from indexed structures)
- Lock hold time is microseconds → contention is negligible
- Simpler mental model: "lock, transition, unlock"
- No channel overhead for high-frequency push/pop

**Concurrency Model**
- `DashMap` at manager level provides parallelism across different queues
- Single `Mutex` per queue serializes operations within that queue (acceptable for queue semantics)
- Background "Pulse" task wakes via `Notify` only when needed (no polling)

## Key Structures

| Structure | Purpose |
|-----------|---------|
| `BTreeMap<priority, LinkedHashSet<Uuid>>` | Ready queue: priority ordering + FIFO within same priority |
| `BTreeMap<timestamp, LinkedHashSet<Uuid>>` | Scheduled: messages waiting for delay |
| `BTreeMap<timestamp, LinkedHashSet<Uuid>>` | InFlight: messages waiting for ack (timeout tracking) |
| `HashMap<Uuid, Message>` | Registry: source of truth for message data |

## Message Lifecycle

1. **Push** → `Scheduled` (if delay) or `Ready`
2. **Pop** → `Ready` → `InFlight` (with visibility timeout)
3. **Ack** → `InFlight` → `Deleted`
4. **Timeout** → `InFlight` → `Ready` (retry) or `DLQ` (max retries exceeded)

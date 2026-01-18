# Queue Broker

## Architecture: Actor per Queue

```
┌─────────────────────────────────────────────────────────┐
│                    QueueManager                         │
│            mpsc::channel<ManagerCommand>                │
│                        │                                │
│         ┌──────────────┼──────────────┐                 │
│         ▼              ▼              ▼                 │
│   ┌───────────┐  ┌───────────┐  ┌───────────┐          │
│   │QueueActor │  │QueueActor │  │QueueActor │          │
│   │"orders"   │  │"tasks"    │  │"orders_dlq"│         │
│   ├───────────┤  ├───────────┤  ├───────────┤          │
│   │QueueState │  │QueueState │  │QueueState │          │
│   │ registry  │  │ registry  │  │ registry  │          │
│   │ ready     │  │ ready     │  │ ready     │          │
│   │ scheduled │  │ scheduled │  │ scheduled │          │
│   │ in_flight │  │ in_flight │  │ in_flight │          │
│   └───────────┘  └───────────┘  └───────────┘          │
│                                                         │
│   Pulse Loop (per queue): ProcessExpired periodically   │
└─────────────────────────────────────────────────────────┘
```

## Design Rationale

**Why Actor Model?**
- Prepares for future persistence layer (async I/O without lock hell)
- DLQ handling via `ManagerCommand::MoveToDLQ` (no circular refs)
- Each queue processes commands sequentially → no internal locks
- Natural parallelism between different queues

**State Machine (inside QueueState)**
```
┌─────────┐    ┌──────────┐    ┌─────────┐
│ Ready   │───▶│ InFlight │───▶│ Deleted │  (ack)
│ (FIFO)  │    │ (timeout)│    └─────────┘
└────▲────┘    └────┬─────┘
     │              │ timeout
     └──────────────┘ (retry or DLQ)

┌───────────┐
│ Scheduled │──────▶ Ready  (delay elapsed)
└───────────┘
```

## Key Structures

| Structure | Purpose |
|-----------|---------|
| `BTreeMap<priority, LinkedHashSet<Uuid>>` | Ready: priority ordering + FIFO |
| `BTreeMap<timestamp, LinkedHashSet<Uuid>>` | Scheduled: delayed messages |
| `BTreeMap<timestamp, LinkedHashSet<Uuid>>` | InFlight: timeout tracking |
| `HashMap<Uuid, Message>` | Registry: source of truth |

## DLQ Flow

1. Message exceeds `max_retries`
2. QueueActor sends `ManagerCommand::MoveToDLQ`
3. Manager creates/gets `{queue}_dlq` actor
4. Message pushed to DLQ

No circular references, no `Arc<Self>` hacks.

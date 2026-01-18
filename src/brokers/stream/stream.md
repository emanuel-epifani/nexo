# Stream Broker

## Architecture: Actor per Topic

```
┌──────────────────────────────────────────────────────────┐
│                   StreamManager                          │
│            mpsc::channel<ManagerCommand>                 │
│                        │                                 │
│         ┌──────────────┼──────────────┐                  │
│         ▼              ▼              ▼                  │
│   ┌───────────┐  ┌───────────┐  ┌───────────┐           │
│   │TopicActor │  │TopicActor │  │TopicActor │           │
│   │"orders"   │  │"events"   │  │"logs"     │           │
│   ├───────────┤  ├───────────┤  ├───────────┤           │
│   │TopicState │  │TopicState │  │TopicState │           │
│   │ P0 P1 P2  │  │ P0 P1     │  │ P0        │           │
│   ├───────────┤  └───────────┘  └───────────┘           │
│   │Groups     │                                          │
│   │ gen_id=3  │                                          │
│   │ members[] │                                          │
│   │ offsets{} │                                          │
│   └───────────┘                                          │
└──────────────────────────────────────────────────────────┘
```

## Design Rationale

**Why Actor per Topic?**
- Consumer groups require coordinated state (members, assignments, offsets)
- Rebalancing must be atomic → no partial states visible to clients
- Actor serialization guarantees consistency without complex multi-lock coordination
- Each topic is independent → natural parallelism

**Why not Mutex?**
- Group operations involve multiple steps (validate → modify → respond)
- With Mutex, would need to hold lock across async boundaries or risk inconsistency
- Actor model makes the "one operation at a time" guarantee explicit and safe

## Rebalancing & Epoch Fencing

```
┌─────────────────────────────────────────────────────────┐
│  1. JOIN/LEAVE triggers rebalance                       │
│  2. generation_id++ (immediately invalidates old epoch) │
│  3. Partitions redistributed among active members       │
│  4. Old consumers get REBALANCE_NEEDED on next op       │
│  5. They rejoin with new generation_id                  │
└─────────────────────────────────────────────────────────┘
```

This prevents split-brain: a consumer with stale `generation_id` cannot fetch or commit.

## Delivery Guarantees

| Guarantee | How |
|-----------|-----|
| **At-least-once** | Commit happens after processing |
| **Duplicates possible** | On crash/rebalance before commit |
| **Consumer responsibility** | Must be idempotent |

## Disconnect Handling

- `StreamSession` calls `disconnect()` explicitly before socket closes
- Manager awaits all `TopicActor` rebalances to complete
- Synchronous cleanup prevents race conditions (new consumer joining while old still leaving)

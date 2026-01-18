# Store Broker

## Architecture: DashMap (Lock-Free KV)

```
┌──────────────────────────────────────────────────────────┐
│                    StoreManager                          │
│                                                          │
│              DashMap<String, Entry>                      │
│         ┌─────┬─────┬─────┬─────┬─────┐                 │
│         │shard│shard│shard│shard│shard│  (fine-grained) │
│         └─────┴─────┴─────┴─────┴─────┘                 │
│                        │                                 │
│    SET/GET/DEL ────────┘                                │
│                                                          │
│    Background: TTL cleanup every N secs                  │
│                retain(|e| e.expires_at > now)            │
└──────────────────────────────────────────────────────────┘
```

## Design Rationale

**Why DashMap?**
- KV operations are independent (no cross-key coordination needed)
- `DashMap` provides fine-grained locking per shard → concurrent reads/writes scale linearly
- Simplest possible model: no actors, no channels, no complex state machines

**Why not Actor?**
- Actor overhead (channel send/recv) would be pure waste for single-key ops
- No ordering guarantees needed between different keys
- `DashMap` already provides the concurrency we need

## TTL Management

- Each `Entry` has optional `expires_at: Instant`
- Background task runs every `cleanup_interval_secs`
- Uses `retain()` to atomically remove expired entries
- Default TTL applied if client doesn't specify

## Operations

| Op | Behavior |
|----|----------|
| `SET key value [TTL]` | Insert/update, optional TTL (default from config) |
| `GET key` | Return value if exists and not expired |
| `DEL key` | Remove entry |

## Limitations (by design)

- **No transactions**: single-key operations only
- **No pub/sub on changes**: use PubSub broker for that pattern
- **Memory-only**: no persistence (yet)

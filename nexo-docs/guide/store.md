# Store

**In-memory concurrent data structures.** Ideal for high-velocity data that needs to be instantly accessible across all your services — user sessions, API rate-limiting counters, temporary caching.

## Map

The primary data structure — a distributed key-value map with per-key TTL and full type safety.

### Basic Usage

```typescript
// Set a key
await client.store.map.set("user:1", { name: "Max", role: "admin" });

// Get a key (with type inference)
const user = await client.store.map.get<User>("user:1");

// Delete a key
await client.store.map.del("user:1");
```

### Features

- **Granular TTL:** Set expiration per-key or globally. Ideal for temporary API caches and rate-limiting counters.
- **Type Safety:** Full TypeScript generics support for type-safe reads.
- **Binary Support:** Store raw `Buffer` data with zero JSON overhead.

### Architecture

```
┌──────────────┐     SET(key, val)      ┌──────────────────┐
│   Client A   │───────────────────────▶│    NEXO STORE    │
└──────────────┘                        │   (Shared RAM)   │
┌──────────────┐      GET(key)          │    [Map<K,V>]    │
│   Client B   │◀───────────────────────│                  │
└──────────────┘                        └──────────────────┘
```

## Set :badge[coming soon]{type=info}

Distributed unique-value collection. Stay tuned.

## List :badge[coming soon]{type=info}

Distributed ordered list. Stay tuned.

## Performance

**4.5M ops/sec** with sub-microsecond latency for SET operations.

# Store

**In-memory concurrent data structures.** Ideal for high-velocity data that needs to be instantly accessible across all your services — user sessions, API rate-limiting counters, temporary caching.

## Map

The primary data structure — a distributed key-value map with per-key.

```
┌──────────────┐     SET(key, val)      ┌──────────────────┐
│   Client A   │───────────────────────▶│    NEXO STORE    │
└──────────────┘                        │   (Shared RAM)   │
┌──────────────┐      GET(key)          │    [Map<K,V>]    │
│   Client B   │◀───────────────────────│                  │
└──────────────┘                        └──────────────────┘
```

### Basic Usage

```typescript
// Set a key
await client.store.map.set("user:1", { name: "Max", role: "admin" });

// Get a key (with type inference)
const user = await client.store.map.get<User>("user:1");

// Delete a key
await client.store.map.del("user:1");
```





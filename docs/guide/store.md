# Store

**In-memory concurrent data structures.** Ideal for high-velocity data that needs to be instantly accessible across all your services — user sessions, API rate-limiting counters, temporary caching.

## Map

The primary data structure — a distributed key-value map with per-key TTL.

```
┌──────────────┐     SET(key, val)      ┌──────────────────┐
│   Client A   │───────────────────────▶│    NEXO STORE    │
└──────────────┘                        │   (Shared RAM)   │
┌──────────────┐      GET(key)          │    [Map<K,V>]    │
│   Client B   │◀───────────────────────│                  │
└──────────────┘                        └──────────────────┘
```

### Basic Usage

::: code-group

```typescript
// Set a key (no TTL = persistent)
await client.store.map.set("user:1", { name: "Max", role: "admin" });

// Get a key (with type inference)
const user = await client.store.map.get<User>("user:1");

// Delete a key
await client.store.map.del("user:1");
```

```python
# Set a key (no TTL = persistent)
await client.store.map.set("user:1", {"name": "Max", "role": "admin"})

# Get a key
user: User | None = await client.store.map.get("user:1")

# Delete a key
await client.store.map.delete("user:1")
```

:::

### TTL (Time-to-Live)

Keys can be set with an optional TTL in **seconds**. When the TTL expires the key is automatically removed.

| Behavior | Description |
|---|---|
| `set(key, val)` | No expiry — key persists until explicitly deleted |
| `set(key, val, { ttl: n })` with `n > 0` | Key expires after `n` seconds |
| `set(key, val, { ttl: 0 })` | **Error** — `ttl` must be greater than 0 |

::: code-group

```typescript
// Persistent key — no TTL, lives until del()
await client.store.map.set("config:feature_flags", { darkMode: true });

// Temporary key — expires after 60 seconds
await client.store.map.set("session:abc", { userId: 42 }, { ttl: 60 });

// Short-lived cache — expires after 5 seconds
await client.store.map.set("cache:hot_data", payload, { ttl: 5 });

// ttl: 0 is rejected by the server
await client.store.map.set("bad", "val", { ttl: 0 });
```

```python
# Persistent key — no TTL, lives until delete()
await client.store.map.set("config:feature_flags", {"darkMode": True})
flags: FeatureFlags | None = await client.store.map.get("config:feature_flags")

# Temporary key — expires after 60 seconds
await client.store.map.set("session:abc", {"userId": 42}, {"ttl": 60})
session: Session | None = await client.store.map.get("session:abc")

# Short-lived cache — expires after 5 seconds
await client.store.map.set("cache:hot_data", payload, {"ttl": 5})

# ttl: 0 is rejected by the server
await client.store.map.set("bad", "val", {"ttl": 0})
```

:::





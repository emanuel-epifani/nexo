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


### INCR

Atomically increment (or decrement) a key's integer value by `delta`.

- If the key doesn't exist, it starts from `0`.
- If the key was set with `set(key, number)`, the value is incremented.
- If the key was set with a string or other type, `incr` returns an error.
- TTL is preserved — incrementing doesn't reset or remove an existing TTL.
- `incr` returns the new value as a number (`number` in TS, `int` in Python).
- `get` on a key created by `incr` also returns a number, not a string.

| Call | Result |
|---|---|
| `incr(key)` | Increment by 1 |
| `incr(key, delta)` | Increment by `delta` (negative = decrement) |
| Key doesn't exist | Starts from `0`, returns `delta` |
| Key set with `set(key, "hello")` | **Error** — value is not an integer |
| Overflow / underflow | **Error** — increment would overflow |

::: code-group

```typescript
// Create a counter — use a number, not a string
await client.store.map.set("user:1:score", 10);

// Increment by 1 (default)
const views = await client.store.map.incr("page:views"); // → 1

// Increment by a custom amount
const score = await client.store.map.incr("user:1:score", 10); // → 20

// Decrement by 5
const remaining = await client.store.map.incr("quota:user:1", -5); // → -5

// Reading the value back gives you a number
const val = await client.store.map.get("page:views"); // → 1 (number)
```

```python
# Create a counter — use a number, not a string
await client.store.map.set("user:1:score", 10)

# Increment by 1 (default)
views = await client.store.map.incr("page:views")  # → 1

# Increment by a custom amount
score = await client.store.map.incr("user:1:score", 10)  # → 20

# Decrement by 5
remaining = await client.store.map.incr("quota:user:1", -5)  # → -5

# Reading the value back gives you an int
val = await client.store.map.get("page:views")  # → 1 (int)
```

:::

### CLEAR

Remove keys in bulk. Both methods return the number of keys removed.

- `clearAll()` — remove every key from the store
- `clearWithPrefix(prefix)` — remove all keys that start with `prefix`

`clearWithPrefix` is O(N) — it scans every key to check the prefix. However, unlike single-threaded databases (e.g. Redis), Nexo is multi-threaded: the scan locks one shard at a time, so other connections, other store operations, and other brokers (queue, stream, pubsub) continue running in parallel while the scan is in progress. It is safe to call in production, even with millions of keys.

| Call | Result |
|---|---|
| `clearAll()` | Returns count of all keys removed |
| `clearWithPrefix("session:")` | Returns count of keys starting with `session:` |
| `clearWithPrefix("")` | Same as `clearAll()` — every key starts with `""` |
| No keys match | Returns `0` |

The returned count includes keys that were already expired but not yet cleaned up by the background TTL task. This is intentional — both live and expired entries are removed from memory.

::: code-group

```typescript
// Remove all keys (e.g. between test runs or on deploy)
const removed = await client.store.map.clearAll(); // → 347

// Remove only keys for a specific service
const sessions = await client.store.map.clearWithPrefix("session:"); // → 12
```

```python
# Remove all keys (e.g. between test runs or on deploy)
removed = await client.store.map.clear_all()  # → 347

# Remove only keys for a specific service
sessions = await client.store.map.clear_with_prefix("session:")  # → 12
```

:::

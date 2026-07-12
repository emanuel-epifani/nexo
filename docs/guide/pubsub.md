# Pub/Sub

**Transient message bus with Topic-based routing.** Designed for "fire-and-forget" scenarios where low latency is critical — live chat, stock tickers, multi-service notifications.

Unlike [Queues](/guide/queue) and [Streams](/guide/stream) (pull-based with long-polling), Pub/Sub is **push-based**: the server delivers messages to subscribers immediately as they are published, with no polling loop on the client side. This makes it the lowest-latency primitive in Nexo. Topics are **auto-created** on first publish or subscribe — no `.create()` needed.

## Basic Usage

```typescript
// Define a topic
const alerts = client.pubsub<AlertMsg>("system-alerts");

// Subscribe
await alerts.subscribe((msg) => console.log(msg));

// Publish
await alerts.publish({ level: "high" });

// Unsubscribe
await alerts.unsubscribe();
```

## Wildcards

Nexo supports MQTT-style wildcard subscriptions:

### Single-Level Wildcard (+)

Matches exactly one segment.

```typescript
// Matches: 'home/kitchen/light', 'home/garage/light'
const roomLights = client.pubsub<LightStatus>('home/+/light');
await roomLights.subscribe((status) => console.log('Light is:', status.state));
```

### Multi-Level Wildcard (#)

Matches all remaining segments.

```typescript
// Matches all topics under 'sensors/'
const allSensors = client.pubsub<SensorData>('sensors/#');
await allSensors.subscribe((data) => console.log('Sensor value:', data.value));
```

::: warning Wildcards are subscribe-only
You can only subscribe with wildcards. Publishing must always target a **concrete topic** (no `+` or `#`). The `#` wildcard must be the **last segment** in a subscribe pattern (e.g. `sensors/#` is valid, `sensors/#/temp` is rejected). Empty segments are not allowed in either publish or subscribe (e.g. `sensors//temp` is rejected).
:::

## Retained Messages

By default, Pub/Sub messages are ephemeral — if no one is subscribed, the message is lost. With `retain: true`, the **last published value** is stored and automatically delivered to any new subscriber on that topic.

```typescript
// Publish with retain — this value is stored
await client.pubsub<string>('config/theme').publish('dark', { retain: true });

// A new subscriber connecting later instantly receives 'dark'
await client.pubsub<string>('config/theme').subscribe((theme) => {
  console.log(theme); // 'dark' — received immediately
});
```

Retained messages are **persisted to SQLite** and survive server restarts. They have a default **TTL of 1 hour** (configurable via `PUBSUB_DEFAULT_RETAINED_TTL_SECS`), after which they are automatically cleaned up.

To clear a retained message, use `clear()`:

```typescript
await client.pubsub<string>('config/theme').clear();
```

A later subscriber on that topic will not receive a retained value.

## Configuration

### Environment Variables

Global, set at server startup.

| Variable | Default | Description |
|:---|:---|:---|
| `PUBSUB_ROOT_PERSISTENCE_PATH` | `./data/pubsub` | Directory for retained messages SQLite DB |
| `PUBSUB_DEFAULT_RETAINED_TTL_SECS` | `3600` (1h) | Default TTL for retained messages when no explicit `ttl` is provided |
| `PUBSUB_CLEANUP_INTERVAL_SECS` | `60` | Background cleanup interval for expired retained messages |
| `PUBSUB_RETAINED_FLUSH_MS` | `500` | How often retained messages are flushed to SQLite |

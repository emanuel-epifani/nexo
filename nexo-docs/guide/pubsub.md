# Pub/Sub

**Transient message bus with Topic-based routing.** Designed for "fire-and-forget" scenarios where low latency is critical — live chat, stock tickers, multi-service notifications.

Unlike [Queues](/guide/queue) (pull-based), Pub/Sub is **push-based**: the server delivers messages to subscribers immediately as they are published. Topics are **auto-created** on first publish or subscribe — no `.create()` needed.

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
You can only subscribe with wildcards. Publishing must always target a **concrete topic** (no `+` or `#`).
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

Retained messages are **persisted to disk** and survive server restarts. They have a default **TTL of 1 hour** (configurable via `PUBSUB_DEFAULT_RETAINED_TTL_SECS`), after which they are automatically cleaned up.

To clear a retained message, publish an empty payload with `retain: true`.

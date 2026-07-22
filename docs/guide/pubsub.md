# Pub/Sub

**Transient message bus with Topic-based routing.** Designed for "fire-and-forget" scenarios where low latency is critical — live chat, stock tickers, multi-service notifications.

Unlike [Queues](/guide/queue) and [Streams](/guide/stream) (pull-based with long-polling), Pub/Sub is **push-based**: the server delivers messages to subscribers immediately as they are published, with no polling loop on the client side. This makes it the lowest-latency primitive in Nexo. Topics are **auto-created** on first publish or subscribe — no `.create()` needed.

## Basic Usage

::: code-group

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

```python
# Define a topic
alerts: NexoTopic[AlertMsg] = client.pubsub("system-alerts")

# Subscribe
async def on_alert(msg: AlertMsg) -> None:
    print(msg)

await alerts.subscribe(on_alert)

# Publish
await alerts.publish({"level": "high"})

# Unsubscribe
await alerts.unsubscribe()
```

:::

## Wildcards

Nexo supports MQTT-style wildcard subscriptions:

### Single-Level Wildcard (+)

Matches exactly one segment.

::: code-group

```typescript
// Matches: 'home/kitchen/light', 'home/garage/light'
const roomLights = client.pubsub<LightStatus>('home/+/light');
await roomLights.subscribe((status) => console.log('Light is:', status.state));
```

```python
# Matches: 'home/kitchen/light', 'home/garage/light'
room_lights: NexoTopic[LightStatus] = client.pubsub('home/+/light')

async def on_status(status: LightStatus) -> None:
    print('Light is:', status["state"])

await room_lights.subscribe(on_status)
```

:::

### Multi-Level Wildcard (#)

Matches all remaining segments.

::: code-group

```typescript
// Matches all topics under 'sensors/'
const allSensors = client.pubsub<SensorData>('sensors/#');
await allSensors.subscribe((data) => console.log('Sensor value:', data.value));
```

```python
# Matches all topics under 'sensors/'
all_sensors: NexoTopic[SensorData] = client.pubsub('sensors/#')

async def on_data(data: SensorData) -> None:
    print('Sensor value:', data["value"])

await all_sensors.subscribe(on_data)
```

:::

::: warning Wildcards are subscribe-only
You can only subscribe with wildcards. Publishing must always target a **concrete topic** (no `+` or `#`). The `#` wildcard must be the **last segment** in a subscribe pattern (e.g. `sensors/#` is valid, `sensors/#/temp` is rejected). Empty segments are not allowed in either publish or subscribe (e.g. `sensors//temp` is rejected).
:::

## Retained Messages

By default, Pub/Sub messages are ephemeral — if no one is subscribed, the message is lost. With `retain: true`, the **last published value** is stored and automatically delivered to any new subscriber on that topic.

::: code-group

```typescript
// Publish with retain — this value is stored
await client.pubsub<string>('config/theme').publish('dark', { retain: true });

// A new subscriber connecting later instantly receives 'dark'
await client.pubsub<string>('config/theme').subscribe((theme) => {
  console.log(theme); // 'dark' — received immediately
});
```

```python
# Publish with retain — this value is stored
theme_topic: NexoTopic[str] = client.pubsub('config/theme')
await theme_topic.publish('dark', {"retain": True})

# A new subscriber connecting later instantly receives 'dark'
async def on_theme(theme: str) -> None:
    print(theme)  # 'dark' — received immediately

await theme_topic.subscribe(on_theme)
```

:::

Retained messages are **persisted to SQLite** and survive server restarts. They have a default **TTL of 1 hour** (configurable via `PUBSUB_DEFAULT_RETAINED_TTL_SECS`), after which they are automatically cleaned up.

To clear a retained message, use `clear()`:

::: code-group

```typescript
await client.pubsub<string>('config/theme').clear();
```

```python
theme_topic: NexoTopic[str] = client.pubsub('config/theme')
await theme_topic.clear()
```

:::

A later subscriber on that topic will not receive a retained value.

## Callback Execution

Each subscription runs in its own dedicated consumer loop, isolated from the connection's read loop. See [Broker Semantics](/guide/introduction#broker-semantics) for details on how all brokers dispatch callbacks.

## Configuration

### Environment Variables

Global, set at server startup.

| Variable | Default | Description |
|:---|:---|:---|
| `PUBSUB_ROOT_PERSISTENCE_PATH` | `./data/pubsub` | Directory for retained messages SQLite DB |
| `PUBSUB_DEFAULT_RETAINED_TTL_SECS` | `3600` (1h) | Default TTL for retained messages when no explicit `ttl` is provided |
| `PUBSUB_CLEANUP_INTERVAL_SECS` | `60` | Background cleanup interval for expired retained messages |
| `PUBSUB_RETAINED_FLUSH_MS` | `500` | How often retained messages are flushed to SQLite |

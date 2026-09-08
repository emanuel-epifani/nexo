# Pub/Sub

**Transient message bus with Topic-based routing.** Designed for "fire-and-forget" scenarios where low latency is critical — live chat, stock tickers, multi-service notifications.

Unlike [Queues](/guide/queue) and [Streams](/guide/stream) (pull-based with long-polling), Pub/Sub is **push-based**: the server delivers messages to subscribers immediately as they are published, with no polling loop on the client side. This makes it the lowest-latency primitive in Nexo. Topics are routing addresses rather than provisioned resources, so they have no `create`, `get`, or `delete` lifecycle.

## Basic Usage

::: code-group

```typescript
const alerts = client.pubsub.topic<AlertMsg>('system-alerts');
const subscription = await alerts.subscribe((message) => console.log(message));
await alerts.publish({ level: 'high' });
await subscription.stop();
```

```python
alerts: NexoTopic[AlertMsg] = client.pubsub.topic("system-alerts")

async def on_alert(message: AlertMsg) -> None:
    print(message)

subscription = await alerts.subscribe(on_alert)
await alerts.publish({"level": "high"})
await subscription.stop()
```

:::

## Wildcards

Nexo supports MQTT-style wildcard subscriptions:

### Single-Level Wildcard (+)

Matches exactly one segment.

::: code-group

```typescript
// Matches: 'home/kitchen/light', 'home/garage/light'
const roomLights = client.pubsub.pattern<LightStatus>('home/+/light');
await roomLights.subscribe((status) => console.log('Light is:', status.state));
```

```python
# Matches: 'home/kitchen/light', 'home/garage/light'
room_lights: NexoPattern[LightStatus] = client.pubsub.pattern('home/+/light')

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
const allSensors = client.pubsub.pattern<SensorData>('sensors/#');
await allSensors.subscribe((data) => console.log('Sensor value:', data.value));
```

```python
# Matches all topics under 'sensors/'
all_sensors: NexoPattern[SensorData] = client.pubsub.pattern('sensors/#')

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
const themeTopic = client.pubsub.topic<string>('config/theme');
await themeTopic.publish('dark', { retain: true });

// A new subscriber connecting later instantly receives 'dark'
await themeTopic.subscribe((theme) => {
  console.log(theme); // 'dark' — received immediately
});
```

```python
# Publish with retain — this value is stored
theme_topic: NexoTopic[str] = client.pubsub.topic('config/theme')
await theme_topic.publish('dark', retain=True)

# A new subscriber connecting later instantly receives 'dark'
async def on_theme(theme: str) -> None:
    print(theme)  # 'dark' — received immediately

await theme_topic.subscribe(on_theme)
```

:::

Retained messages are **persisted to SQLite** and survive server restarts. They have a default **TTL of 1 hour** (configurable via `PUBSUB_DEFAULT_RETAINED_TTL_SECS`), after which they are automatically cleaned up.

To clear a retained message, use `clearRetained()` / `clear_retained()`:

::: code-group

```typescript
await client.pubsub.topic<string>('config/theme').clearRetained();
```

```python
theme_topic: NexoTopic[str] = client.pubsub.topic('config/theme')
await theme_topic.clear_retained()
```

:::

A later subscriber on that topic will not receive a retained value.

## Multiple Local Subscribers

A client may register multiple independent subscriptions for the same topic or pattern. The SDK sends one server-side `SUB` for the first local listener, fans messages out locally, and sends `UNSUB` only after the final listener stops.

```typescript
const alerts = client.pubsub.topic<Alert>('alerts');
const ui = await alerts.subscribe(renderAlert);
const metrics = await alerts.subscribe(recordMetric);

await ui.stop();      // metrics remains active
await metrics.stop(); // final listener: UNSUB is sent
```

Each subscription preserves its own message order and callback failures are isolated. Registering the same callback twice intentionally produces two deliveries to that callback. Retained replay occurs on the first local listener, when the SDK creates the server-side subscription; listeners added while that pattern is already active receive subsequent messages.

## Callback Execution

Each subscription runs in its own dedicated consumer loop, isolated from the connection's read loop. See [Broker Semantics](/guide/introduction#broker-semantics) for details on how all brokers dispatch callbacks.

## Slow-Consumer Disconnect

Each subscriber has a **bounded push channel** (capacity configurable via `PUBSUB_PUSH_CHANNEL_CAPACITY`, default 1024). If a subscriber cannot drain messages fast enough and the channel fills up, the server **disconnects** that subscriber to prevent OOM and head-of-line blocking on other subscribers. This matches the semantics of Redis, NATS, and MQTT.

The disconnected client will automatically reconnect and re-subscribe (see [Reconnection](/guide/introduction#reconnection)). Other subscribers on the same topic are unaffected.

## Configuration

### Environment Variables

Global, set at server startup.

| Variable | Default | Description |
|:---|:---|:---|
| `PUBSUB_ROOT_PERSISTENCE_PATH` | `./data/pubsub` | Directory for retained messages SQLite DB |
| `PUBSUB_DEFAULT_RETAINED_TTL_SECS` | `3600` (1h) | Default TTL for retained messages when no explicit `ttl` is provided |
| `PUBSUB_CLEANUP_INTERVAL_SECS` | `60` | Background cleanup interval for expired retained messages |
| `PUBSUB_RETAINED_FLUSH_MS` | `500` | How often retained messages are flushed to SQLite |
| `PUBSUB_PUSH_CHANNEL_CAPACITY` | `8192` | Per-subscriber bounded channel capacity. When full, the subscriber is disconnected (slow-consumer protection) |

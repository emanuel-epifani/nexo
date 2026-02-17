# Pub/Sub

**Transient message bus with Topic-based routing.** Designed for "fire-and-forget" scenarios where low latency is critical — live chat, stock tickers, multi-service notifications.

## Basic Usage

```typescript
// Define a topic
const alerts = client.pubsub<AlertMsg>("system-alerts");

// Subscribe
await alerts.subscribe((msg) => console.log(msg));

// Publish
await alerts.publish({ level: "high" });
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

## Retained Messages

Last value is stored and immediately sent to new subscribers.

```typescript
await client.pubsub<string>('config/theme').publish('dark', { retain: true });
```

## Performance

**3.8M msg/sec** fan-out with sub-microsecond latency (1 Publisher → 1k Subscribers).

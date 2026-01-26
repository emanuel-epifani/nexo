# Nexo Client SDK

High-performance TypeScript client for [Nexo Broker](https://github.com/emanuel-epifani/nexo).


## Quick Start
```bash
# install SDK
npm install @emanuelepifani/nexo-client
```
```typescript
// Initialize client
const client = await NexoClient.connect({ host: 'localhost', port: 7654 });
```

### 1. STORE

```typescript
await client.store.kv.set("user:1", { name: "Max" });
const user = await client.store.kv.get<User>("user:1");
```

### 2. QUEUE

```typescript
// Create queue
const mailQ = client.queue<MailJob>("emails").create();
// Push message
await mailQ.push({ to: "test@test.com" });
// Subscribe
await mailQ.subscribe((msg) => console.log(msg));
```

<details>
<summary><strong>Advanced Queue Features (Retry, Delay, Priority, Concurrency)</strong></summary>

```typescript
// 1. Queue Configuration (Policy)
// Defines default behavior for ALL messages in this queue
const criticalQueue = client.queue('critical-tasks').create({
    visibilityTimeoutMs: 10000, // If worker doesn't ACK in 10s, retry delivery
    maxRetries: 5,              // Move to DLQ after 5 failed attempts
    ttlMs: 60000,               // Expire message if not consumed within 60s
    delayMs: 0                  // Delay for scheduled msg (default=0, no delay)
});

// 2. Push Options (Per-message Override)
// Priority: Higher priority (255) delivered before lower (0)
await criticalQueue.push({ type: 'urgent' }, { priority: 255 });

// Delay: This specific message becomes visible only after 1 hour
// (Overrides queue's default delayMs)
await criticalQueue.push({ type: 'scheduled' }, { delayMs: 3600000 });

// 3. Consume Options (Worker Tuning)
await criticalQueue.subscribe(
    async (task) => { await processTask(task); },
    {
        batchSize: 10,   // Fetch 10 messages at once to optimizes throughput (default: 50)
        concurrency: 5,  // Process 5 messages in parallel (default: 5)
        waitMs: 5000     // Long Polling: wait up to 5s if queue is empty
    }
);
```
</details>

### 3. PUB/SUB 

```typescript
// Define topic (not need to create, auto-created on first publish)
const alerts = client.topic<AlertMsg>("system-alerts");
// Subscribe
await alerts.subscribe((msg) => console.log(msg));
// Publish
await alerts.publish({ level: "high" });
```

<details>
<summary><strong>Wildcards & Retained Messages</strong></summary>

```typescript
// WILDCARD SUBSCRIPTIONS
// ----------------------

// 1. Single-Level Wildcard (+)
// Matches: 'home/kitchen/light', 'home/garage/light'
const roomLights = client.pubsub<'ON' | 'OFF'>('home/+/light');
await roomLights.subscribe((status) => console.log('Light is:', status));

// 2. Multi-Level Wildcard (#)
// Matches all topics under 'sensors/'
const allSensors = client.pubsub<{ value: number }>('sensors/#');
await allSensors.subscribe((data) => console.log('Sensor value:', data.value));

// PUBLISHING (No wildcards allowed!)
// ---------------------------------
// You must publish to concrete topics with matching types
await client.pubsub<'ON' | 'OFF'>('home/kitchen/light').publish('ON');
await client.pubsub<{ value: number }>('sensors/kitchen/temp').publish({ value: 22.5 });

// RETAINED MESSAGES
// -----------------
// Last value is stored and immediately sent to new subscribers
await client.pubsub<string>('config/theme').publish('dark', { retain: true });
```
</details>

### 4. STREAM

```typescript
const events = client.stream('user-events', 'analytics-group');
// Publisher
await events.publish({ event: 'login', userId: 123 });
// Consumer
await events.subscribe((msg) => { console.log('Event:', msg.event); });
--------
// Create topic
const producer = client.stream<OrderEvent>('orders').create();
await producer.publish({ id: 'ord_123', amount: 99.00, status: 'paid' });

// 3. Subscribe (Consumer)
// Consumers MUST specify a group ('analytics')
const consumer = client.stream<OrderEvent>('orders', 'analytics');

await consumer.subscribe((msg) => {
    // 'msg' is typed as OrderEvent
    console.log(`Processing order ${msg.id}: ${msg.status}`);
});
```

<details>
<summary><strong>Consumer Groups & Scaling</strong></summary>

```typescript
// SCALING (Competing Consumers)
// Same Group ('workers') -> Load Balancing
// Partitions are distributed among workers.
const w1 = client.stream<OrderEvent>('orders', 'workers');
const w2 = client.stream<OrderEvent>('orders', 'workers');

// BROADCAST (Fan-Out)
// Different Groups -> Each group gets a full copy of the stream
const analytics = client.stream<OrderEvent>('orders', 'analytics');
const audit     = client.stream<OrderEvent>('orders', 'audit');
```
</details>

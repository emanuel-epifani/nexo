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
const mailQ = client.queue<MailJob>("emails").create();
await mailQ.push({ to: "test@test.com" });
```

<details>
<summary><strong>Advanced Queue Features (Retry, Delay, Priority, Concurrency)</strong></summary>

```typescript
// Create Queue with custom policy
const criticalQueue = client.queue('critical-tasks').create({
  maxRetries: 3,  // default 
  visibilityTimeoutMs: 5000, // Wait 5s before retry
  deadLetterQueue: 'dlq-critical'
});

// Priority: Higher priority (255) delivered before lower (0)
await criticalQueue.push({ id: 1 }, { priority: 255 });

// Delay: Schedule message for 1 hour later
await criticalQueue.push({ id: 2 }, { delayMs: 3600000 });

// Concurrency: Process 5 messages in parallel
await criticalQueue.subscribe(
  async (task) => { await processTask(task); },
  { concurrency: 5 }
);
```
</details>

### 3. PUB/SUB 

```typescript
const alerts = client.topic<AlertMsg>("system-alerts");
// Subscribe
await alerts.subscribe((msg) => console.log(msg));
// Publish
await alerts.publish({ level: "high" });
```

<details>
<summary><strong>Wildcards & Retained Messages</strong></summary>

```typescript
// Single-level wildcard (+): matches 'home/kitchen/temp' but not 'home/kitchen/fridge/temp'
const anyRoom = client.pubsub('home/+/temp');
await anyRoom.subscribe((t) => console.log('Room temp:', t));

// Multi-level wildcard (#): matches everything under sensors/
const allSensors = client.pubsub('sensors/#');
await allSensors.subscribe((val) => console.log('Sensor:', val));

// Retained Message: New subscribers get the last value immediately
await client.pubsub('config/theme').publish('dark', { retain: true });
```
</details>

### 4. STREAM

```typescript
const events = client.stream('user-events', 'analytics-group');
// Publisher
await events.publish({ event: 'login', userId: 123 });
// Consumer
await events.subscribe((msg) => { console.log('Event:', msg.event); });
```

<details>
<summary><strong>Consumer Groups & Scaling</strong></summary>

```typescript
// SCALING: Same Group ('workers')
// Partitions are split between consumers. Useful for parallel processing.
const worker1 = client.stream('orders', 'workers');
const worker2 = client.stream('orders', 'workers');
// worker1 gets Partition 0, worker2 gets Partition 1...

// BROADCAST: Different Groups
// Each group receives a full copy of the stream.
const analytics = client.stream('orders', 'analytics');
const auditing = client.stream('orders', 'audit');
// Both analytics and audit services receive all order events independently.
```
</details>

# Nexo Client SDK

High-performance TypeScript client for [Nexo](https://github.com/emanuel-epifani/nexo).

## Quick Start


### Run server
```bash
docker run -p 7654:7654 -p 8080:8080 emanuelepifani/nexo:latest
```
This exposes:
- Port 7654 (TCP): Main server socket for SDK clients.
- Port 8080 (HTTP): Web Dashboard with status of all brokers.

### Install SDK

```bash
npm install @emanuelepifani/nexo-client
```

### Connection
```typescript
const client = await NexoClient.connect({ host: 'localhost', port: 7654 });
```

### 1. STORE

```typescript
// Set key
await client.store.map.set("user:1", { name: "Max", role: "admin" });
// Get key
const user = await client.store.map.get<User>("user:1");
// Del key
await client.store.map.del("user:1");
```

### 2. QUEUE

```typescript
// Create queue
const mailQ = await client.queue<MailJob>("emails").create();
// Push message
await mailQ.push({ to: "test@test.com" });
// Subscribe
await mailQ.subscribe((msg) => console.log(msg));
// Delete queue 
await mailQ.delete();
```

<details>
<summary><strong>Advanced Features (Persistence, Retry, Delay, Priority)</strong></summary>

```typescript
// -------------------------------------------------------------
// 1. CREATION (Default behavior for all messages in this queue)
// -------------------------------------------------------------
interface CriticalTask { type: string; payload: any; }

const criticalQueue = await client.queue<CriticalTask>('critical-tasks').create({
    // RELIABILITY:
    visibilityTimeoutMs: 10000, // Retry delivery if not ACKed within 10s   (default=30s)
    maxRetries: 5,              // Move to DLQ after 5 failures             (default=5)
    ttlMs: 60000,               // Message expires if not consumed in 60s   (default=7days)

    // PERSISTENCE:
    // - 'file_sync':  Save every message (Safest, Slowest)
    // - 'file_async': Flush periodically (Fast & Durable) - 
    // DEFAULT: 'file_async': Flush every 50ms or 5000 msgs
    persistence: 'file_sync',
});


// ---------------------------------------------------------
// 2. PRODUCING (Override specific behaviors per message)
// ---------------------------------------------------------

// PRIORITY: Higher value (255) delivered before lower values (0)
// This message jumps ahead of all priority < 255 messages sent previously and still not consumed.
await criticalQueue.push({ type: 'urgent' }, { priority: 255 });

// SCHEDULING: Delay visibility
// This message is hidden for 1 hour (default delayMs: 0, instant)
await criticalQueue.push({ type: 'scheduled' }, { delayMs: 3600000 });



// ---------------------------------------------------------
// 3. CONSUMING (Worker Tuning to optimize throughput and latency)
// ---------------------------------------------------------
await criticalQueue.subscribe(
    async (task) => { await processTask(task); },
    {
        batchSize: 100,   // Network: Fetch 100 messages in one request
        concurrency: 10,  // Local: Process 10 messages concurrently (useful for I/O tasks)
        waitMs: 5000      // Polling: If empty, wait 5s for new messages before retrying
    }
);
```
</details>

### 3. PUB/SUB

```typescript
// Define topic (not need to create, auto-created on first publish)
const alerts = client.pubsub<AlertMsg>("system-alerts");
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
const roomLights = client.pubsub<LightStatus>('home/+/light');
await roomLights.subscribe((status) => console.log('Light is:', status.state));

// 2. Multi-Level Wildcard (#)
// Matches all topics under 'sensors/'
const allSensors = client.pubsub<SensorData>('sensors/#');
await allSensors.subscribe((data) => console.log('Sensor value:', data.value));

// PUBLISHING (No wildcards allowed!)
// ---------------------------------
// You must publish to concrete topics with matching types
await client.pubsub<LightStatus>('home/kitchen/light').publish({ state: 'ON' });
await client.pubsub<SensorData>('sensors/kitchen/temp').publish({ value: 22.5, unit: 'C' });

// RETAINED MESSAGES
// -----------------
// Last value is stored and immediately sent to new subscribers
await client.pubsub<string>('config/theme').publish('dark', { retain: true });
```
</details>

### 4. STREAM

```typescript
// Create topic
const stream = await client.stream<UserEvent>('user-events').create();
// Publisher
await stream.publish({ type: 'login', userId: 'u1' });
// Consumer (must specify group)
await stream.subscribe('analytics', (msg) => {console.log(`User ${msg.userId} performed ${msg.type}`); });
// Delete topic
await stream.delete();
```

<details>
<summary><strong>Advanced Features (Consumer Groups, Persistence, Retention)</strong></summary>

```typescript
// ---------------------------------------------------------
// 1. STREAM CREATION & POLICY
// ---------------------------------------------------------
const orders = await client.stream<Order>('orders').create({
    // SCALING
    partitions: 4,              // Max concurrent consumers per group on same topic (default=8)

    // PERSISTENCE:
    // - 'file_sync':  Save every message (Safest, Slowest)
    // - 'file_async': Flush periodically (Fast & Durable) - 
    // DEFAULT: 'file_async': Flush every 50ms or 5000 msgs
    persistence: 'file_sync',

    // RETENTION (Cleanup Policy)
    // --------------------------
    // Delete old data when EITHER limit is reached:
    retention: {
        maxAgeMs: 86400000,     // 1 Day (Default: 7 Days)
        maxBytes: 536870912     // 512 MB (Default: 1 GB)
    },
});

// ---------------------------------------------------------
// 2. CONSUMING (Scaling & Broadcast patterns)
// ---------------------------------------------------------

// SCALING (Microservices Replicas / K8s Pods)
// Same Group ('workers') -> Automatic Load Balancing & Rebalancing
// Partitions are distributed among workers.
await orders.subscribe('workers', (order) => process(order));
await orders.subscribe('workers', (order) => process(order));


// BROADCAST (Independent Domains)
// Different Groups -> Each group gets a full copy of the stream.
// Useful for independent services reacting to the same event.
await orders.subscribe('analytics', (order) => trackMetrics(order));
await orders.subscribe('audit-log', (order) => saveAudit(order));
```
</details>



---

### Binary Payloads (Zero-Overhead)

All Nexo brokers (**Store, Queue, Stream, PubSub**) natively support raw binary data (`Buffer`).    
Bypassing JSON serialization drastically reduces Latency, increases Throughput, and saves Bandwidth (~30% smaller payloads) .

**Perfect for:** Video chunks, Images, Protobuf/MsgPack, Encrypted blobs.

```typescript
// Send 1MB raw buffer (30% smaller than JSON/Base64)
const heavyPayload = Buffer.alloc(1024 * 1024);

// 1. STREAM: Replayable Data (e.g. CCTV Recording, Event Sourcing)
await client.stream('cctv-archive').publish(heavyPayload);

// 2. PUBSUB: Ephemeral Live Data (e.g. VoIP, Real-time Sensor)
await client.pubsub('live-audio-call').publish(heavyPayload);

// 3. STORE (Cache Images)
await client.store.map.set('user:avatar:1', heavyPayload);

// 4. QUEUE (Process Files)
await client.queue('pdf-processing').push(heavyPayload);
```

---

## License

MIT


## Links

- **Nexo Broker (Server):** [GitHub Repository](https://github.com/emanuel-epifani/nexo)
- **Server Docs:** [Nexo Internals & Architecture](https://github.com/emanuel-epifani/nexo/tree/main/docs)
- **SDK Source:** [sdk/ts](https://github.com/emanuel-epifani/nexo/tree/main/sdk/ts)
- **Docker Image:** [emanuelepifani/nexo](https://hub.docker.com/r/emanuelepifani/nexo)

## Author

Built by **Emanuel Epifani**.

- [LinkedIn](https://www.linkedin.com/in/emanuel-epifani/)
- [GitHub](https://github.com/emanuel-epifani)
# Nexo Client SDK

High-performance TypeScript client for [Nexo](https://nexo-docs-hub.vercel.app/).



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


### 3. PUB/SUB

```typescript
// Define topic (not need to create, auto-created on first publish)
const alerts = client.pubsub<AlertMsg>("system-alerts");
// Subscribe
await alerts.subscribe((msg) => console.log(msg));
// Publish
await alerts.publish({ level: "high" });
```


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



---

### Binary Payloads

All Nexo brokers (**Store, Queue, Stream, PubSub**) natively support raw binary data (`Buffer`).    
Bypassing JSON serialization drastically reduces Latency, increases Throughput, and saves Bandwidth.

**Perfect for:** Video chunks, Images, Protobuf/MsgPack, Encrypted blobs.

```typescript
// Send 1MB raw buffer (30% smaller than JSON/Base64)
const heavyPayload = Buffer.alloc(1024 * 1024);

// 1. STREAM
await client.stream('cctv-archive').publish(heavyPayload);
// 2. PUBSUB 
await client.pubsub('live-audio-call').publish(heavyPayload);
// 3. STORE
await client.store.map.set('user:avatar:1', heavyPayload);
// 4. QUEUE
await client.queue('pdf-processing').push(heavyPayload);
```

---

## License

MIT


## Links

- **📚 Full Documentation:** [Nexo Docs](https://nexo-docs-hub.vercel.app/)
- **🐳 Docker Image:** [emanuelepifani/nexo](https://hub.docker.com/r/emanuelepifani/nexo)

## Author

Built by **Emanuel Epifani**.

- [LinkedIn](https://www.linkedin.com/in/emanuel-epifani/)
- [GitHub](https://github.com/emanuel-epifani)
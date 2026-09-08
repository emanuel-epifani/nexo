# Nexo Client SDK

High-performance TypeScript client for [Nexo](https://nexo-docs-hub.vercel.app/).



## Quick Start


### Run server
```bash
docker run -p 7654:7654 emanuelepifani/nexo:latest
```
This exposes:
- Port 7654 (TCP): Main server socket for SDK clients.

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
await client.store.map.set('user:1', { name: 'Max', role: 'admin' });
const user = await client.store.map.get<User>('user:1');
await client.store.map.delete('user:1');
```

### 2. QUEUE

Provision durable resources from deployment or administrative code:

```typescript
const result = await client.queue.create('emails', {
  visibilityTimeoutMs: 30_000,
  maxDeliveries: 5,
});
console.log(result.status, result.definition.config);
```

Application code retrieves the existing resource and fails fast when it is missing:

```typescript
const mailQueue = await client.queue.get<MailJob>('emails');
await mailQueue.push({ to: 'test@test.com' });
const subscription = await mailQueue.subscribe(async (message, meta) => {
  console.log(meta.id, message);
});
await subscription.stop();
```

### 3. PUB/SUB

Topics are routing addresses and do not require provisioning:

```typescript
const alerts = client.pubsub.topic<AlertMsg>('system-alerts');
const subscription = await alerts.subscribe(async (message) => console.log(message));
await alerts.publish({ level: 'high' });
await subscription.stop();

const allAlerts = client.pubsub.pattern<AlertMsg>('system-alerts/#');
await allAlerts.subscribe((message, meta) => console.log(meta.topic, message));
```

### 4. STREAM

```typescript
const result = await client.stream.create('user-events');
console.log(result.status, result.definition.config);

const stream = await client.stream.get<UserEvent>('user-events');
await stream.publish({ type: 'login', userId: 'u1' });
await stream.group('analytics').subscribe(async (message, meta) => {
  console.log(meta.seq, message);
});
```



---

### Binary Payloads

All Nexo brokers (**Store, Queue, Stream, PubSub**) natively support raw binary data (`Buffer`).    
Bypassing JSON serialization drastically reduces Latency, increases Throughput, and saves Bandwidth.

**Perfect for:** Video chunks, Images, Protobuf/MsgPack, Encrypted blobs.

```typescript
const heavyPayload = Buffer.alloc(1024 * 1024);
const stream = await client.stream.get<Buffer>('cctv-archive');
const queue = await client.queue.get<Buffer>('pdf-processing');
const audio = client.pubsub.topic<Buffer>('live-audio-call');

await stream.publish(heavyPayload);
await audio.publish(heavyPayload);
await client.store.map.set('user:avatar:1', heavyPayload);
await queue.push(heavyPayload);
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
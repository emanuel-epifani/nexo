# Quick Start

Get Nexo running in under 30 seconds with Docker and the TypeScript SDK.

## 1. Run the Server


The Docker image is available on [Docker Hub](https://hub.docker.com/r/emanuelepifani/nexo).

```bash
docker run -d -p 7654:7654 emanuelepifani/nexo
```

This exposes:

- **Port 7654 (TCP):** Client TCP socket for SDK connections.


## 2. Install the SDK

SDK package on [npm](https://www.npmjs.com/package/@emanuelepifani/nexo-client).

```bash
npm install @emanuelepifani/nexo-client
```


## 3. Connect & Use

::: code-group

```typescript
import { NexoClient } from '@emanuelepifani/nexo-client';

// Connect once
const client = await NexoClient.connect({ host: 'localhost', port: 7654 });

// --- Store (Shared state) ---
await client.store.map.set("user:1", { name: "Max", role: "admin" });
const user = await client.store.map.get("user:1");

// --- Pub/Sub (Realtime events) ---
await client.pubsub('alerts').subscribe(async (msg) => console.log(msg));
await client.pubsub('alerts').publish({ level: "high" });

// --- Queue (Background jobs) ---
const mailQ = await client.queue("emails").create();
await mailQ.push({ to: "test@test.com" });
await mailQ.subscribe(async (msg) => console.log(msg));

// --- Stream (Event log) ---
const stream = await client.stream('user-events').create();
await stream.publish({ type: 'login', userId: 'u1' });
await stream.subscribe('analytics', async (msg) => console.log(msg));
```

```python
from nexo import NexoClient

# Connect once
client = await NexoClient.connect(host="localhost", port=7654)

# --- Store (Shared state) ---
await client.store.map.set("user:1", {"name": "Max", "role": "admin"})
user = await client.store.map.get("user:1")

# --- Pub/Sub (Realtime events) ---
async def on_alert(msg):
    print(msg)

await client.pubsub("alerts").subscribe(on_alert)
await client.pubsub("alerts").publish({"level": "high"})

# --- Queue (Background jobs) ---
mail_q = await client.queue("emails").create()
await mail_q.push({"to": "test@test.com"})

async def handle_email(msg):
    print(msg)

await mail_q.subscribe(handle_email)

# --- Stream (Event log) ---
stream = await client.stream("user-events").create()
await stream.publish({"type": "login", "userId": "u1"})

async def on_event(msg):
    print(msg)

await stream.subscribe("analytics", on_event)
```

:::


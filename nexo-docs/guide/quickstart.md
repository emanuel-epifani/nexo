# Quick Start

Get Nexo running in under 30 seconds with Docker and the TypeScript SDK.

## 1. Run the Server

### Option A: Quick Start (Ephemeral)

Run in-memory for quick testing. Data persists across restarts but is lost if the container is removed.

```bash
docker run -d -p 7654:7654 -p 8080:8080 emanuelepifani/nexo
```

This exposes:

- **Port 7654 (TCP):** Main server socket for SDK clients.
- **Port 8080 (HTTP):** Built-in Web Dashboard — open `http://localhost:8080` to inspect all brokers in real-time. No extra tools needed.

The Docker image is available on [Docker Hub](https://hub.docker.com/r/emanuelepifani/nexo).

### Option B: Production Mode (Persistent)

In production, `NEXO_ENV=prod` disables the Web Dashboard (only the TCP socket is exposed) and you mount volumes to persist durable engine data.

#### Storage by design

- **Store** — Volatile by design. Pure in-memory key-value; no persistence needed.
- **Queue** — Durable by design. Messages are persisted asynchronously by default (`file_async`), configurable to flush synchronously on every message (`file_sync`).
- **Stream** — Durable by design. Same persistence modes as Queue. Ideal for event sourcing and audit trails.
- **Pub/Sub** — Fire-and-forget by design. However, **retained messages** are persisted and require their own volume.

Each durable engine maps to a separate volume, so you can attach different storage devices (e.g. a fast NVMe SSD for queues, a large HDD for streams). If storage isolation isn't a concern, you can point multiple engines to the same mount.

```bash
docker run -d \
  --name nexo \
  -p 7654:7654 \
  -v $(pwd)/nexo_queue:/storage/queue \
  -v $(pwd)/nexo_stream:/storage/stream \
  -v $(pwd)/nexo_pubsub:/storage/pubsub \
  -e QUEUE_ROOT_PERSISTENCE_PATH=/storage/queue \
  -e STREAM_ROOT_PERSISTENCE_PATH=/storage/stream \
  -e PUBSUB_ROOT_PERSISTENCE_PATH=/storage/pubsub \
  -e NEXO_ENV=prod \
  emanuelepifani/nexo:latest
```

> Port 8080 is intentionally omitted — the Dashboard is disabled in production mode.

## 2. Install the SDK

```bash
npm install @emanuelepifani/nexo-client
```

SDK package on [npm](https://www.npmjs.com/package/@emanuelepifani/nexo-client).

## 3. Connect & Use

```typescript
import { NexoClient } from '@emanuelepifani/nexo-client';

// Connect once
const client = await NexoClient.connect({ host: 'localhost', port: 7654 });

// --- Store (Shared state) ---
await client.store.map.set("user:1", { name: "Max", role: "admin" });
const user = await client.store.map.get("user:1");

// --- Pub/Sub (Realtime events) ---
await client.pubsub('alerts').subscribe((msg) => console.log(msg));
await client.pubsub('alerts').publish({ level: "high" });

// --- Queue (Background jobs) ---
const mailQ = await client.queue("emails").create();
await mailQ.push({ to: "test@test.com" });
await mailQ.subscribe((msg) => console.log(msg));

// --- Stream (Event log) ---
const stream = await client.stream('user-events').create();
await stream.publish({ type: 'login', userId: 'u1' });
await stream.subscribe('analytics', (msg) => console.log(msg));
```

## 4. Open the Dashboard

Navigate to `http://localhost:8080` to access the **built-in Web Dashboard**.
It's included in the container — no extra installations, no external monitoring tools.
Inspect stores, monitor queues, trace streams, and debug pub/sub topics in real-time.

![Nexo built-in web dashboard](/dashboard-preview.png)

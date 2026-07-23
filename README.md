<div align="center">

# NEXO
### The High-Performance All-in-One Broker


One Binary. Four Brokers. Zero Operational Headaches.

### STORE • PUB/SUB • QUEUE • STREAM

[📚 Documentation](https://nexo-docs-hub.vercel.app/) &nbsp;•&nbsp; [npm](https://www.npmjs.com/package/@emanuelepifani/nexo-client) &nbsp;•&nbsp; [PyPI](https://pypi.org/project/nexo-client/) &nbsp;•&nbsp; [Docker](https://hub.docker.com/r/emanuelepifani/nexo)

</div>

---

## Table of Contents
- [What is Nexo?](#what-is-nexo)
- [Client SDKs](#client-sdks)
- [How to Run](#how-to-run)
- [Architecture](#architecture)
- [Brokers](#brokers)
  - [STORE (Shared State)](#1-store-shared-state)
  - [PUB/SUB (Real-Time Broadcast)](#2-pubsub-real-time-broadcast)
  - [QUEUE (Job Processing)](#3-queue-job-processing)
  - [STREAM (Event Log)](#4-stream-event-log)
- [Performance](#-performance)
- [Getting Started](#getting-started)

---

## What is Nexo?

Modern backend architecture suffers from **Infrastructure Fatigue**. A typical stack requires juggling multiple specialized systems—Redis for caching, RabbitMQ for jobs, Kafka for streams—each with its own protocol, configuration, and maintenance overhead.

Nexo is an **all-in-one broker** that brings four messaging semantics into **one binary, one TCP connection, one SDK**. Same API style across every broker, same connection, same deployment:

- **Store** — in-memory key-value with TTL (like Redis)
- **Pub/Sub** — transient topic broadcast with wildcard matching (like MQTT)
- **Queue** — durable FIFO with acks, retries, priority, DLQ (like RabbitMQ)
- **Stream** — append-only event log with consumer groups (like Kafka)



### Scaling

Nexo is deliberately designed **not to scale horizontally** — no distributed locks, no consensus protocols, no clustering layer. A single instance handles all four brokers on its own thread pools, scaling vertically.

This goes against the current trend of distributed-everything, but the reality is that **most projects will never need horizontal scaling**. Their backends bottleneck long before a single Rust-based broker does. For the vast majority of teams, this is more than enough.

If you do need to scale out, you can run **multiple Nexo instances**, each dedicated to a single broker (e.g. one for Store, one for Queue, one for Stream). This gives horizontal separation without the complexity of distributed coordination.

## Client SDKs

- [TypeScript SDK](https://www.npmjs.com/package/@emanuelepifani/nexo-client)
- [Python SDK](https://pypi.org/project/nexo-client/)

## How to Run

Nexo is distributed as a **Docker image**:

```bash
docker run -d -p 7654:7654 emanuelepifani/nexo
```

This exposes:
- **Port 7654 (TCP)** — Main server socket for SDK clients

> Docker image: [`emanuelepifani/nexo`](https://hub.docker.com/r/emanuelepifani/nexo) on Docker Hub.
>
> See the [Deployment Guide](https://nexo-docs-hub.vercel.app/guide/deployment.html) for configuration options and production setup.

## Architecture

Nexo runs as a **single binary** with a **single TCP socket**. All four brokers share one connection but run on **isolated thread pools** — heavy processing on the *Queue* won't block *Pub/Sub* latency.

```
                                          ┌──────────────────────────────────────┐
                                          │              NEXO SERVER             │
                                          │   ┌──────────────────────────────┐   │
                                          │   │            STORE             │   │
                                          │   │        (Shared State)        │   │
                                          │   └──────────────────────────────┘   │
     ┌─────────────┐                      │                                      │
     │   Client    │                      │   ┌──────────────────────────────┐   │
     │   (SDK)     │───── TCP Socket ────▶│   │            PUBSUB            │   │
     └─────────────┘                      │   │          (Realtime)          │   │
                                          │   └──────────────────────────────┘   │
                                          │                                      │
                                          │   ┌──────────────────────────────┐   │
                                          │   │            QUEUE             │   │
                                          │   │       (Job Processing)       │   │
                                          │   └──────────────────────────────┘   │
                                          │                                      │
                                          │   ┌──────────────────────────────┐   │
                                          │   │           STREAM             │   │
                                          │   │          (Event Log)         │   │
                                          │   └──────────────────────────────┘   │
                                          └──────────────────────────────────────┘
```

Each broker uses a different communication model:

| Broker | Communication | How it works |
|--------|--------------|--------------|
| **Store** | Request/Response | Client sends a command, server replies synchronously |
| **Pub/Sub** | Server-side push | Server pushes messages to subscribed clients in real-time |
| **Queue** | Fetch with server-side wait | Client requests jobs; server holds the request until jobs are available (long poll) |
| **Stream** | Fetch with server-side wait | Client requests events; server holds the request until new events are available (long poll) |

## ⚙️ BROKERS

Nexo is built on the four pillars of modern event-driven architecture. Instead of managing four separate clusters, you get four specialized engines in one API.

Each broker is purpose-built to solve a specific architectural pattern:

*   **Store**: in-memory key-value with TTL for shared state across services.
*   **Pub/Sub**: transient message bus with wildcard topic routing for real-time broadcast.
*   **Queue**: durable FIFO with acks, retries, priority, and Dead Letter Queues for reliable background work.
*   **Stream**: append-only event log with consumer groups and server-side key ordering for durable history.

Everything is available instantly via a unified Client.

### 1. 💾 STORE (Shared State)
**In-memory concurrent data structures.**

**Use Case:** Ideal for high-velocity data that needs to be instantly accessible across all your services, such as user sessions, API rate-limiting counters, and temporary caching.

```text
┌──────────────┐     SET(key, val)      ┌──────────────────┐
│   Client A   │───────────────────────▶│    NEXO STORE    │
└──────────────┘                        │   (Shared RAM)   │
┌──────────────┐      GET(key)          │    [Map<K,V>]    │
│   Client B   │◀───────────────────────│                  │
└──────────────┘                        └──────────────────┘
```



### 2. 📡 PUB/SUB (Real-Time Broadcast)

**Transient message bus with Topic-based routing.**

**Use Case:** Designed for "fire-and-forget" scenarios where low latency is critical and message persistence is not required, such as live chat updates, stock tickers, or multi-service notifications.

```text
                                           ┌───────────────────────────┐
                                           │        NEXO PUBSUB        │
                                           │                           │──────▶ Sub 1 (Exact)
┌─────────────┐         PUBLISH            │  Topic: "home/kitchen/sw" │        "home/kitchen/sw"
│  Publisher  │───────────────────────────▶│                           │
└─────────────┘  msg: "home/kitchen/sw"    │  Topic: "home/+/sw"       │──────▶ Sub 2 (Wildcard +)
                                           │                           │        "matches single level"
                                           │  Topic: "home/#"          │
                                           │                           │──────▶ Sub 3 (Wildcard #)       
                                           └───────────────────────────┘        "matches everything under home"
```

*   **Fan-Out Routing:** Efficiently broadcasts a single incoming message to thousands of connected subscribers.
*   **Pattern Matching:**
    *   `+` **Single Level Wildcard:** Matches exactly one segment.
        *   *Example:* `sensors/+/temp` matches `sensors/kitchen/temp`.
    *   `#` **Multi Level Wildcard:** Matches all remaining segments to the end.
        *   *Example:* `logs/#` matches `logs/error`, `logs/app/backend`, etc.


### 3. 📬 QUEUE (Job Processing)

**Durable FIFO buffer with acknowledgments.**

**Use Case:** Essential for load leveling and ensuring reliable background processing. Use it to decouple heavy tasks (like video transcoding or email sending) from your user-facing API.

```text
┌──────────────┐        PUSH            ┌──────────────────┐
│   Producer   │───────────────────────▶│ 1. [ Job A ]     │
└──────────────┘                        │ 2. [ Job B ]     │───┐
                                        └────────▲─────────┘   │ POP
                                                 │             │
                                             ACK │             │
                                        ┌────────┴─────────┐   │
                                        │     Consumer     │◀──┘
                                        └──────────────────┘
```
*   **Priority Queues:** Supports **Priority** (urgent jobs first).
*   **Failure Recovery:** Automatically retries failed jobs and isolates permanent failures in **Dead Letter Queues (DLQ)**.
*   **Disk Persistence:** Safely persists all jobs to a Write-Ahead Log (WAL) to ensure data survival across restarts.

### 4. 📝 STREAM (Event Log)

**Strictly ordered append-only log without partition complexity.**

**Use Case:** The source of truth for your system's history. Perfect for Event Sourcing, audit trails, and replaying historical data for analytics or debugging where global ordering is critical.

```text
┌──────────────┐       APPEND           ┌────────────────────────────────────┐
│   Producer   │───────────────────────▶│ 0:Event | 1:Event | 2:Event | ...  │
└──────────────┘                        └────────────────────────────────────┘
                                            ▲             ▲
                                     OFFSET │      OFFSET │
                                     ┌────────────┐   ┌────────────┐
                                     │ Consumer A │   │ Consumer B │
                                     └────────────┘   └────────────┘
```

*   **Partition-Free Architecture:** Nexo streams are a single, contiguous log with **server-side key ordering**. No partition keys to choose, no rebalancing, no out-of-order events across partitions. What you append is what consumers read, in the exact order.
*   **Immutable History:** Events are strictly appended and never modified, ensuring a tamper-proof audit log.
*   **Consumer Groups:** Maintains separate read cursors (offsets) for different consumers, allowing independent processing speeds.
*   **Replayability:** Consumers can rewind their offset to re-process historical events from any point in time.


## 📊 Performance

Benchmarks run on MacBook Pro M4 (Single Node):

| Engine   | Throughput     | Latency (p99) |
|----------|----------------|---------------|
| Store    | 4.5M ops/sec   | < 1 µs        |
| PubSub   | 3.8M msg/sec   | < 1 µs        |
| Stream   | 1.9M ops/sec   | < 1 µs        |
| Queue    | 400k ops/sec   | 2 µs          |

---

## 🚀 Getting Started

### 1. Run the Server

```bash
docker run -d -p 7654:7654 emanuelepifani/nexo
```
This exposes:
- Port 7654 (TCP): Main server socket for SDK clients.


### 2. Install the SDK

```bash
npm install @emanuelepifani/nexo-client
```

### 3. Usage Example
Connect and execute operations.

```typescript
import { NexoClient } from '@emanuelepifani/nexo-client';
// Connect once
const client = await NexoClient.connect({ host: 'localhost', port: 7654 });


// --- 1. Store (Shared state Redis-like) ---
await client.store.map.set("user:1", { name: "Max", role: "admin" });
const user = await client.store.map.get<User>("user:1");
await client.store.map.del("user:1");


// --- 2. Pub/Sub (Realtime events MQTT-style + wildcards) ---
client.pubsub<Heartbeat>('edge/42/hb').publish({ ts: Date.now() });
await client.pubsub<Heartbeat>('edge/+/hb').subscribe(hb => console.log('edge alive:', hb.ts));
await client.pubsub<EdgeEvent>('edge/42/#').subscribe(ev => console.log('edge event:', ev.type));


// --- 3. Queue (Reliable background jobs) ---
const mailQ = await client.queue<MailJob>("emails").create();
await mailQ.push({ to: "test@test.com" });
await mailQ.subscribe((msg) => console.log(msg));


// --- 4. Stream (Durable history Event Log) ---
const stream = await client.stream<UserEvent>('user-events').create();
await stream.publish({ type: 'login', userId: 'u1' });
await stream.subscribe('analytics', (msg, meta) => {console.log(`User ${msg.userId} performed ${msg.type}`); });


//Every broker support Binary format (zero JSON overhead)
const chunk = Buffer.alloc(1024 * 1024);
await client.store.map.set("blob", chunk);
client.pubsub<Buffer>('edge/42/video').publish(chunk);
client.stream<Buffer>('video-archive').publish(chunk);
client.queue<Buffer>('video-processing').push(chunk);
```

> **📚 Full Documentation:** For detailed API usage, configuration, and advanced patterns, visit the [**Nexo Docs**](https://nexo-docs-hub.vercel.app/).

---

<div align="center">

**Built for performance, designed for simplicity.**
<br />
Crafted with ❤️ by [Emanuel Epifani](https://github.com/emanuel-epifani).

</div>


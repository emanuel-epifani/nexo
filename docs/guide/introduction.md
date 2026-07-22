# Introduction

**Nexo** is a high-performance, all-in-one message broker built in Rust.
It unifies **Caching**, **Pub/Sub**, **Queues**, and **Streams** into a single binary with zero external dependencies.

## The Problem

Modern event-driven backends suffer from **Infrastructure Fatigue**. A typical stack requires juggling multiple specialized services — one for caching, one for job queues, one for event streams, one for real-time messaging — each with its own container, protocol, configuration, and SDK.

On top of that, many teams rely on **managed cloud services** like AWS SQS/SNS, Azure Service Bus, or GCP Pub/Sub. This means your **local development environment will never match production**: you either run heavy emulators (LocalStack, Azure Service Bus Emulator) with partial fidelity, or mock everything and discover bugs only after deploy.

The operational overhead is disproportionate to the actual problems being solved. Keeping dev, staging and production in sync becomes a constant source of friction.

## The Solution

Nexo is an **all-in-one broker** designed to make project setup, local development, and developer experience as smooth as possible. One binary, one TCP connection, one SDK — four communication models ready to use out of the box.

- **Unified:** One TCP connection for Caching, Pub/Sub, Queues, and Streams.
- **Simple:** Deploy a single binary. No clusters to manage. No JVMs to tune.
- **Fast:** Built in Rust on top of Tokio for extreme throughput and incredibly low latency.
- **Consistent:** Same setup locally and in production. One Docker container, one endpoint.
- **Zero Dependencies:** No external databases, no JVM, no Erlang VM. Just one executable.

## True Dev/Prod Parity

With Nexo, the binary you run on your laptop is **the exact same binary** you run in production. No emulators, no mocks, no "it works on my machine".

```yaml
# docker-compose.yml (local dev)
services:
  nexo:
    image: emanuelepifani/nexo:latest
    ports: ["7654:7654"]
```

Same image, same protocol, same guarantees — from your laptop to your Kubernetes cluster. The dev loop stays fast, the surprises stay out of production.

## Performance

Benchmarks run on MacBook Pro M4 (Single Node):

| Engine | Throughput   | Latency (p99) |
|--------|--------------|---------------|
| Store | 4.5M ops/sec | < 1 µs        |
| PubSub | 3.8M msg/sec | < 1 µs        |
| Stream | 1.9M ops/sec | < 1 µs        |
| Queue | 400k ops/sec | 2 µs          |

## Quick Example

::: code-group

```typescript
import { NexoClient } from '@emanuelepifani/nexo-client';

const client = await NexoClient.connect({ host: 'localhost', port: 7654 });

// Store
await client.store.map.set("user:1", { name: "Max", role: "admin" });

// Pub/Sub
await client.pubsub('alerts').publish({ level: "high" });

// Queue
const q = await client.queue("emails").create();
await q.push({ to: "test@test.com" });

// Stream
const stream = await client.stream('events').create();
await stream.publish({ type: 'login', userId: 'u1' });
```

```python
from nexo import NexoClient, NexoQueue, NexoStream, NexoTopic

client = await NexoClient.connect(host="localhost", port=7654)

# Store
await client.store.map.set("user:1", {"name": "Max", "role": "admin"})
user: User | None = await client.store.map.get("user:1")

# Pub/Sub
alerts: NexoTopic[Alert] = client.pubsub("alerts")
await alerts.publish({"level": "high"})

# Queue
q: NexoQueue[Email] = await client.queue("emails").create()
await q.push({"to": "test@test.com"})

# Stream
stream: NexoStream[Event] = await client.stream("events").create()
await stream.publish({"type": "login", "userId": "u1"})
```

:::

## When NOT to Use Nexo

Nexo is built for vertical deployments and developer experience, not for every scenario. It is **NOT** the right choice if:

- **You need multi-region replication** — Nexo is a single-node broker. If you need geo-distributed replication, use Kafka or NATS with clustering.
- **You're at Kafka-scale throughput** (>1M msg/sec sustained with multiple TB/day) — Nexo handles impressive throughput for a single node, but it won't replace a multi-broker Kafka cluster at petabyte scale.
- **You need exactly-once delivery semantics across distributed consumers** — Nexo Queue provides at-least-once with acks and retries. If you need exactly-once across distributed systems, look elsewhere.

If none of the above applies to you, Nexo might be exactly what you're looking for.

## Delivery Model

Each broker has a fundamentally different way of delivering messages to the client:

| Broker | Delivery | How it works |
|---|---|---|
| **Store** | Request/Response | Client sends a command, server replies. Synchronous round-trip. |
| **Queue** | Long-Poll | Client subscribes, server holds the request until a job is available, then responds. Client acks. Repeat. |
| **Stream** | Long-Poll | Same as Queue — client subscribes, server holds until events are available at the consumer's offset. |
| **Pub/Sub** | Server-Side Push | Server pushes messages to the client the instant they arrive. No polling, no waiting. Lowest latency. |

This is why Pub/Sub is the lowest-latency primitive: there is no round-trip. The server pushes data the moment it arrives.

## Persistence

| Broker | Storage | On Restart |
|:---|:---|:---|
| **Store** | RAM | Lost. Ephemeral cache / shared state. |
| **Pub/Sub** | RAM + SQLite (retained) | Messages lost; retained topics restored. |
| **Queue** | SQLite | Survives. Jobs, state, and DLQ restored. |
| **Stream** | Binary log files | Survives. Full history with retention. |

## Broker Semantics

All brokers share a **single TCP connection**. One read loop demultiplexes incoming frames and routes them to the right broker. A slow consumer in one broker never blocks the others.

### Store

**Ideal use case:** caching, session state, counters, feature flags.

**Semantics:** shared in-memory key/value store. Pure request/response — no callbacks, no consumer loop. Think Redis `SET`/`GET`/`DEL`.

```
  Client                         Server (RAM)
    │                                 │
    │── "SET user:1 Max" ───────────► │
    │◄────────── "OK" ────────────────│
    │                                 │
    │── "GET user:1" ───────────────► │
    │◄──────── "{name:Max}" ──────────│
```

Every operation is a request/response round-trip. No background tasks.

### Queue

**Ideal use case:** email sending, PDF generation, background jobs — work that must not be lost.

**Semantics:** durable FIFO with acks. No ack → retry → dead-letter queue. Configurable `batch_size` and `concurrency`.

```
Producer publishes jobs one by one, in this order:

  push(j1)  push(j2)  push(j3)  push(j4)
     │         │         │         │
     ▼         ▼         ▼         ▼
  ┌───────────────────────────────────────┐
  │  Queue "emails" (FIFO)                │
  │  [j1] [j2] [j3] [j4]                  │
  └───────────────────┬───────────────────┘
                      │
                      │ long-poll: pull batch(3)
                      ▼
  ┌───────────────────────────────────────┐
  │  Consumer task (concurrency = 3)      │
  │  ┌─────┐  ┌─────┐  ┌─────┐            │
  │  │ j1  │  │ j2  │  │ j3  │            │
  │  └──┬──┘  └──┬──┘  └──┬──┘            │
  │     │        │        │               │
  │    ack      nack     ack              │
  │   (done)   (retry)  (done)            │
  └───────────────────────────────────────┘

  j4 stays in the queue, pulled in the next batch.
  Each job is independent: ack/nack decides its fate.
```

`concurrency=3` means 3 jobs run in parallel. Each queue has its own consumer task.

### Stream

**Ideal use case:** event sourcing, audit trails, CDC — when you need to replay history.

**Semantics:** append-only log with offsets. Consumer groups track their own position. Ordered per key, parallel across different keys.

```
Producer publishes events, each with a key:

  publish("login",    key=A)
  publish("signup",   key=B)
  publish("logout",   key=A)
  publish("purchase", key=C)
         │         │         │         │
         ▼         ▼         ▼         ▼
  ┌─────────────────────────────────────────────┐
  │  Stream Log (append-only, immutable)        │
  │  offset 0: login     key=A                  │
  │  offset 1: signup    key=B                  │
  │  offset 2: logout    key=A                  │
  │  offset 3: purchase  key=C                  │
  └──────────────────────┬──────────────────────┘
                         │
                         │ consumer group "analytics"
                         │ reads from its offset
                         ▼
  ┌─────────────────────────────────────────────┐
  │  Same consumer group:                       │
  │  • key=A: [0:login] → [2:logout]  (ordered) │
  │  • key=B: [1:signup]              (ordered) │
  │  • key=C: [3:purchase]            (ordered) │
  │                                             │
  │  key=A and key=B run in parallel            │
  │  (different keys are independent)           │
  └─────────────────────────────────────────────┘

  Another consumer group "billing" can read the same log
  from a different offset, independently.
```

Same key = serial order. Different keys = parallel. Consumer groups read independently.

### Pub/Sub

**Ideal use case:** real-time notifications, IoT heartbeats, config push — fire-and-forget events.

**Semantics:** transient topic broadcast. No ack, no retry, no persistence (except retained messages). Each subscription has its own queue + consumer task.

```
  Publisher
     │
     │── publish "alerts" ────────────────────────►
     │                                              │
     ▼                                              ▼
  Server broadcasts to all matching subscribers:

  ┌─────────────────────┐      ┌─────────────────────┐
  │  subscription A     │      │  subscription B     │
  │  topic "alerts"     │      │  topic "metrics"    │
  │  queue: [m1, m2]    │      │  queue: [m1]        │
  │       │             │      │       │             │
  │       ▼             │      │       ▼             │
  │  cb(m1) → cb(m2)    │      │  cb(m1)             │
  │  (FIFO, serial)     │      │  (FIFO, serial)     │
  └─────────────────────┘      └─────────────────────┘

  subscription A and subscription B run in parallel
  but each one processes its own messages in order
```

Server pushes instantly. Each subscription has its own queue and consumer.

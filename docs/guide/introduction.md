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

## When NOT to Use Nexo

Nexo is built for vertical deployments and developer experience, not for every scenario. It is **NOT** the right choice if:

- **You need multi-region replication** — Nexo is a single-node broker. If you need geo-distributed replication, use Kafka or NATS with clustering.
- **You're at Kafka-scale throughput** (>1M msg/sec sustained with multiple TB/day) — Nexo handles impressive throughput for a single node, but it won't replace a multi-broker Kafka cluster at petabyte scale.
- **You need exactly-once delivery semantics across distributed consumers** — Nexo Queue provides at-least-once with acks and retries. If you need exactly-once across distributed systems, look elsewhere.

If none of the above applies to you, Nexo might be exactly what you're looking for.

# Introduction

**Nexo** is a high-performance, all-in-one message broker built in Rust.
It unifies **Caching**, **Pub/Sub**, **Queues**, and **Streams** into a single binary with zero external dependencies.

## The Problem

Modern event-driven backends suffer from **Infrastructure Fatigue**. A typical stack requires juggling multiple specialized services — one for caching, one for job queues, one for event streams, one for real-time messaging — each with its own container, protocol, configuration, and SDK.

The operational overhead is disproportionate to the actual problems being solved. Setting up a local environment that mirrors production becomes a project in itself, and keeping environments in sync is a constant source of friction.

## The Solution

Nexo offers a **pragmatic trade-off**: it sacrifices "infinite horizontal scale" for **operational simplicity** and **vertical performance**. One TCP connection. One binary. Four engines.

- **Unified:** One TCP connection for Caching, Pub/Sub, Queues, and Streams.
- **Simple:** Deploy a single binary. No clusters to manage. No JVMs to tune.
- **Fast:** Built in Rust on top of Tokio for extreme throughput and incredibly low latency.
- **Observable:** Built-in Web UI for local development. Inspect all engines in real-time.
- **Consistent:** Same setup locally and in production. One Docker container, one endpoint.

## Performance

Benchmarks run on MacBook Pro M4 (Single Node):

| Engine | Throughput | Latency (p99) |
|--------|-----------|---------------|
| Store | 4.5M ops/sec | < 1 µs |
| PubSub | 3.8M msg/sec | < 1 µs |
| Stream | 650k ops/sec | 1 µs |
| Queue | 160k ops/sec | 3 µs |

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

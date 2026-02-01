<div align="center">

# NEXO
### The All-in-One Broker for High-Performance Scale-Ups

[![Rust](https://img.shields.io/badge/built_with-Rust-dca282.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Unified Infrastructure.** Zero overhead. Maximum velocity.

[Get Started](#getting-started) • [Documentation](docs/) • [Dashboard](#dashboard)

</div>

---

## 📑 Table of Contents
- [Overview](#-overview)
- [Architecture](#-architecture)
- [Core Brokers](#-core-brokers)
- [Performance](#-performance)
- [Why Nexo?](#-why-nexo)
- [Getting Started](#-getting-started)

---

## 🎯 The Mission
Modern backend architecture suffers from **Infrastructure Fatigue**. A typical startup stack needs:
*   Redis for caching/sessions.
*   RabbitMQ/SQS for background jobs.
*   Mosquitto/MQTT for real-time events.
*   Kafka for event sourcing.

**Nexo** is a pragmatic trade-off. It sacrifices "infinite horizontal scale" (distributed clustering complexities) for **operational simplicity** and **vertical performance**. It is designed to run on a single instance and handle millions of operations per second, serving the needs of 99% of scale-ups with zero operational overhead.

## Architecture

Nexo sits at the heart of your stack, bridging your applications with the data patterns they need.

```
                                     ┌──────────────────────────────┐
                                     │         NEXO SERVER          │
                                     │                              │       ┌──────────────┐
                                     │   ┌──────────────────────┐   │──────▶│              │
                                     │   │   Store (Key-Value)  │   │       │     RAM      │
                                     │   └──────────────────────┘   │       │  (Volatile)  │
                                     │                              │       │              │
              ┌─────────────┐        │   ┌──────────────────────┐   │       │              │
              │             │        │   │  Pub/Sub (Realtime)  │   │──────▶└──────────────┘
              │   Client    │───────▶│   └──────────────────────┘   │
              │  (SDK/API)  │        │                              │
              │             │        │   ┌──────────────────────┐   │       ┌──────────────┐
              └─────────────┘        │   │   Queue (Buffered)   │   │──────▶│              │
                                     │   └──────────────────────┘   │       │     DISK     │
                                     │                              │       │   (Durable)  │
                                     │   ┌──────────────────────┐   │       │              │
                                     │   │    Stream (Ledger)   │   │──────▶└──────────────┘
                                     │   └──────────────────────┘   │
                                     └──────────────┬───────────────┘
                                                    │
                                                    ▼
                                             ┌─────────────┐
                                             │  Dashboard  │
                                             │  (Web UI)   │
                                             └─────────────┘
```

## BROKERS

Everything you need to handle data flow, available instantly via a unified API.


### 1. STORE (Cache in memory)
*   **Use Case:** Session storage, API caching, temporary state.
*   **Features:** In-memory, O(1) access, TTL (Time-To-Live).

```text
┌──────────────┐     SET(key, val)      ┌──────────────────┐
│   Client A   │───────────────────────▶│     NEXO MAP     │
└──────────────┘                        │    (In-Memory)   │
┌──────────────┐      GET(key)          │    [Map<K,V>]    │
│   Client B   │◀───────────────────────│                  │
└──────────────┘                        └──────────────────┘
```

### 2. QUEUE (Job Processing)
*   **Use Case:** Background jobs, email sending, video processing.
*   **Features:** FIFO, At-least-once delivery, Manual ACK/NACK, Dead Letter Queues (DLQ), Retries.

```text
┌──────────────┐        PUSH            ┌──────────────────┐
│   Producer   │───────────────────────▶│ 1. [ Job A ]     │
└──────────────┘                        │ 2. [ Job B ]     │───┐ POP
                                        └──────────────────┘   │
                                                               ▼
                                        ┌──────────────────┐ ACK
                                        │     Consumer     │◀──┘
                                        └──────────────────┘
```

### 3. PUBSUB (Realtime)
*   **Use Case:** Chat systems, live updates, device coordination.
*   **Features:** Hierarchical topics (`sensors/+/temp`), fan-out broadcasting, transient messaging.

```text
┌──────────────┐       PUBLISH          ┌──────────────────┐      ⚡ msg
│  Publisher   │───────────────────────▶│   TOPIC: "sub"   │─────▶ Sub 1
└──────────────┘                        │    (Fan-Out)     │      ⚡ msg
                                        │                  │─────▶ Sub 2
                                        └──────────────────┘
```

### 4. STREAMS (Event Log)
*   **Use Case:** Event Sourcing, Audit Logs.
*   **Features:** Append-only persistence, Offset-based reading, Replayability.

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


## 📊 Performance


```shell
# Benchmarks run on MacBook Pro M4.

📊 STORE: SET operations (In-Memory)
   Throughput:  4576941 ops/sec
   Latency:     Avg: 0µs | p50: 0µs | p95: 0µs | p99: 0µs | Max: 518µs

📊 QUEUE: PUSH operations (FAsync, flush every 100ms)
   Throughput:  159281 ops/sec
   Latency:     Avg: 5µs | p50: 2µs | p95: 2µs | p99: 3µs | Max: 213853µs

📊 STREAM: PUBLISH operations (FAsync, flush every 100ms)
   Throughput:  658667 ops/sec
   Latency:     Avg: 1µs | p50: 1µs | p95: 1µs | p99: 1µs | Max: 1079µs

📊 PUBSUB: Fanout 1->1000 (10k msgs -> 10M deliveries)
   Ingestion:   3864 msg/sec (Publish)
   Fanout:      3848881 msg/sec (Delivery)
```


## 🚀 Why Nexo?

**One Binary. Four Brokers. Zero Headaches.**

Nexo is the antidote to **Infrastructure Fatigue**. Instead of stitching together three different systems (Cache, Queue, Stream) with three different protocols, Nexo provides a **Unified Infrastructure** for your entire data flow.

*   **Unified:** One connection for Caching, Pub/Sub, Queues, and Streams.
*   **Simple:** Deploy a single binary. No clusters to manage. No JVMs to tune.
*   **Fast:** Built in Rust on top of Tokio for extreme throughput and low latency.
*   **Efficient:** Hybrid storage engine uses RAM for speed and Disk for durability where it matters.

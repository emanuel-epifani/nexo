<div align="center">

# NEXO
### The High-Performance All-in-One Broker for Scale-Ups


**Unified Infrastructure.** One Binary. Four Brokers. Zero Operational Headaches.


</div>

---

## 📑 Table of Contents
- [The Mission](#-the-mission)
- [Architecture](#-arc)
- [Core Brokers](#-core-brokers)
- [Performance](#-performance)
- [Why Nexo?](#-why-nexo)
- [Getting Started](#-getting-started)

---

## The Mission

Modern backend architecture suffers from **Infrastructure Fatigue**. A typical stack requires juggling multiple specialized systems—Redis for caching, RabbitMQ for jobs, Kafka for streams—each with its own protocol, configuration, and maintenance overhead.


Nexo is the antidote. It is a pragmatic trade-off that sacrifices "infinite horizontal scale" for **operational simplicity** and **vertical performance**. Designed to run on a single instance, Nexo handles millions of operations per second, serving the needs of 99% of scale-ups with zero operational overhead.

*   **Unified:** One TCP connection for Caching, Pub/Sub, Queues, and Streams.
*   **Simple:** Deploy a single binary. No clusters to manage. No JVMs to tune.
*   **Fast:** Built in Rust on top of Tokio for extreme throughput and low latency.
*   **Efficient:** Hybrid storage engine uses RAM for speed and Disk for durability where it matters.

## Architecture

Nexo runs as a **single binary** that exposes 4 distinct brokers and a built-in dashboard.

*   **Zero Dependencies:** No external databases, no JVM, no Erlang VM. Just one executable.
*   **Thread-Isolated:** Each broker runs on its own dedicated thread pool. Heavy processing on the *Queue* won't block *Pub/Sub* latency.
*   **Unified Interface:** A single TCP connection handles all protocols, reducing connection overhead.
*   **Embedded Observability:** The server hosts its own Web UI, giving you instant visibility into every broker's internal state without setting up external monitoring tools.


```
                                     ┌──────────────────────────────┐
                                     │         NEXO SERVER          │
                                     │                              │       ┌──────────────┐
                                     │   ┌──────────────────────┐   │──────▶│              │
                                     │   │   Store (Key-Value)  │   │       │     RAM      │
                                     │   └──────────────────────┘   │       │              │
              ┌─────────────┐        │                              │       │  (Volatile)  │
              │             │        │   ┌──────────────────────┐   │──────▶│              │
              │   Client    │───────▶│   │  Pub/Sub (Realtime)  │   │       └──────────────┘
              │  (SDK/API)  │        │   └──────────────────────┘   │
              │             │        │                              │
              └─────────────┘        │   ┌──────────────────────┐   │       ┌──────────────┐
                                     │   │   Queue (Buffered)   │   │──────▶│              │
                                     │   └──────────────────────┘   │       │     DISK     │
                                     │                              │       │              │
                                     │   ┌──────────────────────┐   │       │   (Durable)  │
                                     │   │    Stream (Ledger)   │   │──────▶│              │
                                     │   └──────────────────────┘   │       └──────────────┘
                                     └──────────────┬───────────────┘
                                                    │
                                                    ▼
                                             ┌─────────────┐
                                             │  Dashboard  │
                                             │  (Web UI)   │
                                             └─────────────┘
```

## BROKERS

Nexo is built on the four pillars of modern event-driven architecture. Instead of managing four separate clusters, you get four specialized engines in one API.

Each broker is purpose-built to solve a specific architectural pattern:

*   **Store** replaces external caches (like Redis) for shared state.
*   **Pub/Sub** replaces message buses (like MQTT/Redis PubSub) for real-time volatility.
*   **Queue** replaces job queues (like RabbitMQ/SQS) for reliable background work.
*   **Stream** replaces event logs (like Kafka) for durable history.

Everything is available instantly via a unified Client.

### 1. STORE (Shared State)
**Use Case:** Ideal for high-velocity data that needs to be instantly accessible across all your services, such as user sessions, API rate-limiting counters, and temporary caching.

```text
┌──────────────┐     SET(key, val)      ┌──────────────────┐
│   Client A   │───────────────────────▶│    NEXO STORE    │
└──────────────┘                        │   (Shared RAM)   │
┌──────────────┐      GET(key)          │    [Map<K,V>]    │
│   Client B   │◀───────────────────────│                  │
└──────────────┘                        └──────────────────┘
```

*   **Granular TTL:** Set expiration per-key or globally. Ideal for temporary API caches and rate-limiting counters.


### 2. PUB/SUB (Real-Time Broadcast)

**Transient message bus with Topic-based routing.**

**Use Case:** Designed for "fire-and-forget" scenarios where low latency is critical and message persistence is not required, such as live chat updates, stock tickers, or multi-service notifications.

```text
┌──────────────┐       PUBLISH          ┌──────────────────┐      ⚡ msg
│  Publisher   │───────────────────────▶│   TOPIC: "sub"   │─────▶ Sub 1
└──────────────┘                        │    (Fan-Out)     │      ⚡ msg
                                        │                  │─────▶ Sub 2
                                        └──────────────────┘
```

*   **Fan-Out Routing:** Efficiently broadcasts a single incoming message to thousands of connected subscribers.
*   **Wildcard Subscriptions:** Supports hierarchical patterns (e.g., `sensors/*/temp`) for flexible filtering.
*   **Low Latency:** Optimized for maximum throughput with no disk I/O overhead.


### 3. QUEUE (Job Processing)

**Durable FIFO buffer with acknowledgments.**

**Use Case:** Essential for load leveling and ensuring reliable background processing. Use it to decouple heavy tasks (like video transcoding or email sending) from your user-facing API.

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

*   **At-Least-Once Delivery:** Jobs are only removed after explicit acknowledgment, preventing data loss on worker crashes.
*   **Dead Letter Queues (DLQ):** Automatically isolates failing jobs after a configurable number of retries.
*   **Disk Persistence:** Uses a Write-Ahead Log (WAL) to ensure jobs survive server restarts.


### 4. STREAM (Event Log)

**Append-only immutable log with offset tracking.**

**Use Case:** The source of truth for your system's history. Perfect for Event Sourcing, audit trails, and replaying historical data for analytics or debugging.

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

*   **Immutable History:** Events are strictly appended and never modified, ensuring a tamper-proof audit log.
*   **Consumer Groups:** Maintains separate read cursors (offsets) for different consumers, allowing independent processing speeds.
*   **Replayability:** Consumers can rewind their offset to re-process historical events from any point in time.


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

## 🎛️ Dashboard

Nexo comes with a built-in, zero-config real-time dashboard exposed to debug/developing

![Nexo Dashboard Screenshot](docs/assets/dashboard-preview.png)
*(Monitor throughput, inspect queues, and debug streams in real-time)*


## 🚀 Why Nexo?

**One Binary. Four Brokers. Zero Headaches.**

Nexo is the antidote to **Infrastructure Fatigue**. Instead of stitching together three different systems (Cache, Queue, Stream) with three different protocols, Nexo provides a **Unified Infrastructure** for your entire data flow.

*   **Unified:** One connection for Caching, Pub/Sub, Queues, and Streams.
*   **Simple:** Deploy a single binary. No clusters to manage. No JVMs to tune.
*   **Fast:** Built in Rust on top of Tokio for extreme throughput and low latency.
*   **Efficient:** Hybrid storage engine uses RAM for speed and Disk for durability where it matters.

# Architecture

Nexo runs as a **single binary** that exposes 4 distinct brokers and a built-in dashboard.

- **Zero Dependencies:** No external databases, no JVM, no Erlang VM. Just one executable.
- **Thread-Isolated:** Each broker runs on its own dedicated thread pool. Heavy processing on the Queue won't block Pub/Sub latency.
- **Unified Interface:** A single TCP connection handles all protocols, reducing connection overhead.
- **Embedded Observability:** The server hosts its own Web UI for instant visibility into every broker's state.

## System Diagram

```
                         ┌──────────────────────────────────────┐
                         │              NEXO SERVER             │
                         │                                      │       ┌──────────────┐
                         │   ┌──────────────────────────────┐   │──────▶│     RAM      │
                         │   │            STORE             │   │       │  (Volatile)  │
   ┌─────────────┐       │   └──────────────────────────────┘   │       └──────────────┘
   │   Client    │──────▶│   ┌──────────────────────────────┐   │
   │  (SDK/API)  │       │   │            PUBSUB            │   │
   └─────────────┘       │   └──────────────────────────────┘   │
                         │   ┌──────────────────────────────┐   │       ┌──────────────┐
                         │   │            QUEUE             │   │──────▶│     DISK     │
                         │   └──────────────────────────────┘   │       │  (Durable)   │
                         │   ┌──────────────────────────────┐   │──────▶└──────────────┘
                         │   │           STREAM             │   │
                         │   └──────────────────────────────┘   │
                         └───────────────┬──────────────────────┘
                                         │
                                         ▼
                                 ┌─────────────────┐
                                 │    Dashboard    │
                                 └─────────────────┘
```

## The Four Brokers

Each broker is purpose-built to solve a specific architectural pattern:

- **Store** replaces external caches (like Redis) for shared state.
- **Pub/Sub** replaces message buses (like MQTT/Redis PubSub) for real-time volatility.
- **Queue** replaces job queues (like RabbitMQ/SQS) for reliable background work.
- **Stream** replaces event logs (like Kafka) for durable history.

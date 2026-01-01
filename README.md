# Nexo: The All-in-One Broker for Scale-Ups

> **Stop juggling Redis, RabbitMQ, and Kafka for your MVP.**
> Nexo unifies Cache, Pub/Sub, and Queues into a single, high-performance binary written in Rust.

## 🎯 The Mission
Modern backend architecture suffers from **Infrastructure Fatigue**. A typical startup stack needs:
*   Redis for caching/sessions.
*   RabbitMQ/SQS for background jobs.
*   Mosquitto/MQTT for real-time events.
*   Kafka for event sourcing.

**Nexo** is a pragmatic trade-off. It sacrifices "infinite horizontal scale" (distributed clustering complexities) for **operational simplicity** and **vertical performance**. It is designed to run on a single instance and handle millions of operations per second, serving the needs of 99% of scale-ups with zero operational overhead.

## ⚡ Core Features

### 1. Store (The Cache)
*   **Use Case:** Session storage, API caching, temporary state.
*   **Features:** In-memory, O(1) access, TTL (Time-To-Live).
*   **Alternative to:** Redis, Memcached.

### 2. Message Queue (The Workhorse)
*   **Use Case:** Background jobs, email sending, video processing.
*   **Features:** FIFO, At-least-once delivery, Manual ACK/NACK, Dead Letter Queues (DLQ), Retries.
*   **Alternative to:** RabbitMQ, Amazon SQS, Sidekiq.

### 3. Pub/Sub Topics (The Realtime)
*   **Use Case:** Chat systems, live updates, device coordination.
*   **Features:** Hierarchical topics (`sensors/+/temp`), fan-out broadcasting, transient messaging.
*   **Alternative to:** MQTT, Redis Pub/Sub.

### 4. Streams (The Ledger)
*   **Use Case:** Event Sourcing, Audit Logs.
*   **Features:** Append-only persistence, Offset-based reading, Replayability.
*   **Alternative to:** Kafka, Redpanda.

## 🛠 Architecture
*   **Language:** Rust (Tokio Async Runtime).
*   **Protocol:** Custom Binary Protocol (Compact & Fast).
*   **Storage:** Hybrid (In-memory for speed + WAL/Snapshots for durability - *Coming Soon*).
*   **Deployment:** Single binary or Docker container.

## 🚀 Why Nexo?
| Feature | Traditional Stack | Nexo Stack |
|---------|-------------------|------------|
| **Components** | Redis + RabbitMQ + Kafka | **Nexo** |
| **Ops Overhead** | High (3x configs, 3x updates) | **Zero** (1 binary) |
| **Protocol** | RESP + AMQP + Kafka Proto | **Unified Binary Protocol** |
| **Resource Usage** | High (JVM + Erlang VM) | **Low** (<50MB RAM idle) |

---
*Built for speed, designed for sanity.*

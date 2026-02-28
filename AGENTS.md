
# NEXO

## The Mission

Modern backend architecture suffers from **Infrastructure Fatigue**. A typical stack requires juggling multiple specialized systems—Redis for caching, RabbitMQ for jobs, Kafka for streams—each with its own protocol, configuration, and maintenance overhead.

Nexo offers a **pragmatic trade-off**: it sacrifices "infinite horizontal scale" for **operational simplicity** and **vertical performance**.

Here's the reality: most scale-ups will **never** reach the scale where horizontal distribution becomes necessary. Their backends will bottleneck long before a single Rust-based broker does. Nexo is designed for that 99%—companies that need **high throughput without operational complexity**.

## Architecture

Nexo runs as a **single binary** that exposes 4 distinct brokers and a built-in dashboard.

*   **Thread-Isolated:** Each broker runs on its own dedicated thread pool. Heavy processing on the *Queue* won't block *Pub/Sub* latency.
*   **Dev Dashboard:** The server expose built-in Web UI, giving you instant visibility into every broker's internal state without setting up external monitoring tools.

```
                            ┌──────────────────────────────────────┐
                            │              NEXO SERVER             │
                            │                                      │       ┌──────────────┐
                            │   ┌──────────────────────────────┐   │──────▶│              │
                            │   │            STORE             │   │       │     RAM      │
                            │   │        (Shared State)        │   │       │              │
     ┌─────────────┐        │   └──────────────────────────────┘   │       │  (Volatile)  │
     │             │        │                                      │       │              │
     │   Client    │───────▶│   ┌──────────────────────────────┐   │──────▶│              │
     │  (SDK/API)  │        │   │            PUBSUB            │   │       └──────────────┘
     │             │        │   │          (Realtime)          │   │
     └─────────────┘        │   └──────────────────────────────┘   │
                            │                                      │
                            │   ┌──────────────────────────────┐   │       ┌──────────────┐
                            │   │            QUEUE             │   │──────▶│              │
                            │   │        (Job Processing)      │   │       │     DISK     │
                            │   └──────────────────────────────┘   │       │              │
                            │                                      │       │   (Durable)  │
                            │   ┌──────────────────────────────┐   │──────▶│              │
                            │   │           STREAM             │   │       └──────────────┘
                            │   │          (Event Log)         │   │
                            │   └──────────────────────────────┘   │
                            └───────────────┬──────────────────────┘
                                            │
                                            ▼
                                    ┌─────────────────┐
                                    │    Dashboard    │
                                    │     (Web UI)    │
                                    └─────────────────┘
```

## BROKERS

### 1. STORE (Shared State)
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

for more detailed info:
- docs: docs/guide/store.md
- code: src/brokers/store/store_manager.rs



### 2. PUB/SUB (Real-Time Broadcast)

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

for more detailed info:
- docs: docs/guide/store.md
- code: src/brokers/pub-sub/pub_sub_manager.rs

### 3. QUEUE (Job Processing)

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
*   **Smart Scheduling:** Supports **Delayed Jobs** (process in the future) and **Priority Queues** (urgent jobs first).
*   **Failure Recovery:** Automatically retries failed jobs and isolates permanent failures in **Dead Letter Queues (DLQ)**.
*   **Disk Persistence:** Safely persists all jobs to a Write-Ahead Log (WAL) to ensure data survival across restarts.

for more detailed info:
- docs: docs/guide/queue.md
- code: src/brokers/queue/queue_manager.rs

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

for more detailed info:
- docs: docs/guide/stream.md
- code: src/brokers/stream/stream_manager.rs

##  Dashboard

Nexo comes with a built-in, zero-config dashboard exposed to local development.
Instantly verify if your microservices are communicating correctly by inspecting the actual contents of your Stores, Queues, and Streams in real-time.

for more detailed info:
- code: dashboard/src/pages/dashboard/components

## Getting Started

### 1. Run the Server

```bash
docker run -d -p 7654:7654 -p 8080:8080 nexobroker/nexo
```
This exposes:
- Port 7654 (TCP): Main server socket for SDK clients.
- Port 8080 (HTTP): Web Dashboard with status of all brokers.


### 2. Install the SDK

```bash
npm install @emanuelepifani/nexo-client
```

### 3. Usage Example
Connect, execute operations, inspect data via the dashboard at `http://localhost:8080`.

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
await stream.subscribe('analytics', (msg) => {console.log(`User ${msg.userId} performed ${msg.type}`); });


//Every broker support Binary format (zero JSON overhead)
const chunk = Buffer.alloc(1024 * 1024);
await client.store.map.set("blob", chunk);
client.pubsub<Buffer>('edge/42/video').publish(chunk);
client.stream<Buffer>('video-archive').publish(chunk);
client.queue<Buffer>('video-processing').push(chunk);
```
---

AI-RULES:
- for REACT dashboard: ai-rules/rules-react.md    
- for RUST server: ai-rules/rules-rust.md     
- for SDK ts: ai-rules/rules-sdk-ts.md    

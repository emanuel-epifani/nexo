# Nexo Binary Protocol & Server Flow

This document describes the binary protocol and the internal architecture of the Nexo server.

## 1. Binary Protocol (Nexo Protocol v1)

Nexo uses a fixed-header binary protocol designed for performance, multiplexing, and extensibility.

### Header (9 Bytes)

| Field | Size | Description |
| :--- | :--- | :--- |
| **FrameType** | 1 Byte | `0x01` (Request), `0x02` (Response), `0x03` (Push), `0x05` (Ping), `0x06` (Pong) |
| **CorrelationID** | 4 Bytes | Unique ID (Big Endian) to match responses to requests. |
| **PayloadLen** | 4 Bytes | Length of the payload following the header (Big Endian). |

### Payload Structure

The payload starts with a 1-byte **Opcode** (for Requests) or **Status** (for Responses), followed by the specific data.


```agsl
REQUEST (Dal Client -> Server)
Esempio: KV_SET "key" "val" (ID=100)
+-------------------+---------------------------------------+
|      HEADER       |                PAYLOAD                |
|      (9 bytes)    |           (Variabile: N bytes)        |
+-------------------+---------------------------------------+
| Type | ID | Len   | Opcode | KeyLen | Key... | Value... |
| 0x01 | 100| 11    |  0x02  |   3    | "key"  |  "val"   |
+------+----+-------+--------+--------+--------+----------+
  ^      ^     ^        ^        ^
  |      |     |        |        |
Request  |   7+1+3    KV_SET   String
         |   = 11              Format
         
------------------------------------------------------------------------------------------------------------------------

         
RESPONSE (Dal Server -> Client)
Esempio: Risposta OK per l'ID 100
+-------------------+-----------------------+
|      HEADER       |        PAYLOAD        |
+-------------------+-----------------------+
| Type | ID | Len   | Status |     Body     |
| 0x02 | 100|  1    |  0x00  |   (vuoto)    |
+------+----+-------+--------+--------------+
  ^      ^     ^        ^
  |      |     |        |
Response |   1 byte    OK

------------------------------------------------------------------------------------------------------------------------

PUSH (Dal Server -> Client, es. PubSub)
Esempio: Messaggio su topic "news"
+-------------------+----------------------------------+
|      HEADER       |             PAYLOAD              |
+-------------------+----------------------------------+
| Type | ID | Len   |            Body Data             |
| 0x03 | 0  |  N    |   ...messaggio custom...         |
+------+----+-------+----------------------------------+
  ^      ^
  |      |
 PUSH   ID=0 (o TopicID)

```

### Data Encoding

All strings are **length-prefixed** for safety and consistency:
- `[Length: 4 bytes BE] [UTF-8 String: N bytes]`

---

## 2. Server Architecture (Multiplexing & Parallelism)

The server is built on **Tokio** and implements a split-loop architecture for each connection, enabling true parallel request processing.

### Connection Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. TCP Accept (main.rs)                                      │
│    └─> Spawn handle_connection() per client                  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Split Socket (network.rs)                                 │
│    ├─> Reader Half  (Read Loop)                              │
│    ├─> Writer Half  (Write Loop)                             │
│    └─> mpsc Channel (Communication)                          │
└─────────────────────────────────────────────────────────────┘
```

### The Three Components

#### **1. Read Loop** (Lines 46-87 in `network.rs`)
- Accumulates bytes into a `BytesMut` buffer
- Uses `parse_frame()` to extract complete frames (9-byte header + payload)
- For each valid frame:
  - Extracts `id`, `frame_type`, and `payload`
  - **Clones payload** (`.to_vec()`) because:
    - `frame.payload` borrows from `buffer` (short lifetime)
    - `tokio::spawn` requires `'static` (infinite lifetime)
    - Buffer is reused for next read (data would be corrupted)
  - **Spawns a new task** per request → parallel execution
  
**Why spawn?** A slow command (e.g., large stream read) doesn't block fast commands (e.g., KV GET).

#### **2. Worker Tasks** (Spawned by Read Loop)
```rust
tokio::spawn(async move {
    match frame_type {
        TYPE_REQUEST => {
            let response = route(&payload, &engine);
            tx.send(Response(id, response)).await;
        }
        TYPE_PING => { /* ... */ }
    }
});
```
- Each request runs in **parallel** on the tokio runtime
- Calls `route()` to dispatch to the correct broker
- Sends result back via `mpsc` channel

#### **3. Write Loop** (Lines 29-40 in `network.rs`)
- Waits for messages from spawned tasks via `mpsc::Receiver`
- Encodes response using `encode_response()` (adds header)
- Writes bytes back to the socket
- **Serial writes** (FIFO order preserved on channel)

---

## 3. File Architecture (Separation of Concerns)

The server code is organized in **three layers**, each with a single responsibility:

### **Layer 1: Network** (`network.rs`)
**Responsibility:** TCP I/O and connection management

- Handles raw socket operations (read/write)
- Manages buffer lifecycle (`BytesMut`)
- Spawns parallel tasks for request processing
- No protocol knowledge beyond frame boundaries

**Key Functions:**
- `handle_connection(socket, engine)` - Main connection handler

---

### **Layer 2: Protocol** (`protocol.rs`)
**Responsibility:** Binary protocol specification (The Bible)

- **Constants:** FrameTypes, Opcodes, Status codes
- **Types:** `Frame`, `Response`, `ParseError`
- **Parser:** `parse_frame(&[u8])` - Extracts header + payload
- **Encoders:** `encode_response()`, `encode_push()` - Creates binary frames

**NO business logic, NO routing.** Pure protocol implementation.

**Lines:** 122 (was 236 before refactor)

---

### **Layer 3: Router** (`router.rs`)
**Responsibility:** Request routing and payload parsing

- **Parsing Helpers** (private):
  - `parse_string()` - Length-prefixed string
  - `parse_string_and_bytes()` - String + remaining bytes
  - `parse_string_u64()` - String + 64-bit integer
  
- **Routing Logic:**
  - `route(payload, engine)` - **The Mega Switch**
  - Decodes opcode from payload
  - Parses command-specific data
  - Dispatches to correct broker (`engine.kv`, `engine.queue`, etc.)
  - Returns `Response` enum

**Why centralized routing?** All opcodes visible in one place. Easy to audit, extend, and maintain.

**Lines:** 160

---

### **Layer 4: Brokers** (`brokers/`)
**Responsibility:** Business logic (storage, queues, pub/sub, streams)

Each broker is isolated and thread-safe:
- `KvManager` - Key-value store with TTL
- `QueueManager` - FIFO message queues
- `TopicManager` - Pub/Sub with hierarchical topics
- `StreamManager` - Append-only logs

Brokers know nothing about the network or protocol.

---

## 4. Data Flow Example: KV SET Command

```
Client                Network              Router               Broker
  │                     │                     │                    │
  │  Binary Frame       │                     │                    │
  ├────────────────────>│                     │                    │
  │                     │                     │                    │
  │                     │ 1. parse_frame()    │                    │
  │                     │    ✓ Valid header   │                    │
  │                     │                     │                    │
  │                     │ 2. Clone payload    │                    │
  │                     │    & spawn task     │                    │
  │                     │                     │                    │
  │                     │ 3. route(payload)   │                    │
  │                     ├────────────────────>│                    │
  │                     │                     │                    │
  │                     │                     │ 4. Parse opcode    │
  │                     │                     │    OP_KV_SET       │
  │                     │                     │                    │
  │                     │                     │ 5. parse_string    │
  │                     │                     │    _and_bytes()    │
  │                     │                     │    key="user:42"   │
  │                     │                     │    val=[bytes]     │
  │                     │                     │                    │
  │                     │                     │ 6. kv.set()        │
  │                     │                     ├───────────────────>│
  │                     │                     │                    │
  │                     │                     │    DashMap insert  │
  │                     │                     │                    │
  │                     │                     │ 7. Ok              │
  │                     │                     │<───────────────────┤
  │                     │                     │                    │
  │                     │ 8. Response::Ok     │                    │
  │                     │<────────────────────┤                    │
  │                     │                     │                    │
  │                     │ 9. encode_response()│                    │
  │                     │    [0x02][id][len]  │                    │
  │                     │    [0x00]           │                    │
  │                     │                     │                    │
  │  Response bytes     │                     │                    │
  │<────────────────────┤                     │                    │
  │                     │                     │                    │
```

**Key Points:**
- Payload cloned at step 2 (necessary for `'static` lifetime in spawned task)
- Parsing happens in `router.rs` (not in network layer)
- Broker receives clean Rust types (`String`, `Vec<u8>`), not raw bytes
- Response flows back through channel to Write Loop

---

## 5. Why This Architecture Matters

### **1. No Head-of-Line Blocking**
Multiple requests process in parallel on a single connection:
```
Time ──────────────────────────>

Request A (slow)  [════════════════]
Request B (fast)     [═══]
Request C (fast)        [═══]

Response B ──────────────[OK]
Response C ──────────────────[OK]  
Response A ───────────────────────[DATA]
```

### **2. True Multiplexing**
Each request has a `CorrelationID`. Responses can arrive out-of-order.

### **3. Future-Proof Pub/Sub**
Server can send `TYPE_PUSH` frames at any time (not implemented yet):
```rust
// Future implementation
encode_push(subscriber_id, b"topic:sensor/temp data:42")
```

### **4. Safety First**
- All string parsing is **length-prefixed** (no null-byte ambiguity)
- Zero `.unwrap()` in parsing helpers (no panics)
- DashMap/RwLock for thread-safe broker access

### **5. Maintainability**
- Adding a new opcode: Edit `router.rs` only
- Changing protocol: Edit `protocol.rs` only
- Adding broker features: Edit `brokers/` only

---

# NEXO - AI Project Overview

## 1) What is Nexo

Single-binary Rust broker server exposing **four data-communication models**:






- **Store**: shared in-memory state (sessions, cache, counters)
- **Pub/Sub**: transient topic broadcast, low-latency, wildcard matching (`+`, `#`)
- **Queue**: durable FIFO with acks, delays, priority, retries, DLQ
- **Stream**: append-only log with offsets and consumer groups

Goal: reduce operational complexity vs multi-system stacks (Redis + Kafka + RabbitMQ + ...) while keeping high performance for vertical deployments.

Ships with:
- **Rust server** (core runtime + binary TCP protocol)
- **TypeScript SDK** (`@emanuelepifani/nexo-client`)

Default port: TCP `7654` (SDK ↔ server).

---

## 2) Architecture at a Glance

```
                    ┌─────────────────────┐
  TS SDK  ──TCP──►  │   transport/tcp     │
                    │ (binary protocol)   │──┐
                    └─────────────────────┘  │
                                             ▼
                                       ┌──────────┐
                                       │ brokers/ │ ← managers (pure domain)
                                       └──────────┘
                                             ▲
```

**Design principle**: each broker owns its TCP surface. `transport/` contains only broker-agnostic plumbing (framing, codec, opcode dispatcher).

---

## 3) Repo Map

```
src/
  main.rs, lib.rs              # entrypoint, NexoEngine (holds 4 managers)
  config.rs                    # config types
  transport/
    tcp/
      connection.rs            # per-client TCP session lifecycle
      dispatcher.rs            # opcode → brokers::<b>::tcp::handle
      protocol/                # codec, frame, wire (read+write), errors
  brokers/
    <broker>/                  # store, queue, pub-sub, stream
      manager.rs               # public API + orchestration, returns neutral types
      snapshot.rs              # neutral introspection types (no serde)
      options.rs               # shared option structs (manager + tcp), if present
      tcp.rs                   # OPCODE_MIN/MAX, Command parse, Response, handle()
      config.rs                # broker-specific config, if present
      domain/                  # business logic + durable I/O for that broker
        mod.rs
        persistence.rs         # optional: same filename everywhere durabilità esiste
                                 # (Queue SQLite, Stream log/segmenti, Pub/Sub retained SQLite)
                                 # Store: no persistence.rs (solo in-memory)
        ...                    # queue/dlq/map/topic/group/message/types/radix_tree/retained, ecc.

tests/                         # Rust integration tests, one file per broker
sdk/ts/src/                    # TypeScript SDK
docs/guide/                    # functional docs (store/queue/pubsub/stream)
```

**Per-broker dependency rule**: `manager.rs` must NOT import from `tcp.rs`, `transport/`, or any adapter layer. Adapters depend on the manager, never the reverse.

---

## 4) Request Flow

**TCP (SDK → server):**
```
socket bytes → connection → codec → frame (opcode+payload)
  → dispatcher → brokers::<b>::tcp::handle
  → Command::parse → manager.<op>() → Response
  → encode_* free fn (PayloadWriter) → socket
```

### Binary protocol conventions

When touching the wire, keep it uniform and unambiguous:

- **Header (11 bytes, versioned, zero-copy `bytemuck` Pod):**
  `[Version:1][FrameType:1][Meta:1][CorrelationID:4 BE][PayloadLen:4 BE]`.
  First byte is `PROTOCOL_VERSION`; mismatched frames are rejected on both ends
  (stateless, no handshake). `Meta` is opcode (Request) / status (Response) /
  push-type (Push).
- **Response payload rules (one read rule each):** `OK`/`NULL` → empty;
  `DATA` → bytes read to end; `ERR` → utf8 message read to end (no inner length).
  Booleans (`exists`/`ack`/`nack`) are `DATA` with a single `0/1` byte — a
  negative outcome is data, not an error.
- **Serialization:** broker `tcp.rs` uses named `encode_*` functions and
  `PayloadWriter`/`PayloadCursor` from `wire.rs` — the single file where the on-wire
  conventions live (big-endian, u32 length-prefix, UUID 16B raw). Keep `put_*`/`read_*`
  pairs aligned; never hand-roll length prefixes / endianness.
- **Command fields:** consistent order across opcodes (e.g. stream `topic` then
  `group`); per-request tuning (batch size, wait ms) is sent explicitly by the
  SDK rather than relying on server-side defaults.
- **`DataType` prefix** (`raw/string/json`) is a client-side payload contract
  shared by all SDKs; the server treats payloads as opaque.
- **Session identity:** the transport generates an opaque session id and passes
  it to brokers as a plain `&str`. `ClientId` is a PubSub-internal key type, not
  a shared cross-broker model.
- **Pushes** are not correlated to a request → `CorrelationID = 0`.

Any wire change must stay symmetric across `src/` (codec + broker `tcp.rs`) and
`sdk/ts/` (`codec.ts`, `connection.ts`, broker files), and bump
`PROTOCOL_VERSION` if it breaks the layout.

---

## 5) How to Run Tests

```bash
cargo test                         # all Rust suites
cargo test --test queue_tests      # single suite (also: store_/pubsub_/stream_tests)
cargo test --release bench_<name> -- --test-threads=1 --nocapture
cd sdk/ts && npm test              # TS SDK (vitest)
```

---

## 6) Tech Stack

- Rust edition `2021`, async runtime `tokio` (full)
- Concurrent state: `dashmap`, `parking_lot`
- Persistence: `rusqlite` (bundled)
- Serialization: `serde` + `serde_json`; binary frames: `bytes` + `bytemuck`
- Logging: `tracing` + `tracing-subscriber`

### Concurrency primitives

| Use case | Primitive |
|---|---|
| Managers shared across connections | `Arc<Manager>` inside `NexoEngine` |
| Resource registry (name → resource) | `Arc<DashMap<String, Arc<Shared>>>` |
| Per-resource mutable state | `std::sync::Mutex<Inner>` |
| PubSub subscription tree | `parking_lot::RwLock<Node>` |
| Async wake-up | `tokio::sync::Notify` |
| Background I/O | `mpsc` / `oneshot` |
| Graceful shutdown | `CancellationToken` |
| Stream offsets | `AtomicU64` |

### Error handling

- No `thiserror` / `anyhow`. Errors are `Result<_, String>` or `Response::Error(String)`.
- Codec errors: local `ParseError`.
- `unwrap()` / `expect()` only in tests; in `src/` use `?` or explicit handling.

---

## 7) Engineering Rules

**Core**
- Brokers are self-contained by domain; `manager.rs` is the only public entrypoint.
- Managers return neutral types (`snapshot.rs`, raw primitives). Never import `transport/` or DTOs.
- `tcp.rs` / `http.rs` are the adapters that translate to/from wire formats.
- Remove dead code and stale comments during refactors.

**Alignment (always check)**
For any change touching protocol or behavior, verify:
- `src/` (server)
- `sdk/ts/` (SDK)
- `docs/` (user docs)

If one area is not impacted, state it explicitly.

**Tests**
Any behavior/protocol change must update or add tests in `tests/` (Rust) and `sdk/ts/tests/` (TS), then run the relevant suite(s).

---

## 8) Refactor Checklist

Before coding any non-trivial refactor, produce:

1. **Scope**: broker / module touched.
2. **Change**: what + why.
3. **Impact**: server / sdk / docs → impacted or not (one line each).
4. **Complexity**: `low` / `medium` / `high`. If high or cross-layer, propose a simpler alternative first.
5. **Tests**: existing to review + new to add.
6. **Commit message**: conventional commits format.

---

## 9) Release

Push a `v*` tag → CI builds:
- Docker → `emanuelepifani/nexo:<tag>` + `latest`
- SDK → npm `@emanuelepifani/nexo-client`
- Docs → Vercel
- GitHub Release created after all jobs succeed

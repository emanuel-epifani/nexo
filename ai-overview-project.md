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
- **React dashboard** (local dev UI, read-only)
- **TypeScript SDK** (`@emanuelepifani/nexo-client`)

Default ports: TCP `7654` (SDK ↔ server), HTTP `8080` (dashboard).

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
                    ┌─────────────────────┐  │
  Dashboard ──HTTP─►│   transport/http    │──┘
                    │ (axum + JSON DTO)   │
                    └─────────────────────┘
```

**Design principle**: each broker owns its TCP and HTTP surface. `transport/` contains only broker-agnostic plumbing (framing, codec, axum root, static assets, opcode dispatcher).

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
      protocol/                # codec, frame, cursor, errors, ToWire trait
    http/
      router.rs                # axum root, merges broker routes
      assets.rs                # embedded dashboard static files
      payload.rs               # payload → JSON conversion helpers
  brokers/
    <broker>/                  # store, queue, pub-sub, stream
      manager.rs               # public domain API, returns neutral types
      snapshot.rs              # neutral introspection types (no serde)
      options.rs               # shared option structs (manager + tcp)
      tcp.rs                   # OPCODE_MIN/MAX, Command parse, Response, handle()
      http.rs                  # DTOs (serde), axum handlers, routes()
      ...                      # domain internals (storage, topic, radix_tree, ...)

tests/                         # Rust integration tests, one file per broker
dashboard/src/                 # React frontend
sdk/ts/src/                    # TypeScript SDK
docs/guide/                    # functional docs (store/queue/pubsub/stream)
```

**Per-broker dependency rule**: `manager.rs` must NOT import from `tcp.rs`, `http.rs`, `transport/`, or `dashboard/`. Adapters depend on the manager, never the reverse.

---

## 4) Request Flow

**TCP (SDK → server):**
```
socket bytes → connection → codec → frame (opcode+payload)
  → dispatcher → brokers::<b>::tcp::handle
  → Command::parse → manager.<op>() → Response
  → ToWire::to_wire → socket
```

**HTTP (Dashboard → server):**
```
HTTP request → axum router → brokers::<b>::http::<handler>
  → manager.<snapshot>() → Snapshot type
  → From<Snapshot> for Dto → JSON response
```

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
- HTTP: `axum` + `tower-http`; embedded assets: `rust-embed`
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
- `dashboard/` (dev UI)

If one area is not impacted, state it explicitly.

**Tests**
Any behavior/protocol change must update or add tests in `tests/` (Rust) and `sdk/ts/tests/` (TS), then run the relevant suite(s).

**Frontend (dashboard)**
- Stack: React + TypeScript + `shadcn/ui` + Tailwind. No new libs without justification.
- Semantic styling only (no arbitrary color classes).
- Explicit `loading` / `empty` / `error` states; no implicit fallbacks.
- Use `??` (not `||`) only when nullable behavior is intentional.
- Strict typing: no `any`, no `@ts-ignore`.

---

## 8) Refactor Checklist

Before coding any non-trivial refactor, produce:

1. **Scope**: broker / module touched.
2. **Change**: what + why.
3. **Impact**: server / sdk / dashboard / docs → impacted or not (one line each).
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

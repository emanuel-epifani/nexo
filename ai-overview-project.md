# NEXO - AI Project Overview (Pragmatic)

## 1) Project Purpose

Nexo is a Rust broker server shipped as a single binary with four data communication models:

- `Store`: shared in-memory state
- `Pub/Sub`: low-latency realtime broadcast
- `Queue`: reliable durable job processing
- `Stream`: append-only event log with offsets

Practical goal: reduce operational complexity compared to multi-system stacks while keeping high performance for vertical deployments.

---

## 2) High-Level Architecture

- Rust server: core runtime and protocol
- React dashboard: local development UI (read-only)
- TypeScript SDK: client API for server interaction

Typical runtime ports:
- TCP `7654` -> client/server protocol
- HTTP `8080` -> local dashboard

---

## 3) Repo Map (Where to Touch What)

- `src/main.rs`, `src/lib.rs`: server entrypoint and global wiring
- `src/server/`: protocol, connection handling, routing
- `src/brokers/store/`: Store logic
- `src/brokers/pub-sub/`: Pub/Sub logic
- `src/brokers/queue/`: Queue logic 
- `src/brokers/stream/`: Stream logic
- `src/dashboard/`: server-side dashboard endpoints/integration
- `dashboard/src/`: React dashboard frontend
- `sdk/ts/src/`: TypeScript SDK implementation
- `tests/`: Rust integration tests
- `docs/guide/`: functional technical docs

Pragmatic rule: first identify the domain (`Store`, `Pub/Sub`, `Queue`, `Stream`), then work inside that broker directory.

---

## 4) How to Run Tests

```bash
# All Rust integration tests
cargo test

# Single suite
cargo test --test queue_tests
cargo test --test store_tests
cargo test --test pubsub_tests
cargo test --test stream_tests

# Benchmark/throughput tests (print output)
cargo test --release bench_<name> -- --test-threads=1 --nocapture

# TypeScript SDK tests
cd sdk/ts && npm test   # uses vitest
```

---

## 5) Broker Semantics (Must Know)

### Store
- Shared in-memory state
- Good for sessions, volatile cache, counters
- Focus: concurrency and low-latency reads/writes

Docs/code:
- `docs/guide/store.md`
- `src/brokers/store/store_manager.rs`

### Pub/Sub
- Transient topic-based messages (non-persistent)
- Supports wildcard matching (`+`, `#`)
- Focus: realtime fan-out and low latency

Docs/code:
- `docs/guide/pubsub.md`
- `src/brokers/pub-sub/pub_sub_manager.rs`

### Queue
- Durable FIFO with acknowledgments
- Delays, priority, retries, DLQ
- Focus: reliable async job processing

Docs/code:
- `docs/guide/queue.md`
- `src/brokers/queue/queue_manager.rs`

### Stream
- Immutable append-only log
- Offset and consumer-group based reads
- Focus: audit, replay, event history

Docs/code:
- `docs/guide/stream.md`
- `src/brokers/stream/stream_manager.rs`

---

## 6) Engineering Rules (Pragmatic)

### Core rules
- Brokers must be self-contained by domain; internal implementations can differ.
- Each broker should expose one clear public entrypoint (usually a manager), unless a different shape is explicitly justified.
- Remove dead code and stale comments during refactors.

### Alignment rule (server/sdk/docs/dashboard)
- For changes affecting protocol or behavior, always check alignment across:
  - `src/` (server)
  - `sdk/ts/` (SDK)
  - `docs/` (documentation)
  - `dashboard/` (dev UI, when relevant)
- If no update is needed for one area, explicitly state it in the final summary.

### Test alignment rule
- Any behavior/protocol change must trigger:
    - semantic review of existing Rust tests (`tests/`) and SDK TS tests (`sdk/ts/tests/`);
    - update/addition of tests for new behavior and edge cases;
    - execution of relevant test suites with explicit reporting.

### Frontend rules (Dashboard, minimal)
- Use the existing stack first (`React` + `TypeScript` + `shadcn/ui` + `Tailwind`); no new UI/state/data library without explicit justification.
- Keep each feature self-contained (types, logic, UI in the same feature area).
- Use `shadcn/ui` components by default; custom components only when no suitable primitive exists.
- Use semantic styling only (no hardcoded/arbitrary color classes).
- Avoid implicit fallback values; prefer explicit `loading` / `empty` / `error` states.
- Do not use `||` for data defaults; use `??` only when nullable behavior is intentional.
- Keep strict typing (`no any`, `no @ts-ignore` shortcuts).

---

## 7) Refactor Playbook (Mandatory)

For every refactor, always provide this output before coding:

1. **Scope**: affected broker/module.
2. **Change**: what changes + why.
3. **Impact**: `server` / `sdk` / `dashboard` / `docs` -> `impacted` or `not impacted` (+ one-line reason).
4. **Code complexity / performance trade-off**: acceptable or not, with quick estimate (`low`/`medium`/`high`).
  - **Acceptable**: complexity is mostly local to one function/class/module/broker, with limited cross-layer touch and no long-term synchronization burden.
  - **Not acceptable by default**: complexity is distributed across multiple files/layers/services, adds hidden coupling, or increases operational/debug risk.
  - If complexity is `high` or distributed, propose a simpler alternative before implementation.
5. **Tests**: semantic check on existing tests (`tests/`, `sdk/ts/tests/`), plus new tests needed (if any).
6. **Alignment**: confirm final alignment across server/sdk/dashboard/docs.
7. **Commit message**: recomend commit message aligned to "conventional commits" standard

---

## 8) Code Patterns (Must Know)

### Rust edition and core stack
- Edition `2021`; async runtime: `tokio` (features: `full`)
- Shared state: `dashmap` (concurrent map), `parking_lot` (fast RwLock)
- Persistence: `rusqlite` (bundled)
- Serialization: `serde` + `serde_json`; binary frames: `bytes` + `bytemuck`
- HTTP (dashboard): `axum` + `tower-http`
- Logging: `tracing` + `tracing-subscriber`

### Concurrency model
Each broker manager is a cheap-clone struct wrapping `Arc` internals:

| Layer | Primitive |
|---|---|
| Manager shared across connections | `Arc<Manager>` (via `NexoEngine`) |
| Name → resource map | `Arc<DashMap<String, Arc<ResourceShared>>>` |
| Per-resource mutable state | `std::sync::Mutex<Inner>` |
| PubSub subscription tree | `parking_lot::RwLock<Node>` |
| Async wake-up | `tokio::sync::Notify` |
| Background I/O tasks | `mpsc` / `oneshot` channels |
| Graceful shutdown | `CancellationToken` |
| Atomic counters (stream offsets) | `AtomicU64` |

### Error handling
- No `thiserror` / `anyhow` in the codebase; errors are `Result<_, String>` or `Response::Error(String)` at the broker/wire level.
- Codec errors use a local `ParseError(Invalid(String))`.
- Use `tracing::error!` / `tracing::warn!` for non-fatal failures; never silently swallow errors.
- **`unwrap()`/`expect()` are acceptable only in tests.** In `src/` hot paths use `?` or explicit error handling.

### Protocol / server shape
- Binary frames: fixed 10-byte header (`bytemuck` `Pod`), opcodes partitioned by broker range.
- `handle_connection` splits TCP socket into read/write tasks with `mpsc` channels; `JoinSet` for per-request concurrency.
- Routing: `RequestHandler::route(opcode, payload)` dispatches to `handle_store` / `handle_queue` / `handle_pubsub` / `handle_stream`.

### TypeScript SDK shape
- `NexoClient.connect()` → holds `NexoConnection` (TCP + EventEmitter) + lazy-cached broker handles.
- Binary codec mirrors Rust (`FrameCodec` / `Cursor`).
- Custom error classes in `errors.ts`; no `any`, no `@ts-ignore`.

---

## 9) Release Contract (Tag-Driven)

Releases are triggered by pushing a tag matching `v*`.

- Docker image → `emanuelepifani/nexo:<tag>` and `latest`
- TypeScript SDK → npm `@emanuelepifani/nexo-client`
- Docs → Vercel
- GitHub Release → created after all previous jobs succeed

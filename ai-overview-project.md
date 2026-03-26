# NEXO - AI Project Overview (Pragmatic)

## 1) Project Purpose

Nexo is a Rust broker server shipped as a single binary with four data communication models:

- `Store`: shared in-memory state
- `Pub/Sub`: low-latency realtime broadcast
- `Queue`: reliable durable job processing (ack/retry)
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
- `src/brokers/queue/`: Queue logic (durability/retry/DLQ)
- `src/brokers/stream/`: Stream logic (append/offsets/groups)
- `src/dashboard/`: server-side dashboard endpoints/integration
- `dashboard/src/`: React dashboard frontend
- `sdk/ts/src/`: TypeScript SDK implementation
- `tests/`: Rust integration tests
- `docs/guide/`: functional technical docs

Pragmatic rule: first identify the domain (`Store`, `Pub/Sub`, `Queue`, `Stream`), then work inside that broker directory.

---

## 4) Quick Run

Run server via Docker:

```bash
docker run -d -p 7654:7654 -p 8080:8080 nexobroker/nexo
```

Install SDK:

```bash
npm install @emanuelepifani/nexo-client
```

Dashboard URL:
- `http://localhost:8080`

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
- Focus: reliable async processing

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
- Keep changes small and scoped to the request.
- Brokers must be self-contained by domain; internal implementations can differ.
- Each broker should expose one clear public entrypoint (usually a manager), unless a different shape is explicitly justified.
- Avoid unrelated refactors.
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

---

## 8) Release Contract (Tag-Driven)

Releases are triggered by pushing a tag matching `v*`.

Pipeline contract:
- Docker image is published (`emanuelepifani/nexo:<tag>` and `latest`).
- TypeScript SDK is published to npm (`@emanuelepifani/nexo-client`).
- Docs are deployed to Vercel.
- GitHub Release is created after all previous jobs succeed.

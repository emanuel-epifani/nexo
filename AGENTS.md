# NEXO - AI Engineering Guide

Project overview: see [README.md](./README.md) and [docs/guide/introduction.md](./docs/guide/introduction.md).

## Repo Map

```
src/
  main.rs, lib.rs              # entrypoint, NexoEngine (holds 4 managers)
  config.rs                    # config types
  protocol/                    # transport-agnostic wire protocol (framing, codec, constants)
    codec.rs                   # tokio Decoder/Encoder
    frame.rs                   # FrameHeader, InboundFrame, OutboundFrame
    wire.rs                    # PayloadWriter/PayloadCursor (big-endian read/write)
    errors.rs                  # ParseError
    generated.rs               # AUTO-GENERATED from protocol.json — do not edit
  transport/tcp/
    connection.rs              # per-client TCP session lifecycle
    dispatcher.rs              # opcode → brokers::<b>::tcp::handle
  durable/                     # shared SQLite writer engine (queue + stream)
    engine.rs                  # Domain trait + writer thread (drain, tx, savepoints, effects)
    storage.rs                 # Store + Spec: WAL db lifecycle, lock file, fail-closed layout
    handle.rs                  # EngineHandle/PendingReply: async submit side for managers
    types.rs                   # CmdError, Admission, Stats, shared SQL helpers
  brokers/<broker>/            # store, queue, pub-sub, stream
    manager.rs                 # public API, returns neutral types (snapshot.rs)
    tcp.rs                     # Command parse, Response, handle()
    worker.rs                  # queue/stream: Domain impl for the durable engine
    domain/persistence.rs      # durable I/O (PubSub retained)
    domain/storage.rs          # queue/stream: schema + Spec + recovery over durable::Store
    domain/recipes.rs          # queue/stream: transactions over the engine's savepoints
protocol.json                  # single source of truth for all protocol constants
scripts/generate-protocol.js   # codegen: protocol.json → generated.{rs,ts,py}
tests/                         # Rust integration tests, one file per broker
sdk/ts/src/                    # TypeScript SDK
  protocol/                    # transport-agnostic wire contract
    generated.ts               # AUTO-GENERATED from protocol.json — do not edit
    codec.ts                   # frame serialization/deserialization
  transport/tcp/connection.ts  # TCP adapter
sdk/py/src/nexo/               # Python SDK package (typed, py.typed)
  protocol/                    # transport-agnostic wire contract
    generated.py               # AUTO-GENERATED from protocol.json — do not edit
    codec.py                   # frame serialization/deserialization
  transport/tcp/connection.py  # TCP adapter
sdk/integration-test-matrix.md # source of truth: test scenario IDs + TS/Python test names
tests/stress_tests.rs          # in-process manager performance tests (no TCP or SDK)
sdk/ts/tests/brokers/test-stress.test.ts # SDK end-to-end performance against local binary
sdk/py/tests/brokers/test_stress.py     # Python equivalent
docs/guide/                    # functional docs
```


**Dependency rule**: `manager.rs` must NOT import from `tcp.rs`, `transport/`, or any adapter layer. Adapters depend on the manager, never the reverse. Across server and SDKs, `protocol/` must remain transport-agnostic; `transport/tcp/` may depend on `protocol/`, never the reverse.

## Wire Protocol

**Request flow**: `socket → connection → codec → frame → dispatcher → brokers::<b>::tcp::handle → Command::parse → manager.<op>() → Response → encode_* → socket`

**Header** (11 bytes, `bytemuck` Pod): `[Version:1][FrameType:1][Meta:1][CorrelationID:4 BE][PayloadLen:4 BE]`. Mismatched `PROTOCOL_VERSION` → rejected both ends (stateless). `Meta` = opcode (Request) / status (Response) / push-type (Push).

**Response payloads**: `OK`/`NULL` → empty; `DATA` → bytes to end; `ERR` → `[ErrorCode:1][MessageLen:4 BE][Message:utf8][Details:optional JSON to end]`. Booleans = `DATA` with single `0/1` byte.

**Serialization**: `PayloadWriter`/`PayloadCursor` in `wire.rs` — big-endian, u32 length-prefix, UUID 16B raw. Keep `put_*`/`read_*` pairs aligned.

**Conventions**: consistent field order across opcodes; `DataType` prefix is client-side only; pushes have `CorrelationID = 0`; session id is opaque `&str` from transport.

Any wire change must stay symmetric across `src/`, `sdk/ts/`, `sdk/py/`, and bump `PROTOCOL_VERSION` if layout breaks.

## Protocol Constants (Single Source of Truth)

All protocol constants — `PROTOCOL_VERSION`, frame types, response statuses, error codes, provisioning statuses, data types, opcodes, command flag bits, and wire limits — are defined in `protocol.json` and generated into three files by `scripts/generate-protocol.js`:

- `src/protocol/generated.rs` (Rust)
- `sdk/ts/src/protocol/generated.ts` (TypeScript)
- `sdk/py/src/nexo/protocol/generated.py` (Python)

**These generated files must never be edited by hand.** The generator validates uniqueness, range constraints, and cross-broker opcode collisions at generation time.

**Workflow when changing protocol constants:**
```bash
# 1. Edit the source of truth
vim protocol.json

# 2. Regenerate all three files
node scripts/generate-protocol.js

# 3. Commit the spec + all generated files together
git add protocol.json src/protocol/generated.rs sdk/ts/src/protocol/generated.ts sdk/py/src/nexo/protocol/generated.py
```

**Verify sync (used in release script and CI):**
```bash
node scripts/generate-protocol.js --check   # exits non-zero if any generated file diverges
```

**Command flag bits** (e.g. `FLAG_PUBSUB_PUB_RETAIN`, `FLAG_QUEUE_Q_CREATE_HAS_VISIBILITY_TIMEOUT`) are also defined in `protocol.json` under `commandFlags` and generated the same way. Never hardcode `0x01`/`0x02`/`0x04` flag values in broker code — import the generated constant.

## Cross-Cutting Rules

- **Alignment**: any new feature/protocol/behavior change must be verified across `src/`, `sdk/ts/`, `sdk/py/`, `docs/`. State if one area is not impacted.
- **Test parity**: any new feature/behavior/protocol change requires updates in `tests/`, `sdk/ts/tests/`, `sdk/py/tests/`. `sdk/integration-test-matrix.md` is the source of truth. Every feature should have:
  - **Rust unit test** if the logic is algorithmically complex or CPU-intensive
  - **Integration tests** (Rust + all SDKs) covering happy path, edge cases, and error paths
  - **Fuzz test** when the feature involves parsing/serialization of untrusted input (wire protocol, client payloads)
- **Regression**: any bug fix must include a regression test in the affected broker/SDK, as a mod/describe block inside that broker's own test file — never as a standalone ad-hoc test file.
- **No leftover dev artifacts**: temporary files used during development (repro scripts, ad-hoc test files, debug harnesses, scratch configs) must be removed before committing, or folded into the proper permanent location. This applies to generated/committed files too — e.g. `sdk/codec-fixtures.json` must be regenerated in the same commit that bumps `protocol.json`'s `protocolVersion`, never left stale.
- **Performance**: compare always before/after of test-stress.test.ts, test_stress.py, tests/stress_tests.rs (keep alignes benchmark on docstring)
- **Algorithm**: only O(1) / O(log n) is acceptable. Never implement O(n)+ solutions — go back to redesign and pick better data structures.
- **Refactors**: prefer clean and robust refactors over backward compatibility while the project is pre-production. Hard breaking changes are acceptable; removed features must leave no residual references or compatibility layers.
- **Comments**: minimal and high-value only. Write "why", not "what" — if the code already says it, don't repeat it. No stale references to past refactors or old structures.

## Commit Conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/) — see [docs/commit-conventions.md](./docs/commit-conventions.md) for the full legend.

**One commit per feature/refactor.** Cross-scope changes (broker + SDKs + tests + docs) go in a single commit. Use `git commit --amend` to keep squashing until the change is complete.

## Commands

```bash
cargo test --test queue_tests          # single suite (also: store_/pubsub_/stream_tests)
cargo test --release --test stress_tests stream::bench_stream_ -- --test-threads=1 --nocapture
cd sdk/ts && npm test                  # TS SDK (vitest)
cd sdk/ts && npm run build             # clean TypeScript package build
cd sdk/py && pytest                    # Python SDK (pytest)
cd sdk/py && mypy src/nexo             # type checks
cd sdk/py && uv build                  # Python sdist + wheel
node scripts/generate-protocol.js      # regenerate protocol constants from protocol.json
node scripts/generate-protocol.js --check  # verify generated files are in sync
```

Release: `node scripts/release.js` → bumps version, runs checks, tags. Push `v*` tag → CI builds Docker, npm, PyPI, docs.

**SDK integration test safety:** `sdk/ts/tests/global-setup.ts` and `sdk/py/tests/conftest.py` fail fast if port 7654 is already bound (they never kill a foreign listener) and run the server against an isolated temporary persistence directory (queues/streams/pubsub under an `mkdtemp` dir), never the repository's `data/`.

## Stream Contract

- Protocol version **10**; stream storage schema **2** (unreleased layout — no migrations). Group identity is a single opaque UUID epoch (`groups.active_epoch`); there is no numeric generation.
- Delivery is the **keyed cumulative batch model**: at most one leased batch per `(stream, group, key)`, cumulative completed-prefix updates via `S_UPDATE_BATCH`/`S_UPDATE_BATCHES`, fresh receipt per assignment, checkpoint/retry/release dispositions.
- SDKs run a **continuous bounded executor**: per-key sequential callbacks, parallel independent keys, single in-flight fetch with message/byte/key-slot credit reservation, coalesced updates, per-callback timeout with cooperative cancellation.
- SDK defaults: `concurrency` 16, `keyBatchSize`/`key_batch_size` 32, `callbackTimeoutMs`/`callback_timeout_ms` 30000, buffered messages `2 × batchSize`, buffered bytes 20 MiB.
- Unused-key metadata is pruned incrementally by the `PruneKeys` continuation (empty lanes first, then `stream_keys` rows; the keyless sentinel `key_id = 0` is never pruned, and positive key IDs and global sequence numbers are never reused).
- Focused verification: `cargo test --lib brokers::stream::`, `cargo test --test stream_tests`, `cargo test --test codec_fixtures`; SDK mock subsets `vitest -t 'stream continuous executor'` / `pytest -k StreamContinuousExecutor` (sequential — both suites share port 7654); `tsc --noEmit`, `mypy sdk/py/src/nexo`, `node scripts/generate-protocol.js --check`.
- Lease ownership and local callback execution are distinct: a retired callback keeps its per-key execution gate until it settles, including across reconnect/seek. Different keys continue if capacity is free. Stop cannot forcibly terminate user code or guarantee external exactly-once effects.
Manager performance lives in tests/stress_tests.rs::stream; SDK stress suites remain separate E2E diagnostics. Stress tests measure operations, counts and timings, with only progress/completion and API-error guards. Functional assertions for payloads, key ordering, delivery identity, DLS and recovery belong in tests/stream_tests.rs and the corresponding SDK suites. STREAM_MANAGER_PERF records report source events/s, completed deliveries/s, assignment sizes, fetch/update calls and writer SQL/timing counters; never mix these with socket/SDK measurements.

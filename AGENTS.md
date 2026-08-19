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
  brokers/<broker>/            # store, queue, pub-sub, stream
    manager.rs                 # public API, returns neutral types (snapshot.rs)
    tcp.rs                     # Command parse, Response, handle()
    domain/persistence.rs      # durable I/O (Queue SQLite, Stream log, PubSub retained)
protocol.json                  # single source of truth for all protocol constants
scripts/generate-protocol.js   # codegen: protocol.json → generated.rs, protocol.ts, protocol.py
tests/                         # Rust integration tests, one file per broker
sdk/ts/src/                    # TypeScript SDK
  protocol.ts                  # AUTO-GENERATED from protocol.json — do not edit
sdk/py/src/                    # Python SDK (typed, py.typed)
  nexo/protocol.py             # AUTO-GENERATED from protocol.json — do not edit
sdk/integration-test-matrix.md # source of truth: test scenario IDs + TS/Python test names
docs/guide/                    # functional docs
```


**Dependency rule**: `manager.rs` must NOT import from `tcp.rs`, `transport/`, or any adapter layer. Adapters depend on the manager, never the reverse.

## Wire Protocol

**Request flow**: `socket → connection → codec → frame → dispatcher → brokers::<b>::tcp::handle → Command::parse → manager.<op>() → Response → encode_* → socket`

**Header** (11 bytes, `bytemuck` Pod): `[Version:1][FrameType:1][Meta:1][CorrelationID:4 BE][PayloadLen:4 BE]`. Mismatched `PROTOCOL_VERSION` → rejected both ends (stateless). `Meta` = opcode (Request) / status (Response) / push-type (Push).

**Response payloads**: `OK`/`NULL` → empty; `DATA` → bytes to end; `ERR` → utf8 to end. Booleans = `DATA` with single `0/1` byte.

**Serialization**: `PayloadWriter`/`PayloadCursor` in `wire.rs` — big-endian, u32 length-prefix, UUID 16B raw. Keep `put_*`/`read_*` pairs aligned.

**Conventions**: consistent field order across opcodes; `DataType` prefix is client-side only; pushes have `CorrelationID = 0`; session id is opaque `&str` from transport.

Any wire change must stay symmetric across `src/`, `sdk/ts/`, `sdk/py/`, and bump `PROTOCOL_VERSION` if layout breaks.

## Protocol Constants (Single Source of Truth)

All protocol constants — `PROTOCOL_VERSION`, frame types, response statuses, data types, opcodes, command flag bits, and wire limits — are defined in `protocol.json` and generated into three files by `scripts/generate-protocol.js`:

- `src/protocol/generated.rs` (Rust)
- `sdk/ts/src/protocol.ts` (TypeScript)
- `sdk/py/src/nexo/protocol.py` (Python)

**These generated files must never be edited by hand.** The generator validates uniqueness, range constraints, and cross-broker opcode collisions at generation time.

**Workflow when changing protocol constants:**
```bash
# 1. Edit the source of truth
vim protocol.json

# 2. Regenerate all three files
node scripts/generate-protocol.js

# 3. Commit the spec + all generated files together
git add protocol.json src/protocol/generated.rs sdk/ts/src/protocol.ts sdk/py/src/nexo/protocol.py
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
- **Regression**: any bug fix must include a regression test in the affected broker/SDK.
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
cd sdk/ts && npm test                  # TS SDK (vitest)
cd sdk/py && pytest                    # Python SDK (pytest)
cd sdk/py && mypy src/nexo             # type checks
node scripts/generate-protocol.js      # regenerate protocol constants from protocol.json
node scripts/generate-protocol.js --check  # verify generated files are in sync
```

Release: `node scripts/release.js` → bumps version, runs checks, tags. Push `v*` tag → CI builds Docker, npm, PyPI, docs.

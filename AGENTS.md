# NEXO - AI Engineering Guide

Project overview: see [README.md](./README.md) and [docs/guide/introduction.md](./docs/guide/introduction.md).

## Repo Map

```
src/
  main.rs, lib.rs              # entrypoint, NexoEngine (holds 4 managers)
  config.rs                    # config types
  transport/tcp/
    connection.rs              # per-client TCP session lifecycle
    dispatcher.rs              # opcode → brokers::<b>::tcp::handle
    protocol/                  # codec, frame, wire (read+write), errors
  brokers/<broker>/            # store, queue, pub-sub, stream
    manager.rs                 # public API, returns neutral types (snapshot.rs)
    tcp.rs                     # OPCODE_MIN/MAX, Command parse, Response, handle()
    domain/persistence.rs      # durable I/O (Queue SQLite, Stream log, PubSub retained)
tests/                         # Rust integration tests, one file per broker
sdk/ts/src/                    # TypeScript SDK
sdk/py/src/                    # Python SDK (typed, py.typed)
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

## Cross-Cutting Rules

- **Alignment**: any protocol/behavior change must be verified across `src/`, `sdk/ts/`, `sdk/py/`, `docs/`. State if one area is not impacted.
- **Test parity**: behavior/protocol changes require updates in `tests/`, `sdk/ts/tests/`, `sdk/py/tests/`. `sdk/integration-test-matrix.md` is the source of truth.

## Commands

```bash
cargo test --test queue_tests          # single suite (also: store_/pubsub_/stream_tests)
cd sdk/ts && npm test                  # TS SDK (vitest)
cd sdk/py && pytest                    # Python SDK (pytest)
cd sdk/py && mypy src/nexo             # type checks
```

Release: `node scripts/release.js` → bumps version, runs checks, tags. Push `v*` tag → CI builds Docker, npm, PyPI, docs.

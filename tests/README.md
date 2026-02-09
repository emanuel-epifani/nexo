# Nexo Tests

This directory contains **Rust-only tests** for the Nexo broker.

## 📁 Structure

```
tests/
├── integration/          # Integration tests (multi-module flows)
│   ├── queue_tests.rs
│   ├── stream_tests.rs
│   ├── pubsub_tests.rs
│   └── store_tests.rs
├── helpers/              # Test helper utilities
└── benchmark_showcase.rs # Performance benchmarks
```

## 🧪 Test Types

### Integration Tests (`integration/*.rs`)
Multi-module tests that verify broker functionality through their managers.

**Run all integration tests**:
```bash
cargo test --test queue_tests
cargo test --test stream_tests
cargo test --test pubsub_tests
cargo test --test store_tests
```

**Run all tests**:
```bash
cargo test
```

### Benchmarks
Performance benchmarks to measure throughput and latency.

```bash
cargo test --test benchmark_showcase --release
```

## 📝 TypeScript/SDK Tests

TypeScript E2E tests are now located in `sdk/ts/tests/`.

See [`sdk/ts/README.md`](../sdk/ts/README.md) for details.

**Run TypeScript tests**:
```bash
cd sdk/ts
npm test
```

---

**Note**: This separation keeps Rust tests and TypeScript tests organized by language/stack.

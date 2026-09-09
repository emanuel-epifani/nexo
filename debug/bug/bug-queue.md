# Queue Broker — Deep Analysis: Confirmed Bugs

## Architecture Overview

```
QueueManager (Arc<DashMap<String, Arc<QueueShared>>>)
  └─ lifecycle: TokioMutex<()>
  └─ timeout sweeper task (cancellation token)
  └─ per-queue: QueueShared
       ├─ Mutex<QueueInner>  (QueueState + DlqState + QueueConfig)
       ├─ Notify            (long-poll wakeups)
       └─ QueueStore         (SQLite, async bounded writer channel)

QueueState:
  registry: HashMap<Uuid, Message>
  ready:    PriorityQueue<Uuid, (priority desc, ready_seq asc)>
  in_flight: PriorityQueue<Uuid, Reverse<(visible_at, delivery_token)>>

Message lifecycle:
  push  → ready (visible_at=0)
  pop   → in_flight (visible_at=now+vt, delivery_token assigned, attempts++)
  ack   → delete (requires visible_at > now AND token match)
  nack  → requeue (visible_at=0, new ready_seq) OR DLQ (attempts >= max_deliveries)
  timeout → same as nack but reason="Timeout"
```

**Core invariant**: a message is ackable/nackable only while its lease is active,
i.e. `visible_at > now`. Once `visible_at <= now`, the lease has expired and the
timeout sweeper owns requeue/DLQ. Ack/nack on an expired lease returns false/no-op.

---

## BUG-QUEUE-001: `visibility_timeout_ms = 0` creates an un-ackable queue

**Severity**: High
**Affected layers**: Rust server (`QueueConfig::from_options`, `create_queue`),
both SDKs (no client-side guard), TCP parser (no range check).

### Root cause

`QueueConfig::from_options` accepts any `u64` for `visibility_timeout_ms`,
including `0`. No validation exists in `create_queue`, the TCP `Q_CREATE`
parser, or either SDK's `create()` call.

With `visibility_timeout_ms = 0`:

1. `pop_single` sets `visible_at = now + 0 = now`.
2. The message enters `in_flight` with `visible_at == now`.
3. `ack` checks `msg.visible_at <= now` → `now <= now` is **true** → returns
   `false`. The message **can never be acked**.
4. `nack` applies the same check → returns `(None, None)`. The message
   **can never be explicitly nacked**.
5. `process_expired` checks `*visible_at > now` → `now > now` is **false** →
   the message is immediately requeued by the timeout sweeper.

**Result**: every pop produces an immediate requeue. The consumer receives the
same message in a tight loop until `attempts >= max_deliveries`, at which point
it is moved to DLQ. No message can ever be successfully processed. The queue is
functionally broken from creation.

### Reproduction (Rust)

```rust
let config = QueueCreateOptions {
    visibility_timeout_ms: Some(0),
    max_deliveries: Some(5),
    ..Default::default()
};
manager.create_queue(q.clone(), config).await.unwrap();
manager.push(q.clone(), Bytes::from("msg"), 0).await.unwrap();

let msg = manager.pop(&q).await.unwrap();
// Ack immediately — should succeed, but returns false
assert!(!manager.ack(&q, msg.id, msg.delivery_token).await);
// Message reappears after sweeper tick
tokio::time::sleep(Duration::from_millis(100)).await;
let msg2 = manager.pop(&q).await.unwrap(); // same message, attempts=2
```

### Reproduction (TypeScript)

```ts
const q = await createQueue(qName, { visibilityTimeoutMs: 0, maxDeliveries: 5 });
await q.push('msg');
// Consumer loops forever receiving the same message until DLQ
```

### Expected behavior

`visibility_timeout_ms = 0` should be rejected at creation with an
`invalid_argument` error. A visibility timeout of zero is semantically
nonsensical — it means the lease expires at the instant it is granted,
making ack/nack impossible.

### Recommended fix

Add validation in `QueueConfig::from_options` (or `create_queue` before the
lifecycle lock):

```rust
if let Some(vt) = opts.visibility_timeout_ms {
    if vt == 0 {
        return Err(BrokerError::invalid_argument(
            "visibility_timeout_ms must be >= 1",
        ));
    }
}
```

Also add the same check in the TCP `Q_CREATE` parser for defense-in-depth, and
in both SDK `create()` methods for client-side fail-fast.

### Required regression tests

- **Rust** (`tests/queue_tests.rs`): `test_create_rejects_visibility_timeout_zero`
- **TypeScript** (`sdk/ts/tests/brokers/test-queue.test.ts`): reject
  `visibilityTimeoutMs: 0` at create
- **Python** (`sdk/py/tests/brokers/test_queue.py`): reject
  `visibility_timeout_ms=0` at create

---

## BUG-QUEUE-002: TypeScript SDK missing `waitMs` validation in `subscribe`

**Severity**: Medium
**Affected layers**: TypeScript SDK (`sdk/ts/src/brokers/queue.ts`).

### Root cause

`subscribe` validates `batchSize` and `concurrency` but **not** `waitMs`:

```ts
// queue.ts lines 391-392
if (batchSize < 1) throw new Error(`batchSize must be >= 1, got ${batchSize}`);
if (concurrency < 1) throw new Error(`concurrency must be >= 1, got ${concurrency}`);
// waitMs: no validation
```

The Python SDK validates all three:

```python
# queue.py lines 585-590
if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 1:
    raise ValueError(f"batch_size must be >= 1, got {batch_size}")
if not isinstance(wait_ms, int) or isinstance(wait_ms, bool) or wait_ms < 1:
    raise ValueError(f"wait_ms must be >= 1, got {wait_ms}")
if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency < 1:
    raise ValueError(f"concurrency must be >= 1, got {concurrency}")
```

**Impact**:

- `waitMs = -1`: `writeUInt32BE(-1)` throws `RangeError: value out of range`
  deep in the protocol codec, not a clean validation error.
- `waitMs = 1.5` (float): silently truncated to `1` by `writeUInt32BE`.
- `waitMs = NaN`: coerced to `0` by `writeUInt32BE`, producing non-blocking
  consume (probably not what the caller intended).
- `waitMs = Infinity`: throws `RangeError` in the codec.

This is a parity gap with the Python SDK and violates the fail-fast principle.

### Reproduction

```ts
const q = await createQueue(qName);
// No validation error — fails later in the protocol layer or silently misbehaves
await q.subscribe(async () => {}, { waitMs: -1 });
await q.subscribe(async () => {}, { waitMs: NaN });
await q.subscribe(async () => {}, { waitMs: 1.5 });
```

### Expected behavior

`subscribe` should validate `waitMs` is a positive integer, matching the
Python SDK's contract.

### Recommended fix

```ts
if (!Number.isInteger(waitMs) || waitMs < 1) {
    throw new Error(`waitMs must be a positive integer >= 1, got ${waitMs}`);
}
```

### Required regression tests

- **TypeScript**: `should reject waitMs=0 in subscribe`,
  `should reject non-integer waitMs in subscribe`,
  `should reject negative waitMs in subscribe`.

---

## BUG-QUEUE-003: Python SDK rejects valid `wait_ms = 0` (non-blocking consume)

**Severity**: Low-Medium
**Affected layers**: Python SDK (`sdk/py/src/nexo/brokers/queue.py`).

### Root cause

The Python SDK requires `wait_ms >= 1`:

```python
if not isinstance(wait_ms, int) or isinstance(wait_ms, bool) or wait_ms < 1:
    raise ValueError(f"wait_ms must be >= 1, got {wait_ms}")
```

But the **server explicitly supports `wait_ms = 0`** as a non-blocking consume
(`consume_batch` in `manager.rs` lines 598-600):

```rust
if wait_val == 0 {
    return Ok(vec![]);
}
```

`wait_ms = 0` means "check for messages and return immediately if none
available" — a valid and useful pattern for poll-based consumers that don't
want to block. The TypeScript SDK also accepts `waitMs = 0` (no validation).

This creates an asymmetry: a Python consumer cannot perform non-blocking
consume, while a TypeScript consumer can.

### Reproduction

```python
q = await _create_queue(nexo, q_name)
# Server supports this, TS SDK supports this, but Python rejects it
with pytest.raises(ValueError, match="wait_ms must be >= 1"):
    await q.subscribe(lambda _: None, wait_ms=0)
```

### Expected behavior

`wait_ms = 0` should be accepted as a valid non-blocking consume. The
validation should be `wait_ms < 0` (reject negatives) rather than
`wait_ms < 1` (reject zero).

### Recommended fix

```python
if not isinstance(wait_ms, int) or isinstance(wait_ms, bool) or wait_ms < 0:
    raise ValueError(f"wait_ms must be >= 0, got {wait_ms}")
```

### Required regression tests

- **Python**: `test_wait_ms_zero_accepted` — subscribe with `wait_ms=0`
  should not raise.

---

## Summary

| ID | Severity | Layer | Issue |
|---|---|---|---|
| BUG-QUEUE-001 | High | Server + both SDKs | `visibility_timeout_ms=0` creates un-ackable queue (no validation) |
| BUG-QUEUE-002 | Medium | TS SDK | Missing `waitMs` validation in `subscribe` (parity gap with Python) |
| BUG-QUEUE-003 | Low-Med | Python SDK | Rejects valid `wait_ms=0` (non-blocking consume) |

### Areas verified as correct (no bugs found)

- **Push/pop/priority/FIFO ordering**: `PriorityQueue` with `(priority desc,
  ready_seq asc)` correctly orders by priority then FIFO. `ready_seq` is
  monotonic and persisted, surviving restart.
- **Delivery token correctness**: `ack`/`nack` require both `id` and
  `delivery_token` match, plus an active lease (`visible_at > now`). Stale
  tokens correctly return false. Token is monotonic and persisted.
- **Max-delivery / DLQ behavior**: `attempts >= max_deliveries` correctly
  triggers DLQ on both nack and timeout paths. `max_deliveries=0` is a valid
  "immediate DLQ" configuration.
- **DLQ replay**: `to_message()` resets attempts/ready_seq/delivery_token
  consistently between in-memory and persistence. Replayed messages get a
  fresh `ready_seq` via `push()`.
- **DLQ peek ordering**: `LinkedHashMap` with `dlq_seq` correctly produces
  most-recent-first ordering, surviving restart.
- **Long polling**: `Notify` registration before state check avoids lost
  wakeups. Per-waiter deadlines are independent. Multiple waiters are all
  notified on push/requeue.
- **Persistence/recovery**: SQLite writer batches transactionally with
  rollback on failure. Warm start auto-discovers queue DBs. Corrupted DBs
  fail-fast (queue not registered). In-flight messages recover and expire
  on restart.
- **Queue lifecycle**: `lifecycle_mutex` serializes create/delete. Path
  traversal names rejected. Symlink DB files rejected. Delete shuts down
  store before removing files.
- **Protocol parsing**: Push count limits enforced. Huge counts rejected
  before allocation. Priority range validated.
- **Concurrent consumers**: Visibility timeout correctly reserves messages
  so duplicate delivery across consumers is prevented.
- **`process_expired` notifications**: Requeued messages notify waiters; DLQ
  moves do not (correct — DLQ has no long-poll consumers).

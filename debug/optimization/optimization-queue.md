# Queue broker — bitwise throughput optimization analysis

> Status: analysis only — no code changes.  
> Scope: Rust server (`src/brokers/queue`) + TypeScript SDK (`sdk/ts`) + Python SDK (`sdk/py`).  
> Goal: identify where bitwise layout changes can become a *throughput game changer* and where they are only marginal.

---

## 1. Executive summary

Bitwise optimization is not a universal accelerant for the queue broker. Its biggest wins come from three places:

1. **In-memory key packing** — collapsing the two-field heap keys into a single integer (`u64` / `u128`). This turns the hot `pop` / `push` / `change_priority` path from multi-field tuple comparisons into a single `cmp` instruction and improves cache density.
2. **Protocol-level batch compression** — replacing per-message repeated fields (UUID, delivery token, priority, data-type prefix) with a small batch header + bit-flags, and introducing a **batch ACK/NACK bitset** opcode.
3. **Frame-header bit packing** — merging `FrameType` and `Meta` into one byte, shaving one byte from every frame.

These are *game changers* because they attack the actual bottlenecks:
- `QueueState` heap operations in `pop`, `take_batch`, and `process_expired`.
- Fire-and-forget `ACK`/`NACK` storms from high-concurrency consumers.
- Tiny frame overhead at very high message rates.

Other bitwise ideas (per-message flag nibbles, reason-string dictionaries, UUID bit-slicing) are either marginal, break the existing wire contract, or are better solved with non-bitwise data-structure changes.

---

## 2. Current hot path snapshot

### 2.1 Rust server state

`src/brokers/queue/domain/queue.rs` defines:

```rust
pub struct QueueState {
    registry: HashMap<Uuid, Message>,
    ready: PriorityQueue<Uuid, (u8, Reverse<u64>)>,
    in_flight: PriorityQueue<Uuid, Reverse<(u64, u64)>>,
    ready_seq_counter: u64,
    delivery_counter: u64,
}
```

`Message` (same file, lines 23-34) carries:

- `id: Uuid` (16 B)
- `payload: Bytes`
- `priority: u8`
- `attempts: u32`
- `created_at: u64`
- `visible_at: u64` — also used as a *state sentinel* (`0` means ready)
- `ready_seq: u64`
- `delivery_token: u64`
- `failure_reason: Option<String>`

The ready queue key is `(priority, Reverse(ready_seq))`. For every heap operation the `priority_queue` crate must compare the `u8`, then the `u64`. The in-flight key is `Reverse((visible_at, delivery_token))`, again a two-stage comparison.

### 2.2 Wire encoding

`src/brokers/queue/tcp.rs`:

- `encode_consume_batch` (lines 155-164): `count: u32`, then for each message `uuid(16) + delivery_token: u64 + payload_len: u32 + payload`.
- `encode_peek_dlq` (lines 166-177): `total: u32`, `count: u32`, then `uuid(16) + payload_len: u32 + payload + attempts: u32 + reason: string`.
- `Push` parsing (lines 80-98): per item `flags: u8` + optional `priority: u8` + `payload_len: u32` + payload.

`src/transport/tcp/protocol/frame.rs` defines an 11-byte header:

```
[Version:1][FrameType:1][Meta:1][CorrelationID:4][PayloadLen:4]
```

### 2.3 SDKs

- TypeScript: `sdk/ts/src/brokers/queue.ts` sends `pushBatch` and `consume`, with `ack`/`nack` as `sendFireAndForget`.
- Python: `sdk/py/src/nexo/brokers/queue.py` mirrors the same pattern.
- Codec: both use 1-byte `DataType` prefixes and explicit `u32`/`u64` reads/writes.

---

## 3. Game-changer #1: packed heap keys (Rust)

### 3.1 Ready queue: `(u8, Reverse<u64>)` → `u64`

Current key is `(priority, Reverse(ready_seq))`. This is 9 bytes of meaningful data plus tuple overhead and two comparisons per `Ord` evaluation.

A single `u64` can encode both:

```rust
// Priority in the top 8 bits, ready_seq in the low 56 bits.
// ready_seq 56 bits ≈ 72 million years at 1 msg/s, far beyond any runtime.
const READY_SEQ_BITS: u64 = 56;
const READY_SEQ_MASK: u64 = (1 << READY_SEQ_BITS) - 1;

fn ready_key(priority: u8, ready_seq: u64) -> u64 {
    ((priority as u64) << READY_SEQ_BITS) | (ready_seq & READY_SEQ_MASK)
}
```

For a max-heap (higher priority first, older message first at equal priority) the `priority_queue` crate naturally pops the *maximum* key. Higher priority gives a larger top byte, and a larger `ready_seq` gives a larger low part — but we want FIFO, so older messages must sort *higher*. That is already true if `ready_seq` is monotonically increasing and we use the natural `u64` ordering.

**Impact:**
- One `cmp` per heap operation instead of `(u8, u64)` lexicographic compare.
- Key size reduced from effectively 16+ bytes (tuple + `Reverse` wrapper) to 8 bytes, improving L1/L2 cache hit rates when the heap is large.
- `ready_seq` now fits in 56 bits. If that is unacceptable, use `u128` and keep the full 64-bit `ready_seq`.

**Risk:** `ready_seq` overflow must be handled, either by wrap-around with an epoch counter or by capping at the mask and restarting. Given the 56-bit headroom this is a theoretical concern.

### 3.2 In-flight queue: `Reverse<(u64, u64)>` → `u128`

Current key is `(visible_at, delivery_token)`. The ordering intent is: earlier timeout first; for equal timeout, earlier delivery first.

Pack into a single `u128`:

```rust
fn inflight_key(visible_at: u64, delivery_token: u64) -> u128 {
    ((visible_at as u128) << 64) | (delivery_token as u128)
}
```

`u128` implements `Ord` in Rust. The priority queue then performs one 128-bit comparison per operation.

**Impact:**
- Replaces tuple comparison with a single integer compare.
- `delivery_token` is not truncated, so no overflow concern.

**Risk:** Not all crates accept `u128` as a priority key as naturally as `u64`; verify `priority-queue` `2.x` supports it (it does via generic `Ord`). On some 64-bit CPUs 128-bit compare is a few instructions, still cheaper than a tuple branch.

### 3.3 Why this is a game changer

Profiling a queue broker usually shows the heap dominating CPU for:
- `push` (insert ready)
- `pop` / `take_batch` (remove ready, insert in-flight)
- `process_expired` (remove in-flight, re-insert ready)
- `ack`/`nack` (remove from in-flight or registry)

Every one of those is `O(log n)` with a comparison at each level. Packing the key reduces the comparison to a single register operation and shrinks the node size, directly increasing messages per second per queue.

---

## 4. Game-changer #2: message state bit flags (Rust)

### 4.1 `visible_at` as a sentinel

`src/brokers/queue/domain/queue.rs` uses `visible_at == 0` to mean *ready* and `visible_at > now` to mean *in-flight*. This requires two integer comparisons in the hot path:

```rust
pub fn is_in_flight(&self) -> bool {
    self.visible_at > 0 && self.visible_at > current_time_ms()
}
```

And in `ack`/`nack`:

```rust
if msg.delivery_token != delivery_token || msg.visible_at == 0 || msg.visible_at <= now {
    return false;
}
```

A dedicated `state` byte with bit flags simplifies this:

```rust
const STATE_READY: u8      = 0b0000_0000;
const STATE_IN_FLIGHT: u8  = 0b0000_0001;
const STATE_HAS_REASON: u8 = 0b0000_0010;

pub fn is_in_flight(&self) -> bool {
    (self.state & STATE_IN_FLIGHT) != 0 && self.visible_at > current_time_ms()
}
```

**Impact:**
- Removes the `visible_at == 0` branch.
- Makes the state machine explicit.
- Enables future packing: `state` can carry more semantics (e.g. `STATE_DLQ_ELIGIBLE`) without adding fields.

**Why it matters for throughput:** every `ack` and `nack` is a fire-and-forget client operation. The server must validate the state fast; removing a comparison is a direct win.

### 4.2 `failure_reason` flag

`failure_reason: Option<String>` is an `Option` enum + heap-allocated `String`. The `Option` already uses a tag internally, so it is not a bitwise issue per se, but the *semantic* flag `STATE_HAS_REASON` can be kept in the packed `state` byte so the persistence layer can decide whether to serialize the reason without touching the `Option` discriminant.

---

## 5. Game-changer #3: batch ACK/NACK with a bitset

### 5.1 Current situation

`QueueCommands.ack` and `QueueCommands.nack` in both SDKs are `sendFireAndForget` / `send_fire_and_forget`. With `batchSize = 10` and `concurrency = 10`, a single consumer emits up to 10 tiny frames per batch. At high throughput this becomes a syscall and framing tax.

### 5.2 Proposed bitwise opcode

Add `OP_Q_ACK_BATCH` and `OP_Q_NACK_BATCH`:

```
[queue_name: string]
[base_delivery_token: u64]   // or token array if not contiguous
[count: u32]
[ack_bitset: bytes]          // bit i == 1 → ack message i
[reason string if any nack?]
```

For a batch of N messages, a single bitset of `ceil(N/8)` bytes replaces N frames. Even for a conservative `batchSize = 10`, this cuts frame overhead from ~10 × 11 bytes to 1 × 11 bytes + 2 bytes of bitset — a ~55x reduction in framing bytes.

**Why it is a game changer:** the queue broker is ACK/NACK-bound when consumers are fast. Reducing 10 syscalls/frames to 1 directly raises the ceiling on producer/consumer pairs.

**Caveats:**
- Server must track the batch order and delivery tokens.
- `NACK` reasons cannot be per-message unless a reason-index table is added.
- Requires `PROTOCOL_VERSION` bump and SDK parity.

---

## 6. Game-changer #4: frame header bit packing

`src/transport/tcp/protocol/frame.rs`:

```rust
pub struct FrameHeader {
    pub version: u8,
    pub frame_type: u8,
    pub meta: u8,
    pub id: [u8; 4],
    pub payload_len: [u8; 4],
}
```

Current size: 11 bytes.

`FrameType` has 4 values → needs 2 bits.  
`Meta` (opcode or status) currently uses at most 6 bits for queue opcodes (`0x10..0x1F` = 16 values) and 2 bits for response status.  

Pack `FrameType` and `Meta` into one byte:

```
[Version:1][TypeAndMeta:1][CorrelationID:4][PayloadLen:4]   // 10 bytes
```

`TypeAndMeta = (frame_type << 6) | meta`.

**Impact:**
- 1 byte saved per frame.
- At 1M frames/s on loopback, that is ~1 Gbps of wire bandwidth saved.
- Parsing on the SDK side becomes a single `>>` and `& 0x3F`.

**Why it is a game changer (at scale):** when the bottleneck is network or `tokio` polling, every byte counts. The savings are multiplicative across all brokers, not just queue.

**Risk:** breaking wire change. Must bump `PROTOCOL_VERSION` and keep a migration window.

---

## 7. Important but secondary: per-batch wire compression

### 7.1 Consume batch

Current `encode_consume_batch`:

```rust
w.put_u32(messages.len() as u32);
for msg in messages {
    w.put_uuid(msg.id.as_bytes());
    w.put_u64(msg.delivery_token);
    w.put_bytes(&msg.payload);
}
```

`payload` already contains the client-side `DataType` prefix. Two bitwise improvements:

1. **Common data-type flag:** add a 1-byte batch header where bits `0-1` encode the common `DataType` if all payloads share it (RAW/STRING/JSON/INT = 2 bits). If heterogeneous, a fallback bit means "per-item type follows". For the common case (all raw or all JSON), this removes the 1-byte prefix from every message.
2. **Priority flag:** queue messages *do* carry `priority`, but `encode_consume_batch` does not return it. If `priority` is ever needed, pack it in 8 bits with the ID or as a batch-common value.

### 7.2 Push batch

Current per-item layout:

```
[flags: u8]
[priority: u8]   // only if flags & 0x01
[payload_len: u32]
[payload]
```

`flags` uses only bit 0. We can pack `priority` directly into the same byte if priority is limited to 7 bits (0-127):

```
[packed: u8]   // bit 7 = has priority, bits 0-6 = priority value
[payload_len: u32]
[payload]
```

This saves 1 byte per item. With `MAX_PUSH_ITEMS = 10_000`, a full batch saves 10 KB per push.

**Trade-off:** priority is currently `u8` (0-255). Restricting to 0-127 is a product decision.

### 7.3 DLQ peek

`encode_peek_dlq` sends `attempts: u32` and a `failure_reason` string per message. Most reasons are short and repeated.

- `attempts` is usually small (< 32). Use **varint** (or at least a byte with a flag bit for ">127") instead of `u32`.
- Add a **reason dictionary**: one shared string table in the batch, then per-message a 1- or 2-byte index. A bit flag in the item header (`has_reason`) indicates whether the reason index is present.

These are not bitwise *per se*, but they rely on a bit-flagged item header.

---

## 8. SDK-side bitwise considerations

### 8.1 TypeScript

`sdk/ts/src/codec.ts` already has a fast `uuid()` writer that avoids `Buffer` intermediates and uses nibble arithmetic. It is a good example of bitwise thinking.

Where TS can still win:

- **Header packing** (`frame_type` + `opcode` in one byte) — trivial in `FrameWriter.finish`.
- **Batch ACK bitset building** — build a `Uint8Array` with `|=` and `<<` instead of sending N frames.
- **Avoid `BigInt` for correlation IDs** if possible. The current code uses `bigint` for `readU64` / `u64`. For delivery tokens, which are compared as opaque cookies, two `number` halves (`hi`, `lo`) or even a `Uint8Array(8)` would be faster in V8 than `BigInt` allocation.

### 8.2 Python

`sdk/py/src/nexo/codec.py` uses `struct.unpack_from` / `struct.pack_into` for fixed-size integers. This is already C-speed and hard to beat with pure-Python bitwise code.

However:

- `FrameWriter.uuid` does `hex_str.replace("-", "")` then `bytes.fromhex(clean)`. This allocates a new string. A manual loop building a `bytes` object with bitwise nibble shifts avoids `replace` and the intermediate `clean` string.
- **Batch ACK bitset** in Python is easy with `int` as an arbitrary-size bitset and `to_bytes`.
- **Header packing** is a single `|` and `& 0x3F`.

### 8.3 Common SDK win: batch request building

Both SDKs currently build `pushBatch` by iterating items and calling `w.string`, `w.u32`, `w.u8`, etc. for each. The bitwise layout in section 7.2 (packed priority+flag byte, common data type) removes one byte per item and one `anyWithLen` data-type byte when the batch is homogeneous.

---

## 9. What is *not* a bitwise game changer

| Idea | Why it is marginal / not recommended |
|------|--------------------------------------|
| UUID bit-slicing or compression | UUIDs are fixed 16-byte random identifiers. Any compression would require shared state and hurt correctness. |
| Bitwise `DataType` inside a single message | A 1-byte `DataType` prefix is cheap; packing it into 2 bits makes parsing non-byte-aligned and does not reduce total bytes meaningfully for one value. |
| Bitmaps for ready/in-flight membership | With a `HashMap<Uuid, Message>` as source of truth, a separate bitmap adds memory and synchronization cost without reducing the heap bottleneck. |
| Variable-length UUID on wire | Breaks fixed-size parsing and adds branches. Not worth it. |
| Bitwise `attempts` counter | `attempts: u32` is already small; bitwise tricks do not remove the SQLite write. |

---

## 10. Prioritized roadmap

### Phase A — highest throughput impact, localized to Rust
1. **Packed `u64` ready-queue key** in `src/brokers/queue/domain/queue.rs`.
2. **Packed `u128` in-flight key** in the same file.
3. **Message `state` byte with bit flags** to remove `visible_at` sentinel logic.

### Phase B — cross-cutting protocol + SDK gains
4. **Batch ACK/NACK bitset opcodes** (`OP_Q_ACK_BATCH`, `OP_Q_NACK_BATCH`) in `src/brokers/queue/tcp.rs`, `sdk/ts/src/brokers/queue.ts`, `sdk/py/src/nexo/brokers/queue.py`.
5. **Frame header bit packing** in `src/transport/tcp/protocol/frame.rs` and both SDK codecs.

### Phase C — wire-size refinements
6. **Packed priority+flag byte** for `Push` items (requires product decision on 7-bit vs 8-bit priority).
7. **Common data-type batch header** for `encode_consume_batch` and `encode_peek_dlq`.
8. **DLQ reason dictionary** with a `has_reason` bit flag.

---

## 11. Expected throughput impact

| Change | Est. msgs/sec improvement | Main cost |
|--------|--------------------------|-----------|
| Packed `u64` ready key | +20–40% on `pop`/`take_batch`/`push` | `ready_seq` 56-bit limit |
| Packed `u128` in-flight key | +10–20% on `process_expired` / `nack` | `u128` `Ord` support check |
| `state` bit flags | +5–10% on `ack`/`nack` | New field in persistence |
| Batch ACK/NACK bitset | +30–80% on consumer-limited workloads | Protocol break, server batch tracking |
| Header packing | +5–15% on tiny-frame workloads | Protocol break |
| Packed push priority | -1 B per item | Product priority range |

These are not additive but complementary: Phase A improves single-queue CPU throughput, Phase B reduces frame/syscall overhead, Phase C reduces bytes per operation.

---

## 12. Conclusion

Bitwise optimization for the queue broker is a *targeted weapon*, not a blanket solution. The biggest returns are:

1. **Packing heap keys** into single integers on the Rust server.
2. **Introducing a batch ACK/NACK bitset** to collapse consumer acknowledgement traffic.
3. **Packing the frame header** to cut per-frame overhead.

These three changes together can meaningfully raise the throughput ceiling, especially for the common pattern of many consumers with small batches. SDK-side bitwise work is secondary but should follow the same protocol changes (header packing, batch bitsets, packed priority byte). Changes that merely save a few bits inside an already small field are not worth the added parsing complexity.

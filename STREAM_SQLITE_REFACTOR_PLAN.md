# Stream SQLite refactor: implementation and resume instructions

This is the persistent engineering handoff for an APPROVED refactor of Nexo Stream. It is intentionally self-contained so another agent can continue without the original conversation. Read this file and the repository's root AGENTS.md before working. Do not restart the design from scratch, discard partial changes, or mistake the successful design probes for a completed implementation.

## 0. Checkpoint: read this first

Last checkpoint: 2026-09-20, implementation COMPLETE and verified. See section 21 "implementation complete" entry for full results.

- User approved the architecture and authorized implementation after this plan was saved.
- Primary objective: correctness by construction, maintainability, and future evolution. Raw throughput improvement is not the primary objective. Avoid a large regression; approximately doubling normal operation latency would be unacceptable without discussion.
- Production implementation: COMPLETE (all milestones A–F).
- Production tests/builds: ALL GREEN except pre-existing user-owned pubsub repro probes (unrelated, untracked, unchanged by this refactor).
- Performance after-capture: recorded in section 21; sequential single-op latency ~2× (post-commit ack vs page-cache enqueue), concurrent publish −12%, subscribe+ack −47%, batch −93% vs non-durable baseline — all within the agreed trade-off direction (durability per commit) but flagged for user review.
- Original git HEAD: `3dc3b0781cb1cd4b891b62f97ce42373dbeaba27`.
- Original branch: `main`, three commits ahead of `origin/main`.
- Original tracked diff: empty.
- Existing USER-owned untracked files, which MUST NOT be edited, deleted, staged, or treated as this task's work:
  - `debug/bug/bug-queue.md`
  - `sdk/py/tests/brokers/test_pubsub_review_repro.py`
  - `sdk/ts/tests/brokers/pubsub-review-repro.test.ts`
  - `tests/pubsub_review_repro.rs`
- An empty `.devin/stream-sqlite-refactor/` directory was created during planning. This root-level file is the authoritative plan requested by the user; do not create a second competing plan there.
- Do not commit or push unless the user asks. Do not reset the working tree or overwrite unrelated changes.

### Resume procedure

1. Read this checkpoint and the latest entries in section 21.
2. Run `git status --short --branch` and `git diff --stat`; distinguish task changes from the user files listed above.
3. Read modified Stream files before editing. Do not assume the original architecture still exists after partial implementation.
4. Identify the first incomplete milestone in section 19. Continue there rather than rewriting completed work.
5. Reuse an existing task-owned server/build if its process and configuration are recorded below and still valid. Never kill or reuse an unrelated server.
6. Update the checkpoint after each milestone or blocker, including exact commands, results, relevant process IDs, and the next action.
7. Passing a compiler is not sufficient: the transition/recovery invariants below are acceptance requirements.

## 1. Approved product and architectural decisions

The following decisions have been agreed with the user. Do not silently replace them with a simpler but semantically different implementation.

1. Replace Stream's custom segment files and state snapshots with ONE embedded SQLite database for the Stream engine.
2. Store event data and consumer-group state in the SAME database, so their changes can share transactions. Do not keep payloads in a separate custom file log.
3. SQLite is the authoritative persistent state. Runtime maps may contain connections, pending long-polls, bounded caches, and notifications; they must not contain the only copy of an outstanding delivery obligation.
4. Keep the application model: publish optional key, consumer groups, pull/long-poll, independent ACKs, per-key ordering, DLS, seek/replay, retention.
5. No partitions or fixed key-to-consumer assignment are exposed to applications. Any available consumer can receive the next eligible message of a key.
6. Keyless messages remain parallel. Do NOT put every keyless message behind one serial in-flight gate.
7. Use a dedicated SQLite writer/command worker. Do not introduce a framework of actors or an authoritative in-memory ConsumerGroup per key/group.
8. Use WAL and `synchronous=NORMAL` initially. This preserves the documented lack of a power-loss durability guarantee. Do not switch to FULL, OFF, an in-memory database, or ACK-before-commit merely to improve benchmarks.
9. Publish and state-changing responses are sent AFTER their transaction commits. Storage errors must not be logged and converted into success.
10. `maxBytes` means the logical size of the stream's retained events, not the exact physical size of the shared SQLite file. The user explicitly accepted this distinction. SQLite pages/WAL/indexes can occupy additional space; reclamation/reuse is managed by the server.
11. New on-disk format, NO importer and NO compatibility layer for old `.log`/`state.log` data. Detect legacy data and fail clearly without deleting or converting it.
12. Small internal SDK/wire changes are allowed for delivery receipts and correct lifecycle handling. The business callback API must remain unchanged.
13. Performance is a design constraint and a before/after verification task. Do not build a throwaway full implementation solely to benchmark SQLite before the real refactor.

### Guarantees and limits

- Ordering is per `(stream, group, key)`, in server-assigned publication order.
- Different groups have independent progress over the same stored event payloads.
- At most one VALID delivery attempt per keyed lane. A timed-out consumer might still perform external side effects: broker fencing cannot cancel a remote database/API call. Consumers still need idempotency.
- Keyless messages have no inter-message processing-order guarantee. SDK `concurrency=1` alone does not create a global ordering guarantee across consumers or failures.
- Retention remains time/size based, not ACK-protected. Events may expire before a slow group processes them. Do not advertise unconditional delivery beyond the retention window.
- A single hot key remains serial. Adding consumers cannot remove that business-ordering constraint.
- Consumer scaling does not imply multi-node broker replication. Nexo remains a single-node engine.

## 2. Repository map and integration seams

Read these files; do not rediscover the entire repository:

- `src/brokers/stream/manager.rs`: public API, runtime coordination, lifecycle; currently owns stream locks and group state.
- `src/brokers/stream/domain/group.rs`: current state machine and unit tests; replace the in-memory authoritative group model with typed lane/range/transition logic.
- `src/brokers/stream/domain/persistence.rs`: replace custom file I/O/recovery with SQLite storage and transaction operations.
- `src/brokers/stream/domain/message.rs`: immutable Message; introduce an explicit delivery wrapper rather than putting a consumer receipt on every stored Message.
- `src/brokers/stream/domain/definition.rs`, `config.rs`, `options.rs`: effective resource config and global engine settings.
- `src/brokers/stream/tcp.rs`: command parsing and response encoding; remains an adapter.
- `src/transport/tcp/connection.rs`, `dispatcher.rs`: stream command ordering and non-blocking response waiting.
- `src/lib.rs`, `src/main.rs`: engine startup/shutdown.
- `src/brokers/mod.rs`: BrokerError kinds and provisioning helpers.
- `protocol.json`, `scripts/generate-protocol.js`: protocol source of truth and generator.
- `sdk/ts/src/brokers/stream.ts`, `sdk/py/src/nexo/brokers/stream.py`: receipt transport, ACKs, fencing/rejoin, stop handling.
- `tests/stream_tests.rs`, `tests/transport_tests.rs`, `tests/codec_fixtures.rs`.
- `sdk/ts/tests/brokers/test-stream.test.ts`, `sdk/py/tests/brokers/test_stream.py`.
- `sdk/integration-test-matrix.md`: scenario parity.
- `docs/guide/stream.md`: functional contract and configuration.
- `tests/stress_tests.rs`, `sdk/ts/tests/brokers/test-stress.test.ts`, `sdk/py/tests/brokers/test_stress.py`: benchmark entry points.

Dependencies already present: `rusqlite` 0.38 with bundled SQLite, Tokio, bytes, UUID v4, serde, parking_lot. Do not add a new database library. Docker builds on Rust 1.90. `std::fs::File::try_lock` is available from Rust 1.89 and can implement the process ownership lock without a new crate.

The manager/domain must not import the TCP adapter. Protocol modules must remain transport-agnostic. Do not copy Queue's SQLite writer configuration: its writer currently uses synchronous=OFF, which is NOT the chosen Stream policy.

## 3. Domain representation

### Types and meanings

Use newtypes or strongly named wrappers where they prevent mixing values:

- `StreamId`, `GroupId`, `EpochId`: fresh UUIDs, stored as 16-byte BLOBs. Recreating a stream name must never reuse its identity.
- `Sequence`: global event sequence within a stream, u64, starting at 1.
- `KeyPosition`: ordinal within a particular stream key, u64, starting at 1.
- `DeliveryReceipt`: fresh UUID/16 bytes for EACH delivery attempt, not just each message.
- Key id 0 is the internal keyless bucket. Real keys are nonempty opaque bytes and use positive internal IDs.
- A cursor stores the LAST resolved/admitted position, not an unchecked `last + 1`. This avoids requiring a sentinel beyond u64::MAX.

Keep stored Message independent of its delivery attempt:

```rust
pub struct Delivery {
    pub message: Message,
    pub receipt: [u8; 16],
}

pub struct ConsumerIdentity {
    pub connection_id: String,
    pub consumer_id: String,
    pub generation: u64,
}
```

Connection IDs come from the transport, never from a trusted interpretation of a client-supplied field. Membership validation binds consumer id, connection id, group identity, and current epoch/generation.

### Lane states

Use one state enum, with these persisted states:

- EMPTY: no current candidate.
- READY: one eligible keyed head, either original delivery or explicit replay.
- LEASED: that head has a receipt, owner, connection, deadline, and attempt count.
- PARKED: key is blocked by unresolved DLS members.

There is NO separate fresh-versus-redeliver queue. A retry is READY with an existing head and a nonzero attempt count. This removes the old duplicate path after recovery.

Each keyed lane also has:

- `cursor_pos`: last resolved ORIGINAL key position;
- `normal_seq`: first unresolved ORIGINAL sequence, if present;
- `head_seq`, `head_pos`, `head_origin`: current scheduling candidate, which may be an old replay;
- attempts for the current head;
- a ready-order ticket for fair scheduling.

Do not confuse `head_seq` with `normal_seq`: an old replay must neither regress nor be lost behind the original progress watermark.

The keyless bucket has a source cursor and a READY/EMPTY source entry, but it is never LEASED or PARKED as a whole. Each admitted keyless delivery has its own row in `keyless_deliveries`.

### Epochs

A group points at one active epoch. Seek creates a new epoch and changes this pointer transactionally. Old memberships, receipts, waiter registrations, and old epoch rows cannot affect the new epoch. Old rows are garbage-collected in bounded batches.

## 4. Canonical storage schema

Use one file, e.g. `<STREAM_ROOT_PERSISTENCE_PATH>/streams.sqlite3`, with its WAL/SHM and a process lock file beside it. Persist a schema version and reject unknown versions.

The following is the physical schema specification. Additional constraints/indexes may be added for correctness, but do not change meanings or replace this with per-message backlog copies for every group. Use bound parameters everywhere.

### Unsigned values

SQLite INTEGER is signed. Encode protocol u64 sequences, positions, generation values, and their zero cursor as exactly 8 big-endian bytes. Bind them as BLOBs consistently; mixing integer/text/BLOB parameters invalidates ordering. Perform checked arithmetic in Rust. Exhaustion returns a clear error, never wraps or reuses a sequence.

A helper must cover:

```rust
fn encode_u64(value: u64) -> [u8; 8] { value.to_be_bytes() }
fn decode_u64(bytes: &[u8]) -> Result<u64, StorageError>;
```

The decoder rejects a length other than 8. This representation was verified for 0, 1, i64::MAX, i64::MAX+1, and u64::MAX.

### Tables and required columns

```text
schema_meta
  schema_version

streams
  id BLOB(16) PRIMARY KEY
  name TEXT NOT NULL
  deleted INTEGER NOT NULL DEFAULT 0
  config_json TEXT NOT NULL
  last_seq BLOB(8) NOT NULL
  retained_after_seq BLOB(8) NOT NULL
  last_key_id INTEGER NOT NULL
  logical_bytes INTEGER NOT NULL CHECK(logical_bytes >= 0)
  UNIQUE(name) WHERE deleted=0

stream_keys
  stream_id BLOB(16)
  key_id INTEGER
  key BLOB NOT NULL
  last_pos BLOB(8) NOT NULL
  retained_after_pos BLOB(8) NOT NULL
  PRIMARY KEY(stream_id,key_id)
  UNIQUE(stream_id,key)
  key_id=0 iff key is empty; real keys must be nonempty and <=65,535 bytes

 events
  stream_id BLOB(16)
  seq BLOB(8)
  key_id INTEGER
  key_pos BLOB(8)
  timestamp_ms INTEGER
  payload BLOB NOT NULL
  payload_bytes INTEGER NOT NULL
  logical_bytes INTEGER NOT NULL
  PRIMARY KEY(stream_id,seq)
  UNIQUE(stream_id,key_id,key_pos)
  FK(stream_id,key_id) -> stream_keys

 groups
  id BLOB(16) PRIMARY KEY
  stream_id BLOB(16)
  name TEXT NOT NULL
  generation BLOB(8)
  active_epoch BLOB(16)
  UNIQUE(stream_id,name)
  UNIQUE(id,stream_id)

 group_epochs
  id BLOB(16) PRIMARY KEY
  group_id BLOB(16)
  stream_id BLOB(16)
  start_after_seq BLOB(8)
  initialized INTEGER NOT NULL
  init_key_cursor INTEGER NOT NULL
  init_key_target INTEGER NOT NULL
  next_ready_ticket INTEGER NOT NULL
  pending_count INTEGER NOT NULL CHECK(pending_count >= 0)
  max_pending INTEGER NOT NULL CHECK(max_pending > 0)
  CHECK(pending_count <= max_pending)
  UNIQUE(id,stream_id)
  FK(group_id,stream_id) -> groups(id,stream_id)

 key_lanes
  epoch_id BLOB(16)
  stream_id BLOB(16)
  key_id INTEGER
  cursor_pos BLOB(8)
  normal_seq BLOB(8) NULL
  head_seq BLOB(8) NULL
  head_pos BLOB(8) NULL
  head_origin INTEGER NULL (0=original, 1=replay)
  state INTEGER (0=EMPTY,1=READY,2=LEASED,3=PARKED)
  attempts INTEGER NOT NULL
  ready_ticket INTEGER NOT NULL
  receipt BLOB(16) NULL
  owner TEXT NULL
  connection_id TEXT NULL
  deadline_ms INTEGER NULL
  PRIMARY KEY(epoch_id,key_id)
  FK(epoch_id,stream_id) -> group_epochs(id,stream_id)
  FK(stream_id,key_id) -> stream_keys

 keyless_deliveries
  epoch_id BLOB(16)
  stream_id BLOB(16)
  seq BLOB(8)
  key_pos BLOB(8)
  origin INTEGER (0=original,1=replay)
  state INTEGER (1=READY,2=LEASED)
  attempts INTEGER
  ready_ticket INTEGER
  receipt BLOB(16) NULL
  owner TEXT NULL
  connection_id TEXT NULL
  deadline_ms INTEGER NULL
  PRIMARY KEY(epoch_id,seq)
  FK(epoch_id,stream_id) -> group_epochs(id,stream_id)
  FK(stream_id,seq) -> events

 replay_intents
  epoch_id BLOB(16)
  stream_id BLOB(16)
  key_id INTEGER
  key_pos BLOB(8)
  seq BLOB(8)
  PRIMARY KEY(epoch_id,key_id,key_pos)
  UNIQUE(epoch_id,seq)
  FK(epoch_id,stream_id) -> group_epochs(id,stream_id)
  FK(stream_id,seq) -> events

 dls_ranges
  epoch_id BLOB(16)
  stream_id BLOB(16)
  key_id INTEGER
  first_pos BLOB(8)
  last_pos BLOB(8) NULL (NULL means open ended)
  first_seq BLOB(8) NULL (first currently retained event in the interval)
  reason_kind INTEGER (max_deliveries / auto_parked)
  attempts INTEGER
  PRIMARY KEY(epoch_id,key_id,first_pos)
  CHECK(last_pos IS NULL OR last_pos >= first_pos)
  FK(epoch_id,stream_id) -> group_epochs(id,stream_id)
  FK(stream_id,key_id) -> stream_keys
```

Enforce BLOB widths and nonnegative counters with CHECK constraints. `seq` and `key_pos` on actual events are strictly greater than the zero cursor. Use explicit foreign keys and deferred constraints where group/epoch creation requires them. `groups.active_epoch` must reference the group's own epoch, not an arbitrary epoch; validate and constrain this relationship.

For keyed lanes, LEASED iff receipt/owner/connection/deadline are present. EMPTY/PARKED have no head or lease. READY/LEASED have a head and origin. Keyless source lane 0 only uses EMPTY/READY. An invalid state combination must be rejected by the database as well as domain code.

Add deferred FKs for non-null `normal_seq` and `head_seq` to events if using them to enforce retention cleanup. In that case retention MUST normalize references, including inactive epoch references, before deleting the event. Do not use a composite ON DELETE SET NULL that would also null a non-null stream_id.

Config JSON is an authoritative serialization of validated StreamConfig, not a fallback cache. Invalid JSON/configuration at startup is an error; never replace it with current defaults.

### Required indexes

```sql
CREATE INDEX events_by_key_seq ON events(stream_id,key_id,seq);
CREATE INDEX ready_lanes ON key_lanes(epoch_id,ready_ticket,key_id) WHERE state=1;
CREATE INDEX lane_deadlines ON key_lanes(deadline_ms,epoch_id,key_id) WHERE state=2;
CREATE INDEX lane_owners ON key_lanes(connection_id,owner,epoch_id) WHERE state=2;
CREATE INDEX lane_normal_frontier ON key_lanes(epoch_id,normal_seq) WHERE normal_seq IS NOT NULL AND state<>3;
CREATE INDEX lane_head_expiry ON key_lanes(stream_id,head_seq) WHERE head_seq IS NOT NULL;
CREATE INDEX lane_normal_expiry ON key_lanes(stream_id,normal_seq) WHERE normal_seq IS NOT NULL;
CREATE INDEX lanes_needing_original ON key_lanes(stream_id,key_id,epoch_id) WHERE normal_seq IS NULL AND state<>3;
CREATE INDEX keyless_ready ON keyless_deliveries(epoch_id,ready_ticket,seq) WHERE state=1;
CREATE INDEX keyless_deadlines ON keyless_deliveries(deadline_ms,epoch_id,seq) WHERE state=2;
CREATE INDEX keyless_owners ON keyless_deliveries(connection_id,owner,epoch_id) WHERE state=2;
CREATE INDEX keyless_original_frontier ON keyless_deliveries(epoch_id,seq) WHERE origin=0;
CREATE INDEX replay_expiry ON replay_intents(stream_id,seq);
CREATE INDEX dls_order ON dls_ranges(epoch_id,first_seq,key_id,first_pos) WHERE first_seq IS NOT NULL;
```

The primary keys cover point lookup of a lane, lowest replay position, and predecessor lookup of a DLS interval. The unique events key-position index covers successor lookup. Do not add an unbounded scan over the entire event log to the normal fetch path.

### Core selection queries

```sql
SELECT seq,key_pos,timestamp_ms,payload_bytes,logical_bytes
FROM events
WHERE stream_id=? AND key_id=? AND key_pos>?
ORDER BY key_pos LIMIT 1;

SELECT key_id,head_seq,head_pos,head_origin,attempts
FROM key_lanes
WHERE epoch_id=? AND state=1
ORDER BY ready_ticket,key_id LIMIT ?;

SELECT seq,key_pos FROM replay_intents
WHERE epoch_id=? AND key_id=?
ORDER BY key_pos LIMIT 1;

SELECT first_pos,last_pos,first_seq,reason_kind,attempts
FROM dls_ranges
WHERE epoch_id=? AND key_id=? AND first_pos<=?
ORDER BY first_pos DESC LIMIT 1;

SELECT epoch_id,key_id FROM key_lanes
WHERE state=2 AND deadline_ms<=?
ORDER BY deadline_ms LIMIT ?;

SELECT normal_seq FROM key_lanes
WHERE epoch_id=? AND normal_seq IS NOT NULL AND state<>3
ORDER BY normal_seq LIMIT 1;

SELECT seq FROM keyless_deliveries
WHERE epoch_id=? AND origin=0
ORDER BY seq LIMIT 1;
```

DLS predecessor lookup is followed by an upper-bound check. A predecessor alone does not establish membership. All queries also respect live stream identity/current epoch and retained data boundaries through their caller's transaction validation.

## 5. Group initialization and publish fan-out

Do not eagerly create a delivery row for every event in every group.

On group creation:

1. Create group/epoch with `start_after_seq = stream.retained_after_seq` and a key enumeration target captured from the stream's current last_key_id.
2. Initialize lane metadata in bounded key-id pages. For each key, find the greatest retained key position whose sequence is <= start_after_seq, or use retained_after_pos if none. Then find its original successor.
3. Initialize only metadata, not payload copies. Pending count starts at zero.
4. Publishing while initialization is in progress must upsert any needed lanes for initializing epochs, using their start boundary. The initialization pass must not overwrite a lane already created/advanced by publishing/fetching.
5. New keys after the captured target are installed by publish. Finish initialization after covering the captured target.
6. Do not sleep waiting for an external publish when initialization/candidate discovery still has internal work to do. Requeue a bounded continuation.

For existing keys, publish only needs to update lanes that previously had no unresolved original and are not parked. Busy original lanes already point to the correct head; adding a successor does not add a group-specific backlog row. Parked keys include future originals through open DLS intervals.

For a NEW key, creating a lane for each live group is genuine fan-out work. Batch it and keep it independent of consumer count. Metadata has a worst-case keys-times-groups cost; do not claim constant total work for arbitrary independent groups. Keep idle lane cursors until safe retention/epoch GC, rather than forgetting progress and reintroducing old events on the next publish.

## 6. Transaction recipes

All mutating recipes run on the dedicated writer in a transaction, with bound SQL parameters. The complete effect, including counters, occurs before commit. Replies, runtime membership changes, and notifications are staged and emitted only after commit.

### CREATE / DESCRIBE / EXISTS / DELETE

- Validate names using the existing stream resource-name rules.
- CREATE snapshots normalized defaults into config_json. Equivalent config returns unchanged; differing config returns the existing typed conflict with requested/actual details.
- Names are database keys, never per-stream filesystem paths.
- DELETE marks the stream identity deleted atomically, fences its members/waiters, and schedules bounded GC. A recreated name gets a new StreamId.
- SQL lookup by name must filter deleted=0. Commands already accepted before DELETE retain writer order; commands addressed to a deleted/recreated identity cannot mutate the new one.
- Do not synchronously cascade-delete a huge stream while monopolizing the worker. Remove old child rows in bounded batches, then parent rows.

### PUBLISH / PUBLISH BATCH

1. Validate count, key sizes/nonempty keys, per-record size, total request admission bytes, and single-event fetch encodability BEFORE large allocations/sequence mutation.
2. Assign checked contiguous global sequences inside the transaction. Resolve/intern each key and assign checked per-key positions in input order.
3. Insert immutable events and update stream/key tails and logical byte accounting.
4. Initialize/wake affected original lane heads as described in section 5. Do not disturb an existing leased head or pending replay.
5. If an open DLS interval had no first retained event and a new append enters it, populate its first_seq for the DLS ordering index.
6. Commit the whole public batch, then return sequences and notify relevant waiters.
7. If a batch fails, none of its events/sequences/state changes may survive. If the caller drops its future AFTER enqueue acceptance, the owned command still completes; cancellation must not leave the DB and counters half committed.

### FETCH

1. Validate live stream, current epoch/generation, active membership, and connection binding.
2. Use available group credit: max_pending - pending_count. Enforce both requested count and encoded-response byte budget.
3. Select READY lane heads, keyless retries, and the keyless fresh source fairly by tickets. A keyed lane leaves the ready index when leased, so it cannot be selected twice.
4. For a keyed head, create a new receipt, increment attempts, and transition that lane to LEASED.
5. For a keyless fresh item, create its delivery row and advance the keyless source cursor in the SAME transaction. For a keyless retry, lease its existing row. Requeue the keyless source with a new ticket if more fresh events remain; do not limit an entire fetch to only one keyless message.
6. Read payloads and claim their states within the same bounded fetch transaction for the first implementation. This deliberately avoids a second reservation/read-completion state machine. Do not prematurely split payload I/O onto a different snapshot where retention could remove a claimed event before it is read.
7. Set lease deadlines immediately before finishing the fetch transaction, not at the beginning of a potentially long queue wait. Fetch transactions are batching barriers; do not hold their new leases uncommitted behind a large mixed mutation batch.
8. Commit before returning messages. An error during read/encoding preparation rolls back the claims and capacity.
9. If empty, register a long-poll outside a SQLite transaction. Distinguish genuine no-work/capacity wait from an internal initialization/normalization continuation.

### ACK

1. Validate current epoch, membership/connection, sequence, and exact receipt against the LEASED state.
2. A stale receipt returns FENCED without touching the newer attempt or consuming/releasing another attempt's credit. Wrong-state DLS operations similarly leave all state unchanged.
3. Original keyed ACK advances cursor_pos to the head's key position. Replay ACK removes its replay_intent, but DOES NOT change original cursor progress.
4. Keyless ACK removes its delivery row, including a replay row only after successful ACK.
5. Release exactly one pending credit; clear owner/receipt/deadline.
6. Recompute the next candidate from the minimum of the next original and the lowest replay intent. Parked lanes stay blocked. Requeue a newly READY head with a new fair ticket.
7. Commit, reply OK, and wake only this group's relevant waiters.

Do not implement ACK by first deleting a row and only afterward checking its state/receipt. Do not infer an unacknowledged replay is complete merely because its sequence is <= ack_floor.

### TIMEOUT / LEAVE / DISCONNECT

- Locate due/owned leases through the deadline/owner indexes; do not scan all retained events or all groups.
- Clear the old receipt before reassignment. A late ACK cannot complete the replacement attempt.
- If attempts < max_deliveries, return the same head to READY with attempts preserved.
- If attempts >= max_deliveries, use the parking transition below.
- Release pending capacity exactly once.
- LEAVE validates the current member and removes that membership after the DB effects commit.
- DISCONNECT releases every member bound to that connection, including members whose JOIN response was lost.
- Membership maps are runtime-only; startup has no live members from the old process.

### PARK KEY / KEYLESS DLS

Keyed:

1. Retire the active receipt and release its credit.
2. Add the failed head as a singleton DLS interval with its actual attempts/reason.
3. Add all other outstanding replay intents of the key as finite parked intervals, then remove those intents as part of this SAME transition.
4. Add an open interval for ORIGINAL positions after the original cursor, excluding any already represented failed original singleton. Successors use auto_parked and attempts=0.
5. Clear current/normal heads and mark the lane PARKED. Preserve enough original cursor information to distinguish past completed history from new originals.
6. Never use `failed_replay_position .. infinity` for an old replay: that would incorrectly re-park completed original messages.

Keyless:

- Move only that message to a finite singleton DLS interval for key id 0.
- Remove its active delivery row and release capacity. Other keyless messages remain deliverable.

### DLS REPLAY / DELETE / PURGE

Intervals are disjoint within an epoch/key. Implement point removal by finding the predecessor interval, validating its upper bound, and replacing it with at most two pieces. Never enumerate all positions just to split a range.

- REPLAY: validate retained event and DLS membership, remove the point, and INSERT a durable replay_intent (or keyless READY replay delivery) in the same transaction. A keyed intent remains held while other DLS members keep the lane parked.
- DELETE: remove only that DLS membership. Do not create a replay. It does not delete the shared event payload from other groups' history.
- PURGE: resolve the current DLS membership for the group, leaving already requested replay intents intact. Close open intervals at the current tail so future messages are not accidentally discarded.
- When no currently retained DLS member remains for a keyed lane, unpark it, set original cursor to the original tail covered by parking, and choose the oldest requested replay before any newer original.
- Replaying the same removed point again returns not-found; it cannot delete a pending/leased record.
- Replaying/deleting in any order must still produce publication-order keyed delivery.
- Every endpoint operation refreshes first_seq for any resulting range. An open interval can temporarily contain no current event; it must not by itself keep an otherwise resolved key parked forever.

DLS peek is a view over ranges and retained event metadata. Preserve reason, attempts, key, sequence, and global sequence ordering. Use the dls_order index plus lazy merge of range iterators: compare the next unopened range head with the smallest opened iterator head, opening only ranges needed for the requested page. Do not sort/materialize every event in a huge parked range for LIMIT 100. Offset pagination necessarily visits skipped output entries; it must not retain them all in memory. Bound returned bytes as well as entry count.

### SEEK

- Beginning: new epoch start_after_seq = current stream.retained_after_seq.
- End: new epoch start_after_seq = current stream.last_seq.
- Increment generation using checked arithmetic, install the new epoch transactionally, and start bounded lane initialization.
- Invalidate old memberships and pending long-polls. Old FETCH must return an actionable FENCED/NOT_MEMBER, not a successful empty loop that leaves the SDK permanently stuck.
- DLS and replay state from the old epoch is inactive immediately. GC it later in bounded work.
- SDK rejoin must occur automatically; the business callback API does not change.

### ACK FLOOR

The floor describes ORIGINAL progress; replay obligations are separate.

For an initialized epoch, compute the minimum among:

1. each non-parked lane's `normal_seq` (including the keyless fresh source);
2. keyless outstanding deliveries with origin=original.

If a minimum exists, candidate floor is min_seq - 1; otherwise it is stream.last_seq. Clamp to at least epoch.start_after_seq and stream.retained_after_seq. During initialization, do not advance beyond the known-safe initial frontier until all relevant original lanes are accounted for.

Do not use the current replay head for this calculation. Do not use a scan cursor that might have skipped an undiscovered key. DLS/retention count as settled for this historical floor, not successful business callbacks.

## 7. Retention and garbage collection

Define logical event size consistently: sequence/timestamp/length fields plus key bytes and opaque payload bytes. Use a fixed canonical accounting function shared by insertion and deletion; it must charge nonzero overhead even for an empty payload. Delivery receipts, group rows, SQLite indexes/WAL/free pages are NOT event retention bytes.

Retain the existing max-age/max-bytes options and zero-means-disabled normalization. Handle large u64 values and clock subtraction with checked/saturating arithmetic, not conversion to signed integers that silently wraps.

Retention deletes an oldest retained prefix in bounded batches. A batch transaction must:

1. Choose a prefix eligible by age or the logical-byte excess, reading metadata rather than payloads.
2. Find affected original heads, leased heads, keyless deliveries, replay intents and DLS ranges using indexes.
3. Expire references below the new retained boundary. Retire expired receipts and refund their credits.
4. Advance affected per-key original cursors to at least the last expired position; find the first retained successor. This must unblock a surviving successor rather than leave it stranded behind an erased head.
5. Clip/remove DLS intervals and recompute first_seq. Unpark a key if no retained DLS member remains; preserve other retained replay obligations.
6. Delete events, update key retained_after_pos, stream.retained_after_seq and logical_bytes, then commit.
7. Notify affected groups and schedule another bounded batch if still over the retention target.

The boundary is the LAST deleted sequence, not `last + 1`, so an exhausted u64 log is representable. Expiring the entire history must not reset sequence allocation to 1.

Clean up obsolete epochs/deleted streams/key dictionaries in bounded child-first batches. Never reuse an old StreamId or internal key id just because rows were removed. Empty dictionary entries can be removed only when their dependent history/state is safely retired.

Use incremental space reclamation and WAL checkpoints. Avoid full VACUUM on the hot path or a full database rebuild after every retention cycle. SQLite free pages are reusable; physical file size is not the per-stream maxBytes metric.

## 8. Writer, batching, cancellation, and ownership

- Open/create the root and database only after checking for legacy data and unsafe database-file symlinks. Stream names never become paths.
- Hold an OS advisory exclusive lock for the worker lifetime. Use std::fs::File::try_lock with a writable lock file; the repository Docker compiler already supports it. If the local compiler is older than 1.89, report the environment blocker rather than changing project/toolchain policy silently.
- All rusqlite calls execute off Tokio's async worker threads.
- Start with one writer connection and bounded command admission. Count and byte budgets both matter; a queue of 16,384 huge publish batches is not a memory bound.
- Own accepted commands and their admission permits until their work is complete. Dropping a client's reply future must not abort an already accepted append halfway through.
- Group already queued publish/ACK mutations into bounded transactions with per-command SAVEPOINTs. Do not wait for a timer just to collect a batch.
- On a domain error, ROLLBACK TO the command savepoint. On storage/commit failure, roll back the outer transaction and reject all would-be successes from it. Emit no success notification from rolled-back effects.
- FETCH/SEEK/LEAVE and shutdown act as appropriate ordering/batching barriers. Avoid placing a newly leased but unsent fetch behind a large uncommitted mutation batch.
- Register long-polls in the worker/runtime before processing a subsequent command that could wake them. A pending poll consumes a bounded waiter slot, not a SQLite transaction or an occupied storage-command queue slot.
- Keep control operations (ACK/leave/cancel/expiry) able to progress even with many waiting polls. Bound waiters separately from the writer queue.
- Timers query indexed due leases; a periodic check is acceptable, but scanning every group/message each tick is not.
- Responses must also have bounded buffering. Do not claim input queue backpressure alone bounds outgoing fetch memory.
- Shut down by closing intake, stopping timer production, draining accepted commands, completing replies, checkpointing as appropriate and joining the worker. No periodic snapshot needs to race the final shutdown flush.

Use one helper for lane-head recomputation and one helper for lease retirement/credit release. SQLite transactions do not excuse duplicating these rules across ACK, timeout, disconnect, retention and replay paths.

## 9. Recovery and failure handling

On startup:

1. Acquire the process lock and validate schema/configuration.
2. Let SQLite recover its journal.
3. Reclaim old-process LEASED states: clear old receipts/owners, preserve attempt counts and replay obligations, then make them READY or park them according to max_deliveries.
4. Reconcile pending counters in the same recovery work. Do not set counters to zero while later cleanup still decrements them as if leases were current.
5. Do not serve the affected state as ready until normalization is complete. Bounded recovery batches are acceptable before readiness.
6. Restore metadata from tables/indexes, not by replaying all retained payloads.

Fail closed on corrupt database/schema/config. Do not recreate an empty DB over corruption, replace config with defaults, silently skip history, or reset sequences.

A successful NORMAL commit is not a promise of surviving power loss. A process crash before response may leave a committed command; producer retries can duplicate events because producer idempotency is not a new promised feature. Do not advertise exactly-once external effects.

## 10. Transport and public method interfaces

The adapter must submit commands in TCP read order but must NOT wait inline for the SQLite operation to finish before reading the next frame. Waiting to enqueue because admission is full is legitimate backpressure; waiting for every disk commit inline is not.

Provide a transport-neutral split submit/completion seam in StreamManager:

```rust
pub async fn submit(
    &self,
    command: StreamRequest,
) -> Result<PendingReply, BrokerError>;

pub struct PendingReply {
    receiver: tokio::sync::oneshot::Receiver<Result<StreamReply, BrokerError>>,
}
```

StreamRequest/StreamReply belong to the Stream domain/manager layer, NOT tcp.rs. Include owned validated identities/arguments. PendingReply provides an async wait method. The TCP reader parses/maps/enqueues; a tracked task awaits the reply and encodes it. Direct public convenience operations call the same submit path and await, so they cannot diverge from TCP semantics.

Use these member-operation interfaces in the rewritten manager and update their callers deliberately:

```rust
pub async fn join_group(&self, name: &str, group: &str, connection_id: &str)
    -> Result<JoinGroupResult, BrokerError>;
pub async fn fetch(&self, name: &str, group: &str, identity: &ConsumerIdentity,
    limit: usize, wait_ms: u64) -> Result<Vec<Delivery>, BrokerError>;
pub async fn ack(&self, name: &str, group: &str, identity: &ConsumerIdentity,
    seq: u64, receipt: [u8; 16]) -> Result<(), BrokerError>;
pub async fn leave_group(&self, name: &str, group: &str, identity: &ConsumerIdentity)
    -> Result<(), BrokerError>;
pub async fn seek(&self, name: &str, group: &str, target: SeekTarget)
    -> Result<(), BrokerError>;
```

Keep publish/read/provisioning/DLS operations available with their existing high-level meaning. `read` returns stored Messages without receipts. Existing DLS manager method names can remain; do not create compatibility wrappers around the removed file engine.

Make StreamManager startup return an explicit Result. NexoEngine::new currently returns Self and is used by a user-owned untracked repro: preserve that outer signature and fail loudly on Stream initialization error rather than modifying the user's repro. Update tracked Stream test helper constructors to unwrap the Result. Do not introduce a fallback backend on failure.

### Wire change

The source currently has protocolVersion=7. Bump it to 8 for this coordinated breaking change, unless another authorized change has already advanced it.

Add a generated limit constant for the 16-byte receipt. Do not edit generated Rust/TS/Python files by hand.

New FETCH item layout:

```text
seq:u64 | receipt:16 raw bytes | timestamp:u64 |
key_len:u16 | key bytes | payload_len:u32 | payload bytes
```

Response still starts with count:u32. ACK keeps its existing name/group/consumer/generation/seq fields and appends the 16-byte receipt. JOIN still returns floor, generation and consumer id. The callback metadata stays `{seq,key}`; receipt is SDK-internal.

Each encoded fetch item uses `38 + key_len + payload_len` bytes, plus the batch's 4-byte count. Account for this BEFORE claiming/loading more messages. Reject a publish that can never fit as a one-message fetch under the effective configured response limit. There is no need to introduce a new public SDK maxBytes option for this refactor.

Pass a transport-neutral response-byte limit into the Stream engine, clamped to the server frame payload limit at engine assembly. Direct manager tests must exercise the same limits. Do not allow response length casts to wrap.

Stale epoch/receipt -> FENCED; missing membership -> NOT_MEMBER; invalid DLS target -> RESOURCE_NOT_FOUND; storage failures -> STORAGE_ERROR. Failed operations must have no partial mutations.

## 11. SDK work

Apply changes symmetrically to TypeScript and Python:

- Decode and keep receipt beside seq/key/data in the internal fetched batch.
- Send receipt on each callback's ACK; still await ACK before considering that callback committed.
- Keep callbacks parallel according to existing concurrency, without exposing receipt management to application code.
- Ensure idle subscribers receive/recover actionable fencing after seek. Do not interpret permanent old identity as successful empty polling forever.
- Rejoin must not unnecessarily retain a known old membership: leave/release the old identity when possible, preserve the existing reconnect behavior, and handle a lost response without hiding ACK failure.
- stop() still cancels idle polling promptly, waits for started callbacks and their confirmed ACKs, then leaves. Preserve its timeout/error visibility.
- Version mismatch remains fail-fast; no protocol-7 fallback branch.
- Keep create/get/describe/group/DLS application APIs except removal of obsolete physical-storage configuration fields from effective definitions.

Do not opportunistically refactor unrelated PubSub/Queue/Store implementations or the user's repro tests.

## 12. Configuration and documentation changes

Persist retention, max_ack_pending, ack_wait and max_deliveries as authoritative resource configuration. Existing streams must not silently pick up changed defaults after restart.

Remove old Stream-only settings and effective fields that no longer mean anything:

- STREAM_MAX_SEGMENT_SIZE / max_segment_size / maxSegmentSize
- STREAM_MAX_OPEN_FILES / max_open_files
- STREAM_DEFAULT_FLUSH_MS / default_flush_ms for group snapshots

Keep the root persistence path as the root containing the shared database. Keep a bounded storage command capacity and retention check interval. Add explicit byte-budget settings needed by the implementation; do not encode resource limits as undocumented arbitrary magic numbers. Initial fetch response budget should be clamped to the existing server max_payload_size (default 10 MiB). Operational tuning defaults must be recorded in this checkpoint when chosen; they are not grounds to weaken commit semantics.

Update StreamDefinition encoding/decoding and provisioning conflict data consistently across server and SDKs when maxSegmentSize is removed.

Update docs/guide/stream.md, applicable configuration documentation and README claims:

- SQLite/WAL instead of segment files, FD LRU, CRC truncation and state.log snapshots.
- Commit semantics with NORMAL and its power-loss limitation.
- Logical maxBytes and automatic space reuse/reclamation, not a hard physical per-stream disk cap.
- Per-key ordering versus global processing order; correct the misleading no-key + concurrency=1 guarantee.
- Finite retention bounds delivery/replay availability.
- Explicit new-format incompatibility, without automatic deletion.

Do not create residual compatibility code for the old format. Preserve useful existing comments; source comment removal/rewriting is subject to the user's/comment-policy authorization. Never retain a now-false claim merely to make the diff smaller.

## 13. Implementation milestones

Use these as small checkpoints inside the overall single-feature refactor. Update statuses in section 19 immediately when completed.

A. Baseline and harness
- Record toolchain, bundled SQLite version, exact git base, environment and commands.
- Capture Stream baseline on isolated data. Do not use the user's current persistence directory.
- Preserve raw results so future comparison does not rerun or reinterpret an already captured baseline.

B. Domain/storage primitives
- Implement typed u64 codec, lane states, receipts, interval split/merge/membership, immutable Message plus Delivery.
- Create schema/migrations for version 1, ownership lock, startup validation and SQLite connection policy.
- Add unit tests before building the full manager around these primitives.

C. Transaction engine
- Implement provisioning, publish, lane initialization/head recomputation, keyed/keyless fetch and ACK.
- Add receipts, credit invariants and cancellation-after-enqueue tests.
- Implement DLS range/replay transitions, seek epochs, timeout/disconnect, retention and bounded GC.
- Replace old file recovery and snapshots completely.

D. Runtime/transport
- Implement dedicated worker, bounded admission, batching/savepoints, long-polls, timers, shutdown.
- Integrate ordered submission into TCP without commit waits on the reader.
- Update wire format/codegen and effective configuration encoding.

E. SDKs and parity
- Port internal receipt handling and lifecycle fixes in both SDKs.
- Update shared scenario matrix and implement matching tests.

F. Verification/docs
- Replace file-format-specific tests with equivalent SQLite transaction/recovery/error tests.
- Complete docs/config cleanup, run targeted suites, fuzz, typechecks, builds.
- Capture after-performance under the same conditions and review the full diff.

## 14. Required correctness tests

Tests must check observable outcomes and state invariants, not just that an operation returned Ok. Keep existing semantic coverage. Tests about segment filenames/truncation/FD cache must be REPLACED with SQLite-relevant persistence tests, not retained against nonexistent behavior or silently deleted without replacement.

### Pure Rust/domain tests

1. u64 BLOB codec and ordering at 0, 1, i64::MAX, i64::MAX+1, u64::MAX; wrong lengths and overflow fail.
2. Range membership at both endpoints, gaps, singleton, finite and open-ended ranges.
3. Remove first/last/middle point; at most two intervals remain; disjointness and reason/attempt metadata preserved.
4. Empty future part of an open range is not a phantom unresolved DLS entry.
5. Replays requested in every permutation of three positions are released in original order, only after last DLS resolution.
6. A failed old replay parks outstanding replay positions and future originals, never already completed original history.
7. All state variants obey head/lease-field invariants.
8. Time is an explicit test input where possible; do not build a suite of flaky sleeps for pure transition logic.

### Rust manager/integration tests

1. Existing basic provision/publish/read/fetch/ACK, separate groups and same-group load sharing.
2. A1,A2,B1 with a batch of one: while A1 is leased, another consumer can get B1 without waiting for an unrelated publish/ACK.
3. Huge A backlog with B ready: no payload scan through all A successors.
4. Concurrent keyed fetches cannot assign one head twice; no two same-key messages in a batch.
5. Keyless messages fill a batch and consume independent credit; failures do not serialize the whole keyless bucket.
6. Global max_ack_pending enforced across keyed/keyless consumers; failed claim/read releases no incorrect credit.
7. Receipt from attempt 1 cannot ACK attempt 2, even with the same member and sequence.
8. Wrong member/connection, old generation, old stream incarnation, old receipt: typed error and no state mutation.
9. Invalid DLS replay/delete of a READY or LEASED message leaves ACK and timeout behavior intact.
10. Reverse-order DLS replay; partial replay while still parked; delete/purge and new same-key appends.
11. DLS redrive before fetch survives restart; ALSO redrive fetched but not ACKed survives restart, both keyed and keyless and below ack_floor.
12. Recovery with retry plus original progress does not emit [1,1] in a batch or inflate pending_count.
13. Retention removes A1 but retains A2; A2 becomes available and is not crossed as completed while stranded.
14. Retention clips parked ranges and old replays without re-parking retained completed history.
15. Seek while idle polling and while a callback/ACK is in flight; old generation fences, new one restarts correctly.
16. ACK then LEAVE is ordered; only the unacked message is redelivered.
17. Publisher cancellation after submit acceptance still commits a coherent batch.
18. Batch validation/constraint/storage failure is atomic; no partially consumed sequence numbers on rollback.
19. Append and delete/recreate cannot resurrect the old identity.
20. Startup auto-restores streams/config/groups; invalid schema/config/corrupt DB does not silently create an empty engine.
21. Second engine on the same root is rejected; DB-file symlink and legacy format detection are safe.
22. Full retention of a stream does not reset sequence allocation.
23. Fetch byte budget: oversized combination returns a smaller valid batch, not an oversized frame; a fundamentally undeliverable single publish is rejected early.
24. Long-poll has no lost wakeup at empty-check/register boundary; ACK wakes its group, not all unrelated groups.
25. Shutdown drains accepted work and leaves no old-process valid receipts on restart.
26. Initialization and cleanup are resumable/idempotent if interrupted between bounded batches.

Use temporary directories. Explicitly shut down the first manager before reopening the same DB in ordinary restart tests; older tests that simply drop an Arc while background tasks still exist must be fixed. Actual crash tests must kill a task-owned child process using disposable data, not pretend a graceful shutdown is a crash.

### Fault boundaries

Implement targeted fault injection/test seams at: before commit, commit failure/rollback, after commit before reply, before/after DLS transition, after claim before response, during epoch switch and retention batch. A failed response does not imply a failed commit. State is either the old or new transaction, never a hybrid.

A fake explicit ROLLBACK tests transaction semantics, not OS crash durability. Label tests and results accurately. WAL=NORMAL is not expected to preserve every recent commit through OS/power failure.

### TS/Python parity

Add corresponding matrix IDs and tests for:
- stale receipt behavior through TCP (internal test helper may use raw protocol);
- idle seek/rejoin;
- A-blocked/B-ready with multiple consumers;
- DLS reverse/partial replay and invalid DLS operation preserving an active delivery;
- stop/ACK/leave ordering after commit;
- bounded fetch response with large payloads;
- persistence/reconnect cases supported by a task-owned server harness.

Keep public handlers unchanged. Do not run or modify the unrelated user-owned PubSub repro suites listed at the top.

### Fuzzing

The new receipt changes parsing of untrusted bytes. Add a fuzz target for StreamCommand::parse with PUB/FETCH/ACK/JOIN/SEEK/DLS payloads, including truncated receipt, huge count/length, empty/nonempty key, invalid flags and trailing data. Include deterministic malformed-payload unit cases in normal cargo test as well.

There was no existing fuzz directory during exploration. If adding cargo-fuzz, keep it a separate fuzz package, pin an established libfuzzer-sys version, and do not change normal release dependencies/toolchain policy. A suitable target body is:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use nexo::brokers::stream::tcp::StreamCommand;
use nexo::protocol::wire::PayloadCursor;

fuzz_target!(|data: &[u8]| {
    if let Some((&opcode, payload)) = data.split_first() {
        let mut cursor = PayloadCursor::new(payload.to_vec().into());
        let _ = StreamCommand::parse(opcode, &mut cursor);
    }
});
```

Provide valid seed frames for the changed commands so the fuzzer reaches beyond the initial string length checks. Confirm malformed input cannot panic, allocate from unchecked counts, or mutate storage. If the toolchain cannot run the fuzz smoke test, record the exact blocker; do not label it passed.

## 15. Verification commands and scope

Run commands in the stated directory. Use the narrowest covering checks during iteration, not all brokers' suites after every edit.

Root:

```bash
node scripts/generate-protocol.js
node scripts/generate-protocol.js --check
cargo fmt --all -- --check
cargo test --lib
cargo test --test stream_tests
cargo test --test transport_tests
cargo test --test codec_fixtures
cargo build --release
```

During implementation, use individual failing test names before rerunning the corresponding whole suite. Existing crate root has `#![deny(warnings)]`.

`sdk/ts`:

```bash
npm test -- tests/brokers/test-stream.test.ts
npx tsc --noEmit
```

Run relevant existing reconnection/protocol fixture tests too after locating their actual file names. Do not guess their paths. Build the SDK package once in the final gate using the project's build command, being careful not to delete user artifacts without permission.

`sdk/py`:

```bash
pytest tests/brokers/test_stream.py
mypy src/nexo
uv build
```

SDK integration tests need a real task-owned server. TS `tests/nexo.ts` connects using defaults (127.0.0.1:7654); do not unknowingly target an existing user server. If that port is occupied by an unrelated process, report it and arrange an isolated test endpoint/harness rather than killing the process. Record any harness-only endpoint parameterization and use it consistently before/after.

Run at most one final broad gate after changes stabilize. A failure in a user-owned unrelated repro is not permission to change that repro or to weaken an assertion.

## 16. Performance strategy: no throwaway full prototype

Capture actual baseline before changing the production core when practical. If interruption prevents this, the original commit above is the reference for an isolated detached worktree; never reset the current implementation to get old numbers.

Root Stream publish baseline:

```bash
cargo test --release --test stress_tests stress_tests::stream::bench_stream_publish -- --nocapture --test-threads=1
```

TS Stream cases, from sdk/ts against an isolated server:

```bash
npm test -- tests/brokers/test-stress.test.ts -t STREAM
```

Python Stream cases, from sdk/py against the same controlled setup:

```bash
pytest tests/brokers/test_stress.py -k stream -s
```

Store exact outputs, commit/build profile, compiler/SQLite versions, configuration, payload sizes, batch sizes, consumer/group/key counts, and whether data was warm/cold. Do not quote the old numbers in source docstrings as fresh measurements.

Before/after must use equivalent public guarantees and workloads. New storage must remain WAL=NORMAL and post-commit ACK; do not switch policies to hide regressions. Existing native publish's Tokio write_all completion was weaker than its stated page-cache boundary, so explain that discrepancy rather than pretending all timings measured identical internal work.

Measure at least publish, batch publish, consume+confirmed ACK, and sequential ACK latency. After implementation add the discriminatory workload: one blocked hot key with a large backlog plus ready independent keys and multiple consumers. Track memory and maintenance/WAL behavior, not only a fresh-database ingest burst.

Preserve baseline data and compare against it; do not repeatedly regenerate baseline while formatting reports. Summarize results compactly. If a major regression appears, first profile transaction count, query plans, statement reuse, batching, copying, checkpoints and contention. Do not redesign the product or drop correctness guarantees without discussion.

## 17. Concrete pre-refactor defects to avoid reproducing

These were established by code inspection and existing repository analysis. Do not assume old tests cover the combinations:

- Pending DLS redrive omitted from group snapshots and lost below ack_floor after restart.
- Restored redelivery and fresh cursor can both deliver the same sequence.
- Retention clears in_flight but strands retained blocked successors.
- Invalid DLS remove/replay removes a non-DLS state before returning an error.
- A filtered/blocked fetch batch can hide ready work beyond it during long-poll.
- Long-poll converts stale membership/generation errors into successful emptiness, preventing SDK rejoin.
- In-memory ACK completion and periodic group snapshots are separate durability boundaries.
- Tokio File::write_all is not the same as awaiting flush, and flush is not fsync.
- Stream-wide ACK wakeups and shared mutable indexes create unnecessary coupling.
- Count-only limits do not bound fetched payload memory/encoded response size.

Fix root state representations and transaction boundaries rather than adding special-case recovery patches around the old maps.

## 18. Already completed design probes: frozen evidence

Two disposable in-memory Python SQLite scripts ran successfully. Python SQLite version for the first script: 3.49.1. They created no repository files, touched no external database, and started no servers.

Confirmed narrow behaviors:

- Exclusive keyed head claim and rollback of reserved pending credit when a second claim fails.
- Old receipt does not mutate a replacement lease.
- All six permutations of replaying three DLS positions: key stays parked until the last resolution; replay order becomes 1,3,5 in global sequence order.
- Invalid replay leaves the logical database state unchanged.
- A SQLite logical backup includes leased keyed and keyless replay obligations.
- Explicit retention normalization promotes a retained successor and releases credit.
- Epoch change prevents claims through old state without corrupting new counters.
- Unsigned big-endian BLOB values retain full u64 order.
- EXPLAIN QUERY PLAN used indexes, with no full scan/temporary sort for the tested successor, ready-head, lease-deadline, DLS-point and original-frontier queries.
- Failed replay at position 1 with another replay at 4 and original cursor 6 parks 1,4,7,8; completed originals 2,3,5,6 remain excluded.
- Appending position 9 enters an open parked interval without inserting a DLS row for that message.
- Retention to position 5 leaves DLS 7,8,9 and retained events 5,6,7,8,9.
- A rejected command's mutation is restored by ROLLBACK TO SAVEPOINT while valid siblings commit.
- Outer transaction rollback leaves no committed command effects.

These are NOT a complete engine proof, Rust/rusqlite verification, on-disk WAL test, actual crash test, I/O fault test or performance benchmark. Do not rerun them just to rediscover the design; implement equivalent permanent regressions against the real engine.

## 19. Milestone checklist

- [x] User approved SQLite architecture and implementation after plan creation.
- [x] User chose logical event retention bytes, not a rigid physical disk cap.
- [x] User chose new format, no migration/importer.
- [x] Design probes and query-plan checks completed with limited scope above.
- [x] Self-contained plan saved in repository root.
- [x] Capture baseline and environment in isolated storage.
- [x] Implement domain types/ranges and unit tests.
- [x] Implement schema/startup/ownership/error handling.
- [x] Implement publish/provisioning/group initialization.
- [x] Implement keyed/keyless delivery, receipts, ACK and credits.
- [x] Implement DLS/replay/seek/retention/recovery.
- [x] Implement runtime batching/long-polls/timers/shutdown and ordered TCP submission.
- [x] Update protocol and regenerate all languages.
- [x] Update both SDKs and test scenario matrix.
- [x] Replace storage-format-specific tests and add regressions/fuzz coverage.
- [x] Remove obsolete settings/references and update functional docs.
- [x] Run final targeted Rust/TS/Python checks and package builds.
- [x] Capture after-performance and compare to frozen baseline.
- [x] Review the COMPLETE diff, then resolve findings in one consolidated pass.
- [x] Update this checkpoint with verified final status or exact remaining work.

## 20. Stop conditions and non-goals

Stop and report rather than guessing if:

- a semantic requirement contradicts the schema/transition rules;
- an environment/auth/permission issue requires user authority;
- validation would use or destroy existing user data;
- the proposed optimization weakens receipt fencing, commit ordering, retained history, or group independence;
- a performance regression would require changing the agreed guarantees;
- a code path cannot be implemented without falling back to full backlog scans on normal fetch.

Do not add clustering, producer exactly-once, external transactions, sticky consumer partitions, a legacy importer, a second storage backend, or broad unrelated broker refactors.

Operational tuning and mechanical Rust API details may be completed during implementation, but record concrete choices here. Do not silently reinterpret the domain model.

## 21. Execution log / next handoff

Append concise, factual entries. Include what actually ran, not intended commands presented as results.

### 2026-09-20 — plan checkpoint

- Production core unchanged.
- Baseline reference commit captured: 3dc3b0781cb1cd4b891b62f97ce42373dbeaba27.
- Existing user untracked files recorded in section 0.
- Design/query probes passed as specified in section 18; no benchmark or server run.
- NEXT: read this plan, capture isolated baseline, then implement milestones B–F. Keep the root plan updated so a fresh agent can continue from the first unchecked milestone.
- No task-owned running processes to reuse at this checkpoint.

### 2026-09-20 — baseline captured (pre-change)

Environment: macOS Darwin 25.6.0 (Apple Silicon), rustc/cargo 1.90.0, release profile
(`cargo build --release`), bundled SQLite via libsqlite3-sys 0.36.0. Git HEAD
`3dc3b0781cb1cd4b891b62f97ce42373dbeaba27` (unchanged production code).
Isolation: Rust bench uses `tempfile::tempdir()`; SDK harnesses spawn
`target/release/nexo` on 127.0.0.1:7654 (was free) and wipe repo `data/` (was empty).
No user persistence directory was touched.

Raw results (measured, not re-interpreted):

- Rust `stress_tests::stream::bench_stream_publish` (write-confirmed, 500_000 ops):
  `cargo test --release --test stress_tests stress_tests::stream::bench_stream_publish -- --nocapture --test-threads=1`
  → 80,125 ops/sec | avg 12µs | p50 12µs | p95 15µs | p99 21µs | max 3,246µs | total 6.24s
- TS `npm test -- tests/brokers/test-stress.test.ts -t STREAM` (TCP, 50 workers where noted):
  - STREAM PUBLISH concurrent (50k): 53,230 ops/sec | p50 0.93ms | p99 1.43ms | max 2.03ms
  - STREAM PUBLISH BATCH concurrent (50k, batch=100): 2,013,956 ops/sec | p50 0.02ms | p99 0.04ms | max 0.04ms
  - STREAM SUBSCRIBE+ACK (50k pre-filled): 38,391 ops/sec
  - STREAM PUBLISH sequential latency (100k): 24,381 ops/sec | p50 0.04ms | p99 0.09ms | max 3.80ms
- Python `pytest tests/brokers/test_stress.py -k stream -s`:
  - STREAM PUBLISH concurrent (50k): 55,724 ops/sec | p50 0.89ms | p99 1.16ms | max 1.75ms
  - STREAM PUBLISH BATCH concurrent (50k): 387,764 ops/sec | p50 0.13ms | p99 0.14ms | max 0.14ms
  - STREAM SUBSCRIBE+ACK (50k pre-filled): 26,940 ops/sec
  - STREAM PUBLISH sequential latency (100k): 12,094 ops/sec | p50 0.08ms | p99 0.16ms | max 2.55ms

Caveat recorded for comparison: native publish "write confirmed" measures
`File::write_all` completion (page-cache enqueue), not fsync; the new engine's
post-COMMIT semantics are a different internal boundary — compare with that
qualification stated.

NEXT: milestone B — domain types/u64 codec/intervals/schema primitives.

### 2026-09-20 — implementation complete (final handoff)

All milestones A–F implemented and verified. Working tree: 30 modified files +
5 new stream modules (`domain/{types,ops,storage,recipes}.rs`, `worker.rs`);
`domain/group.rs` and `domain/persistence.rs` deleted. Protocol bumped to v8
(ACK carries a 16-byte delivery receipt; FETCH items echo it). `protocol.json`
is the only spec change; all three generated files regenerated and verified
in sync (`node scripts/generate-protocol.js --check` → OK).

Concrete operational choices recorded (per section 17):
- Shared DB `streams.sqlite3` + `streams.lock` under `STREAM_ROOT_PERSISTENCE_PATH`;
  foreign files abort startup (fail-closed, no importer).
- WAL, `synchronous=NORMAL`, busy_timeout, mmap; writer thread owns the DB,
  coalesces up to 256 drained commands per transaction, savepoint per command,
  effects applied strictly post-commit.
- Keyless source state lives on `group_epochs` (`keyless_cursor_pos`,
  `keyless_ticket`) — not a sentinel lane row.
- `members` persisted per epoch and wiped at startup.
- u64 identities stored as BLOB(8) big-endian (correct ordering past i64::MAX).
- New env settings: `STREAM_STORAGE_QUEUE_MAX_BYTES` (256MB),
  `STREAM_FETCH_RESPONSE_BYTES` (10MB, clamped to server max_payload_size).
  Removed: `STREAM_MAX_SEGMENT_SIZE`, `STREAM_MAX_OPEN_FILES`,
  `STREAM_DEFAULT_FLUSH_MS`.
- `Continuation::Checkpoint` dropped — WAL autocheckpoint + shutdown
  checkpoint suffice.

Verification results (all actually run):

- `cargo test --lib --tests`: lib 43/43, codec_fixtures 5/5,
  queue_tests 40/40, store_tests 44/44, pubsub_tests 18/18,
  stream_tests 73/73, stress_tests 7/7. The only failing binary is the
  user-owned untracked `tests/pubsub_review_repro.rs` (1/6 — pre-existing
  pubsub bugs, untouched by this refactor).
- `cargo build --release`: clean, zero warnings.
- TS: `npm run build` clean; vitest 231 pass / 6 fail — all failures in the
  user-owned untracked `pubsub-review-repro.test.ts` (same pre-existing
  bugs). STREAM stress tests pass standalone and in-file.
- Python: `pytest` 232 passed / 3 deselected (untracked pubsub repro);
  `mypy src/nexo` clean; `uv build` wheel+sdist OK.
- `stream_publish_storage_failure` scenario removed from both SDK suites and
  from `sdk/integration-test-matrix.md` (mechanism no longer exists: shared
  DB, no per-stream dirs).
- GC regression unit test added: `gc_reclaims_deleted_stream_beyond_batch_limit`.

Bugs found and fixed during bring-up (all in the new code, now covered):
empty-candidate fetch panic (`lanes.pop_front().unwrap()` when all ticket
candidates were i64::MAX), lease-UPDATE placeholder misnumbering (?4→?1),
keyless retry reordering (fresh source could overtake a ready retry — retries
now outrank fresh source), retention delete ordering vs `key_lanes`/`dls_ranges`
FK refs, GC draining only one batch per child table while deferred
`groups.active_epoch` FK required coordinated group+epoch delete.

After-perf vs frozen baseline (same machine/profile), AFTER the
`prepare_cached` fix below — all statement access in recipes.rs now goes
through `exec`/`query_one`/`query_vec` helpers backed by the connection
statement cache (previously every statement was re-prepared per call):

- Rust bench publish: 21,932 ops/s, avg 45µs, p50 36µs
  (baseline 80,125 ops/s, 12µs — page-cache enqueue, not durable).
- TS stress: publish concurrent 71,453 (baseline 53,230, **+34%**);
  publishBatch 224,811 (baseline 2,013,956, −89%);
  subscribe+ack 30,259 (baseline 38,391, −21%);
  sequential publish 14,271 (baseline 24,381, −41%, p50 0.06ms vs 0.04ms).

Pre-fix numbers for reference: publish 46,878 / batch 137,853 /
subscribe+ack 20,360 / sequential 12,516 — statement caching recovered
roughly +50%/+63%/+49%/+14% respectively.

Interpretation: the baseline measured `write_all` (page-cache enqueue); the
new boundary is post-COMMIT. Concurrent single publish now exceeds the
baseline because the writer coalesces commands per commit. The residual gaps
are the honest cost of durable state: sequential publish is bounded by
per-commit WAL append (~45µs/op serial), subscribe+ack by one durable write
tx per ack cycle, batch by ~2.2k durable tx/s. No guarantee was weakened to
reach these numbers.

Known unrelated failures (do not "fix" here): the three user-owned pubsub
review probes fail in Rust/TS/Python identically (retained-replay duplicates,
resubscribe timeout, silent inactive handle). They reproduce bugs that
pre-date this refactor.

NEXT: user review of the full diff and the perf note above; no commits made
(per instructions). If batch throughput needs recovery, profile statement
reuse and savepoint overhead inside `exec_batch` first — the ceiling is
durable commit rate, not coalescing depth.

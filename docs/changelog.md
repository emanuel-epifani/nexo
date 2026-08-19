---
title: Changelog
description: Release notes and download links for every Nexo version.
outline:
  level: [2, 2]
---

# Changelog

Release notes for the Nexo broker and SDKs.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## v5.0.2

**Released:** 2026-08-19

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v5.0.2" target="_blank" rel="noreferrer">Download v5.0.2</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=5.0.2" target="_blank" rel="noreferrer">Docker tag v5.0.2</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/5.0.2" target="_blank" rel="noreferrer">npm v5.0.2</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/5.0.2/" target="_blank" rel="noreferrer">PyPI v5.0.2</a>
</p>

### Changed

- Centralize protocol constants in protocol.json and extract protocol module from transport

## v5.0.1

**Released:** 2026-07-29

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v5.0.1" target="_blank" rel="noreferrer">Download v5.0.1</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=5.0.1" target="_blank" rel="noreferrer">Docker tag v5.0.1</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/5.0.1" target="_blank" rel="noreferrer">npm v5.0.1</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/5.0.1/" target="_blank" rel="noreferrer">PyPI v5.0.1</a>
</p>

### Changed

- Priority-queue per ConsumerGroup deadlines, rimosso delivered_at
- Replace BTreeMap+LinkedHashSet with priority-queue

### Removed

- Remove examples/ and .idea/ from git tracking

## v5.0.0

**Released:** 2026-07-27

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v5.0.0" target="_blank" rel="noreferrer">Download v5.0.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=5.0.0" target="_blank" rel="noreferrer">Docker tag v5.0.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/5.0.0" target="_blank" rel="noreferrer">npm v5.0.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/5.0.0/" target="_blank" rel="noreferrer">PyPI v5.0.0</a>
</p>

### Added

- Bounded per-subscriber channel with slow-consumer disconnect
- Replace unbounded channel with bounded channel for backpressure
- Delivery token per ACK/NACK to prevent stale delivery races

### Changed

- Inline store CRUD and pubsub SUB/UNSUB; drop queue ACK/NACK from inline set
- QUE-015 rename maxRetries to maxDeliveries across all layers

### Fixed

- QUE-014 stop() cancels in-flight long-poll and drains with timeout
- QUE-017 retry shutdown flush up to 5x1s to prevent silent data loss
- QUE-011 fail-fast on recovery error prevents silent data loss
- QUE-006 cap push count before allocation to prevent OOM
- QUE-003 idempotent MoveToDLQ/MoveToMain + discard batch on fatal constraint errors
- QUE-007 persist ready_seq and dlq_seq in push/MoveTo* operations
- QUE-001 delivery token in-flight check and requeue reset
- QUE-008 lifecycle mutex for create/delete serialization
- QUE-007 persistent FIFO ordering with ready_seq and dlq_seq
- QUE-005 fail-fast on storage init errors
- QUE-002 align RAM and storage operation order
- QUE-003 rollback entire batch on first SQLite error
- QUE-004 validate queue names to prevent path traversal
- Inline ACK/LEAVE/SEEK/JOIN to preserve TCP arrival order (STR-009)

## v4.1.5

**Released:** 2026-07-26

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.1.5" target="_blank" rel="noreferrer">Download v4.1.5</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.1.5" target="_blank" rel="noreferrer">Docker tag v4.1.5</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.1.5" target="_blank" rel="noreferrer">npm v4.1.5</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.1.5/" target="_blank" rel="noreferrer">PyPI v4.1.5</a>
</p>

### Changed

- Propose architectural changes
- Propose architectural changes

### Fixed

- Await individual consumer acknowledgements
- Harden recovery and consumer commits
- Confirm writes before publish ack

## v4.1.4

**Released:** 2026-07-24

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.1.4" target="_blank" rel="noreferrer">Download v4.1.4</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.1.4" target="_blank" rel="noreferrer">Docker tag v4.1.4</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.1.4" target="_blank" rel="noreferrer">npm v4.1.4</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.1.4/" target="_blank" rel="noreferrer">PyPI v4.1.4</a>
</p>

### Added

- Add clearAll + clearWithPrefix, revert inline store ops
- Add INT data type (0x03) for typed integer storage
- Add INCR command with signed i64 wire support

### Changed

- Unify ConsumerGroup state into MsgState + KeyState
- Revert "perf(pubsub): reduce write lock scope for subscribe and disconnect"
- Remove MessageState enum and unify StorageOp batch variants

### Fixed

- Remove unused parameter in mapClearAll

### Performance

- Reduce write lock scope for subscribe and disconnect
- Parallelize ReadRange via tokio::spawn
- Use Arc&lt;str&gt; for session_id to eliminate per-request heap allocation

## v4.1.3

**Released:** 2026-07-24

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.1.3" target="_blank" rel="noreferrer">Download v4.1.3</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.1.3" target="_blank" rel="noreferrer">Docker tag v4.1.3</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.1.3" target="_blank" rel="noreferrer">npm v4.1.3</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.1.3/" target="_blank" rel="noreferrer">PyPI v4.1.3</a>
</p>

### Performance

- Inline fast-path for store ops, avoid tokio task spawn

## v4.1.2

**Released:** 2026-07-23

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.1.2" target="_blank" rel="noreferrer">Download v4.1.2</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.1.2" target="_blank" rel="noreferrer">Docker tag v4.1.2</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.1.2" target="_blank" rel="noreferrer">npm v4.1.2</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.1.2/" target="_blank" rel="noreferrer">PyPI v4.1.2</a>
</p>

### Changed

- Change 1-global timeout with per-request tiemout
- refactor(readme)

## v4.1.1

**Released:** 2026-07-23

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.1.1" target="_blank" rel="noreferrer">Download v4.1.1</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.1.1" target="_blank" rel="noreferrer">Docker tag v4.1.1</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.1.1" target="_blank" rel="noreferrer">npm v4.1.1</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.1.1/" target="_blank" rel="noreferrer">PyPI v4.1.1</a>
</p>

### Added

- Introduce Subscription class for uniform subscription handling

### Changed

- Update readme

## v4.1.0

**Released:** 2026-07-23

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.1.0" target="_blank" rel="noreferrer">Download v4.1.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.1.0" target="_blank" rel="noreferrer">Docker tag v4.1.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.1.0" target="_blank" rel="noreferrer">npm v4.1.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.1.0/" target="_blank" rel="noreferrer">PyPI v4.1.0</a>
</p>

### Added

- Add Python SDK publishing to PyPI in release workflow
- Added python sdk

### Changed

- Add generics to py sdk
- Add non blocking event-loop to avoid blocking read from socket
- Graceful shutdown, config cleanup, dead code removal

## v4.0.3

**Released:** 2026-07-19

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.0.3" target="_blank" rel="noreferrer">Download v4.0.3</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.0.3" target="_blank" rel="noreferrer">Docker tag v4.0.3</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.0.3" target="_blank" rel="noreferrer">npm v4.0.3</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.0.3/" target="_blank" rel="noreferrer">PyPI v4.0.3</a>
</p>

### Changed

- BufReader once per segment in read_range, fix docs env vars
- Clean up read_range and add fd cache regression test
- Simplify to single Mutex&lt;TopicState&gt;
- Remove lock_topic wrapper, use .lock() directly
- Optimize ConsumerGroup data structures for O(log n) hot paths
- Unified stress tests of all brokers

### Fixed

- Move next_seq to TopicState, fix parse_message truncation, split ReadOutcome
- Fix 6 issues (P1-P6) in storage and manager
- Fix 5 bugs from review

### Performance

- Unify fd cache for read and write in StorageManager
- Batch Insert/UpdateState StorageOps with single-message fallback
- Replace standard Mutex with parking_lot Mutex for improved performance

## v4.0.2

**Released:** 2026-07-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.0.2" target="_blank" rel="noreferrer">Download v4.0.2</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.0.2" target="_blank" rel="noreferrer">Docker tag v4.0.2</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.0.2" target="_blank" rel="noreferrer">npm v4.0.2</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.0.2/" target="_blank" rel="noreferrer">PyPI v4.0.2</a>
</p>

### Changed

- Bump stream eviction_interval_ms to 10s and ram_soft_limit to 10000

### Fixed

- WAL checkpoint on shutdown, persist failure_reason in main queue, remove no-op page_size pragma

## v4.0.1

**Released:** 2026-07-17

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.0.1" target="_blank" rel="noreferrer">Download v4.0.1</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.0.1" target="_blank" rel="noreferrer">Docker tag v4.0.1</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.0.1" target="_blank" rel="noreferrer">npm v4.0.1</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.0.1/" target="_blank" rel="noreferrer">PyPI v4.0.1</a>
</p>

### Changed

- Remove legacy groups.log compat, extract parse_message
- Simplify MessageState by removing timestamp from InFlight
- Eliminate MessageToAppend, use Message directly
- Extract recompute_earliest_deadline helper

### Performance

- Skip O(n) pending scan in check_redelivery via earliest_deadline
- Add early return in clamp_head when head_seq unchanged

## v4.0.0

**Released:** 2026-07-14

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v4.0.0" target="_blank" rel="noreferrer">Download v4.0.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=4.0.0" target="_blank" rel="noreferrer">Docker tag v4.0.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/4.0.0" target="_blank" rel="noreferrer">npm v4.0.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/4.0.0/" target="_blank" rel="noreferrer">PyPI v4.0.0</a>
</p>

### Changed

- Extract header constants to protocol.ts

### Fixed

- Queue stop() returns immediately and clean stale comment
- Per-client signal listeners and remove process.exit
- Reject pending requests on disconnect and clean test setup
- Handle Uint8Array and ArrayBuffer as raw bytes in any() and anyWithLen()

### Performance

- Reuse TextEncoder singleton and eliminate Buffer.from wrapper in bytes()
- Read frame header directly from buffer in handleFrame
- Replace Array.shift() with index counter in runConcurrent
- Avoid per-message Cursor allocation in consume/fetch paths
- Eliminate double JSON serialization in queue/stream push

## v3.0.0

**Released:** 2026-07-13

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v3.0.0" target="_blank" rel="noreferrer">Download v3.0.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=3.0.0" target="_blank" rel="noreferrer">Docker tag v3.0.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/3.0.0" target="_blank" rel="noreferrer">npm v3.0.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/3.0.0/" target="_blank" rel="noreferrer">PyPI v3.0.0</a>
</p>

### Added

- Batch push for queue and stream publish
- Add REQUEST_NO_RESPONSE frame type for fire-and-forget commands
- Add clear functionality for retained messages and enforce TTL validation
- Enhance configuration management with per-topic settings and environment variable support
- Add Dead Letter Topic (DLT) with persistence and auto-unblock
- Add per-key delivery ordering

### Changed

- Add nested StoreCommand per data structure, remove
- Remove dead is_earliest/any_earliest code from pop/take_batch
- Nested enum in store tcp, rename MapStore→Map, remove dead snapshot system
- Enforce module encapsulation and deny warnings
- Subscribe/publish return Result, make validate_* private
- Remove dead code, simplify ClientId, and fix is_expired
- Rename ai-overview-project.md to AGENTS.md and consolidate setup helpers into common module

### Fixed

- Node 22 + restore npm@latest for OIDC provenance publishing
- Repair README ASCII architecture diagram and Docker image name
- Bump Node 20 to 22, remove npm@latest update step
- Truncate segment on corruption instead of skipping or losing data
- Preserve batch on commit failure to prevent silent data loss
- Remove exists() gate from subscribe() to stop masking connection errors
- Stop consumer on non-retryable server errors instead of infinite loop
- Remove nack on shutdown, validate batchSize=0, fix long-polling race
- Reject zero batch size and concurrency in subscribe and consume_batch
- Validate subscribe patterns and publish topics
- Preserve per-key ordering on timeout redelivery, fix zombie member on stop, remove dead code in ack
- Replace Notify with watch, fix ack-floor over parked, prevent cold reads under backpressure

## v2.0.1

**Released:** 2026-06-12

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v2.0.1" target="_blank" rel="noreferrer">Download v2.0.1</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=2.0.1" target="_blank" rel="noreferrer">Docker tag v2.0.1</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/2.0.1" target="_blank" rel="noreferrer">npm v2.0.1</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/2.0.1/" target="_blank" rel="noreferrer">PyPI v2.0.1</a>
</p>

## v2.0.0

**Released:** 2026-06-12

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v2.0.0" target="_blank" rel="noreferrer">Download v2.0.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=2.0.0" target="_blank" rel="noreferrer">Docker tag v2.0.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/2.0.0" target="_blank" rel="noreferrer">npm v2.0.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/2.0.0/" target="_blank" rel="noreferrer">PyPI v2.0.0</a>
</p>

### Added

- Add PROTOCOL_VERSION to each packet
- Introduce dev/serve subcommands to gate dashboard

### Changed

- Removed dev dashboard
- Add protocolWriter to unified decoding across broker
- refactor(protocol)!: uniform binary flags-based wire for all brokers
- Removed env single source of truth
- Removed {} from hot path in all brokers, keep {} only in cold path (create/delete etc)
- Removed cache to make all broker handler stateless
- Reorganize stress test for throughput and latency
- Removed delayed_messages feature

## v1.0.2

**Released:** 2026-04-26

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v1.0.2" target="_blank" rel="noreferrer">Download v1.0.2</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=1.0.2" target="_blank" rel="noreferrer">Docker tag v1.0.2</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/1.0.2" target="_blank" rel="noreferrer">npm v1.0.2</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/1.0.2/" target="_blank" rel="noreferrer">PyPI v1.0.2</a>
</p>

### Changed

- Add optional concurrency
- Disk-source-of-truth, consumer_id sessions, ack-only+parked delivery

### Fixed

- Sdk long-polling timeout
- Align dashboard pagination, sdk long-poll timeout, and simplify subscription layering

### Performance

- Single-pass FrameWriter, drop staging Buffers in request encoding

## v1.0.1

**Released:** 2026-04-23

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v1.0.1" target="_blank" rel="noreferrer">Download v1.0.1</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=1.0.1" target="_blank" rel="noreferrer">Docker tag v1.0.1</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/1.0.1" target="_blank" rel="noreferrer">npm v1.0.1</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/1.0.1/" target="_blank" rel="noreferrer">PyPI v1.0.1</a>
</p>

### Changed

- Add local-to-prod solutions sections
- Optimize search topics pubsub
- Disables Nagle's algorithm
- Grouped business-logic inside domain/*
- Align ai-project-overview to new design of project
- Reordere dashboard and server under diferent transport/ folder
- Rename managers
- Wip refactor separate tcp controlelr by http
- Cleanup code and optimize background task
- Refator ascii diagram architecture
- Removed ttl_ms queue
- Removed dead_code

### Fixed

- Clean callback on evantually subscribe/unsubscribe error

## v1.0.0

**Released:** 2026-04-02

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v1.0.0" target="_blank" rel="noreferrer">Download v1.0.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=1.0.0" target="_blank" rel="noreferrer">Docker tag v1.0.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/1.0.0" target="_blank" rel="noreferrer">npm v1.0.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/1.0.0/" target="_blank" rel="noreferrer">PyPI v1.0.0</a>
</p>

### Added

- Add leave command to cancel in-flight fetches

### Changed

- Unified configs in one file
- Remove PUSHTYPE, use PUSH_PUBSUB frame type only
- Realign docs
- Realign stream-docs
- From bounded to unbounded channel to persistence layer (from 1.1M to 1.9M)
- Replace actor pattern with shared state + mutex
- Replace N actors with 1 sharedState + long polling

## v0.6.3

**Released:** 2026-03-30

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.6.3" target="_blank" rel="noreferrer">Download v0.6.3</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.6.3" target="_blank" rel="noreferrer">Docker tag v0.6.3</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.6.3" target="_blank" rel="noreferrer">npm v0.6.3</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.6.3/" target="_blank" rel="noreferrer">PyPI v0.6.3</a>
</p>

### Added

- Add logo nexo

### Changed

- Centralize broker payload parsing in dashboard layer
- Centralize broker payload parsing in dashboard layer
- Unified all map logic inside same file
- Replace N root actors with single RadixTree + RwLock

## v0.6.2

**Released:** 2026-03-28

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.6.2" target="_blank" rel="noreferrer">Download v0.6.2</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.6.2" target="_blank" rel="noreferrer">Docker tag v0.6.2</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.6.2" target="_blank" rel="noreferrer">npm v0.6.2</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.6.2/" target="_blank" rel="noreferrer">PyPI v0.6.2</a>
</p>

### Added

- Rename environment variables for protocol clarity

### Changed

- Realign some env variables
- Conversion UUID from text to blob on sqlite
- Removed indexes idx_pop &amp; idx_dlq_failed sqlite queue - that data are read from memory and not on db at runtime (not pay cost of sync idx every insert)
- Cleaning code
- Clean-wip
- Ai-project-overview
- Ai-project-overview
- Stream &amp; queue UI/UX graphic
- Moved and renamed AGENTS.md to not attach to all prompt

### Fixed

- Queue wakeup min(expires_at) waiters timeout

## v0.6.1

**Released:** 2026-03-13

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.6.1" target="_blank" rel="noreferrer">Download v0.6.1</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.6.1" target="_blank" rel="noreferrer">Docker tag v0.6.1</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.6.1" target="_blank" rel="noreferrer">npm v0.6.1</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.6.1/" target="_blank" rel="noreferrer">PyPI v0.6.1</a>
</p>

## v0.6.0

**Released:** 2026-03-13

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.6.0" target="_blank" rel="noreferrer">Download v0.6.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.6.0" target="_blank" rel="noreferrer">Docker tag v0.6.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.6.0" target="_blank" rel="noreferrer">npm v0.6.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.6.0/" target="_blank" rel="noreferrer">PyPI v0.6.0</a>
</p>

### Changed

- Various refactor
- Various refactor
- Reorder TCP parallel events on connection_session
- Encapsulate request routing in RequestHandler
- Change default broker config, by clone+props drilling to arc pointer
- Update stream docs to jetstream approach
- Removed fsync persistence by Queue
- Removed channel by communication with QueueManager - QueueActor

### Fixed

- Persist queue &amp; stream config

## v0.5.0

**Released:** 2026-02-27

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.5.0" target="_blank" rel="noreferrer">Download v0.5.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.5.0" target="_blank" rel="noreferrer">Docker tag v0.5.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.5.0" target="_blank" rel="noreferrer">npm v0.5.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.5.0/" target="_blank" rel="noreferrer">PyPI v0.5.0</a>
</p>

### Changed

- Batch msgs on writer stream
- Add routing on topic with dahmap on streamanagre
- Removed tokio::spawn by every publish
- Add global stream persister to avoid too much FD opened
- Edit stream from kafka style (with partitions) to jetbrains style (1 big log) &amp; unified persistency inside same actor of topic
- Edit stream from kafka style (with partitions) to jetbrains style (1 big log)

### Fixed

- Add stream read cold from disk if evicted from RAM

## v0.4.0

**Released:** 2026-02-23

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.4.0" target="_blank" rel="noreferrer">Download v0.4.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.4.0" target="_blank" rel="noreferrer">Docker tag v0.4.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.4.0" target="_blank" rel="noreferrer">npm v0.4.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.4.0/" target="_blank" rel="noreferrer">PyPI v0.4.0</a>
</p>

### Added

- Change N scoped-request timeout with 1 global for all promises
- Add MAX_PAYLOAD_SIZE to cap packet
- Add pagination on queue page

### Changed

- Removed old dependencies
- Unify error types, fix request handle memory leak
- refactor(server)!: unify wire protocol headers and introduce meta byte
- Move broker fetching into views, fix refresh on all cards
- Removed old frame types from protocol
- Removed useMemo optimization
- Add pagination pubsub
- Add debounce persistence retained pubsub
- Add pagination on stream page
- Unified logic http request inside 1file for broker
- Removed docs link from release github

## v0.3.9

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.9" target="_blank" rel="noreferrer">Download v0.3.9</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.9" target="_blank" rel="noreferrer">Docker tag v0.3.9</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.9" target="_blank" rel="noreferrer">npm v0.3.9</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.9/" target="_blank" rel="noreferrer">PyPI v0.3.9</a>
</p>

### Changed

- Add version on footer

## v0.3.8

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.8" target="_blank" rel="noreferrer">Download v0.3.8</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.8" target="_blank" rel="noreferrer">Docker tag v0.3.8</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.8" target="_blank" rel="noreferrer">npm v0.3.8</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.8/" target="_blank" rel="noreferrer">PyPI v0.3.8</a>
</p>

### Changed

- Try to last version of npm

## v0.3.7

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.7" target="_blank" rel="noreferrer">Download v0.3.7</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.7" target="_blank" rel="noreferrer">Docker tag v0.3.7</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.7" target="_blank" rel="noreferrer">npm v0.3.7</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.7/" target="_blank" rel="noreferrer">PyPI v0.3.7</a>
</p>

### Changed

- Remove envs to send to NPM for OIDC auth

## v0.3.6

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.6" target="_blank" rel="noreferrer">Download v0.3.6</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.6" target="_blank" rel="noreferrer">Docker tag v0.3.6</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.6" target="_blank" rel="noreferrer">npm v0.3.6</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.6/" target="_blank" rel="noreferrer">PyPI v0.3.6</a>
</p>

### Changed

- Add envs to send to NPM for OIDC auth

## v0.3.5

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.5" target="_blank" rel="noreferrer">Download v0.3.5</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.5" target="_blank" rel="noreferrer">Docker tag v0.3.5</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.5" target="_blank" rel="noreferrer">npm v0.3.5</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.5/" target="_blank" rel="noreferrer">PyPI v0.3.5</a>
</p>

### Changed

- Refactor scripts package.json
- Refactor from nexo-docs to docs vitepress website

## v0.3.4

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.4" target="_blank" rel="noreferrer">Download v0.3.4</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.4" target="_blank" rel="noreferrer">Docker tag v0.3.4</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.4" target="_blank" rel="noreferrer">npm v0.3.4</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.4/" target="_blank" rel="noreferrer">PyPI v0.3.4</a>
</p>

### Fixed

- Edit NPM step

## v0.3.3

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.3" target="_blank" rel="noreferrer">Download v0.3.3</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.3" target="_blank" rel="noreferrer">Docker tag v0.3.3</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.3" target="_blank" rel="noreferrer">npm v0.3.3</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.3/" target="_blank" rel="noreferrer">PyPI v0.3.3</a>
</p>

### Fixed

- Removed registry-url to use OIDC auth

## v0.3.2

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.2" target="_blank" rel="noreferrer">Download v0.3.2</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.2" target="_blank" rel="noreferrer">Docker tag v0.3.2</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.2" target="_blank" rel="noreferrer">npm v0.3.2</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.2/" target="_blank" rel="noreferrer">PyPI v0.3.2</a>
</p>

### Fixed

- Remove working-directory vercel cli

## v0.3.1-test

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.1-test" target="_blank" rel="noreferrer">Download v0.3.1-test</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.1-test" target="_blank" rel="noreferrer">Docker tag v0.3.1-test</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.1-test" target="_blank" rel="noreferrer">npm v0.3.1-test</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.1-test/" target="_blank" rel="noreferrer">PyPI v0.3.1-test</a>
</p>

### Fixed

- Add versioning of Cargo.lock

## v0.3.1

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.1" target="_blank" rel="noreferrer">Download v0.3.1</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.1" target="_blank" rel="noreferrer">Docker tag v0.3.1</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.1" target="_blank" rel="noreferrer">npm v0.3.1</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.1/" target="_blank" rel="noreferrer">PyPI v0.3.1</a>
</p>

### Changed

- Add versioning explanation &amp; link to dockerhub
- Aligned root_path pubub with queue &amp; streams
- Aligned docs vitepress to advanced features
- Aligned docs vitepress to advanced features

## v0.3.0

**Released:** 2026-02-18

<p class="release-downloads">
  <a class="download-pill" href="https://github.com/emanuel-epifani/nexo/releases/tag/v0.3.0" target="_blank" rel="noreferrer">Download v0.3.0</a>
  <a class="download-pill alt" href="https://hub.docker.com/r/emanuelepifani/nexo/tags?name=0.3.0" target="_blank" rel="noreferrer">Docker tag v0.3.0</a>
  <a class="download-pill npm" href="https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/0.3.0" target="_blank" rel="noreferrer">npm v0.3.0</a>
  <a class="download-pill pypi" href="https://pypi.org/project/nexo-client/0.3.0/" target="_blank" rel="noreferrer">PyPI v0.3.0</a>
</p>

### Added

- Add scaffolding 4 brokers

### Changed

- [refactor-wip] readme &amp; docs
- [feat] add references to website-docs on sdk readme
- [feat] draft release+deploy ci/cd
- [feat] draft release+deploy ci/cd
- [refactor] broker project organization
- [refactor] add create TAG on script release.js
- [refactor] script release.js &amp; deploy.sh
- [refactor(docs)] add AGENTS.md for docs-website rules
- [feat(chore)] setup unified versioning and release scripts
- [refactor(protocol)]: implement zero-copy header parsing via bytemuck
- [refactor] moved socket tuning under config file
- [refactor] moved utils dashboard isnide dashboard module
- [test] refactor local ssh managing
- [refactor] dashboad STORE pagination
- [refactor] protocol
- [refactor] persistence path pubsub
- [refactor] edit dlq paylaod for dashboard fe
- [refactor] moved rust test
- [refactor] clean references to clinetIds zombie inside root
- [feat] add test ro socket reconnect
- [fix] race cond, send to zombie waiters queue
- [refactor] skip to stress test
- [refactor] moved subscribe before push on queue
- [fix] path compiled binary nexo
- [fix] typo fe dashboard
- [refactor] clean dashboard web
- [refactor] splitted broker sdk tests.ts
- [refactor] moved ts test below sdk/ts
- [refactor] README sdk
- [feat] add AGENTS.md
- [refactor] split message &amp; dlqmessage in 2 type
- [feat] implement nack for queue mex
- [feat] implement dlq managing on sdk
- [refactor] unified main queue &amp; dlq inside same actor thread
- [fix] delete msg from queue after send in dlq
- [feat] add versions features sdk ts
- [refactor] moved STATUS from payload to header (breaking change!)
- [refactor] moved OPCODE from payload to header (breaking change!)
- [refactor] cleanup timeouts stress tests
- [feat] add pubsub persistence to readme example
- [feat] add limit number partitions stream
- [fix] race condition stream create on disk
- [fix] race condition queue create on disk
- [fix] read-write permissions sqlite
- [feat] add script memory monitoring
- [fix] scroll stres eventlog fe
- [refactor] update readme
- [refactor] edit store keys for fe
- [refactor] add legend protocol and unified offset
- [refactor] from string to json paylaod store &amp; queue
- [feat] add e2e stress test for all brokers
- [feat] edited qlite settings for high throughput
- [fix-wip] cleanup retained rootactor pubsub
- [feat] add clear retained pubsub
- [feat] add persistency retained pubsub
- [feat] add ttl logic pubsub
- [feat] cleanup empty rootactor pubsub
- [refactor] update readme
- [refactor] eviction RAM stream log
- [refactor] decalre_queue to create_queue
- [fix] recreate actor on warmstart
- [refactor] add last fix readme
- [feat] add NEXO_ENV to switch dev/prod behavior
- [refactor] change FE dashboard from dark to light mode
- [refactor] add dashboard image on readme
- [refactor-wip] readme (queue)
- [refactor-wip] readme (pubsub)
- [refactor-wip] readme (architecture)
- [refactor-wip] readme
- [refactor] add prepare_cached sqlite query queue
- [feat] edited pubsub logic &amp; add tests
- [feat-wip] test benchamrk
- [feat] update readme
- [feat] flushed on dysk on fsync mode
- [refactor] unified module of rust tests
- [refactor] aligned queue &amp; stream models pattern
- [refactor] externalized channel buffer sizes
- [refactor] test vitest
- [feat-PUBSUB] add reconnection on conn closed
- [refactor] layout fe
- [refactor] default partitions number
- [refactor] add retention policy sdk readme
- [refactor] request once broker at time on FE
- [refactor] add deelte example to topic/stream
- [refactor] utilities queue tests
- [refactor] ever broker manager accept config on constructor
- [feat] add editable config for queue on manager
- [feat] revert comment test stream
- [feat] add eviction RAM stream
- [feat] add lazy loading stream
- [feat] add delete topic
- [feat-wip] add disk retention log file
- [feat] add segmentation log file
- [feat] cadd ompaction commits.log file
- [refactor] pubsub logic
- [feat] add persistency stream test
- [feat] add persistency stream test
- [feat] add persistency stream
- [feat] add test rust stream
- [feat] run concurrent partition evetns
- [feat] add tests pubsub
- [feat] add command delete queue
- [refactor] test queue &amp; store
- [feat] add test longpolling queue
- [refactor] deleted some queue tests
- [refactor] moved lopp that move messages bewteen states inside actor queue
- [feat] add test msg queues
- [refactor] store with map inside them
- [feat] implement Persister Queue inside queue actor
- [refactor] revert timeout global to N timeout (1 for promise)
- [refactor] readme sdk
- [feat] add persistency flags to sdk
- [refactor] readme sdk
- [fix] fe stream list visualization
- [refactor] example readme sdk
- [refactor] make envs singleton
- [refactor] optional parameter STORE
- [refactor] optional parameter PUBSUBS
- [refactor] optional parameter queue create
- [feat] setNoDelay to default
- [feat] add optional params createQueue
- [feat] add file setup
- [feat] add file setup
- [refactor] unified commands on his broker &amp; split busines logic from packet creations
- [feat] add failfast if resource not exist
- [refactor] clinet &amp; connection
- [refactor] moved consumerfrom stream to subscriber
- [refactor] comehere, before starts refactor stream
- [refactor] split models on each broker file
- [refactor] clean test
- [refactor] stress test socket
- [fix] improve allocation memory on codec/connection
- [fix] improve allocation memory on codec/connection
- [refactor] dashboard STREAM
- [refactor] dashboard PUBSUB
- [refactor] dashboard PUBSUB
- [refactor] dashboard PUBSUB
- [refactor] update ai rules-project
- [refactor-wip] dashboard PUBSUB
- [refactor-wip] dashboard PUBSUB
- [refactor-wip] dashboard PUBSUB
- [refactor] ai-ruels &amp; test dashboard
- [feat] add gzip compression to http request
- [refactor-wip] dashboard QUEUE
- [refactor] dashboard QUEUE
- [refactor] dashboard STORE
- [refactor-wip] dashboard QUEUE
- [refactor] dashboard QUEUE
- [fix] layout store large paylod
- [refactor] from value_preview to value
- [refactor] add debounce + early exit list STORE
- [refactor] virtualize list + useMemo STORE
- [refactor] dashboard
- [fix] dashboard STORE
- [fix] dashboard STORE
- [fix] layout dashboard
- [refactor-wip] dashboard STORE
- [refactor] add env to zod test suite
- [refactor] dasboard web - dinamic height deatil view
- [refactor] dasboard web - splitted 4 request broker status
- [refactor] dasboard web
- [feat] unified all logs &amp; add dockerfile
- [wip-dashboard] refactor dashboard datas
- [refactor] stress test socket
- [refactor] rename env variables
- [refactor] tests suite
- [refactor] split sdk entity o
- [refactor] env variable name
- [refactor] add env validation &amp; pass queue from multithread to actor model
- [feature] add docs to brokers
- [feature] change pubsub from single-thread to multi (actor model 1 thread 1 tree)
- [wip-feature] partitions on stream
- [wip-feature] partitions on client sdk
- [refactor] dashboard for local develop
- [refactor] wip partitions on sdk
- [refactor] add struct to PAYLOADS legend at every broker
- [refactor] add struct to HEADER legend
- [refactor] add partitions logic to stream server-side
- refactor client sdk for maintenance
- refactor topic from Vec to BTreeMap for efficient scan clients
- refactor queue
- card perfect!!
- updated dashboard web
- updated dashboard web
- wip- from singelton to new instance every subscribe sdk
- from sequential to Sliding Window in queue batch processing
- kill while lopp subscrbe if disconnect socket
- fix handle error in callback stream inside batch
- change of Weak&lt;Notify&gt; of waiters on stream
- change of Weak&lt;Notify&gt; of waiters on stream
- implemented RAII pattern (clean on disconnect) on pubsub
- add fail fast error if queeu not exist on subscribe
- wip - split 2 task for queeu (1 fast for ack/scheduled, other for cleaner orphan uuid)
- wip - consume queue from single request to batch
- rollback stream (from multithread with partitions to single thread + actor model)
- rollback stream (from multithread with partitions to single thread + actor model)
- wip - rebalancing reject old epoch
- update test stream
- rollbakc cork()/uncork()
- cork()/uncork()
- add generics to topic pubsub
- add snapshot on server
- add test performance stream
- WIP - implement consumerGrup- offset - rebalancing features
- WIP - implement consumerGrup- offset - rebalancing features
- implement consumerGrup- offset - rebalancing features
- implement consumerGrup- offset - rebalancing features
- wip - align queue &amp; topics to intial create methods
- wip - align queue &amp; topics to intial create methods
- draft stream
- refactor client sdk for latency &gt; throughput
- refactor test file
- fix subarry bug (copied instead view)
- add expect on stress tests
- add prefetch msgs on queue
- add test under stress protocol
- add 1hour default ttl server side for key in teh store
- refacor performance test
- refactor performance test
- add 512MB Hard Limit to prevent OOM attacks
- implemented resize buffer if not enough
- update curorrules
- unified performance test
- add timeout clearing zombie request
- add TYPE of data inside payload to improve deserialize non-json object on client.ts
- renae TopicManager to PubSub Manager
- add RETAIN lastvalue on mqtt [rust-ts]
- add perforamcne test mqtt broker- wip
- add perforamcne test mqtt broker
- add mqtt broker
- draft mqtt
- refactor KV to implements different data-structure
- add level trace to logger ts &amp; rust
- implemented logger [ts]
- implemented logger [ts]
- implemented logger [ts]
- implemented logger [rust]
- refactor Queue client
- come back here
- refactor pure factory sdk client.ts
- new refactor client.ts
- new refactor client.ts
- refactor client.ts
- add opcdode debug echo to rust
- rename test cases
- end queue feeatures
- refactor Queue + InternalState
- deep_cleanup repaer
- added timeouts (sdk TS)
- added timeouts (Rust)
- pass to array to map to manage pending promise on sdk
- wip test queue
- implemented test describe 3
- implemented test describe 1 e 2
- implemented logic queue manager
- optimize encode_response with pre-calculate exact size of memory allocation
- optimizes performance
- wip
- implemented Buffer.allocUnsafe() to reduce number of allocations
- implemented Zero-Copy (bytes:Bytes), Write batching
- refactor flow server
- add zod valdiation envs
- add draft sdk &amp; test
- wip routing command 2
- wip routing command
- wip routing command
- implement command pattern
- mixin
- refactor protocol from RESP to binary
- refactor
- add syntax example cheatsheet
- avoid clone and use iterator
- from if to matvh on routing
- edit matchcase &amp; incomplete/invalid payload
- reorder project structure
- unified dispatcher
- add draft tcp socket, dispatcher and KV manager
- add overview rust file

### Removed

- [refactor] removed git repo nested for docs
- [refactor] removed readme test
- [refactor] removed ai rules
- [refactor] removed unused code dlq queue
- [refactor] removed memory persistency from stream &amp; queue
- [refactor] removed old debug print
- [refactor-wip] removed why nexo
- [feat] removed fasync properties
- [refactor] removed useless class for export sdk
- [refactor] removed file 1 x broker test
- [refactor] removed echo method sdk
- [refactor] removed confgi passed on queue constructore insted of create
- [refactor] removed default_delay_ms from queue
- [refactor] remove "passive" flag queues
- [fix] removed double lookup map queue
- [fix] removed expired key from dashboard response
- [refactor] removed unnecessary task::spawn tokio
- [refactor] removed early retunrn OOM
- removed key from stream
- removed slower logging
- removed scan O(n) from reaper every 50ms on queue

### Performance

- Avoid double UTF-8 encoding in writeString


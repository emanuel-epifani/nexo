//! NEXO Native Manager Performance Tests
//!
//! Full suite: `cargo test --release --test stress_tests bench_ -- --test-threads=1 --nocapture`
//! Stream: `cargo test --release --test stress_tests stream::bench_stream_ -- --test-threads=1 --nocapture`
//!
//! Calls managers directly; no TCP, SDK or container is part of the measured path.
//! This file measures operations and timings. Only API-error, progress and expected
//! total-count guards remain; payload/order/identity/DLS/recovery assertions belong
//! in the broker's functional test file.
//! STREAM_MANAGER_PERF reports source events/s separately from completed deliveries/s,
//! actual assignment sizes, manager calls and writer SQL/timing counters. Setup and
//! prefill are outside drain/pipeline timing; final update confirmations are inside.
//! No per-event latency samples are collected for these workload records.

mod common;
use bytes::Bytes;
use common::{setup_pubsub_manager, setup_queue_manager, setup_store_manager};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

use nexo::brokers::queue::options::QueueCreateOptions;
use nexo::brokers::stream::config::SystemStreamConfig;
use nexo::brokers::stream::options::StreamCreateOptions;
use nexo::brokers::stream::StreamManager;
use nexo::config::Config;
use std::sync::Arc;

// ==========================================
// BENCHMARK UTILITY
// ==========================================

pub(crate) struct Benchmark {
    pub(crate) name: String,
    pub(crate) start: Instant,
    pub(crate) count: usize,
    pub(crate) samples: Vec<Duration>,
}

impl Benchmark {
    pub(crate) fn start(name: &str, count: usize) -> Self {
        Self {
            name: name.to_string(),
            start: Instant::now(),
            count,
            samples: Vec::with_capacity(count),
        }
    }

    pub(crate) fn record(&mut self, duration: Duration) {
        self.samples.push(duration);
    }

    pub(crate) fn stop(mut self) {
        let total_duration = self.start.elapsed();
        let secs = total_duration.as_secs_f64();
        let ops_sec = self.count as f64 / secs;

        self.samples.sort();
        let len = self.samples.len();

        let p50 = self
            .samples
            .get(len * 50 / 100)
            .unwrap_or(&Duration::ZERO)
            .as_micros();
        let p95 = self
            .samples
            .get(len * 95 / 100)
            .unwrap_or(&Duration::ZERO)
            .as_micros();
        let p99 = self
            .samples
            .get(len * 99 / 100)
            .unwrap_or(&Duration::ZERO)
            .as_micros();
        let max = self.samples.last().unwrap_or(&Duration::ZERO).as_micros();
        let avg = if len > 0 {
            self.samples.iter().sum::<Duration>().as_micros() as u64 / len as u64
        } else {
            0
        };

        println!("\n📊 {}", self.name);
        println!("   Throughput:  {:.0} ops/sec", ops_sec);
        println!("   Total Time:  {:.2?}", total_duration);
        println!(
            "   Latency:     Avg: {}µs | p50: {}µs | p95: {}µs | p99: {}µs | Max: {}µs",
            avg, p50, p95, p99, max
        );
        println!("   Count:       {}\n", self.count);
    }
}

#[cfg(test)]
mod stress_tests {
    use super::*;

    fn get_stream_test_config(path: Option<&str>) -> SystemStreamConfig {
        let mut config = Config::global().stream.clone();
        if let Some(p) = path {
            config.persistence_path = p.to_string();
        }
        config
    }

    async fn build_stream_manager(config: SystemStreamConfig) -> Arc<StreamManager> {
        Arc::new(
            StreamManager::new(Arc::new(config))
                .await
                .expect("stream manager must open"),
        )
    }

    // =========================================================================================
    // STORE BENCHMARKS
    // =========================================================================================

    mod store {
        use super::*;
        const COUNT: usize = 200_000;

        #[tokio::test]
        async fn bench_write_throughput() {
            let (manager, _tmp) = setup_store_manager().await;

            let mut bench = Benchmark::start("STORE - Write (PUT)", COUNT);

            for i in 0..COUNT {
                let start = Instant::now();
                let key = i.to_string();
                let _ = manager.map.set(key, Bytes::from("data"), None);
                bench.record(start.elapsed());
            }
            bench.stop();
        }

        #[tokio::test]
        async fn bench_read_throughput() {
            let (manager, _tmp) = setup_store_manager().await;

            // Pre-fill
            for i in 0..COUNT {
                let key = i.to_string();
                let _ = manager.map.set(key, Bytes::from("data"), None);
            }

            let mut bench = Benchmark::start("STORE - Read (GET)", COUNT);
            for i in 0..COUNT {
                let start = Instant::now();
                let key = i.to_string();
                let _ = manager.map.get(&key).unwrap();
                bench.record(start.elapsed());
            }
            bench.stop();
        }
    }

    // =========================================================================================
    // QUEUE BENCHMARKS
    // =========================================================================================

    mod queue {
        use super::*;
        const COUNT: usize = 500_000;

        #[tokio::test]
        async fn bench_queue_throughput() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("bench_async_{}", Uuid::new_v4());
            let config = QueueCreateOptions {
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            let stats = manager.sql_stats();
            let mut bench = Benchmark::start("PUSH - Queue Throughput (Sequential)", COUNT);
            for _ in 0..COUNT {
                let start = Instant::now();
                manager
                    .push(q.clone(), Bytes::from("data"), 0)
                    .await
                    .unwrap();
                bench.record(start.elapsed());
            }
            // Wait for flush to happen in background (optional, just to be fair to disk)
            tokio::time::sleep(Duration::from_millis(200)).await;
            let (s, f, e, c) = manager.sql_stats();
            println!(
                "   SQL stmts:   {} ({:.2}/op) | flushes {} | exec {:.1}ms | commit {:.1}ms",
                s - stats.0,
                (s - stats.0) as f64 / COUNT as f64,
                f - stats.1,
                (e - stats.2) as f64 / 1e6,
                (c - stats.3) as f64 / 1e6,
            );
            bench.stop();
        }

        /// Concurrent single pushes: the drain merges what concurrent
        /// submitters leave in the channel — organic commit batching.
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn bench_queue_throughput_concurrent() {
            const TASKS: usize = 16;
            const PER_TASK: usize = 5_000;
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("bench_concurrent_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            let stats = manager.sql_stats();
            let mut bench =
                Benchmark::start("QUEUE PUSH concurrent x16", TASKS * PER_TASK);
            let mut handles = Vec::with_capacity(TASKS);
            for _ in 0..TASKS {
                let m = Arc::clone(&manager);
                let queue = q.clone();
                handles.push(tokio::spawn(async move {
                    let mut latencies = Vec::with_capacity(PER_TASK);
                    for _ in 0..PER_TASK {
                        let t0 = Instant::now();
                        m.push(queue.clone(), Bytes::from("data"), 0)
                            .await
                            .unwrap();
                        latencies.push(t0.elapsed());
                    }
                    latencies
                }));
            }
            for h in handles {
                for d in h.await.unwrap() {
                    bench.record(d);
                }
            }
            bench.stop();
            let (s, f, e, c) = manager.sql_stats();
            println!(
                "   SQL stmts:   {} ({:.2}/op) | flushes {} | exec {:.1}ms | commit {:.1}ms",
                s - stats.0,
                (s - stats.0) as f64 / (TASKS * PER_TASK) as f64,
                f - stats.1,
                (e - stats.2) as f64 / 1e6,
                (c - stats.3) as f64 / 1e6,
            );
        }

        /// Batched pushes: one request = one transaction slice carrying N rows —
        /// the fast path the API is shaped around.
        #[tokio::test]
        async fn bench_queue_push_batch() {
            const BATCH: usize = 100;
            const BATCHES: usize = 5_000;
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("bench_batch_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            let stats = manager.sql_stats();
            let mut bench = Benchmark::start("QUEUE PUSH BATCH 100", BATCH * BATCHES);
            let items: Vec<(Bytes, u8)> =
                (0..BATCH).map(|_| (Bytes::from("data"), 0u8)).collect();
            for _ in 0..BATCHES {
                let start = Instant::now();
                manager
                    .push_batch(q.clone(), items.clone())
                    .await
                    .unwrap();
                for _ in 0..BATCH {
                    bench.record(start.elapsed());
                }
            }
            bench.stop();
            let (s, f, e, c) = manager.sql_stats();
            println!(
                "   SQL stmts:   {} ({:.2}/op) | flushes {} | exec {:.1}ms | commit {:.1}ms",
                s - stats.0,
                (s - stats.0) as f64 / (BATCH * BATCHES) as f64,
                f - stats.1,
                (e - stats.2) as f64 / 1e6,
                (c - stats.3) as f64 / 1e6,
            );
        }
    }

    // =========================================================================================
    // PUB/SUB BENCHMARKS
    // =========================================================================================

    mod pubsub {
        use super::*;
        const MSG_COUNT: usize = 500_000;

        #[tokio::test]
        async fn bench_pubsub_throughput_exact_match() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "bench_sub".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            let topic = "bench/speed";
            manager.subscribe(&client_id, topic).unwrap();

            let payload = Bytes::from("fast_data");

            // Spawn consumer to drain channel
            tokio::spawn(async move { while let Some(_) = rx.recv().await {} });

            let mut bench = Benchmark::start("PUBSUB - Exact Match Throughput", MSG_COUNT);

            for _ in 0..MSG_COUNT {
                let start = Instant::now();
                let _ = manager.publish(topic, payload.clone(), false, false, None);
                bench.record(start.elapsed());
            }

            bench.stop();
        }

        #[tokio::test]
        async fn bench_pubsub_throughput_wildcard_match() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "bench_wild".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            // Subscribe with wildcard
            manager.subscribe(&client_id, "bench/+/metric").unwrap();
            let payload = Bytes::from("data");

            tokio::spawn(async move { while let Some(_) = rx.recv().await {} });

            let mut bench = Benchmark::start("PUBSUB - Wildcard Match Throughput", MSG_COUNT);

            for _ in 0..MSG_COUNT {
                let start = Instant::now();
                let _ =
                    manager.publish("bench/server1/metric", payload.clone(), false, false, None);
                bench.record(start.elapsed());
            }

            bench.stop();
        }

        #[tokio::test]
        async fn bench_pubsub_fanout() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "fanout/global";
            let num_subs = 100;

            // Create 100 subscribers
            for i in 0..num_subs {
                let client_id = format!("sub_{}", i);
                let (tx, mut rx) = mpsc::channel(8192);
                manager.connect(&client_id, tx);
                manager.subscribe(&client_id, topic).unwrap();

                tokio::spawn(async move { while let Some(_) = rx.recv().await {} });
            }

            let payload = Bytes::from("broadcast");
            let count = 10_000;

            let mut bench = Benchmark::start(&format!("PUBSUB - Fanout 1->{}", num_subs), count);

            for _ in 0..count {
                let start = Instant::now();
                let _ = manager.publish(topic, payload.clone(), false, false, None);
                bench.record(start.elapsed());
            }

            bench.stop();
        }
    }

    // =========================================================================================
    // STREAM BENCHMARKS
    // =========================================================================================

    mod stream {
        use super::*;
        use nexo::brokers::stream::{
            BatchDisposition, BatchUpdate, ConsumerIdentity, FetchLimits, PubItem,
        };
        use tokio::task::JoinSet;

        const COUNT: usize = 500_000;
        const PERF_STREAM: &str = "native-stream-perf";
        const PAYLOAD_BYTES: usize = 128;
        const CASE_TIMEOUT: Duration = Duration::from_secs(120);

        struct Consumption {
            deliveries: usize,
            batches: usize,
            fetches: usize,
            updates: usize,
        }

        fn event(id: usize, keys: usize) -> PubItem {
            let mut payload = vec![b'x'; PAYLOAD_BYTES];
            payload[..8].copy_from_slice(&(id as u64).to_be_bytes());
            PubItem {
                key: if keys == 0 {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(&((id % keys) as u64).to_be_bytes())
                },
                payload: Bytes::from(payload),
            }
        }

        async fn perf_setup(
            groups: usize,
        ) -> (
            tempfile::TempDir,
            Arc<StreamManager>,
            Vec<(String, ConsumerIdentity)>,
        ) {
            let dir = tempfile::tempdir().unwrap();
            let config = SystemStreamConfig {
                persistence_path: dir.path().to_str().unwrap().to_string(),
                ..SystemStreamConfig::default()
            };
            let manager = build_stream_manager(config).await;
            manager
                .create_stream(PERF_STREAM.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            let mut members = Vec::with_capacity(groups);
            for g in 0..groups {
                let group = format!("g{g}");
                let connection_id = format!("perf-consumer-{g}");
                let joined = manager
                    .join_group(PERF_STREAM, &group, &connection_id)
                    .await
                    .unwrap();
                members.push((
                    group,
                    ConsumerIdentity {
                        connection_id,
                        consumer_id: joined.consumer_id,
                        epoch: joined.epoch,
                    },
                ));
            }
            (dir, manager, members)
        }

        async fn publish_seed(manager: &StreamManager, items: &[PubItem]) {
            for chunk in items.chunks(100) {
                let sequences = manager
                    .publish_batch(PERF_STREAM, chunk.to_vec())
                    .await
                    .unwrap();
                assert_eq!(sequences.len(), chunk.len());
            }
        }

        async fn leave_members(
            manager: &StreamManager,
            members: &[(String, ConsumerIdentity)],
        ) {
            for (group, identity) in members {
                manager
                    .leave_group(PERF_STREAM, group, identity)
                    .await
                    .unwrap();
            }
        }

        async fn consume(
            manager: Arc<StreamManager>,
            group: String,
            identity: ConsumerIdentity,
            count: usize,
            limits: FetchLimits,
            bulk_ack: bool,
            wait_ms: u64,
        ) -> Consumption {
            let mut result = Consumption {
                deliveries: 0,
                batches: 0,
                fetches: 0,
                updates: 0,
            };
            while result.deliveries < count {
                let page = manager
                    .fetch(PERF_STREAM, &group, &identity, limits, wait_ms)
                    .await
                    .unwrap();
                result.fetches += 1;
                if page.batches.is_empty() {
                    assert!(wait_ms > 0, "prefilled stream made no progress");
                    continue;
                }
                let page_messages: usize = page.batches.iter().map(|b| b.messages.len()).sum();
                assert!(page_messages > 0 && result.deliveries + page_messages <= count);
                let mut updates = Vec::with_capacity(page.batches.len());
                for batch in page.batches {
                    assert!(!batch.messages.is_empty());
                    result.batches += 1;
                    let update = BatchUpdate {
                        receipt: batch.receipt,
                        completed: batch.messages.len() as u32,
                        disposition: BatchDisposition::Checkpoint,
                    };
                    if bulk_ack {
                        updates.push(update);
                    } else {
                        manager
                            .update_batch(PERF_STREAM, &group, &identity, update)
                            .await
                            .unwrap();
                        result.updates += 1;
                    }
                }
                if !updates.is_empty() {
                    let failures = manager
                        .update_batches(PERF_STREAM, &group, &identity, updates)
                        .await
                        .unwrap();
                    assert!(failures.is_empty(), "batch confirmation failed: {failures:?}");
                    result.updates += 1;
                }
                result.deliveries += page_messages;
            }
            assert_eq!(result.deliveries, count);
            result
        }

        fn report_case(
            scenario: serde_json::Value,
            elapsed: Duration,
            count: usize,
            before: (u64, u64, u64, u64),
            after: (u64, u64, u64, u64),
            consumers: &[Consumption],
        ) {
            let seconds = elapsed.as_secs_f64();
            assert!(seconds > 0.0);
            let deliveries: usize = consumers.iter().map(|c| c.deliveries).sum();
            let batches: usize = consumers.iter().map(|c| c.batches).sum();
            println!(
                "STREAM_MANAGER_PERF {}",
                serde_json::json!({
                    "scope": "StreamManager in-process; SQLite WAL synchronous=NORMAL; no TCP or SDK",
                    "scenario": scenario,
                    "payload_bytes": PAYLOAD_BYTES,
                    "source_events": count,
                    "completed_deliveries": deliveries,
                    "elapsed_seconds": seconds,
                    "source_events_per_second": count as f64 / seconds,
                    "completed_deliveries_per_second": if consumers.is_empty() {
                        None
                    } else {
                        Some(deliveries as f64 / seconds)
                    },
                    "assigned_batches": batches,
                    "events_per_batch": if batches == 0 {
                        None
                    } else {
                        Some(deliveries as f64 / batches as f64)
                    },
                    "fetch_calls": consumers.iter().map(|c| c.fetches).sum::<usize>(),
                    "update_calls": consumers.iter().map(|c| c.updates).sum::<usize>(),
                    "sql_statements": after.0 - before.0,
                    "sql_statements_per_source_event": (after.0 - before.0) as f64 / count as f64,
                    "writer_batches": after.1 - before.1,
                    "writer_exec_ms": (after.2 - before.2) as f64 / 1e6,
                    "writer_commit_ms": (after.3 - before.3) as f64 / 1e6,
                    "latency_scope": "whole workload through final confirmation; no per-event samples",
                    "completion_checked": true,
                })
            );
        }

        fn sql_stats_diff(
            manager: &StreamManager,
            before: (u64, u64, u64, u64),
            ops: usize,
        ) {
            let (s, b, e, c) = manager.sql_stats();
            let ds = s - before.0;
            println!(
                "   SQL stmts:   {} ({:.1}/op) | batches {} | exec {:.1}ms | commit {:.1}ms",
                ds,
                ds as f64 / ops.max(1) as f64,
                b - before.1,
                (e - before.2) as f64 / 1e6,
                (c - before.3) as f64 / 1e6,
            );
        }

        #[tokio::test]
        async fn bench_stream_publish() {
            let temp_dir = tempfile::tempdir().unwrap();

            let config = get_stream_test_config(Some(temp_dir.path().to_str().unwrap()));

            let manager = build_stream_manager(config).await;
            let name = "bench-write-confirmed";
            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let stats = manager.sql_stats();
            let mut bench = Benchmark::start("STREAM PUBLISH (Write Confirmed)", COUNT);
            for _ in 0..COUNT {
                let start = Instant::now();
                manager
                    .publish(name, Bytes::new(), Bytes::from("data"))
                    .await
                    .unwrap();
                bench.record(start.elapsed());
            }
            bench.stop();
            sql_stats_diff(&manager, stats, COUNT);
        }

        /// 100-item keyless batches: the amortized publish path.
        #[tokio::test]
        async fn bench_stream_publish_batch_keyless() {
            const BATCHES: usize = 3_000;
            const ITEMS: usize = 100;
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_stream_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_stream_manager(config).await;
            let name = "bench-batch-keyless";
            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let items: Vec<PubItem> = (0..ITEMS)
                .map(|_| PubItem {
                    key: Bytes::new(),
                    payload: Bytes::from("data"),
                })
                .collect();
            let stats = manager.sql_stats();
            let mut bench = Benchmark::start("STREAM PUBLISH BATCH 100 keyless", BATCHES);
            for _ in 0..BATCHES {
                let start = Instant::now();
                manager
                    .publish_batch(name, items.clone())
                    .await
                    .unwrap();
                bench.record(start.elapsed());
            }
            bench.stop();
            sql_stats_diff(&manager, stats, BATCHES * ITEMS);
        }

        /// 100-item keyed batches over 64 keys while two groups are joined:
        /// exercises the per-key lane fan-out.
        #[tokio::test]
        async fn bench_stream_publish_batch_keyed_groups() {
            const BATCHES: usize = 2_000;
            const ITEMS: usize = 100;
            const KEYS: usize = 64;
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_stream_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_stream_manager(config).await;
            let name = "bench-batch-keyed";
            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            for g in ["g1", "g2"] {
                manager.join_group(name, g, "producer-conn").await.unwrap();
            }

            let items: Vec<PubItem> = (0..ITEMS)
                .map(|i| PubItem {
                    key: Bytes::from(format!("key-{}", i % KEYS)),
                    payload: Bytes::from("data"),
                })
                .collect();
            let stats = manager.sql_stats();
            let mut bench = Benchmark::start("STREAM PUBLISH BATCH 100 keyed x2 groups", BATCHES);
            for _ in 0..BATCHES {
                let start = Instant::now();
                manager
                    .publish_batch(name, items.clone())
                    .await
                    .unwrap();
                bench.record(start.elapsed());
            }
            bench.stop();
            sql_stats_diff(&manager, stats, BATCHES * ITEMS);
        }

        /// Concurrent single publishes: the drain merges what concurrent
        /// submitters leave in the queue — organic commit batching.
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn bench_stream_publish_concurrent() {
            const TASKS: usize = 16;
            const PER_TASK: usize = 5_000;
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_stream_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_stream_manager(config).await;
            let name = "bench-concurrent";
            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let stats = manager.sql_stats();
            let mut bench =
                Benchmark::start("STREAM PUBLISH concurrent x16", TASKS * PER_TASK);
            let mut handles = Vec::with_capacity(TASKS);
            for _ in 0..TASKS {
                let m = Arc::clone(&manager);
                handles.push(tokio::spawn(async move {
                    let mut latencies = Vec::with_capacity(PER_TASK);
                    for _ in 0..PER_TASK {
                        let t0 = Instant::now();
                        m.publish(name, Bytes::new(), Bytes::from("data"))
                            .await
                            .unwrap();
                        latencies.push(t0.elapsed());
                    }
                    latencies
                }));
            }
            for h in handles {
                for d in h.await.unwrap() {
                    bench.record(d);
                }
            }
            bench.stop();
            sql_stats_diff(&manager, stats, TASKS * PER_TASK);
        }

        /// Consume loop: fetch a page of keyless events then ack each one.
        #[tokio::test]
        async fn bench_stream_fetch_ack_keyless() {
            const MESSAGES: usize = 50_000;
            const PAGE: usize = 100;
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_stream_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_stream_manager(config).await;
            let name = "bench-fetch-ack";
            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            let seed: Vec<PubItem> = (0..1_000)
                .map(|_| PubItem {
                    key: Bytes::new(),
                    payload: Bytes::from("data"),
                })
                .collect();
            for _ in 0..MESSAGES / 1_000 {
                manager.publish_batch(name, seed.clone()).await.unwrap();
            }
            let joined = manager.join_group(name, "g", "consumer-conn").await.unwrap();
            let identity = nexo::brokers::stream::ConsumerIdentity {
                connection_id: "consumer-conn".to_string(),
                consumer_id: joined.consumer_id.clone(),
                epoch: joined.epoch,
            };

            let stats = manager.sql_stats();
            let mut bench = Benchmark::start(
                "STREAM FETCH+ACK keyless (pages of 100)",
                MESSAGES / PAGE,
            );
            let mut consumed = 0usize;
            while consumed < MESSAGES {
                let start = Instant::now();
                let page = manager
                    .fetch(
                        name,
                        "g",
                        &identity,
                        nexo::brokers::stream::FetchLimits {
                            max_messages: PAGE,
                            max_key_messages: 1,
                            max_key_batches: PAGE,
                            max_bytes: 10 * 1024 * 1024,
                        },
                        0,
                    )
                    .await
                    .unwrap();
                for batch in &page.batches {
                    manager
                        .update_batch(
                            name,
                            "g",
                            &identity,
                            nexo::brokers::stream::BatchUpdate {
                                receipt: batch.receipt,
                                completed: batch.messages.len() as u32,
                                disposition: nexo::brokers::stream::BatchDisposition::Checkpoint,
                            },
                        )
                        .await
                        .unwrap();
                    consumed += batch.messages.len();
                }
                bench.record(start.elapsed());
            }
            bench.stop();
            sql_stats_diff(&manager, stats, consumed);
        }

        async fn publish_case(count: usize, keys: usize, groups: usize) {
            let (_dir, manager, _members) = perf_setup(groups).await;
            let items: Vec<PubItem> = (0..count).map(|id| event(id, keys)).collect();
            let before = manager.sql_stats();
            let start = Instant::now();
            publish_seed(&manager, &items).await;
            let elapsed = start.elapsed();
            let after = manager.sql_stats();
            report_case(
                serde_json::json!({
                    "kind": "publish-batch", "keys": keys, "groups": groups,
                    "publish_batch": 100, "producers": 1,
                    "checks": "successful publish results and expected totals",
                }),
                elapsed, count, before, after, &[],
            );
            manager.shutdown().await;
        }

        #[tokio::test]
        async fn bench_stream_publish_fanout() {
            tokio::time::timeout(CASE_TIMEOUT, async {
                for groups in [0, 1, 4] {
                    publish_case(20_000, 64, groups).await;
                }
            })
            .await
            .expect("publish fan-out benchmark exceeded its deadline");
        }

        #[tokio::test]
        async fn bench_stream_publish_high_cardinality() {
            tokio::time::timeout(CASE_TIMEOUT, async {
                for groups in [0, 4] {
                    publish_case(20_000, 20_000, groups).await;
                }
            })
            .await
            .expect("high-cardinality benchmark exceeded its deadline");
        }

        async fn drain_case(keys: usize, key_batch: usize, bulk_ack: bool) {
            const MESSAGES: usize = 24_000;
            let (_dir, manager, members) = perf_setup(1).await;
            let items: Vec<PubItem> = (0..MESSAGES).map(|id| event(id, keys)).collect();
            publish_seed(&manager, &items).await;
            let limits = FetchLimits {
                max_messages: 512,
                max_key_messages: key_batch,
                max_key_batches: 16,
                max_bytes: 10 * 1024 * 1024,
            };
            let before = manager.sql_stats();
            let start = Instant::now();
            let result = consume(
                Arc::clone(&manager), members[0].0.clone(), members[0].1.clone(),
                MESSAGES, limits, bulk_ack, 0,
            )
            .await;
            let elapsed = start.elapsed();
            let after = manager.sql_stats();
            leave_members(&manager, &members).await;
            report_case(
                serde_json::json!({
                    "kind": "drain", "keys": keys, "groups": 1,
                    "max_messages": limits.max_messages,
                    "max_key_messages": limits.max_key_messages,
                    "max_key_batches": limits.max_key_batches,
                    "max_bytes": limits.max_bytes,
                    "bulk_ack": bulk_ack,
                    "checks": "successful confirmations and expected delivery totals",
                }),
                elapsed, MESSAGES, before, after, &[result],
            );
            manager.shutdown().await;
        }

        #[tokio::test]
        async fn bench_stream_drain_keyless_updates() {
            tokio::time::timeout(CASE_TIMEOUT, async {
                for bulk_ack in [false, true] {
                    drain_case(0, 1, bulk_ack).await;
                }
            })
            .await
            .expect("keyless drain benchmark exceeded its deadline");
        }

        #[tokio::test]
        async fn bench_stream_drain_keyed_batches() {
            tokio::time::timeout(CASE_TIMEOUT, async {
                for keys in [1, 64] {
                    for key_batch in [1, 32] {
                        drain_case(keys, key_batch, true).await;
                    }
                }
            })
            .await
            .expect("keyed drain benchmark exceeded its deadline");
        }

        async fn pipeline_case(keys: usize, groups: usize, producers: usize) {
            const MESSAGES: usize = 12_000;
            assert!(keys == 0 || keys % producers == 0);
            let (_dir, manager, members) = perf_setup(groups).await;
            let limits = FetchLimits {
                max_messages: 64,
                max_key_messages: 32,
                max_key_batches: 16,
                max_bytes: 10 * 1024 * 1024,
            };
            let before = manager.sql_stats();
            let start = Instant::now();
            let results = tokio::time::timeout(CASE_TIMEOUT, async {
                let mut readers = JoinSet::new();
                for (group, identity) in &members {
                    readers.spawn(consume(
                        Arc::clone(&manager), group.clone(), identity.clone(),
                        MESSAGES, limits, true, 1000,
                    ));
                }
                let mut publishers = JoinSet::new();
                for producer in 0..producers {
                    let manager = Arc::clone(&manager);
                    publishers.spawn(async move {
                        let mut published = 0;
                        for id in (producer..MESSAGES).step_by(producers) {
                            let item = event(id, keys);
                            manager
                                .publish(PERF_STREAM, item.key, item.payload)
                                .await
                                .unwrap();
                            published += 1;
                        }
                        published
                    });
                }
                let mut published = 0;
                while let Some(result) = publishers.join_next().await {
                    published += result.unwrap();
                }
                assert_eq!(published, MESSAGES);
                let mut results = Vec::with_capacity(groups);
                while let Some(consumed) = readers.join_next().await {
                    results.push(consumed.unwrap());
                }
                results
            })
            .await
            .expect("live pipeline benchmark exceeded its deadline");
            let elapsed = start.elapsed();
            let after = manager.sql_stats();
            assert_eq!(results.len(), groups);
            assert!(results.iter().all(|result| result.deliveries == MESSAGES));
            leave_members(&manager, &members).await;
            report_case(
                serde_json::json!({
                    "kind": "pipeline", "keys": keys, "groups": groups,
                    "producers": producers,
                    "max_messages": limits.max_messages,
                    "max_key_messages": limits.max_key_messages,
                    "max_key_batches": limits.max_key_batches,
                    "max_bytes": limits.max_bytes,
                    "bulk_ack": true,
                    "checks": "successful confirmations and expected delivery totals",
                }),
                elapsed, MESSAGES, before, after, &results,
            );
            manager.shutdown().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn bench_stream_live_pipelines() {
            for (keys, groups, producers) in [
                (0, 1, 4), (0, 4, 4), (64, 1, 4), (64, 4, 4), (1, 1, 1),
            ] {
                pipeline_case(keys, groups, producers).await;
            }
        }
    }
}

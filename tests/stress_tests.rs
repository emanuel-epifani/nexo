//! NEXO Cross-Broker Stress Benchmarks
//!
//! Command run to generate this snapshot:
//! `cargo test --release bench_ -- --test-threads=1 --nocapture`
//!
//! Reference output (macOS, release profile):
//!
//! ```text
//! Running tests/stress_tests.rs (target/release/deps/stress_tests-b8b3d0ca528459fd)
//!
//! running 7 tests
//! test stress_tests::pubsub::bench_pubsub_fanout ...
//! 📊 PUBSUB - Fanout 1->100
//!    Throughput:  180203 ops/sec
//!    Total Time:  55.49ms
//!    Latency:     Avg: 5µs | p50: 5µs | p95: 7µs | p99: 13µs | Max: 113µs
//!    Count:       10000
//!
//! ok
//! test stress_tests::pubsub::bench_pubsub_throughput_exact_match ...
//! 📊 PUBSUB - Exact Match Throughput
//!    Throughput:  5033819 ops/sec
//!    Total Time:  99.33ms
//!    Latency:     Avg: 0µs | p50: 0µs | p95: 0µs | p99: 0µs | Max: 184µs
//!    Count:       500000
//!
//! ok
//! test stress_tests::pubsub::bench_pubsub_throughput_wildcard_match ...
//! 📊 PUBSUB - Wildcard Match Throughput
//!    Throughput:  4411367 ops/sec
//!    Total Time:  113.34ms
//!    Latency:     Avg: 0µs | p50: 0µs | p95: 0µs | p99: 0µs | Max: 130µs
//!    Count:       500000
//!
//! ok
//! test stress_tests::queue::bench_queue_throughput ...
//! 📊 PUSH - Queue Throughput (Sequential)
//!    Throughput:  451531 ops/sec
//!    Total Time:  1.11s
//!    Latency:     Avg: 1µs | p50: 0µs | p95: 1µs | p99: 2µs | Max: 13507µs
//!    Count:       500000
//!
//! ok
//! test stress_tests::store::bench_read_throughput ...
//! 📊 STORE - Read (GET)
//!    Throughput:  8122130 ops/sec
//!    Total Time:  24.62ms
//!    Latency:     Avg: 0µs | p50: 0µs | p95: 0µs | p99: 0µs | Max: 130µs
//!    Count:       200000
//!
//! ok
//! test stress_tests::store::bench_write_throughput ...
//! 📊 STORE - Write (PUT)
//!    Throughput:  8772010 ops/sec
//!    Total Time:  22.80ms
//!    Latency:     Avg: 0µs | p50: 0µs | p95: 0µs | p99: 0µs | Max: 43µs
//!    Count:       200000
//!
//! ok
//! test stress_tests::stream::bench_stream_publish ...
//! 📊 STREAM PUBLISH (Write Confirmed)
//!    Throughput:  78091 ops/sec
//!    Total Time:  6.40s
//!    Latency:     Avg: 12µs | p50: 13µs | p95: 15µs | p99: 21µs | Max: 5807µs
//!    Count:       500000
//!
//! ok
//!
//! test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.79s
//! ```

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
        use nexo::brokers::stream::PubItem;
        const COUNT: usize = 500_000;

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
                generation: joined.generation,
            };

            let stats = manager.sql_stats();
            let mut bench = Benchmark::start(
                "STREAM FETCH+ACK keyless (pages of 100)",
                MESSAGES / PAGE,
            );
            let mut consumed = 0usize;
            while consumed < MESSAGES {
                let start = Instant::now();
                let deliveries = manager
                    .fetch(name, "g", &identity, PAGE, 0)
                    .await
                    .unwrap();
                for d in &deliveries {
                    manager
                        .ack(name, "g", &identity, d.message.seq, d.receipt)
                        .await
                        .unwrap();
                }
                consumed += deliveries.len();
                bench.record(start.elapsed());
            }
            bench.stop();
            sql_stats_diff(&manager, stats, consumed);
        }
    }
}

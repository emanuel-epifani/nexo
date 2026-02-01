use nexo::brokers::queues::QueueManager;
use nexo::brokers::queues::commands::{QueueCreateOptions, PersistenceOptions as QPersistenceOptions};
use nexo::brokers::stream::StreamManager;
use nexo::brokers::stream::commands::{StreamCreateOptions, StreamPublishOptions, PersistenceOptions as SPersistenceOptions};
use nexo::brokers::store::StoreManager;
use nexo::brokers::pub_sub::{PubSubManager, ClientId};
use nexo::config::Config;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Barrier};
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

// --- UTILS ---

pub struct Benchmark {
    pub name: String,
    pub start: Instant,
    pub count: usize,
    pub latencies: Vec<Duration>,
}

impl Benchmark {
    pub fn start(name: &str, count: usize) -> Self {
        println!("🚀 STARTING: {}", name);
        Self {
            name: name.to_string(),
            start: Instant::now(),
            count,
            latencies: Vec::with_capacity(count),
        }
    }

    pub fn record(&mut self, duration: Duration) {
        self.latencies.push(duration);
    }

    pub fn stop(mut self) {
        let total_duration = self.start.elapsed();
        let secs = total_duration.as_secs_f64();
        let ops_sec = self.count as f64 / secs;
        
        self.latencies.sort();
        let len = self.latencies.len();
        
        let p50 = self.latencies.get(len * 50 / 100).unwrap_or(&Duration::ZERO).as_micros();
        let p95 = self.latencies.get(len * 95 / 100).unwrap_or(&Duration::ZERO).as_micros();
        let p99 = self.latencies.get(len * 99 / 100).unwrap_or(&Duration::ZERO).as_micros();
        let max = self.latencies.last().unwrap_or(&Duration::ZERO).as_micros();
        let avg = if len > 0 { self.latencies.iter().sum::<Duration>().as_micros() as u64 / len as u64 } else { 0 };

        // Compact Output Format
        println!("\n📊 {}", self.name);
        println!("   Throughput:  {:.0} ops/sec", ops_sec);
        println!("   Total Time:  {:.2?}", total_duration);
        println!("   Latency:     Avg: {}µs | p50: {}µs | p95: {}µs | p99: {}µs | Max: {}µs", 
            avg, p50, p95, p99, max);
    }
}

async fn setup_env(path: &str) {
    std::env::set_var("QUEUE_ROOT_PERSISTENCE_PATH", path);
    std::env::set_var("STREAM_ROOT_PERSISTENCE_PATH", path);
}

// --- BENCHMARKS ---

#[tokio::test]
async fn bench_01_store_set() {
    let count = 1_000_000;
    let config = Config::global().store.clone();
    let manager = StoreManager::new(config);

    let mut bench = Benchmark::start("STORE: 1M SET operations (In-Memory)", count);

    for i in 0..count {
        let start = Instant::now();
        let key = i.to_string();
        manager.map.set(key, Bytes::from("val"), None);
        bench.record(start.elapsed());
    }
    bench.stop();
}

#[tokio::test]
async fn bench_02_queue_push_fasync() {
    let count = 500_000;
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    setup_env(path).await;

    let mut sys_config = Config::global().queue.clone();
    sys_config.persistence_path = path.to_string();

    let manager = QueueManager::new(sys_config);
    let q_name = format!("bench_q_{}", Uuid::new_v4());
    
    // Config: FAsync with 100ms flush
    let options = QueueCreateOptions {
        persistence: Some(QPersistenceOptions::FileAsync),
        ..Default::default()
    };
    
    manager.declare_queue(q_name.clone(), options).await.unwrap();

    let mut bench = Benchmark::start("QUEUE: 1M PUSH operations (FAsync, flush 100ms)", count);
    let payload = Bytes::from("job_data_payload");

    for _ in 0..count {
        let start = Instant::now();
        manager.push(q_name.clone(), payload.clone(), 0, None).await.unwrap();
        bench.record(start.elapsed());
    }
    
    // Wait for async writer to catch up a bit (optional, just for cleanup)
    tokio::time::sleep(Duration::from_millis(200)).await;
    bench.stop();
}

#[tokio::test]
async fn bench_03_stream_publish_fasync() {
    let count = 1_000_000;
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    setup_env(path).await;

    let mut sys_config = Config::global().stream.clone();
    sys_config.persistence_path = path.to_string();
    
    let manager = StreamManager::new(sys_config);
    let topic = "bench_stream";
    
    // Config: 8 Partitions, FAsync 100ms
    let options = StreamCreateOptions {
        partitions: Some(8),
        persistence: Some(SPersistenceOptions::FileAsync),
        ..Default::default()
    };

    manager.create_topic(topic.to_string(), options).await.unwrap();

    let mut bench = Benchmark::start("STREAM: 1M PUBLISH operations (8 partitions, FAsync 100ms)", count);
    let payload = Bytes::from("event_stream_data");

    for _ in 0..count {
        let start = Instant::now();
        manager.publish(topic, StreamPublishOptions { key: None }, payload.clone()).await.unwrap();
        bench.record(start.elapsed());
    }
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    bench.stop();
}

#[tokio::test]
async fn bench_04_pubsub_fanout() {
    // 10k messaggi * 1k subs = 10M delivery totali
    let msg_count = 10_000;
    let sub_count = 1000;
    let total_delivery = msg_count * sub_count;

    let config = Config::global().pubsub.clone();
    let manager = Arc::new(PubSubManager::new(config));
    let topic = "fanout/global";

    // Shared counter for received messages
    let received_count = Arc::new(AtomicUsize::new(0));
    // Barrier to wait for all subs to be ready
    let barrier = Arc::new(Barrier::new(sub_count + 1));

    // Create 1000 Subscribers
    for i in 0..sub_count {
        let manager = manager.clone();
        let client_id = ClientId(format!("sub_{}", i));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = manager.connect(client_id.clone(), tx);
        let topic = topic.to_string();
        let received_count = received_count.clone();
        let barrier = barrier.clone();

        tokio::spawn(async move {
            // Keep session alive
            let _keep_alive = session;
            
            manager.subscribe(&topic, client_id).await;
            
            // Signal ready
            barrier.wait().await;

            // Consume loop
            while let Some(_) = rx.recv().await {
                received_count.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    // Wait for all subscribers to be ready
    barrier.wait().await;
    

    let start_time = Instant::now();
    let payload = Bytes::from("broadcast_payload");

    // Publish Loop
    let publish_start = Instant::now();
    for _ in 0..msg_count {
        manager.publish(topic, payload.clone(), false).await;
    }
    let publish_time = publish_start.elapsed();
    let publish_ops_sec = msg_count as f64 / publish_time.as_secs_f64();

    // Wait for delivery (Poll counter)
    while received_count.load(Ordering::Relaxed) < total_delivery {
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Timeout safety
        if start_time.elapsed() > Duration::from_secs(60) {
            println!("⚠️  TIMEOUT waiting for delivery. Received: {}/{}", received_count.load(Ordering::Relaxed), total_delivery);
            break;
        }
    }

    let total_duration = start_time.elapsed();
    let secs = total_duration.as_secs_f64();
    let ops_sec = total_delivery as f64 / secs;

    println!("\n📊 PUBSUB: Fanout 1->{} (100k msgs -> 10M deliveries)", sub_count);
    println!("   Ingestion:   {:.0} msg/sec (Publish)", publish_ops_sec);
    println!("   Fanout:      {:.0} msg/sec (Delivery)", ops_sec);
    println!("   Total Time:  {:.2?}", total_duration);
    // Note: No per-request latency here because it's a throughput test
}

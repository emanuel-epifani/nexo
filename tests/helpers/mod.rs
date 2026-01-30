use std::sync::Arc;
use nexo::brokers::queues::QueueManager;
use nexo::brokers::store::StoreManager;
use tempfile::TempDir;
use std::time::{Duration, Instant};
use nexo::brokers::pub_sub::PubSubManager;

pub async fn setup_manager() -> (QueueManager, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let manager = QueueManager::new();
    (manager, temp_dir)
}

pub async fn setup_pubsub_manager() -> (Arc<PubSubManager>, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(PubSubManager::new());
    (manager, temp_dir)
}

pub async fn setup_store_manager() -> (StoreManager, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let manager = StoreManager::new();
    (manager, temp_dir)
}

pub struct Benchmark {
    pub name: String,
    pub start: Instant,
    pub count: usize,
    pub samples: Vec<Duration>,
}

impl Benchmark {
    pub fn start(name: &str, count: usize) -> Self {
        Self {
            name: name.to_string(),
            start: Instant::now(),
            count,
            samples: Vec::with_capacity(count),
        }
    }

    pub fn record(&mut self, duration: Duration) {
        self.samples.push(duration);
    }

    pub fn stop(mut self) {
        let total_duration = self.start.elapsed();
        let secs = total_duration.as_secs_f64();
        let ops_sec = self.count as f64 / secs;
        
        self.samples.sort();
        let len = self.samples.len();
        
        let p50 = self.samples.get(len * 50 / 100).unwrap_or(&Duration::ZERO).as_micros();
        let p95 = self.samples.get(len * 95 / 100).unwrap_or(&Duration::ZERO).as_micros();
        let p99 = self.samples.get(len * 99 / 100).unwrap_or(&Duration::ZERO).as_micros();
        let max = self.samples.last().unwrap_or(&Duration::ZERO).as_micros();

        println!("\n{}", self.name);
        println!(" 🚀 Throughput:  {:.0} ops/sec", ops_sec);
        println!(" ⏱️  Total Time:  {:.2?}", total_duration);
        println!(" 📊 Latency (µs): p50: {} | p95: {} | p99: {} | MAX: {}", 
            p50, p95, p99, max);
        println!(" 📦 Count:       {}\n", self.count);
    }
}

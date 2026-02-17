use std::sync::Arc;
use nexo::brokers::queue::QueueManager;
use nexo::brokers::stream::StreamManager;
use nexo::brokers::store::StoreManager;
use nexo::brokers::pub_sub::PubSubManager;
use nexo::config::Config;
use tempfile::TempDir;
use std::time::{Duration, Instant};

// ==========================================
// SETUP HELPERS
// ==========================================

pub async fn setup_queue_manager() -> (QueueManager, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();
    
    let mut config = Config::global().queue.clone();
    config.persistence_path = path;
    
    let manager = QueueManager::new(config);
    (manager, temp_dir)
}

pub async fn setup_pubsub_manager() -> (Arc<PubSubManager>, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();
    
    let mut config = Config::global().pubsub.clone();
    config.persistence_path = path;
    
    let manager = Arc::new(PubSubManager::new(config));
    (manager, temp_dir)
}

pub async fn setup_stream_manager() -> (StreamManager, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();

    let mut config = Config::global().stream.clone();
    config.persistence_path = path;

    let manager = StreamManager::new(config);
    (manager, temp_dir)
}

pub async fn setup_store_manager() -> (StoreManager, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::global().store.clone();
    let manager = StoreManager::new(config);
    (manager, temp_dir)
}

// ==========================================
// BENCHMARK UTILITY
// ==========================================

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
        let avg = if len > 0 { self.samples.iter().sum::<Duration>().as_micros() as u64 / len as u64 } else { 0 };

        println!("\n📊 {}", self.name);
        println!("   Throughput:  {:.0} ops/sec", ops_sec);
        println!("   Total Time:  {:.2?}", total_duration);
        println!("   Latency:     Avg: {}µs | p50: {}µs | p95: {}µs | p99: {}µs | Max: {}µs", 
            avg, p50, p95, p99, max);
        println!("   Count:       {}\n", self.count);
    }
}

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bytes::Bytes;

#[derive(Clone)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub payload: Bytes,
}

fn build_messages(n: usize) -> VecDeque<Message> {
    let mut q = VecDeque::with_capacity(n);

    for i in 0..n {
        q.push_back(Message {
            offset: i as u64,
            timestamp: i as u64,
            payload: Bytes::from_static(b"hello"), // small payload, realistic
        });
    }

    q
}

fn drain_in_batches(mut q: VecDeque<Message>, batch_size: usize) -> Duration {
    let start = Instant::now();

    while !q.is_empty() {
        let to_remove = batch_size.min(q.len());
        for _ in 0..to_remove {
            q.pop_front();
        }
    }

    start.elapsed()
}

fn main() {
    const TOTAL: usize = 1_000_000;
    let batches = [1_000, 5_000, 10_000];

    println!("Allocating {} messages once per test\n", TOTAL);

    for &batch in &batches {
        let q = build_messages(TOTAL);

        let elapsed = drain_in_batches(q, batch);
        let batch_count = TOTAL / batch;
        let avg_per_batch =
            elapsed.as_secs_f64() * 1_000_000.0 / batch_count as f64;

        println!(
            "Batch size {:>5} → total: {:>6.2?}, avg per batch: {:>8.2} µs",
            batch,
            elapsed,
            avg_per_batch
        );
    }
}
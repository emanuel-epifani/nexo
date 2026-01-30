use nexo::brokers::queues::{QueueManager, QueueConfig};
use nexo::brokers::queues::persistence::types::PersistenceMode;
use bytes::Bytes;
use std::time::{Duration, Instant};
use uuid::Uuid;

mod helpers;
use helpers::{setup_manager, Benchmark};

// =========================================================================================
// 1. FEATURE TESTS (Happy Path + Advanced Logic)
// =========================================================================================

mod features {
    use super::*;

    #[tokio::test]
    async fn test_basic_push_pop_ack() {
        let (manager, _tmp) = setup_manager().await;
        let q = format!("feature_basic_{}", Uuid::new_v4());
        
        manager.declare_queue(q.clone(), QueueConfig::default()).await.unwrap();
        
        // Push
        manager.push(q.clone(), Bytes::from("payload"), 0, None).await.unwrap();
        
        // Pop
        let msg = manager.pop(&q).await.expect("Should pop message");
        assert_eq!(msg.payload, Bytes::from("payload"));
        
        // Ack
        assert!(manager.ack(&q, msg.id).await, "Ack should succeed");
        
        // Check Empty
        assert!(manager.pop(&q).await.is_none(), "Queue should be empty");
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let (manager, _tmp) = setup_manager().await;
        let q = format!("feature_priority_{}", Uuid::new_v4());
        manager.declare_queue(q.clone(), QueueConfig::default()).await.unwrap();

        manager.push(q.clone(), Bytes::from("low"), 0, None).await.unwrap();
        manager.push(q.clone(), Bytes::from("high"), 10, None).await.unwrap();
        manager.push(q.clone(), Bytes::from("mid"), 5, None).await.unwrap();

        let m1 = manager.pop(&q).await.unwrap();
        assert_eq!(m1.payload, Bytes::from("high"));
        
        let m2 = manager.pop(&q).await.unwrap();
        assert_eq!(m2.payload, Bytes::from("mid"));
        
        let m3 = manager.pop(&q).await.unwrap();
        assert_eq!(m3.payload, Bytes::from("low"));
    }

    #[tokio::test]
    async fn test_scheduled_delivery() {
        let (manager, _tmp) = setup_manager().await;
        let q = format!("feature_scheduled_{}", Uuid::new_v4());
        // Configure short visibility to ensure pulse loop runs frequently (min 50ms)
        let config = QueueConfig {
            visibility_timeout_ms: 200, 
            ..Default::default()
        };
        manager.declare_queue(q.clone(), config).await.unwrap();

        // Push with 200ms delay
        manager.push(q.clone(), Bytes::from("future"), 0, Some(200)).await.unwrap();

        // Immediate Pop -> None
        assert!(manager.pop(&q).await.is_none());

        // Wait (Delay 200ms + Pulse Loop latency)
        tokio::time::sleep(Duration::from_millis(350)).await;

        // Pop -> Some
        let msg = manager.pop(&q).await.expect("Should appear after delay");
        assert_eq!(msg.payload, Bytes::from("future"));
    }

    #[tokio::test]
    async fn test_retry_and_dlq() {
        let (manager, _tmp) = setup_manager().await;
        let q = format!("feature_dlq_{}", Uuid::new_v4());
        
        let config = QueueConfig {
            visibility_timeout_ms: 100,
            max_retries: 3, // 3 Retries allow 3 attempts before failure? No. 
                            // Logic: attempts >= max_retries -> DLQ.
                            // If max_retries = 3:
                            // Pop 1 (att=1). Timeout. 1 < 3 -> Requeue.
                            // Pop 2 (att=2). Timeout. 2 < 3 -> Requeue.
                            // Pop 3 (att=3). Timeout. 3 >= 3 -> DLQ.
            ttl_ms: 60000,
            persistence: PersistenceMode::Memory,
        };
        manager.declare_queue(q.clone(), config).await.unwrap();

        manager.push(q.clone(), Bytes::from("fail_me"), 0, None).await.unwrap();

        // Attempt 1
        let m1 = manager.pop(&q).await.unwrap();
        assert_eq!(m1.attempts, 1);
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Attempt 2
        let m2 = manager.pop(&q).await.unwrap();
        assert_eq!(m2.attempts, 2); 
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Attempt 3 (Last)
        let m3 = manager.pop(&q).await.unwrap();
        assert_eq!(m3.attempts, 3);
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Attempt 4 -> Should be gone from Main Queue
        assert!(manager.pop(&q).await.is_none(), "Should be moved to DLQ");

        // Check DLQ
        let dlq_name = format!("{}_dlq", q);
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        let dead = manager.pop(&dlq_name).await.expect("Should be in DLQ");
        assert_eq!(dead.payload, Bytes::from("fail_me"));
    }
}

// =========================================================================================
// 2. PERSISTENCE & RECOVERY
// =========================================================================================

mod persistence {
    use super::*;

    #[tokio::test]
    async fn test_crash_recovery_messages() {
        // Need a fixed path for this test to simulate "same server"
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().to_str().unwrap().to_string();

        // We override config via QueueConfig per queue, assuming we can pass path?
        // Ah, QueueActor reads Global Config for path. This is a limitation for testing isolation.
        // WE CANNOT easily change the path for just this test because Config is static OnceLock.
        // HACK: We will use the default path but unique names, and rely on `Drop` not deleting files.
        // BUT wait, QueueManager does not expose "Restart".
        // To test recovery, we need to:
        // 1. Create Manager, Push, Drop Manager (simulates crash).
        // 2. Create NEW Manager (simulates restart).
        // The problem is `QueueActor` constructor creates the DB.

        // Since we can't change the root path per test easily, we will rely on the real "./data/queues".
        // Ideally we should clean it up or use a test env var before running tests.
        // For now, let's assume we can use a unique queue name.

        let q = format!("persist_crash_{}", Uuid::new_v4());

        {
            let manager1 = QueueManager::new();
            let config = QueueConfig {
                persistence: PersistenceMode::Sync, // Ensure written immediately
                ..Default::default()
            };
            manager1.declare_queue(q.clone(), config).await.unwrap();

            manager1.push(q.clone(), Bytes::from("survivor"), 0, None).await.unwrap();
            // Drop manager1 (Actor stops)
        }

        // Simulating Restart
        {
            let manager2 = QueueManager::new();
            // We must "redeclare" the queue to spawn the actor again,
            // but the actor should find the DB and recover.
            let config = QueueConfig {
                persistence: PersistenceMode::Sync,
                ..Default::default()
            };
            manager2.declare_queue(q.clone(), config).await.unwrap();

            let msg = manager2.pop(&q).await.expect("Message should survive crash");
            assert_eq!(msg.payload, Bytes::from("survivor"));
        }
    }

    #[tokio::test]
    async fn test_scheduled_persistence() {
        let q = format!("persist_scheduled_{}", Uuid::new_v4());

        {
            let manager = QueueManager::new();
            let config = QueueConfig {
                persistence: PersistenceMode::Sync,
                ..Default::default()
            };
            manager.declare_queue(q.clone(), config).await.unwrap();

            // Push with delay 500ms
            manager.push(q.clone(), Bytes::from("future_job"), 0, Some(500)).await.unwrap();
            
            // Immediate check (should be empty)
            assert!(manager.pop(&q).await.is_none());
        }

        // Restart immediately
        {
            let manager2 = QueueManager::new();
            let config = QueueConfig {
                persistence: PersistenceMode::Sync,
                ..Default::default()
            };
            manager2.declare_queue(q.clone(), config).await.unwrap();

            // Should still be invisible (assuming less than 500ms passed)
            assert!(manager2.pop(&q).await.is_none());

            // Wait for delay expiration
            tokio::time::sleep(Duration::from_millis(600)).await;

            // Now it should be visible
            let msg = manager2.pop(&q).await.expect("Scheduled message should appear after delay");
            assert_eq!(msg.payload, Bytes::from("future_job"));
        }
    }

    #[tokio::test]
    async fn test_acked_persistence() {
        let q = format!("persist_acked_{}", Uuid::new_v4());

        {
            let manager = QueueManager::new();
            let config = QueueConfig {
                persistence: PersistenceMode::Sync,
                ..Default::default()
            };
            manager.declare_queue(q.clone(), config).await.unwrap();

            manager.push(q.clone(), Bytes::from("job_done"), 0, None).await.unwrap();
            
            let msg = manager.pop(&q).await.unwrap();
            
            // Ack it (Should delete from DB)
            manager.ack(&q, msg.id).await;
        }

        // Restart
        {
            let manager2 = QueueManager::new();
            let config = QueueConfig {
                persistence: PersistenceMode::Sync,
                ..Default::default()
            };
            manager2.declare_queue(q.clone(), config).await.unwrap();

            // Should be empty (Ack was persisted)
            assert!(manager2.pop(&q).await.is_none(), "Acked message should not reappear");
        }
    }

    #[tokio::test]
    async fn test_inflight_recovery_timeout() {
        let q = format!("persist_inflight_{}", Uuid::new_v4());

        {
            let manager = QueueManager::new();
            let config = QueueConfig {
                visibility_timeout_ms: 500, // Short timeout
                persistence: PersistenceMode::Sync,
                ..Default::default()
            };
            manager.declare_queue(q.clone(), config).await.unwrap();

            manager.push(q.clone(), Bytes::from("job"), 0, None).await.unwrap();

            // Take it (make it InFlight)
            let _ = manager.pop(&q).await.unwrap();

            // Drop manager while message is InFlight (and not Acked)
        }

        tokio::time::sleep(Duration::from_millis(600)).await; // Wait for timeout to theoretically pass

        {
            let manager2 = QueueManager::new();
            let config = QueueConfig {
                visibility_timeout_ms: 500,
                persistence: PersistenceMode::Sync,
                ..Default::default()
            };
            manager2.declare_queue(q.clone(), config).await.unwrap();

            // Should be visible again! (Recovery put it in waiting_for_ack, then expired)
            // Note: Recovery runs, then process_expired runs.
            // Depending on timing, we might need to wait a tick for process_expired.
            tokio::time::sleep(Duration::from_millis(100)).await;

            let msg = manager2.pop(&q).await.expect("InFlight message should expire and reappear");
            assert_eq!(msg.payload, Bytes::from("job"));
        }
    }
}

// =========================================================================================
// 3. PERFORMANCE BENCHMARKS
// =========================================================================================

mod performance {
    use super::*;

    const COUNT: usize = 200_000;

    #[tokio::test]
    async fn bench_memory_throughput() {
        let (manager, _tmp) = setup_manager().await;
        let q = format!("bench_mem_{}", Uuid::new_v4());
        let config = QueueConfig {
            persistence: PersistenceMode::Memory,
            ..Default::default()
        };
        manager.declare_queue(q.clone(), config).await.unwrap();

        let mut bench = Benchmark::start("PUSH - Memory (no persistency)", COUNT);
        for _ in 0..COUNT {
            let start = Instant::now();
            manager.push(q.clone(), Bytes::from("data"), 0, None).await.unwrap();
            bench.record(start.elapsed());
        }
        bench.stop();
    }

    #[tokio::test]
    async fn bench_fsync_throughput() {
        let (manager, _tmp) = setup_manager().await;
        let q = format!("bench_sync_{}", Uuid::new_v4());
        let config = QueueConfig {
            persistence: PersistenceMode::Sync,
            ..Default::default()
        };
        manager.declare_queue(q.clone(), config).await.unwrap();

        let mut bench = Benchmark::start("PUSH - FSync (write on disdk once per message)", COUNT);
        for _ in 0..COUNT {
            let start = Instant::now();
            manager.push(q.clone(), Bytes::from("data"), 0, None).await.unwrap();
            bench.record(start.elapsed());
        }
        bench.stop();
    }

    #[tokio::test]
    async fn bench_fasync_throughput() {
        let (manager, _tmp) = setup_manager().await;
        let q = format!("bench_async_{}", Uuid::new_v4());
        let config = QueueConfig {
            persistence: PersistenceMode::Async { flush_ms: 200 },
            ..Default::default()
        };
        manager.declare_queue(q.clone(), config).await.unwrap();

        let mut bench = Benchmark::start("PUSH - FAsync (write on disk once every x ms)", COUNT);
        for _ in 0..COUNT {
            let start = Instant::now();
            manager.push(q.clone(), Bytes::from("data"), 0, None).await.unwrap();
            bench.record(start.elapsed());
        }
        // Wait for flush to happen in background (optional, just to be fair to disk)
        tokio::time::sleep(Duration::from_millis(200)).await;
        bench.stop();
    }
}



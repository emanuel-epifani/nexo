use nexo::brokers::queue::{QueueManager};
use nexo::brokers::queue::commands::{QueueCreateOptions, PersistenceOptions};
use bytes::Bytes;
use std::time::{Duration, Instant};
use uuid::Uuid;

mod helpers;
use helpers::{setup_queue_manager, Benchmark};



#[cfg(test)]
mod queue_tests {
    use super::*;

    // =========================================================================================
    // 1. FEATURE TESTS (Happy Path + Advanced Logic)
    // =========================================================================================

    mod features {
        use super::*;

        #[tokio::test]
        async fn test_basic_push_pop_ack() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("feature_basic_{}", Uuid::new_v4());

            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

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
        async fn test_fifo_ordering() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("feature_fifo_{}", Uuid::new_v4());
            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

            manager.push(q.clone(), Bytes::from("msg1"), 0, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("msg2"), 0, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("msg3"), 0, None).await.unwrap();

            let m1 = manager.pop(&q).await.unwrap();
            assert_eq!(m1.payload, Bytes::from("msg1"));

            let m2 = manager.pop(&q).await.unwrap();
            assert_eq!(m2.payload, Bytes::from("msg2"));

            let m3 = manager.pop(&q).await.unwrap();
            assert_eq!(m3.payload, Bytes::from("msg3"));
        }

        #[tokio::test]
        async fn test_priority_ordering() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("feature_priority_{}", Uuid::new_v4());
            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

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
        async fn test_priority_than_fifo_ordering() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("feature_priority_{}", Uuid::new_v4());
            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

            //test PRIORITY
            manager.push(q.clone(), Bytes::from("low"), 0, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("high"), 10, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("mid"), 7, None).await.unwrap();
            //normal, test FIFO
            manager.push(q.clone(), Bytes::from("msg1"), 4, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("msg2"), 4, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("msg3"), 4, None).await.unwrap();

            let m1 = manager.pop(&q).await.unwrap();
            assert_eq!(m1.payload, Bytes::from("high"));
            let m2 = manager.pop(&q).await.unwrap();
            assert_eq!(m2.payload, Bytes::from("mid"));

            let m3 = manager.pop(&q).await.unwrap();
            assert_eq!(m3.payload, Bytes::from("msg1"));
            let m4 = manager.pop(&q).await.unwrap();
            assert_eq!(m4.payload, Bytes::from("msg2"));
            let m5 = manager.pop(&q).await.unwrap();
            assert_eq!(m5.payload, Bytes::from("msg3"));

            let m6 = manager.pop(&q).await.unwrap();
            assert_eq!(m6.payload, Bytes::from("low"));
        }


        #[tokio::test]
        async fn test_scheduled_delivery() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("feature_scheduled_{}", Uuid::new_v4());
            // Configure short visibility to ensure pulse loop runs frequently (min 50ms)
            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(200),
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            let delay_ms = 200;
            // Push with delay
            manager.push(q.clone(), Bytes::from("future"), 0, Some(delay_ms)).await.unwrap();

            // Immediate Pop -> None
            assert!(manager.pop(&q).await.is_none());

            // Wait (Delay + Buffer)
            tokio::time::sleep(Duration::from_millis(delay_ms + 150)).await;

            // Pop -> Some
            let msg = manager.pop(&q).await.expect("Should appear after delay");
            assert_eq!(msg.payload, Bytes::from("future"));
        }

        #[tokio::test]
        async fn test_retry_and_dlq() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("feature_dlq_{}", Uuid::new_v4());

            let visibility_timeout = 100;
            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(visibility_timeout),
                max_retries: Some(3), // 3 Retries allow 3 attempts before failure? No.
                // Logic: attempts >= max_retries -> DLQ.
                // If max_retries = 3:
                // Pop 1 (att=1). Timeout. 1 < 3 -> Requeue.
                // Pop 2 (att=2). Timeout. 2 < 3 -> Requeue.
                // Pop 3 (att=3). Timeout. 3 >= 3 -> DLQ.
                ttl_ms: Some(60000),
                persistence: Some(PersistenceOptions::FileSync),
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            manager.push(q.clone(), Bytes::from("fail_me"), 0, None).await.unwrap();

            // Attempt 1
            let m1 = manager.pop(&q).await.unwrap();
            assert_eq!(m1.attempts, 1);

            // Wait for visibility timeout + buffer
            tokio::time::sleep(Duration::from_millis(visibility_timeout + 50)).await;

            // Attempt 2
            let m2 = manager.pop(&q).await.unwrap();
            assert_eq!(m2.attempts, 2);
            tokio::time::sleep(Duration::from_millis(visibility_timeout + 50)).await;

            // Attempt 3 (Last)
            let m3 = manager.pop(&q).await.unwrap();
            assert_eq!(m3.attempts, 3);
            let msg_id = m3.id;
            
            // Wait for visibility timeout + buffer for actor to wake up and process
            tokio::time::sleep(Duration::from_millis(visibility_timeout + 150)).await;

            // Attempt 4 -> Should be gone from Main Queue (moved to internal DLQ)
            // The actor loop should have woken up automatically and moved to DLQ
            assert!(manager.pop(&q).await.is_none(), "Should be moved to DLQ");

            // Verify message is in DLQ using new DLQ methods
            let (total, dlq_msgs) = manager.peek_dlq(&q, 10, 0).await.unwrap();
            assert_eq!(total, 1, "Total should be 1");
            assert_eq!(dlq_msgs.len(), 1, "Should have 1 message in DLQ");
            assert_eq!(dlq_msgs[0].payload, Bytes::from("fail_me"));
            assert_eq!(dlq_msgs[0].id, msg_id);

            // Test move_to_queue (replay)
            let moved = manager.move_to_queue(&q, msg_id).await.unwrap();
            assert!(moved, "Should successfully move message back to main queue");

            // Verify it's back in main queue
            let replayed = manager.pop(&q).await.expect("Message should be back in main queue");
            assert_eq!(replayed.payload, Bytes::from("fail_me"));
            // After move_to_queue (attempts=0) + pop (attempts++), should be 1
            assert_eq!(replayed.attempts, 1, "Attempts should be 1 after replay and pop");

            // Ack it to clean up
            manager.ack(&q, replayed.id).await;
        }

        #[tokio::test]
        async fn test_delete_queue() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_del_{}", Uuid::new_v4());
            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

            manager.push(q.clone(), Bytes::from("msg"), 0, None).await.unwrap();
            assert!(manager.exists(&q).await);

            manager.delete_queue(q.clone()).await.unwrap();

            assert!(!manager.exists(&q).await, "Queue should not exist in RAM");

            // Try to pop -> None
            assert!(manager.pop(&q).await.is_none());

            // Restart to check disk cleanup
            let path = _tmp.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path;
            let manager2 = QueueManager::new(sys_config);

            // Since we can't easily check if file exists without knowing path logic,
            // we check if declaring it again results in an empty queue (no recovery)
            manager2.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();
            assert!(manager2.pop(&q).await.is_none(), "Queue should be empty after delete and recreation");
        }
    }

    // =========================================================================================
    // 2. ADVANCED FEATURES (Batching, Long Polling, Snapshots)
    // =========================================================================================

    mod advanced {
        use super::*;

        #[tokio::test]
        async fn test_batch_consume() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_batch_{}", Uuid::new_v4());
            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

            // Push 10 messages
            for i in 0..10 {
                manager.push(q.clone(), Bytes::from(format!("msg_{}", i)), 0, None).await.unwrap();
            }

            // Consume 4
            let batch1 = manager.consume_batch(q.clone(), Some(4), None).await.unwrap();
            assert_eq!(batch1.len(), 4);
            // Queue is FIFO for same priority
            assert_eq!(batch1[0].payload, Bytes::from("msg_0"));

            // Consume 6 (Remaining)
            let batch2 = manager.consume_batch(q.clone(), Some(10), None).await.unwrap();
            assert_eq!(batch2.len(), 6);

            // Consume (Empty)
            let batch3 = manager.consume_batch(q.clone(), Some(10), None).await.unwrap();
            assert!(batch3.is_empty());
        }

        #[tokio::test]
        async fn test_long_polling() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_poll_{}", Uuid::new_v4());
            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

            // Spawn consumer in background
            let manager_clone = manager.clone();
            let q_clone = q.clone();

            let handle = tokio::spawn(async move {
                let start = Instant::now();
                // Poll with 1000ms wait
                let batch = manager_clone.consume_batch(q_clone, Some(1), Some(1000)).await.unwrap();
                (batch, start.elapsed())
            });

            // Wait a bit to ensure consumer is parked
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Push message
            manager.push(q.clone(), Bytes::from("wake_up"), 0, None).await.unwrap();

            // Join consumer
            let (batch, elapsed) = handle.await.unwrap();

            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0].payload, Bytes::from("wake_up"));
            // Should be roughly 200ms, definitely less than 1000ms
            assert!(elapsed < Duration::from_millis(800), "Should wake up immediately on push");
            assert!(elapsed >= Duration::from_millis(200), "Should wait until push");
        }

        #[tokio::test]
        async fn test_long_polling_timeout() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_poll_to_{}", Uuid::new_v4());
            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

            let start = Instant::now();
            // Wait 300ms, expect empty
            let batch = manager.consume_batch(q.clone(), Some(1), Some(300)).await.unwrap();
            let elapsed = start.elapsed();

            assert!(batch.is_empty());
            assert!(elapsed >= Duration::from_millis(300), "Should wait at least 300ms");
            // Allow some scheduling jitter
            assert!(elapsed < Duration::from_millis(450), "Should timeout reasonably fast");
        }

        #[tokio::test]
        async fn test_snapshot_metrics() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_snap_{}", Uuid::new_v4());
            manager.create_queue(q.clone(), QueueCreateOptions::default()).await.unwrap();

            // 3 Pending
            manager.push(q.clone(), Bytes::from("p1"), 0, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("p2"), 0, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("p3"), 0, None).await.unwrap();

            // 1 InFlight
            let _ = manager.pop(&q).await.unwrap();

            // 1 Scheduled
            manager.push(q.clone(), Bytes::from("s1"), 0, Some(10000)).await.unwrap();

            let snapshot = manager.get_snapshot().await;
            let queue_snap = snapshot.iter().find(|qs| qs.name == q).expect("Queue not found in snapshot");

            // Verify counts based on vectors length in summary
            // 2 Pending (p2, p3) - p1 was popped
            assert_eq!(queue_snap.pending.len(), 2);
            // 1 InFlight (p1)
            assert_eq!(queue_snap.inflight.len(), 1);
            // 1 Scheduled (s1)
            assert_eq!(queue_snap.scheduled.len(), 1);
        }

    }

    // =========================================================================================
    // 2. PERSISTENCE & RECOVERY
    // =========================================================================================

    mod persistence {
        use super::*;

        #[tokio::test]
        async fn test_crash_recovery_messages() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q = format!("persist_crash_{}", Uuid::new_v4());

            {
                let manager1 = QueueManager::new(sys_config.clone());
                let config = QueueCreateOptions {
                    persistence: Some(PersistenceOptions::FileSync), // Ensure written immediately
                    ..Default::default()
                };
                manager1.create_queue(q.clone(), config).await.unwrap();

                manager1.push(q.clone(), Bytes::from("survivor"), 0, None).await.unwrap();
                // Drop manager1 (Actor stops)
            }

            // Simulating Restart
            {
                let manager2 = QueueManager::new(sys_config.clone());
                // We must "redeclare" the queue to spawn the actor again,
                // but the actor should find the DB and recover.
                let config = QueueCreateOptions {
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                manager2.create_queue(q.clone(), config).await.unwrap();

                let msg = manager2.pop(&q).await.expect("Message should survive crash");
                assert_eq!(msg.payload, Bytes::from("survivor"));
            }
        }

        #[tokio::test]
        async fn test_scheduled_persistence() {
            let q = format!("persist_scheduled_{}", Uuid::new_v4());
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            {
                let manager = QueueManager::new(sys_config.clone());
                let config = QueueCreateOptions {
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                // Push with delay 500ms
                manager.push(q.clone(), Bytes::from("future_job"), 0, Some(500)).await.unwrap();

                // Immediate check (should be empty)
                assert!(manager.pop(&q).await.is_none());
            }

            // Restart immediately
            {
                let manager2 = QueueManager::new(sys_config.clone());
                let config = QueueCreateOptions {
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                manager2.create_queue(q.clone(), config).await.unwrap();

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
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            {
                let manager = QueueManager::new(sys_config.clone());
                let config = QueueCreateOptions {
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                manager.push(q.clone(), Bytes::from("job_done"), 0, None).await.unwrap();

                let msg = manager.pop(&q).await.unwrap();

                // Ack it (Should delete from DB)
                manager.ack(&q, msg.id).await;
            }

            // Restart
            {
                let manager2 = QueueManager::new(sys_config.clone());
                let config = QueueCreateOptions {
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                manager2.create_queue(q.clone(), config).await.unwrap();

                // Should be empty (Ack was persisted)
                assert!(manager2.pop(&q).await.is_none(), "Acked message should not reappear");
            }
        }

        #[tokio::test]
        async fn test_inflight_recovery_timeout() {
            let q = format!("persist_inflight_{}", Uuid::new_v4());
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            {
                let manager = QueueManager::new(sys_config.clone());
                let config = QueueCreateOptions {
                    visibility_timeout_ms: Some(500), // Short timeout
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                manager.push(q.clone(), Bytes::from("job"), 0, None).await.unwrap();

                // Take it (make it InFlight)
                let _ = manager.pop(&q).await.unwrap();

                // Drop manager while message is InFlight (and not Acked)
            }

            tokio::time::sleep(Duration::from_millis(600)).await; // Wait for timeout to theoretically pass

            {
                let manager2 = QueueManager::new(sys_config.clone());
                let config = QueueCreateOptions {
                    visibility_timeout_ms: Some(500),
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                manager2.create_queue(q.clone(), config).await.unwrap();

                // Should be visible again! (Recovery put it in waiting_for_ack, then expired)
                // Note: Recovery runs, then process_expired runs.
                // Depending on timing, we might need to wait a tick for process_expired.
                tokio::time::sleep(Duration::from_millis(100)).await;

                let msg = manager2.pop(&q).await.expect("InFlight message should expire and reappear");
                assert_eq!(msg.payload, Bytes::from("job"));
            }
        }

        #[tokio::test]
        async fn test_warm_start_auto_restore() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q1 = format!("warm_q1_{}", Uuid::new_v4());
            let q2 = format!("warm_q2_{}", Uuid::new_v4());

            // Phase 1: Create queues and add messages
            {
                let manager = QueueManager::new(sys_config.clone());
                
                let config = QueueCreateOptions {
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                
                manager.create_queue(q1.clone(), config.clone()).await.unwrap();
                manager.create_queue(q2.clone(), config.clone()).await.unwrap();

                manager.push(q1.clone(), Bytes::from("msg1_q1"), 0, None).await.unwrap();
                manager.push(q1.clone(), Bytes::from("msg2_q1"), 0, None).await.unwrap();
                manager.push(q2.clone(), Bytes::from("msg1_q2"), 0, None).await.unwrap();

                // Drop manager (simulating server shutdown)
            }

            // Small delay to ensure files are flushed
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Phase 2: Restart manager WITHOUT calling create_queue
            // Warm start should automatically discover and restore queues
            {
                let manager2 = QueueManager::new(sys_config.clone());

                // Wait a bit for warm start to complete
                tokio::time::sleep(Duration::from_millis(200)).await;

                // Verify queues exist (warm start should have restored them)
                assert!(manager2.exists(&q1).await, "Queue 1 should be auto-restored");
                assert!(manager2.exists(&q2).await, "Queue 2 should be auto-restored");

                // Verify messages are recovered
                let msg1 = manager2.pop(&q1).await.expect("Should recover msg1_q1");
                assert_eq!(msg1.payload, Bytes::from("msg1_q1"));

                let msg2 = manager2.pop(&q1).await.expect("Should recover msg2_q1");
                assert_eq!(msg2.payload, Bytes::from("msg2_q1"));

                let msg3 = manager2.pop(&q2).await.expect("Should recover msg1_q2");
                assert_eq!(msg3.payload, Bytes::from("msg1_q2"));

                // Verify snapshot includes restored queues
                let snapshot = manager2.get_snapshot().await;
                let queue_names: Vec<String> = snapshot.iter().map(|q| q.name.clone()).collect();
                assert!(queue_names.contains(&q1), "Snapshot should include q1");
                assert!(queue_names.contains(&q2), "Snapshot should include q2");
            }
        }

        #[tokio::test]
        async fn test_dlq_delete_and_purge() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("dlq_ops_{}", Uuid::new_v4());

            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(100),
                max_retries: Some(0), // Immediate DLQ
                persistence: Some(PersistenceOptions::FileSync),
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            // Push 3 messages that will fail
            manager.push(q.clone(), Bytes::from("msg1"), 0, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("msg2"), 0, None).await.unwrap();
            manager.push(q.clone(), Bytes::from("msg3"), 0, None).await.unwrap();

            // Pop all 3 and let them timeout
            let m1 = manager.pop(&q).await.unwrap();
            let _m2 = manager.pop(&q).await.unwrap();
            let _m3 = manager.pop(&q).await.unwrap();

            // Wait for visibility timeout + buffer for actor to wake up and process
            tokio::time::sleep(Duration::from_millis(250)).await;

            // Verify all 3 in DLQ (actor should have moved them automatically)
            let (total, dlq_msgs) = manager.peek_dlq(&q, 10, 0).await.unwrap();
            assert_eq!(total, 3, "Total should be 3");
            assert_eq!(dlq_msgs.len(), 3, "Should have 3 messages in DLQ");

            // Test delete_dlq (remove one specific message)
            let deleted = manager.delete_dlq(&q, m1.id).await.unwrap();
            assert!(deleted, "Should successfully delete message from DLQ");

            // Verify only 2 left
            let (total, dlq_msgs) = manager.peek_dlq(&q, 10, 0).await.unwrap();
            assert_eq!(total, 2, "Total should be 2");
            assert_eq!(dlq_msgs.len(), 2, "Should have 2 messages left in DLQ");

            // Test purge_dlq (remove all)
            let purged_count = manager.purge_dlq(&q).await.unwrap();
            assert_eq!(purged_count, 2, "Should purge 2 messages");

            // Verify DLQ is empty
            let (total, dlq_msgs) = manager.peek_dlq(&q, 10, 0).await.unwrap();
            assert_eq!(total, 0, "Total should be 0");
            assert_eq!(dlq_msgs.len(), 0, "DLQ should be empty after purge");
        }

        #[tokio::test]
        async fn test_dlq_persistence_after_restart() {
            // Test that ensures DLQ (now internal to queue actor) survives a restart
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q = format!("dlq_test_{}", Uuid::new_v4());

            // Phase 1: Trigger DLQ move
            {
                let manager = QueueManager::new(sys_config.clone());
                // Create with max_retries = 0 (1st timeout -> DLQ immediately)
                let config = QueueCreateOptions {
                    visibility_timeout_ms: Some(100),
                    max_retries: Some(0),
                    persistence: Some(PersistenceOptions::FileSync),
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                // Push message destined to fail
                manager.push(q.clone(), Bytes::from("stay_in_dlq"), 0, None).await.unwrap();

                // Attempt 1 (and only attempt allowed)
                let _ = manager.pop(&q).await.unwrap();
                
                // Wait for visibility timeout + buffer for actor to wake up and process
                tokio::time::sleep(Duration::from_millis(250)).await;

                // Verify main queue is empty (message moved to internal DLQ by actor)
                assert!(manager.pop(&q).await.is_none(), "Main queue should be empty");

                // Verify message is in DLQ
                let (total, dlq_msgs) = manager.peek_dlq(&q, 10, 0).await.unwrap();
                assert_eq!(total, 1);
                assert_eq!(dlq_msgs.len(), 1, "Should have 1 message in DLQ");
                assert_eq!(dlq_msgs[0].payload, Bytes::from("stay_in_dlq"));
            }

            // Small delay for flush
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Phase 2: Restart
            {
                let manager2 = QueueManager::new(sys_config.clone());
                tokio::time::sleep(Duration::from_millis(200)).await;

                // Queue should be auto-restored with DLQ messages
                assert!(manager2.exists(&q).await, "Queue should survive restart");

                // Main queue should still be empty (DLQ message persisted internally)
                assert!(manager2.pop(&q).await.is_none(), "Main queue should still be empty after restart");
                
                // Verify DLQ message survived restart
                let (total, dlq_msgs) = manager2.peek_dlq(&q, 10, 0).await.unwrap();
                assert_eq!(total, 1);
                assert_eq!(dlq_msgs.len(), 1, "DLQ message should survive restart");
                assert_eq!(dlq_msgs[0].payload, Bytes::from("stay_in_dlq"));
            }
        }
    }

    // =========================================================================================
    // 3. PERFORMANCE BENCHMARKS
    // =========================================================================================

    // cargo test --release --test queue_tests performance -- --test-threads=1 --nocapture
    mod performance {
        use super::*;

        const COUNT: usize = 200_000;

        #[tokio::test]
        async fn bench_fsync_throughput() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("bench_sync_{}", Uuid::new_v4());
            let config = QueueCreateOptions {
                persistence: Some(PersistenceOptions::FileSync),
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

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
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("bench_async_{}", Uuid::new_v4());
            let config = QueueCreateOptions {
                persistence: Some(PersistenceOptions::FileAsync),
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

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

}
use bytes::Bytes;
use nexo::brokers::queue::options::QueueCreateOptions;
use nexo::brokers::queue::QueueManager;
use nexo::brokers::{BrokerErrorKind, ProvisionOutcome};
use std::time::{Duration, Instant};
use uuid::Uuid;

mod common;
use common::setup_queue_manager;

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

            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            // Push
            manager
                .push(q.clone(), Bytes::from("payload"), 0)
                .await
                .unwrap();

            // Pop
            let msg = manager.pop(&q).await.expect("Should pop message");
            assert_eq!(msg.payload, Bytes::from("payload"));

            // Ack
            assert!(
                manager.ack(&q, msg.id, msg.delivery_token).await,
                "Ack should succeed"
            );

            // Check Empty
            assert!(manager.pop(&q).await.is_none(), "Queue should be empty");
        }

        #[tokio::test]
        async fn test_provisioning_result_describe_and_conflict() {
            let (manager, _tmp) = setup_queue_manager().await;
            let name = format!("feature_definition_{}", Uuid::new_v4());
            let options = QueueCreateOptions {
                visibility_timeout_ms: Some(12_345),
                max_deliveries: Some(7),
            };

            let created = manager
                .create_queue(name.clone(), options.clone())
                .await
                .unwrap();
            assert_eq!(created.outcome, ProvisionOutcome::Created);
            assert_eq!(created.definition.name, name);
            assert_eq!(created.definition.config.visibility_timeout_ms, 12_345);
            assert_eq!(created.definition.config.max_deliveries, 7);
            assert_eq!(manager.describe(&name).await.unwrap(), created.definition);

            let unchanged = manager.create_queue(name.clone(), options).await.unwrap();
            assert_eq!(unchanged.outcome, ProvisionOutcome::Unchanged);
            assert_eq!(unchanged.definition, created.definition);

            let error = manager
                .create_queue(
                    name,
                    QueueCreateOptions {
                        visibility_timeout_ms: Some(12_345),
                        max_deliveries: Some(8),
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind, BrokerErrorKind::ResourceConfigConflict);
            assert_eq!(
                error.details.unwrap()["differences"][0]["path"],
                "config.maxDeliveries"
            );
        }

        #[tokio::test]
        async fn test_fifo_ordering() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("feature_fifo_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("msg2"), 0)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("msg3"), 0)
                .await
                .unwrap();

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
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push(q.clone(), Bytes::from("low"), 0)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("high"), 10)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("mid"), 5)
                .await
                .unwrap();

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
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            //test PRIORITY
            manager
                .push(q.clone(), Bytes::from("low"), 0)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("high"), 10)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("mid"), 7)
                .await
                .unwrap();
            //normal, test FIFO
            manager
                .push(q.clone(), Bytes::from("msg1"), 4)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("msg2"), 4)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("msg3"), 4)
                .await
                .unwrap();

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
        async fn test_retry_and_dlq() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("feature_dlq_{}", Uuid::new_v4());

            let visibility_timeout = 100;
            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(visibility_timeout),
                max_deliveries: Some(3), // 3 deliveries: pop→timeout→requeue, pop→timeout→requeue, pop→timeout→DLQ
                                         // Logic: attempts >= max_deliveries -> DLQ.
                                         // If max_deliveries = 3:
                                         // Pop 1 (att=1). Timeout. 1 < 3 -> Requeue.
                                         // Pop 2 (att=2). Timeout. 2 < 3 -> Requeue.
                                         // Pop 3 (att=3). Timeout. 3 >= 3 -> DLQ.
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            manager
                .push(q.clone(), Bytes::from("fail_me"), 0)
                .await
                .unwrap();

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
            let replayed = manager
                .pop(&q)
                .await
                .expect("Message should be back in main queue");
            assert_eq!(replayed.payload, Bytes::from("fail_me"));
            // After move_to_queue (attempts=0) + pop (attempts++), should be 1
            assert_eq!(
                replayed.attempts, 1,
                "Attempts should be 1 after replay and pop"
            );

            // Ack it to clean up
            manager.ack(&q, replayed.id, replayed.delivery_token).await;
        }

        #[tokio::test]
        async fn test_delete_queue() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_del_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push(q.clone(), Bytes::from("msg"), 0)
                .await
                .unwrap();
            assert!(manager.exists(&q).await);

            manager.delete_queue(q.clone()).await.unwrap();

            assert!(!manager.exists(&q).await, "Queue should not exist in RAM");

            // Try to pop -> None
            assert!(manager.pop(&q).await.is_none());

            // Restart to check disk cleanup
            let path = _tmp.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path;
            let manager2 = std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config)));

            // Since we can't easily check if file exists without knowing path logic,
            // we check if declaring it again results in an empty queue (no recovery)
            manager2
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();
            assert!(
                manager2.pop(&q).await.is_none(),
                "Queue should be empty after delete and recreation"
            );
        }
    }

    // =========================================================================================
    // 2. ADVANCED FEATURES (Batching, Long Polling, Snapshots)
    // =========================================================================================

    mod advanced {
        use super::*;

        #[tokio::test]
        async fn test_consume_batch_rejects_zero() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_batch_zero_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push(q.clone(), Bytes::from("msg"), 0)
                .await
                .unwrap();

            let result = manager.consume_batch(q.clone(), Some(0), Some(100)).await;
            assert!(result.is_err(), "batch_size=0 should return error");
            assert_eq!(result.unwrap_err().message, "batch_size must be >= 1");
        }

        #[tokio::test]
        async fn test_batch_consume() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_batch_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            // Push 10 messages
            for i in 0..10 {
                manager
                    .push(q.clone(), Bytes::from(format!("msg_{}", i)), 0)
                    .await
                    .unwrap();
            }

            // Consume 4
            let batch1 = manager
                .consume_batch(q.clone(), Some(4), None)
                .await
                .unwrap();
            assert_eq!(batch1.len(), 4);
            // Queue is FIFO for same priority
            assert_eq!(batch1[0].payload, Bytes::from("msg_0"));

            // Consume 6 (Remaining)
            let batch2 = manager
                .consume_batch(q.clone(), Some(10), None)
                .await
                .unwrap();
            assert_eq!(batch2.len(), 6);

            // Consume (Empty)
            let batch3 = manager
                .consume_batch(q.clone(), Some(10), None)
                .await
                .unwrap();
            assert!(batch3.is_empty());
        }

        #[tokio::test]
        async fn test_long_polling() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_poll_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            // Spawn consumer in background
            let manager_clone = manager.clone();
            let q_clone = q.clone();

            let handle = tokio::spawn(async move {
                // Poll with 1000ms wait
                let batch = manager_clone
                    .consume_batch(q_clone, Some(1), Some(1000))
                    .await
                    .unwrap();
                batch
            });

            let start = Instant::now();

            // Wait a bit to ensure consumer is parked
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Push message
            manager
                .push(q.clone(), Bytes::from("wake_up"), 0)
                .await
                .unwrap();

            // Join consumer
            let batch = handle.await.unwrap();
            let elapsed = start.elapsed();

            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0].payload, Bytes::from("wake_up"));
            // Total time in main thread should be roughly 200ms (the sleep) + small overhead
            assert!(
                elapsed < Duration::from_millis(800),
                "Should wake up immediately on push"
            );
            assert!(
                elapsed >= Duration::from_millis(150),
                "Should have waited for our sleep"
            );
        }

        #[tokio::test]
        async fn test_long_polling_timeout() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_poll_to_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            let start = Instant::now();
            // Wait 300ms, expect empty
            let batch = manager
                .consume_batch(q.clone(), Some(1), Some(300))
                .await
                .unwrap();
            let elapsed = start.elapsed();

            assert!(batch.is_empty());
            assert!(
                elapsed >= Duration::from_millis(300),
                "Should wait at least 300ms"
            );
            // Allow some scheduling jitter
            assert!(
                elapsed < Duration::from_millis(450),
                "Should timeout reasonably fast"
            );
        }

        #[tokio::test]
        async fn test_long_polling_uses_earliest_waiter_expiration() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_poll_order_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            let manager_long = manager.clone();
            let q_long = q.clone();
            let long_handle = tokio::spawn(async move {
                let started = Instant::now();
                let batch = manager_long
                    .consume_batch(q_long, Some(1), Some(900))
                    .await
                    .unwrap();
                (started.elapsed(), batch)
            });

            tokio::time::sleep(Duration::from_millis(50)).await;

            let manager_short = manager.clone();
            let q_short = q.clone();
            let short_handle = tokio::spawn(async move {
                let started = Instant::now();
                let batch = manager_short
                    .consume_batch(q_short, Some(1), Some(150))
                    .await
                    .unwrap();
                (started.elapsed(), batch)
            });

            let (short_elapsed, short_batch) =
                tokio::time::timeout(Duration::from_millis(400), short_handle)
                    .await
                    .expect("Short waiter should time out before the long waiter")
                    .unwrap();

            assert!(short_batch.is_empty());
            assert!(
                short_elapsed >= Duration::from_millis(150),
                "Short waiter should wait for its own timeout"
            );
            assert!(
                short_elapsed < Duration::from_millis(350),
                "Short waiter should not be delayed by an earlier long waiter"
            );
            assert!(
                !long_handle.is_finished(),
                "Long waiter should still be pending after the short timeout"
            );

            let (long_elapsed, long_batch) = long_handle.await.unwrap();
            assert!(long_batch.is_empty());
            assert!(
                long_elapsed >= Duration::from_millis(850),
                "Long waiter should remain parked until its own timeout"
            );
        }

        #[tokio::test]
        async fn test_long_polling_dispatch_both_waiters_get_messages() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("adv_poll_fifo_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            // Two concurrent waiters
            let manager_a = manager.clone();
            let q_a = q.clone();
            let handle_a = tokio::spawn(async move {
                manager_a
                    .consume_batch(q_a, Some(1), Some(2000))
                    .await
                    .unwrap()
            });

            tokio::time::sleep(Duration::from_millis(50)).await;

            let manager_b = manager.clone();
            let q_b = q.clone();
            let handle_b = tokio::spawn(async move {
                manager_b
                    .consume_batch(q_b, Some(1), Some(2000))
                    .await
                    .unwrap()
            });

            tokio::time::sleep(Duration::from_millis(50)).await;

            // Push two messages — both waiters should each get one
            manager
                .push(q.clone(), Bytes::from("msg_x"), 0)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("msg_y"), 0)
                .await
                .unwrap();

            let batch_a = tokio::time::timeout(Duration::from_millis(500), handle_a)
                .await
                .expect("Waiter A should receive a message")
                .unwrap();
            let batch_b = tokio::time::timeout(Duration::from_millis(500), handle_b)
                .await
                .expect("Waiter B should receive a message")
                .unwrap();

            assert_eq!(batch_a.len(), 1);
            assert_eq!(batch_b.len(), 1);

            // Both messages delivered, each to a different waiter (order is best-effort)
            let mut payloads: Vec<Bytes> =
                vec![batch_a[0].payload.clone(), batch_b[0].payload.clone()];
            payloads.sort();
            assert_eq!(payloads, vec![Bytes::from("msg_x"), Bytes::from("msg_y")]);

            assert!(
                manager
                    .ack(&q, batch_a[0].id, batch_a[0].delivery_token)
                    .await
            );
            assert!(
                manager
                    .ack(&q, batch_b[0].id, batch_b[0].delivery_token)
                    .await
            );
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
                let manager1 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                let config = QueueCreateOptions {
                    ..Default::default()
                };
                manager1.create_queue(q.clone(), config).await.unwrap();

                manager1
                    .push(q.clone(), Bytes::from("survivor"), 0)
                    .await
                    .unwrap();

                // Wait for async flush before dropping manager
                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // Simulating Restart
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                // We must "redeclare" the queue to spawn the actor again,
                // but the actor should find the DB and recover.
                let config = QueueCreateOptions {
                    ..Default::default()
                };
                manager2.create_queue(q.clone(), config).await.unwrap();

                let msg = manager2
                    .pop(&q)
                    .await
                    .expect("Message should survive crash");
                assert_eq!(msg.payload, Bytes::from("survivor"));
            }
        }

        #[tokio::test]
        async fn test_fifo_order_survives_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q = format!("persist_fifo_{}", Uuid::new_v4());

            {
                let manager1 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                manager1
                    .create_queue(q.clone(), QueueCreateOptions::default())
                    .await
                    .unwrap();

                // Push 5 messages in order
                for i in 0..5 {
                    manager1
                        .push(q.clone(), Bytes::from(format!("msg{}", i)), 0)
                        .await
                        .unwrap();
                }

                // Wait for async flush
                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // Restart - messages should come out in the same FIFO order
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                manager2
                    .create_queue(q.clone(), QueueCreateOptions::default())
                    .await
                    .unwrap();

                for i in 0..5 {
                    let msg = manager2.pop(&q).await.expect("Should have message");
                    assert_eq!(
                        msg.payload,
                        Bytes::from(format!("msg{}", i)),
                        "FIFO order must survive restart: expected msg{}, got {:?}",
                        i,
                        msg.payload
                    );
                    manager2.ack(&q, msg.id, msg.delivery_token).await;
                }
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
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                let config = QueueCreateOptions {
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                manager
                    .push(q.clone(), Bytes::from("job_done"), 0)
                    .await
                    .unwrap();

                let msg = manager.pop(&q).await.unwrap();

                // Ack it (Should delete from DB)
                manager.ack(&q, msg.id, msg.delivery_token).await;

                // Wait for the async writer to flush (using double the default max flush time just to be safe)
                tokio::time::sleep(Duration::from_millis(300)).await;
            }

            // Restart
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                let config = QueueCreateOptions {
                    ..Default::default()
                };
                manager2.create_queue(q.clone(), config).await.unwrap();

                // Should be empty (Ack was persisted)
                assert!(
                    manager2.pop(&q).await.is_none(),
                    "Acked message should not reappear"
                );
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
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                let config = QueueCreateOptions {
                    visibility_timeout_ms: Some(500), // Short timeout
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                manager
                    .push(q.clone(), Bytes::from("job"), 0)
                    .await
                    .unwrap();

                // Take it (make it InFlight)
                let _ = manager.pop(&q).await.unwrap();

                // Drop manager while message is InFlight (and not Acked)
            }

            tokio::time::sleep(Duration::from_millis(600)).await; // Wait for timeout to theoretically pass

            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                let config = QueueCreateOptions {
                    visibility_timeout_ms: Some(500),
                    ..Default::default()
                };
                manager2.create_queue(q.clone(), config).await.unwrap();

                // Should be visible again! (Recovery put it in waiting_for_ack, then expired)
                // Note: Recovery runs, then process_expired runs.
                // Depending on timing, we might need to wait a tick for process_expired.
                tokio::time::sleep(Duration::from_millis(100)).await;

                let msg = manager2
                    .pop(&q)
                    .await
                    .expect("InFlight message should expire and reappear");
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
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));

                let config = QueueCreateOptions {
                    ..Default::default()
                };

                manager
                    .create_queue(q1.clone(), config.clone())
                    .await
                    .unwrap();
                manager
                    .create_queue(q2.clone(), config.clone())
                    .await
                    .unwrap();

                manager
                    .push(q1.clone(), Bytes::from("msg1_q1"), 0)
                    .await
                    .unwrap();
                manager
                    .push(q1.clone(), Bytes::from("msg2_q1"), 0)
                    .await
                    .unwrap();
                manager
                    .push(q2.clone(), Bytes::from("msg1_q2"), 0)
                    .await
                    .unwrap();

                // Wait for async flush before dropping manager
                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // Small delay to ensure files are flushed
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Phase 2: Restart manager WITHOUT calling create_queue
            // Warm start should automatically discover and restore queues
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));

                // Wait a bit for warm start to complete
                tokio::time::sleep(Duration::from_millis(200)).await;

                // Verify queues exist (warm start should have restored them)
                assert!(
                    manager2.exists(&q1).await,
                    "Queue 1 should be auto-restored"
                );
                assert!(
                    manager2.exists(&q2).await,
                    "Queue 2 should be auto-restored"
                );

                // Verify messages are recovered
                let msg1 = manager2.pop(&q1).await.expect("Should recover msg1_q1");
                assert_eq!(msg1.payload, Bytes::from("msg1_q1"));

                let msg2 = manager2.pop(&q1).await.expect("Should recover msg2_q1");
                assert_eq!(msg2.payload, Bytes::from("msg2_q1"));

                let msg3 = manager2.pop(&q2).await.expect("Should recover msg1_q2");
                assert_eq!(msg3.payload, Bytes::from("msg1_q2"));

                // Verify queues restored after recovery
                assert!(manager2.exists(&q1).await, "Queue q1 should be restored");
                assert!(manager2.exists(&q2).await, "Queue q2 should be restored");
            }
        }

        #[tokio::test]
        async fn test_corrupted_db_prevents_queue_registration() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q = format!("corrupt_{}", Uuid::new_v4());

            // Phase 1: Create queue and push a message
            {
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                manager
                    .create_queue(q.clone(), QueueCreateOptions::default())
                    .await
                    .unwrap();
                manager
                    .push(q.clone(), Bytes::from("survivor"), 0)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // Corrupt the DB file
            let db_path = std::path::PathBuf::from(&path).join(format!("{}.db", q));
            std::fs::write(&db_path, b"corrupted garbage data").unwrap();

            // Phase 2: Restart — queue should NOT be registered (fail-fast)
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                tokio::time::sleep(Duration::from_millis(200)).await;

                assert!(
                    !manager2.exists(&q).await,
                    "Queue with corrupted DB must NOT be registered — fail-fast prevents silent data loss"
                );
            }
        }

        #[tokio::test]
        async fn test_dlq_delete_and_purge() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("dlq_ops_{}", Uuid::new_v4());

            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(100),
                max_deliveries: Some(0), // Immediate DLQ
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            // Push 3 messages that will fail
            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("msg2"), 0)
                .await
                .unwrap();
            manager
                .push(q.clone(), Bytes::from("msg3"), 0)
                .await
                .unwrap();

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
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                // Create with max_deliveries = 0 (1st timeout -> DLQ immediately)
                let config = QueueCreateOptions {
                    visibility_timeout_ms: Some(100),
                    max_deliveries: Some(0),
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                // Push message destined to fail
                manager
                    .push(q.clone(), Bytes::from("stay_in_dlq"), 0)
                    .await
                    .unwrap();

                // Attempt 1 (and only attempt allowed)
                let _ = manager.pop(&q).await.unwrap();

                // Wait for visibility timeout + buffer for actor to wake up and process
                tokio::time::sleep(Duration::from_millis(250)).await;

                // Verify main queue is empty (message moved to internal DLQ by actor)
                assert!(
                    manager.pop(&q).await.is_none(),
                    "Main queue should be empty"
                );

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
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                tokio::time::sleep(Duration::from_millis(200)).await;

                // Queue should be auto-restored with DLQ messages
                assert!(manager2.exists(&q).await, "Queue should survive restart");

                // Main queue should still be empty (DLQ message persisted internally)
                assert!(
                    manager2.pop(&q).await.is_none(),
                    "Main queue should still be empty after restart"
                );

                // Verify DLQ message survived restart
                let (total, dlq_msgs) = manager2.peek_dlq(&q, 10, 0).await.unwrap();
                assert_eq!(total, 1);
                assert_eq!(dlq_msgs.len(), 1, "DLQ message should survive restart");
                assert_eq!(dlq_msgs[0].payload, Bytes::from("stay_in_dlq"));
            }
        }

        #[tokio::test]
        async fn test_failure_reason_survives_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q = format!("persist_reason_{}", Uuid::new_v4());

            {
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                let config = QueueCreateOptions {
                    max_deliveries: Some(5),
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                manager
                    .push(q.clone(), Bytes::from("failer"), 0)
                    .await
                    .unwrap();

                // Pop → InFlight
                let msg = manager.pop(&q).await.unwrap();

                // Nack with a reason (requeue, not DLQ since attempts < max_deliveries)
                manager
                    .nack(&q, msg.id, msg.delivery_token, "bad_payload".to_string())
                    .await;

                // Wait for flush
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            // Restart
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                tokio::time::sleep(Duration::from_millis(200)).await;

                assert!(manager2.exists(&q).await, "Queue should survive restart");

                let msg = manager2
                    .pop(&q)
                    .await
                    .expect("Requeued message should survive restart");
                assert_eq!(msg.payload, Bytes::from("failer"));
                assert_eq!(
                    msg.failure_reason.as_deref(),
                    Some("bad_payload"),
                    "failure_reason should survive restart for requeued messages"
                );
            }
        }

        #[tokio::test]
        async fn test_ready_seq_persists_for_new_pushes() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q = format!("persist_ready_seq_{}", Uuid::new_v4());

            {
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                manager
                    .create_queue(q.clone(), QueueCreateOptions::default())
                    .await
                    .unwrap();

                for i in 1..=3 {
                    manager
                        .push(q.clone(), Bytes::from(format!("msg{}", i)), 0)
                        .await
                        .unwrap();
                }

                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // Restart - new pushes must come out in original FIFO order
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                tokio::time::sleep(Duration::from_millis(200)).await;

                manager2
                    .create_queue(q.clone(), QueueCreateOptions::default())
                    .await
                    .unwrap();

                for i in 1..=3 {
                    let msg = manager2.pop(&q).await.expect("Message should be recovered");
                    assert_eq!(
                        msg.payload,
                        Bytes::from(format!("msg{}", i)),
                        "FIFO order must survive restart for new pushes"
                    );
                    assert!(msg.ready_seq > 0, "ready_seq must be persisted");
                    manager2.ack(&q, msg.id, msg.delivery_token).await;
                }
            }
        }

        #[tokio::test]
        async fn test_ready_seq_order_with_timeout_requeue_after_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q = format!("persist_ready_seq_requeue_{}", Uuid::new_v4());

            {
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                let config = QueueCreateOptions {
                    visibility_timeout_ms: Some(50),
                    max_deliveries: Some(5),
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                manager
                    .push(q.clone(), Bytes::from("first"), 0)
                    .await
                    .unwrap();
                let msg = manager.pop(&q).await.unwrap();
                assert_eq!(msg.payload, Bytes::from("first"));

                // Wait for timeout so the message is requeued with a new ready_seq
                tokio::time::sleep(Duration::from_millis(150)).await;

                // Push a second message after the requeue
                manager
                    .push(q.clone(), Bytes::from("second"), 0)
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // Restart - requeued message must still come before the newer push
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                tokio::time::sleep(Duration::from_millis(200)).await;

                assert_eq!(
                    manager2
                        .describe(&q)
                        .await
                        .unwrap()
                        .config
                        .visibility_timeout_ms,
                    50
                );

                let first = manager2
                    .pop(&q)
                    .await
                    .expect("First message should be first after restart");
                assert_eq!(
                    first.payload,
                    Bytes::from("first"),
                    "Requeued message must come before newer push after restart"
                );
                manager2.ack(&q, first.id, first.delivery_token).await;

                let second = manager2
                    .pop(&q)
                    .await
                    .expect("Second message should follow");
                assert_eq!(second.payload, Bytes::from("second"));
            }
        }

        #[tokio::test]
        async fn test_dlq_seq_persists_and_peek_order() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path.clone();

            let q = format!("persist_dlq_seq_{}", Uuid::new_v4());

            {
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                let config = QueueCreateOptions {
                    visibility_timeout_ms: Some(50),
                    max_deliveries: Some(1),
                    ..Default::default()
                };
                manager.create_queue(q.clone(), config).await.unwrap();

                manager
                    .push(q.clone(), Bytes::from("first"), 0)
                    .await
                    .unwrap();
                manager
                    .push(q.clone(), Bytes::from("second"), 0)
                    .await
                    .unwrap();

                // Pop both: each has attempts=1 >= max_deliveries=1, so timeout moves them to DLQ
                let _ = manager.pop(&q).await.unwrap();
                let _ = manager.pop(&q).await.unwrap();

                tokio::time::sleep(Duration::from_millis(150)).await;

                // Verify peek order before restart (most recent first)
                let (total, dlq_msgs) = manager.peek_dlq(&q, 10, 0).await.unwrap();
                assert_eq!(total, 2);
                assert_eq!(dlq_msgs[0].payload, Bytes::from("second"));
                assert_eq!(dlq_msgs[1].payload, Bytes::from("first"));

                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // Restart - DLQ ordering and dlq_seq must survive
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                tokio::time::sleep(Duration::from_millis(200)).await;

                assert_eq!(
                    manager2.describe(&q).await.unwrap().config.max_deliveries,
                    1
                );

                let (total, dlq_msgs) = manager2.peek_dlq(&q, 10, 0).await.unwrap();
                assert_eq!(total, 2);
                assert!(dlq_msgs[0].dlq_seq > 0, "dlq_seq must be persisted");
                assert!(dlq_msgs[1].dlq_seq > 0, "dlq_seq must be persisted");
                assert!(
                    dlq_msgs[0].dlq_seq > dlq_msgs[1].dlq_seq,
                    "DLQ peek must remain most-recent first"
                );
                assert_eq!(dlq_msgs[0].payload, Bytes::from("second"));
                assert_eq!(dlq_msgs[1].payload, Bytes::from("first"));
            }
        }
    }

    mod batch {
        use super::*;

        #[tokio::test]
        async fn test_batch_push_single_item() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("batch_single_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push_batch(q.clone(), vec![(Bytes::from("hello"), 0)])
                .await
                .unwrap();

            let msg = manager.pop(&q).await.expect("Should pop message");
            assert_eq!(msg.payload, Bytes::from("hello"));
        }

        #[tokio::test]
        async fn test_batch_push_multiple_items() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("batch_multi_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            let items: Vec<(Bytes, u8)> = (0..5)
                .map(|i| (Bytes::from(format!("msg_{}", i)), 0))
                .collect();
            manager.push_batch(q.clone(), items).await.unwrap();

            for i in 0..5 {
                let msg = manager.pop(&q).await.expect("Should pop message");
                assert_eq!(msg.payload, Bytes::from(format!("msg_{}", i)));
            }
            assert!(manager.pop(&q).await.is_none(), "Queue should be empty");
        }

        #[tokio::test]
        async fn test_batch_push_mixed_priorities() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("batch_prio_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            let items = vec![
                (Bytes::from("low"), 0u8),
                (Bytes::from("high"), 10u8),
                (Bytes::from("mid"), 5u8),
            ];
            manager.push_batch(q.clone(), items).await.unwrap();

            assert_eq!(manager.pop(&q).await.unwrap().payload, Bytes::from("high"));
            assert_eq!(manager.pop(&q).await.unwrap().payload, Bytes::from("mid"));
            assert_eq!(manager.pop(&q).await.unwrap().payload, Bytes::from("low"));
        }

        #[tokio::test]
        async fn test_batch_push_empty() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("batch_empty_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager.push_batch(q.clone(), vec![]).await.unwrap();
            assert!(manager.pop(&q).await.is_none(), "Queue should be empty");
        }

        #[tokio::test]
        async fn test_batch_push_nonexistent_queue() {
            let (manager, _tmp) = setup_queue_manager().await;
            let result = manager
                .push_batch("nonexistent".to_string(), vec![(Bytes::from("data"), 0)])
                .await;
            assert!(result.is_err());
        }
    }

    mod shutdown {
        use super::*;

        #[tokio::test]
        async fn test_shutdown_flushes_pending_messages() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            let mut sys_config = nexo::config::Config::global().queue.clone();
            sys_config.persistence_path = path;

            let q = format!("shutdown_flush_{}", Uuid::new_v4());

            {
                let manager =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config.clone())));
                manager
                    .create_queue(q.clone(), QueueCreateOptions::default())
                    .await
                    .unwrap();
                manager
                    .push(q.clone(), Bytes::from("survivor"), 0)
                    .await
                    .unwrap();

                // Shutdown immediately — no sleep, no waiting for flush timer
                manager.shutdown().await;
            }

            // Recover with a new manager
            {
                let manager2 =
                    std::sync::Arc::new(QueueManager::new(std::sync::Arc::new(sys_config)));
                manager2
                    .create_queue(q.clone(), QueueCreateOptions::default())
                    .await
                    .unwrap();
                let msg = manager2
                    .pop(&q)
                    .await
                    .expect("Message should survive shutdown flush");
                assert_eq!(msg.payload, Bytes::from("survivor"));
            }
        }
    }

    mod security {
        use super::*;

        #[tokio::test]
        async fn test_create_rejects_path_traversal_names() {
            let (manager, _tmp) = setup_queue_manager().await;

            for invalid in [
                "../outside",
                "nested/queue",
                "nested\\queue",
                "/tmp/queue",
                "..",
                ".",
            ] {
                let result = manager
                    .create_queue(invalid.to_string(), QueueCreateOptions::default())
                    .await;
                assert!(result.is_err(), "create_queue({invalid:?}) should fail");
            }
        }

        #[tokio::test]
        async fn test_delete_rejects_path_traversal_names() {
            let (manager, _tmp) = setup_queue_manager().await;

            let result = manager.delete_queue("../outside".to_string()).await;
            assert!(
                result.is_err(),
                "delete_queue with traversal name should fail"
            );
        }

        #[tokio::test]
        async fn test_exists_returns_false_for_invalid_names() {
            let (manager, _tmp) = setup_queue_manager().await;

            assert!(!manager.exists("../outside").await);
            assert!(!manager.exists("nested/queue").await);
            assert!(!manager.exists("").await);
        }

        #[tokio::test]
        async fn test_create_rejects_empty_name() {
            let (manager, _tmp) = setup_queue_manager().await;

            let result = manager
                .create_queue("".to_string(), QueueCreateOptions::default())
                .await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_create_rejects_name_with_spaces() {
            let (manager, _tmp) = setup_queue_manager().await;

            let result = manager
                .create_queue("queue name".to_string(), QueueCreateOptions::default())
                .await;
            assert!(result.is_err());
        }
    }

    // =========================================================================================
    // 5. DELIVERY TOKEN
    // =========================================================================================

    mod delivery_token {
        use super::*;

        #[tokio::test]
        async fn test_ack_with_valid_token_succeeds() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("token_ack_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();
            let msg = manager.pop(&q).await.unwrap();

            assert!(
                manager.ack(&q, msg.id, msg.delivery_token).await,
                "Valid token ACK should succeed"
            );
            assert!(
                manager.pop(&q).await.is_none(),
                "Queue should be empty after ACK"
            );
        }

        #[tokio::test]
        async fn test_ack_with_stale_token_returns_false() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("token_stale_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();
            let msg = manager.pop(&q).await.unwrap();

            // ACK with wrong token should fail and NOT delete the message
            let stale_token = msg.delivery_token + 999;
            assert!(
                !manager.ack(&q, msg.id, stale_token).await,
                "Stale token ACK should return false"
            );

            // Message should still be in-flight (not deleted)
            // Pop should return None (message is in-flight, not ready)
            assert!(
                manager.pop(&q).await.is_none(),
                "Message should still be in-flight"
            );
        }

        #[tokio::test]
        async fn test_nack_with_stale_token_returns_false() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("token_nack_stale_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();
            let msg = manager.pop(&q).await.unwrap();

            // NACK with wrong token should fail and NOT requeue
            let stale_token = msg.delivery_token + 999;
            let result = manager
                .nack(&q, msg.id, stale_token, "reason".to_string())
                .await;
            assert!(!result, "Stale token NACK should return false");

            // Message should still be in-flight (not requeued to ready)
            assert!(
                manager.pop(&q).await.is_none(),
                "Message should still be in-flight after stale NACK"
            );
        }

        #[tokio::test]
        async fn test_token_changes_on_requeue() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("token_change_{}", Uuid::new_v4());
            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(50),
                max_deliveries: Some(5),
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();

            // First pop: token = 1
            let msg1 = manager.pop(&q).await.unwrap();
            let token1 = msg1.delivery_token;
            assert_eq!(token1, 1, "First delivery token should be 1");

            // Wait for visibility timeout → message gets requeued
            tokio::time::sleep(Duration::from_millis(150)).await;

            // Second pop: token should be different (2)
            let msg2 = manager.pop(&q).await.unwrap();
            let token2 = msg2.delivery_token;
            assert_eq!(token2, 2, "Second delivery token should be 2");
            assert_ne!(token1, token2, "Tokens must differ across deliveries");

            // ACK with old token should fail
            assert!(
                !manager.ack(&q, msg1.id, token1).await,
                "ACK with old token should fail"
            );

            // ACK with current token should succeed
            assert!(
                manager.ack(&q, msg2.id, token2).await,
                "ACK with current token should succeed"
            );
        }

        #[tokio::test]
        async fn test_ack_with_zero_token_on_inflight_is_stale() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("token_zero_{}", Uuid::new_v4());
            manager
                .create_queue(q.clone(), QueueCreateOptions::default())
                .await
                .unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();
            let msg = manager.pop(&q).await.unwrap();

            // Token 0 means "never delivered" — should be stale for an in-flight message
            assert!(
                !manager.ack(&q, msg.id, 0).await,
                "ACK with token=0 on in-flight message should be stale"
            );
        }

        #[tokio::test]
        async fn test_ack_with_old_token_after_timeout_fails_before_repop() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("token_stale_ack_{}", Uuid::new_v4());
            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(50),
                max_deliveries: Some(5),
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();

            let msg1 = manager.pop(&q).await.unwrap();
            let token1 = msg1.delivery_token;

            // Wait for timeout → process_expired requeues the message
            tokio::time::sleep(Duration::from_millis(150)).await;

            // ACK with the old token must fail before the message is repopped
            assert!(
                !manager.ack(&q, msg1.id, token1).await,
                "ACK with old token after requeue should fail"
            );

            // A new pop assigns a new token; ACK with that token must succeed
            let msg2 = manager.pop(&q).await.unwrap();
            assert_eq!(msg2.delivery_token, 2, "Second delivery token should be 2");
            assert!(
                manager.ack(&q, msg2.id, msg2.delivery_token).await,
                "ACK with current token should succeed"
            );
        }

        #[tokio::test]
        async fn test_nack_with_old_token_after_timeout_fails_before_repop() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("token_stale_nack_{}", Uuid::new_v4());
            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(50),
                max_deliveries: Some(5),
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();

            let msg1 = manager.pop(&q).await.unwrap();
            let token1 = msg1.delivery_token;

            // Wait for timeout → process_expired requeues the message
            tokio::time::sleep(Duration::from_millis(150)).await;

            // NACK with the old token must fail before the message is repopped
            assert!(
                !manager.nack(&q, msg1.id, token1, "stale".to_string()).await,
                "NACK with old token after requeue should fail"
            );

            // A new pop assigns a new token; NACK with that token must succeed
            let msg2 = manager.pop(&q).await.unwrap();
            assert_eq!(msg2.delivery_token, 2, "Second delivery token should be 2");
            assert!(
                manager
                    .nack(&q, msg2.id, msg2.delivery_token, "valid".to_string())
                    .await,
                "NACK with current token should succeed"
            );
        }

        #[tokio::test]
        async fn test_ack_after_expired_lease_fails() {
            let (manager, _tmp) = setup_queue_manager().await;
            let q = format!("token_expired_ack_{}", Uuid::new_v4());
            let config = QueueCreateOptions {
                visibility_timeout_ms: Some(1),
                max_deliveries: Some(5),
                ..Default::default()
            };
            manager.create_queue(q.clone(), config).await.unwrap();

            manager
                .push(q.clone(), Bytes::from("msg1"), 0)
                .await
                .unwrap();

            let msg1 = manager.pop(&q).await.unwrap();
            let token1 = msg1.delivery_token;

            // Wait just enough for the lease to expire but before the timeout task requeues it
            tokio::time::sleep(Duration::from_millis(5)).await;

            assert!(
                !manager.ack(&q, msg1.id, token1).await,
                "ACK after lease expiration should fail"
            );

            // Give the timeout task time to requeue and redeliver
            tokio::time::sleep(Duration::from_millis(100)).await;

            let msg2 = manager.pop(&q).await.unwrap();
            assert_eq!(msg2.delivery_token, 2, "Redelivery should have a new token");
            assert!(
                manager.ack(&q, msg2.id, msg2.delivery_token).await,
                "ACK with new token should succeed"
            );
        }
    }
}

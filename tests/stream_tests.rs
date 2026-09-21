use bytes::Bytes;
use nexo::brokers::stream::options::{RetentionOptions, StreamCreateOptions};
use nexo::brokers::stream::{ConsumerIdentity, Delivery, Message, PubItem};
use nexo::brokers::{BrokerErrorKind, ProvisionOutcome};
use nexo::config::Config;
use std::time::{Duration, Instant};
mod common;

#[cfg(test)]
mod stream_tests {
    use super::*;
    use nexo::brokers::stream::manager::JoinGroupResult;
    use nexo::brokers::stream::StreamManager;
    use std::sync::Arc;

    fn get_test_config(path: Option<&str>) -> nexo::brokers::stream::config::SystemStreamConfig {
        let mut config = Config::global().stream.clone();
        if let Some(p) = path {
            config.persistence_path = p.to_string();
        }
        config
    }

    async fn build_manager(
        config: nexo::brokers::stream::config::SystemStreamConfig,
    ) -> Arc<StreamManager> {
        Arc::new(
            StreamManager::new(Arc::new(config))
                .await
                .expect("stream manager must open"),
        )
    }

    /// A joined consumer: the connection id is the transport session, the
    /// consumer id and generation come back from JOIN.
    struct Session {
        connection_id: String,
        consumer_id: String,
        generation: u64,
        ack_floor: u64,
    }

    impl Session {
        fn identity(&self) -> ConsumerIdentity {
            ConsumerIdentity {
                connection_id: self.connection_id.clone(),
                consumer_id: self.consumer_id.clone(),
                generation: self.generation,
            }
        }
    }

    async fn join_session(manager: &StreamManager, group: &str, name: &str, client: &str) -> Session {
        let JoinGroupResult {
            ack_floor,
            consumer_id,
            generation,
        } = manager.join_group(name, group, client).await.unwrap();
        Session {
            connection_id: client.to_string(),
            consumer_id,
            generation,
            ack_floor,
        }
    }

    async fn fetch_deliveries(
        manager: &StreamManager,
        group: &str,
        name: &str,
        consumer: &Session,
        limit: usize,
        wait_ms: u64,
    ) -> Vec<Delivery> {
        manager
            .fetch(name, group, &consumer.identity(), limit, wait_ms)
            .await
            .unwrap()
    }

    async fn fetch_messages(
        manager: &StreamManager,
        group: &str,
        name: &str,
        consumer: &Session,
        limit: usize,
        wait_ms: u64,
    ) -> Vec<Message> {
        fetch_deliveries(manager, group, name, consumer, limit, wait_ms)
            .await
            .into_iter()
            .map(|d| d.message)
            .collect()
    }

    /// ACK consumes the lease fence carried by the delivery.
    async fn ack_delivery(
        manager: &StreamManager,
        group: &str,
        name: &str,
        consumer: &Session,
        delivery: &Delivery,
    ) {
        manager
            .ack(
                name,
                group,
                &consumer.identity(),
                delivery.message.seq,
                delivery.receipt,
            )
            .await
            .unwrap();
    }

    fn item(key: Bytes, payload: Bytes) -> PubItem {
        PubItem { key, payload }
    }

    mod features {
        use super::*;

        #[tokio::test]
        async fn test_stream_basic_flow() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "test-basic-flow";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            assert!(manager.exists(name).await.unwrap());

            let payload = Bytes::from("hello world");
            let seq = manager
                .publish(name, Bytes::new(), payload.clone())
                .await
                .unwrap();
            assert_eq!(seq, 1);

            let msgs = manager.read(name, 1, 100).await.unwrap();
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].payload, payload);
            assert_eq!(msgs[0].seq, 1);
        }

        #[tokio::test]
        async fn test_provisioning_result_describe_and_conflict() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "definition-stream";
            let options = StreamCreateOptions {
                retention: Some(RetentionOptions {
                    max_age_ms: Some(12_345),
                    max_bytes: Some(67_890),
                }),
            };

            let created = manager
                .create_stream(name.to_string(), options.clone())
                .await
                .unwrap();
            assert_eq!(created.outcome, ProvisionOutcome::Created);
            assert_eq!(created.definition.name, name);
            assert_eq!(created.definition.config.retention.max_age_ms, Some(12_345));
            assert_eq!(created.definition.config.retention.max_bytes, Some(67_890));
            assert_eq!(manager.describe(name).await.unwrap(), created.definition);

            let unchanged = manager
                .create_stream(name.to_string(), options)
                .await
                .unwrap();
            assert_eq!(unchanged.outcome, ProvisionOutcome::Unchanged);
            assert_eq!(unchanged.definition, created.definition);

            let error = manager
                .create_stream(
                    name.to_string(),
                    StreamCreateOptions {
                        retention: Some(RetentionOptions {
                            max_age_ms: Some(12_346),
                            max_bytes: Some(67_890),
                        }),
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind, BrokerErrorKind::ResourceConfigConflict);
            assert_eq!(
                error.details.unwrap()["differences"][0]["path"],
                "config.retention.maxAgeMs"
            );
        }

        #[tokio::test]
        async fn test_stream_ordering() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "ordering-stream";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=3 {
                let payload = Bytes::from(format!("msg-{}", i));
                manager.publish(name, Bytes::new(), payload).await.unwrap();
            }

            let msgs = manager.read(name, 1, 10).await.unwrap();
            assert_eq!(msgs.len(), 3);
            assert_eq!(msgs[0].payload, Bytes::from("msg-1"));
            assert_eq!(msgs[1].payload, Bytes::from("msg-2"));
            assert_eq!(msgs[2].payload, Bytes::from("msg-3"));
            assert_eq!(msgs[0].seq, 1);
            assert_eq!(msgs[1].seq, 2);
            assert_eq!(msgs[2].seq, 3);
        }

        #[tokio::test]
        async fn test_long_poll_fetch_wakes_on_publish() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "long-poll-wakeup";
            let group = "g-long-poll";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            let consumer = join_session(&manager, group, name, "client-A").await;

            let publisher = manager.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                publisher
                    .publish(name, Bytes::new(), Bytes::from("wake-me"))
                    .await
                    .unwrap();
            });

            let start = Instant::now();
            let msgs = fetch_messages(&manager, group, name, &consumer, 10, 2_000).await;

            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].payload, Bytes::from("wake-me"));
            assert!(start.elapsed() < Duration::from_millis(1_000));
        }

        #[tokio::test]
        async fn test_long_poll_fetch_times_out() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "long-poll-timeout";
            let group = "g-timeout";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            let consumer = join_session(&manager, group, name, "client-A").await;

            let start = Instant::now();
            let msgs = fetch_messages(&manager, group, name, &consumer, 10, 250).await;
            let elapsed = start.elapsed();

            assert!(msgs.is_empty());
            assert!(elapsed >= Duration::from_millis(200));
            assert!(elapsed < Duration::from_millis(1_000));
        }

        #[tokio::test]
        async fn test_timeout_redelivery_and_max_deliveries_park() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "timeout-park-flow";
            let group = "g-timeout-park";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            manager
                .publish(name, Bytes::new(), Bytes::from("msg-1"))
                .await
                .unwrap();
            manager
                .publish(name, Bytes::new(), Bytes::from("msg-2"))
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;

            let first = fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(first.len(), 1);
            assert_eq!(first[0].seq, 1);

            tokio::time::sleep(Duration::from_millis(120)).await;

            let second = fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(second.len(), 1);
            assert_eq!(second[0].seq, 1);

            tokio::time::sleep(Duration::from_millis(120)).await;

            let third = fetch_deliveries(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(third.len(), 1);
            assert_eq!(third[0].message.seq, 2);
            ack_delivery(&manager, group, name, &consumer, &third[0]).await;

            let probe = join_session(&manager, group, name, "client-B").await;
            assert_eq!(
                probe.ack_floor, 2,
                "ack_floor must advance over DLS entries (msg-1 parked, msg-2 acked)"
            );

            // Verify msg-1 is in DLS
            let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(dls.len(), 1);
            assert_eq!(dls[0].seq, 1, "msg-1 should be in DLS");
        }

        #[tokio::test]
        async fn test_ack_floor_advancement() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "ack-floor";
            let group = "g-floor";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=5 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;
            let msgs = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(msgs.len(), 5);

            ack_delivery(&manager, group, name, &consumer, &msgs[0]).await;
            ack_delivery(&manager, group, name, &consumer, &msgs[2]).await;

            let probe = join_session(&manager, group, name, "client-B").await;
            assert_eq!(probe.ack_floor, 1);

            ack_delivery(&manager, group, name, &consumer, &msgs[1]).await;
            let probe = join_session(&manager, group, name, "client-C").await;
            assert_eq!(probe.ack_floor, 3);
        }

        #[tokio::test]
        async fn test_seek_beginning_end() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "seek-stream";
            let group = "g-seek";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=10 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;
            let msgs = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            for msg in &msgs {
                ack_delivery(&manager, group, name, &consumer, msg).await;
            }

            let probe = join_session(&manager, group, name, "client-B").await;
            assert_eq!(probe.ack_floor, 10);

            let empty = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert!(empty.is_empty());

            use nexo::brokers::stream::options::SeekTarget;
            manager
                .seek(name, group, SeekTarget::Beginning)
                .await
                .unwrap();

            let fenced = manager
                .fetch(name, group, &consumer.identity(), 10, 0)
                .await;
            assert!(
                matches!(fenced, Err(ref error) if error.kind == nexo::brokers::BrokerErrorKind::Fenced)
            );

            let replay = join_session(&manager, group, name, "client-A").await;
            let from_start = fetch_deliveries(&manager, group, name, &replay, 10, 0).await;
            assert_eq!(from_start.len(), 10);
            assert_eq!(from_start[0].message.seq, 1);

            for d in &from_start {
                ack_delivery(&manager, group, name, &replay, d).await;
            }
            manager.seek(name, group, SeekTarget::End).await.unwrap();

            let tail = join_session(&manager, group, name, "client-A").await;
            let after_end = fetch_messages(&manager, group, name, &tail, 10, 0).await;
            assert!(after_end.is_empty());
        }

        #[tokio::test]
        async fn test_seek_cancels_inflight_fetch() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "seek-cancel-fetch";
            let group = "g-seek-cancel";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=5 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;
            let msgs = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            for msg in &msgs {
                ack_delivery(&manager, group, name, &consumer, msg).await;
            }

            let fetch_manager = manager.clone();
            let identity = consumer.identity();
            let fetch_handle = tokio::spawn(async move {
                let start = Instant::now();
                let result = fetch_manager
                    .fetch(name, group, &identity, 10, 5_000)
                    .await;
                (start.elapsed(), result)
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            use nexo::brokers::stream::options::SeekTarget;
            manager
                .seek(name, group, SeekTarget::Beginning)
                .await
                .unwrap();

            let (elapsed, result) = fetch_handle.await.unwrap();

            assert!(
                elapsed < Duration::from_millis(1_000),
                "Fetch should have been cancelled by seek, took {:?}",
                elapsed
            );
            // The woken resubmit hits the bumped generation → FENCED.
            assert!(
                matches!(result, Err(ref e) if e.kind == BrokerErrorKind::Fenced),
                "stale-generation fetch must be fenced after seek"
            );

            let from_start_consumer = join_session(&manager, group, name, "client-A").await;
            let from_start =
                fetch_messages(&manager, group, name, &from_start_consumer, 10, 0).await;
            assert!(
                !from_start.is_empty(),
                "Should have messages from beginning after seek"
            );
            assert_eq!(from_start[0].seq, 1);
        }

        #[tokio::test]
        async fn test_leave_cancels_inflight_fetch() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "leave-cancel-fetch";
            let group = "g-leave-cancel";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            let consumer = join_session(&manager, group, name, "client-A").await;

            let fetch_manager = manager.clone();
            let identity = consumer.identity();
            let fetch_handle = tokio::spawn(async move {
                let start = Instant::now();
                let result = fetch_manager
                    .fetch(name, group, &identity, 10, 5_000)
                    .await;
                (start.elapsed(), result)
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            manager
                .leave_group(name, group, &consumer.identity())
                .await
                .unwrap();

            let (elapsed, result) = fetch_handle.await.unwrap();

            assert!(
                elapsed < Duration::from_millis(1_000),
                "Fetch should have been cancelled by leave, took {:?}",
                elapsed
            );
            // The woken resubmit finds the member gone → NOT_MEMBER.
            assert!(
                matches!(result, Err(ref e) if e.kind == BrokerErrorKind::NotMember),
                "fetch after leave must fail NOT_MEMBER"
            );
        }

        #[tokio::test]
        async fn test_multi_consumer_parallel_fetch() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "multi-consumer";
            let group = "g-multi";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=6 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer_a = join_session(&manager, group, name, "client-A").await;
            let consumer_b = join_session(&manager, group, name, "client-B").await;

            let msgs_a = fetch_messages(&manager, group, name, &consumer_a, 3, 0).await;
            let msgs_b = fetch_messages(&manager, group, name, &consumer_b, 3, 0).await;

            assert_eq!(msgs_a.len(), 3);
            assert_eq!(msgs_b.len(), 3);

            let seqs_a: Vec<u64> = msgs_a.iter().map(|m| m.seq).collect();
            let seqs_b: Vec<u64> = msgs_b.iter().map(|m| m.seq).collect();
            for seq in &seqs_a {
                assert!(
                    !seqs_b.contains(seq),
                    "Messages should not overlap between consumers"
                );
            }
        }

        #[tokio::test]
        async fn test_delete_stream() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let manager = build_manager(config).await;
            let name = "delete-stream-test";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            manager
                .publish(name, Bytes::new(), Bytes::from("msg1"))
                .await
                .unwrap();

            assert!(manager.exists(name).await.unwrap());

            manager.delete_stream(name.to_string()).await.unwrap();

            assert!(!manager.exists(name).await.unwrap());
            let err = manager.read(name, 1, 10).await.unwrap_err();
            assert_eq!(err.kind, BrokerErrorKind::ResourceNotFound);
        }

        #[tokio::test]
        async fn test_disconnect_redelivers_inflight_to_another_consumer() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "disconnect-redeliver";
            let group = "g-disconnect-redeliver";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=3 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer_a = join_session(&manager, group, name, "client-A").await;
            let msgs_a = fetch_messages(&manager, group, name, &consumer_a, 10, 0).await;
            assert_eq!(msgs_a.len(), 3, "Consumer A should receive all 3 messages");

            // Simulate transport teardown.
            manager.disconnect(&consumer_a.connection_id).await;

            let consumer_b = join_session(&manager, group, name, "client-B").await;
            let msgs_b = fetch_messages(&manager, group, name, &consumer_b, 10, 0).await;
            assert_eq!(
                msgs_b.len(),
                3,
                "Consumer B should receive redelivered messages"
            );

            let seqs_b: Vec<u64> = msgs_b.iter().map(|m| m.seq).collect();
            assert!(seqs_b.contains(&1), "seq 1 should be redelivered");
            assert!(seqs_b.contains(&2), "seq 2 should be redelivered");
            assert!(seqs_b.contains(&3), "seq 3 should be redelivered");
        }

        #[tokio::test]
        async fn test_max_ack_pending_backpressure() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 3;
            let manager = build_manager(config).await;
            let name = "backpressure-test";
            let group = "g-backpressure";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=5 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;

            let batch1 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(
                batch1.len(),
                3,
                "Should only get 3 messages (max_ack_pending=3)"
            );

            let batch2 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 0, "Should be backpressured (no new messages)");

            ack_delivery(&manager, group, name, &consumer, &batch1[0]).await;

            let batch3 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1, "Should get 1 more after acking 1");
            assert_eq!(batch3[0].seq, 4);
        }

        /// A stale receipt must never consume a lease: after a timeout
        /// redelivery issues a fresh receipt, the old one is fenced.
        #[tokio::test]
        async fn test_stale_receipt_ack_is_fenced() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 10;
            let manager = build_manager(config).await;
            let name = "receipt-fence";
            let group = "g-receipt-fence";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            manager
                .publish(name, Bytes::new(), Bytes::from("msg-1"))
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;
            let first = fetch_deliveries(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(first.len(), 1);
            let stale_receipt = first[0].receipt;

            // Let the lease expire, then redelivery issues a new receipt.
            tokio::time::sleep(Duration::from_millis(120)).await;
            let second = fetch_deliveries(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(second.len(), 1);
            assert_eq!(second[0].message.seq, 1);
            assert_ne!(second[0].receipt, stale_receipt);

            // The stale receipt is fenced.
            let err = manager
                .ack(
                    name,
                    group,
                    &consumer.identity(),
                    1,
                    stale_receipt,
                )
                .await
                .unwrap_err();
            assert_eq!(err.kind, BrokerErrorKind::Fenced);

            // A foreign receipt is fenced too.
            let err = manager
                .ack(name, group, &consumer.identity(), 1, [0xEE; 16])
                .await
                .unwrap_err();
            assert_eq!(err.kind, BrokerErrorKind::Fenced);

            // The live receipt works.
            ack_delivery(&manager, group, name, &consumer, &second[0]).await;
        }
    }

    mod persistence {
        use super::*;

        #[tokio::test]
        async fn test_write_and_recover() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let name = "persist-recover";

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                manager
                    .publish(name, Bytes::new(), Bytes::from("msg1"))
                    .await
                    .unwrap();
                manager
                    .publish(name, Bytes::new(), Bytes::from("msg2"))
                    .await
                    .unwrap();
                manager.shutdown().await;
            }

            {
                let manager = build_manager(config.clone()).await;

                let msgs = manager.read(name, 1, 10).await.unwrap();
                assert_eq!(msgs.len(), 2);
                assert_eq!(msgs[0].payload, Bytes::from("msg1"));
                assert_eq!(msgs[1].payload, Bytes::from("msg2"));
                assert_eq!(msgs[0].seq, 1);
                assert_eq!(msgs[1].seq, 2);
            }
        }

        #[tokio::test]
        async fn test_ack_floor_persistence() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let name = "persist-ack";
            let group = "g-persist";

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                manager
                    .publish(name, Bytes::new(), Bytes::from("msg1"))
                    .await
                    .unwrap();
                manager
                    .publish(name, Bytes::new(), Bytes::from("msg2"))
                    .await
                    .unwrap();
                let consumer = join_session(&manager, group, name, "client-A").await;
                let msgs = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
                for msg in &msgs {
                    ack_delivery(&manager, group, name, &consumer, msg).await;
                }

                manager
                    .publish(name, Bytes::new(), Bytes::from("msg3"))
                    .await
                    .unwrap();
                manager.shutdown().await;
            }

            {
                let manager = build_manager(config.clone()).await;

                let probe = join_session(&manager, group, name, "client-A").await;
                assert_eq!(probe.ack_floor, 2, "Ack floor should be recovered");
            }
        }

        /// A corrupted database file must fail closed at open — the engine
        /// must never silently rebuild or serve over a damaged store.
        #[tokio::test]
        async fn corrupt_db_fails_closed() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream("s".to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();
                manager
                    .publish("s", Bytes::new(), Bytes::from("msg"))
                    .await
                    .unwrap();
                manager.shutdown().await;
            }

            // Corrupt the database header (the WAL is checkpointed away on
            // shutdown, so the main file must parse).
            let db = temp_dir.path().join("streams.sqlite3");
            let mut data = std::fs::read(&db).unwrap();
            for (i, b) in data.iter_mut().take(64).enumerate() {
                *b = (i as u8).wrapping_add(0xA5);
            }
            std::fs::write(&db, &data).unwrap();

            let result = StreamManager::new(Arc::new(config)).await;
            assert!(result.is_err(), "corrupt database must fail closed");
        }

        /// Anything in the storage root that is not part of the SQLite layout
        /// (legacy segment dirs/files, foreign files) must fail closed.
        #[tokio::test]
        async fn legacy_layout_fails_closed() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            // Seed a legacy-looking artefact before first open.
            std::fs::create_dir_all(temp_dir.path().join("old-stream")).unwrap();
            std::fs::write(temp_dir.path().join("old-stream/1.log"), b"legacy").unwrap();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str;

            let result = StreamManager::new(Arc::new(config)).await;
            assert!(result.is_err(), "legacy layout must fail closed");
        }

        /// Restart mid-flight: deliveries leased but not acked are re-offered
        /// to whoever joins next (leases are durable, membership is not).
        #[tokio::test]
        async fn test_restart_preserves_events_and_group_progress() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let name = "restart-progress";
            let group = "g-restart";

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                for i in 1..=4 {
                    manager
                        .publish(name, Bytes::new(), Bytes::from(format!("msg-{i}")))
                        .await
                        .unwrap();
                }

                let consumer = join_session(&manager, group, name, "client-A").await;
                let msgs = fetch_deliveries(&manager, group, name, &consumer, 2, 0).await;
                assert_eq!(msgs.len(), 2);
                ack_delivery(&manager, group, name, &consumer, &msgs[0]).await;
                manager.shutdown().await;
            }

            {
                let manager = build_manager(config.clone()).await;
                // A fresh member resumes at the unresolved frontier: seq 2 was
                // leased (never acked) → redelivered; seq 1 stays resolved.
                let consumer = join_session(&manager, group, name, "client-B").await;
                assert_eq!(consumer.ack_floor, 1);
                let msgs = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
                let seqs: Vec<u64> = msgs.iter().map(|m| m.seq).collect();
                assert_eq!(seqs, vec![2, 3, 4]);
            }
        }

        #[tokio::test]
        async fn test_stream_log_retention() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            config.retention_check_interval_ms = 100;
            config.default_retention_bytes = 250;

            let manager = build_manager(config).await;
            let name = "stream_retention";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=7 {
                manager
                    .publish(
                        name,
                        Bytes::new(),
                        Bytes::from(format!(
                            "msg{}-50bytes-payload-0000000000000000000000000000",
                            i
                        )),
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(60)).await;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;

            let retained = manager.read(name, 1, 20).await.unwrap();
            assert!(!retained.is_empty());
            assert!(retained[0].seq > 1);

            let consumer = join_session(&manager, "g-retained", name, "client-retained").await;
            let fetched = fetch_messages(&manager, "g-retained", name, &consumer, 10, 0).await;
            assert_eq!(
                fetched.first().map(|msg| msg.seq),
                retained.first().map(|msg| msg.seq)
            );
        }

        #[tokio::test]
        async fn test_warm_start_auto_restore() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let stream1 = "warm_stream1";
            let stream2 = "warm_stream2";
            let group = "warm_group";

            {
                let manager = build_manager(config.clone()).await;

                manager
                    .create_stream(stream1.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();
                manager
                    .create_stream(stream2.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                manager
                    .publish(stream1, Bytes::new(), Bytes::from("msg1_t1"))
                    .await
                    .unwrap();
                manager
                    .publish(stream1, Bytes::new(), Bytes::from("msg2_t1"))
                    .await
                    .unwrap();
                manager
                    .publish(stream2, Bytes::new(), Bytes::from("msg1_t2"))
                    .await
                    .unwrap();

                let consumer = join_session(&manager, group, stream1, "client-A").await;
                let msgs = fetch_deliveries(&manager, group, stream1, &consumer, 1, 0).await;
                if !msgs.is_empty() {
                    ack_delivery(&manager, group, stream1, &consumer, &msgs[0]).await;
                }

                manager
                    .publish(stream1, Bytes::new(), Bytes::from("msg3_t1"))
                    .await
                    .unwrap();
                manager.shutdown().await;
            }

            {
                let manager2 = build_manager(config.clone()).await;

                assert!(
                    manager2.exists(stream1).await.unwrap(),
                    "Stream 1 should be auto-restored"
                );
                assert!(
                    manager2.exists(stream2).await.unwrap(),
                    "Stream 2 should be auto-restored"
                );

                let msgs2 = manager2.read(stream2, 1, 10).await.unwrap();
                assert_eq!(msgs2.len(), 1, "Should recover 1 message from stream2");
                assert_eq!(msgs2[0].payload, Bytes::from("msg1_t2"));
            }
        }

        /// Volume beyond any cache: reads must be correct for both ends of
        /// the retained log.
        #[tokio::test]
        async fn test_large_volume_read_back() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let manager = build_manager(config).await;
            let name = "volume_test";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 0..500 {
                let payload = Bytes::from(format!("msg_{:04}", i));
                manager.publish(name, Bytes::new(), payload).await.unwrap();
            }

            let old_msgs = manager.read(name, 1, 10).await.unwrap();
            assert_eq!(old_msgs.len(), 10);
            assert_eq!(old_msgs[0].payload, Bytes::from("msg_0000"));
            assert_eq!(old_msgs[9].payload, Bytes::from("msg_0009"));

            let recent_msgs = manager.read(name, 491, 10).await.unwrap();
            assert_eq!(recent_msgs.len(), 10);
            assert_eq!(recent_msgs[0].payload, Bytes::from("msg_0490"));
            assert_eq!(recent_msgs[9].payload, Bytes::from("msg_0499"));
        }

        /// Fetch after restart reads from the committed store.
        #[tokio::test]
        async fn test_fetch_after_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            let name = "fetch-after-restart";
            let group = "g-cold";

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                for i in 1..=5 {
                    manager
                        .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                        .await
                        .unwrap();
                }
                manager.shutdown().await;
            }

            let manager = build_manager(config).await;
            let consumer = join_session(&manager, group, name, "client-A").await;
            let msgs = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;

            assert_eq!(msgs.len(), 5);
            assert_eq!(msgs[0].message.seq, 1);
            assert_eq!(msgs[0].message.payload, Bytes::from("msg-1"));
            assert_eq!(msgs[4].message.seq, 5);
            assert_eq!(msgs[4].message.payload, Bytes::from("msg-5"));

            for d in &msgs {
                ack_delivery(&manager, group, name, &consumer, d).await;
            }
        }
    }

    mod error_handling {
        use super::*;

        #[tokio::test]
        async fn test_publish_nonexistent_stream() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();

            let manager = build_manager(config).await;

            let result = manager
                .publish("nonexistent_stream", Bytes::new(), Bytes::from("msg"))
                .await;

            assert!(
                result.is_err(),
                "Publishing to nonexistent stream should fail"
            );
            assert_eq!(
                result.err().unwrap().kind,
                nexo::brokers::BrokerErrorKind::ResourceNotFound
            );
        }

        #[tokio::test]
        async fn test_read_beyond_high_watermark() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();

            let manager = build_manager(config).await;
            manager
                .create_stream("test_stream".to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 0..10 {
                manager
                    .publish("test_stream", Bytes::new(), Bytes::from(format!("msg{}", i)))
                    .await
                    .unwrap();
            }

            let msgs = manager.read("test_stream", 1000, 10).await.unwrap();
            assert!(
                msgs.is_empty(),
                "Reading beyond high watermark should return empty"
            );

            let msgs = manager.read("test_stream", 1, 100).await.unwrap();
            assert!(!msgs.is_empty(), "Should read messages");
            assert_eq!(msgs[0].seq, 1, "First message should have seq 1");

            for i in 1..msgs.len() {
                assert_eq!(
                    msgs[i].seq,
                    msgs[i - 1].seq + 1,
                    "Messages should be sequential"
                );
            }
        }

        #[tokio::test]
        async fn test_fetch_without_join() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();

            let manager = build_manager(config).await;
            manager
                .create_stream("test_stream".to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            manager
                .publish("test_stream", Bytes::new(), Bytes::from("msg"))
                .await
                .unwrap();

            let identity = ConsumerIdentity {
                connection_id: "client-A".to_string(),
                consumer_id: "ghost".to_string(),
                generation: 1,
            };
            let result = manager
                .fetch("test_stream", "test_group", &identity, 10, 0)
                .await;
            assert!(result.is_err(), "Fetch without join should fail");
        }

        #[tokio::test]
        async fn test_per_key_ordering_same_key_serial() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "per-key-serial";
            let group = "g-pk-serial";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let key = Bytes::from("order-A");
            for i in 1..=3 {
                manager
                    .publish(name, key.clone(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;

            let batch1 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].message.seq, 1);

            let batch2 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 0, "Same-key messages must wait for ack");

            ack_delivery(&manager, group, name, &consumer, &batch1[0]).await;
            let batch3 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1);
            assert_eq!(batch3[0].seq, 2);
        }

        #[tokio::test]
        async fn test_per_key_ordering_different_keys_parallel() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "per-key-parallel";
            let group = "g-pk-parallel";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let key_a = Bytes::from("key-A");
            let key_b = Bytes::from("key-B");
            manager
                .publish(name, key_a.clone(), Bytes::from("msg-1"))
                .await
                .unwrap();
            manager
                .publish(name, key_b.clone(), Bytes::from("msg-2"))
                .await
                .unwrap();
            manager
                .publish(name, key_a.clone(), Bytes::from("msg-3"))
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;

            let batch = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(
                batch.len(),
                2,
                "Different keys should be delivered in parallel"
            );
            assert_eq!(batch[0].seq, 1);
            assert_eq!(batch[1].seq, 2);

            let batch2 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 0, "Same-key msg-3 must wait for msg-1 ack");
        }

        #[tokio::test]
        async fn test_per_key_ordering_ack_unblocks_blocked() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "per-key-unblock";
            let group = "g-pk-unblock";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let key = Bytes::from("key-X");
            for i in 1..=3 {
                manager
                    .publish(name, key.clone(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;

            let batch1 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].message.seq, 1);

            let batch2 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 0);

            ack_delivery(&manager, group, name, &consumer, &batch1[0]).await;
            let batch3 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1);
            assert_eq!(batch3[0].message.seq, 2);

            ack_delivery(&manager, group, name, &consumer, &batch3[0]).await;
            let batch4 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch4.len(), 1);
            assert_eq!(batch4[0].seq, 3);
        }

        #[tokio::test]
        async fn test_per_key_ordering_no_key_unaffected() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "per-key-none";
            let group = "g-pk-none";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=5 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;
            let batch = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(
                batch.len(),
                5,
                "No-key messages should all be delivered without ordering constraints"
            );
        }

        #[tokio::test]
        async fn test_per_key_ordering_park_all_same_key() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "per-key-park";
            let group = "g-pk-park";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let key = Bytes::from("poison-key");
            for i in 1..=3 {
                manager
                    .publish(name, key.clone(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;

            let batch1 = fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].seq, 1);

            tokio::time::sleep(Duration::from_millis(120)).await;
            let batch2 = fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(batch2.len(), 1);
            assert_eq!(batch2[0].seq, 1);

            tokio::time::sleep(Duration::from_millis(300)).await;
            let batch3 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(
                batch3.len(),
                0,
                "All same-key messages should be parked when one is parked"
            );

            let dls_entries = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(
                dls_entries.len(),
                3,
                "all 3 same-key messages should be in DLS"
            );
        }

        #[tokio::test]
        async fn test_per_key_ordering_timeout_does_not_break_ordering() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 10;
            let manager = build_manager(config).await;
            let name = "per-key-timeout-order";
            let group = "g-pk-timeout-order";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let key = Bytes::from("key-K");
            manager
                .publish(name, key.clone(), Bytes::from("msg-1"))
                .await
                .unwrap();
            manager
                .publish(name, key.clone(), Bytes::from("msg-2"))
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;

            // Fetch msg-1 only (limit=1). msg-2 stays fresh, not yet delivered.
            let batch1 = fetch_deliveries(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].message.seq, 1);

            // Wait for msg-1 to timeout (ack_wait=50ms). It goes back to
            // redeliver; the lane stays leased-locked so msg-2 must NOT come.
            tokio::time::sleep(Duration::from_millis(120)).await;

            let batch2 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(
                batch2.len(),
                1,
                "only msg-1 should be available (key K still locked)"
            );
            assert_eq!(batch2[0].message.seq, 1, "msg-1 must be redelivered before msg-2");

            // The redelivery carries a fresh receipt — ack that one.
            ack_delivery(&manager, group, name, &consumer, &batch2[0]).await;

            let batch3 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1);
            assert_eq!(batch3[0].seq, 2, "msg-2 delivered after msg-1 acked");
        }

        #[tokio::test]
        async fn test_dls_peek_after_park() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "dls-peek";
            let group = "g-dls-peek";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            manager
                .publish(name, Bytes::new(), Bytes::from("poison"))
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;
            let batch = fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            assert_eq!(batch.len(), 1);

            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(dls.len(), 1);
            assert_eq!(dls[0].seq, 1, "seq 1 should be in DLS");
            assert!(
                dls[0].reason.contains("max_deliveries"),
                "reason should mention max_deliveries"
            );
            assert_eq!(dls[0].attempts, 2, "attempts should be 2");
        }

        #[tokio::test]
        async fn test_dls_move_to_stream_redelivers() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "dls-move";
            let group = "g-dls-move";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            manager
                .publish(name, Bytes::new(), Bytes::from("poison"))
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(dls.len(), 1);

            manager.move_to_stream(name, group, 1).await.unwrap();

            let dls_after = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(dls_after.len(), 0, "DLS should be empty after moveToStream");

            let batch = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch.len(), 1);
            assert_eq!(
                batch[0].seq, 1,
                "msg should be redelivered after moveToStream"
            );
        }

        #[tokio::test]
        async fn test_dls_move_to_stream_preserves_order() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "dls-order";
            let group = "g-dls-order";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            let key = Bytes::from("order-key");
            for i in 1..=3 {
                manager
                    .publish(name, key.clone(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            fetch_messages(&manager, group, name, &consumer, 10, 0).await;

            let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(dls.len(), 3);

            manager.move_to_stream(name, group, 3).await.unwrap();
            manager.move_to_stream(name, group, 2).await.unwrap();
            manager.move_to_stream(name, group, 1).await.unwrap();

            let batch1 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(
                batch1[0].message.seq, 1,
                "seq-1 must be first despite moveToStream(3) called first"
            );
            ack_delivery(&manager, group, name, &consumer, &batch1[0]).await;

            let batch2 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 1);
            assert_eq!(batch2[0].message.seq, 2, "seq-2 must be second");
            ack_delivery(&manager, group, name, &consumer, &batch2[0]).await;

            let batch3 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1);
            assert_eq!(batch3[0].seq, 3, "seq-3 must be third");
        }

        #[tokio::test]
        async fn test_dls_delete_removes_entry() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "dls-delete";
            let group = "g-dls-delete";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            manager
                .publish(name, Bytes::new(), Bytes::from("poison"))
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            manager.delete_dls(name, group, 1).await.unwrap();

            let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(dls.len(), 0, "DLS should be empty after delete");

            let batch = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch.len(), 0, "Deleted message should not be redelivered");
        }

        #[tokio::test]
        async fn test_dls_purge_clears_all() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "dls-purge";
            let group = "g-dls-purge";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            for i in 1..=3 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;
            fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert!(dls.len() >= 1, "Should have entries in DLS");

            let count = manager.purge_dls(name, group).await.unwrap();
            assert!(count >= 1, "Purge should return count of removed entries");

            let dls_after = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(dls_after.len(), 0, "DLS should be empty after purge");
        }

        /// Deleting every parked point for a key resumes the lane at the key
        /// tail: later publishes flow again, deleted positions never resurface.
        #[tokio::test]
        async fn test_dls_auto_unblock_on_last_entry() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "dls-autounblock";
            let group = "g-dls-autounblock";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let key = Bytes::from("block-key");
            for i in 1..=3 {
                manager
                    .publish(name, key.clone(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let consumer = join_session(&manager, group, name, "client-A").await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, name, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            let batch_park = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(
                batch_park.len(),
                0,
                "All same-key messages should be auto-parked"
            );

            let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
            assert_eq!(dls.len(), 3, "All 3 same-key messages should be in DLS");

            // Delete one — the open range still covers the key tail.
            manager.delete_dls(name, group, 1).await.unwrap();
            // Publish while parked: the open range covers the new position too.
            manager
                .publish(name, key.clone(), Bytes::from("msg-4"))
                .await
                .unwrap();
            let batch = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(
                batch.len(),
                0,
                "Key should still be parked with remaining DLS entries"
            );

            manager.delete_dls(name, group, 2).await.unwrap();
            manager.delete_dls(name, group, 3).await.unwrap();
            let batch_still = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(
                batch_still.len(),
                0,
                "Key still parked because msg-4 is in DLS"
            );

            manager.delete_dls(name, group, 4).await.unwrap();

            manager
                .publish(name, key.clone(), Bytes::from("msg-5"))
                .await
                .unwrap();
            let batch_final = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch_final.len(), 1);
            assert_eq!(
                batch_final[0].seq, 5,
                "msg-5 should be delivered after all DLS entries cleared"
            );
        }

        #[tokio::test]
        async fn test_dls_persistence_across_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;

            let name = "dls-persist";
            let group = "g-dls-persist";
            let key = Bytes::from("persist-key");

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                for i in 1..=2 {
                    manager
                        .publish(name, key.clone(), Bytes::from(format!("msg-{}", i)))
                        .await
                        .unwrap();
                }

                let consumer = join_session(&manager, group, name, "client-A").await;
                fetch_messages(&manager, group, name, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(120)).await;
                fetch_messages(&manager, group, name, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(300)).await;

                let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
                assert!(dls.len() >= 1, "Should have DLS entries before restart");
                manager.shutdown().await;
            }

            {
                let manager2 = build_manager(config.clone()).await;

                let dls = manager2.peek_dls(name, group, 10, 0).await.unwrap();
                assert!(dls.len() >= 1, "DLS entries should persist across restart");

                manager2
                    .publish(name, key.clone(), Bytes::from("msg-3"))
                    .await
                    .unwrap();
                let consumer = join_session(&manager2, group, name, "client-A").await;
                let batch = fetch_messages(&manager2, group, name, &consumer, 10, 0).await;
                assert_eq!(
                    batch.len(),
                    0,
                    "Parked key should survive restart — msg-3 auto-parked"
                );
            }
        }

        #[tokio::test]
        async fn test_per_key_ordering_mixed_key_and_no_key() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "per-key-mixed";
            let group = "g-pk-mixed";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let key_a = Bytes::from("key-A");
            manager
                .publish(name, key_a.clone(), Bytes::from("msg-1"))
                .await
                .unwrap();
            manager
                .publish(name, Bytes::new(), Bytes::from("msg-2"))
                .await
                .unwrap();
            manager
                .publish(name, key_a.clone(), Bytes::from("msg-3"))
                .await
                .unwrap();
            manager
                .publish(name, Bytes::new(), Bytes::from("msg-4"))
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;

            let batch = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
            assert_eq!(batch.len(), 3, "Should get 1 key-A + 2 no-key messages");
            let seqs: Vec<u64> = batch.iter().map(|m| m.seq).collect();
            assert!(seqs.contains(&1));
            assert!(seqs.contains(&2));
            assert!(seqs.contains(&4));
            assert!(!seqs.contains(&3), "msg-3 (same key-A) must be blocked");
        }

        #[tokio::test]
        async fn test_generation_fencing_old_generation_rejected() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "fencing-test";
            let group = "g-fencing";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();
            manager
                .publish(name, Bytes::new(), Bytes::from("msg-1"))
                .await
                .unwrap();

            let consumer_a = join_session(&manager, group, name, "client-A").await;
            let old_gen = consumer_a.generation;

            let msgs = fetch_deliveries(&manager, group, name, &consumer_a, 10, 0).await;
            assert_eq!(msgs.len(), 1);

            manager
                .seek(
                    name,
                    group,
                    nexo::brokers::stream::options::SeekTarget::Beginning,
                )
                .await
                .unwrap();

            let consumer_b = join_session(&manager, group, name, "client-B").await;
            assert!(
                consumer_b.generation > old_gen,
                "New generation should be greater after seek"
            );

            let stale = ConsumerIdentity {
                connection_id: consumer_a.connection_id.clone(),
                consumer_id: consumer_a.consumer_id.clone(),
                generation: old_gen,
            };
            let result = manager.fetch(name, group, &stale, 10, 0).await;
            assert!(
                matches!(result, Err(ref error) if error.kind == nexo::brokers::BrokerErrorKind::Fenced),
                "Old generation fetch should be FENCED"
            );

            let ack_result = manager
                .ack(name, group, &stale, 1, msgs[0].receipt)
                .await;
            assert!(
                matches!(ack_result, Err(ref error) if error.kind == nexo::brokers::BrokerErrorKind::Fenced),
                "Old generation ack should be FENCED"
            );

            let msgs_b = fetch_deliveries(&manager, group, name, &consumer_b, 10, 0).await;
            assert_eq!(
                msgs_b.len(),
                1,
                "New consumer should receive redelivered msg"
            );
            assert_eq!(msgs_b[0].message.seq, 1);
            ack_delivery(&manager, group, name, &consumer_b, &msgs_b[0]).await;
        }
    }

    mod batch {
        use super::*;

        #[tokio::test]
        async fn test_batch_publish_single_item() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "batch-single";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let seqs = manager
                .publish_batch(name, vec![item(Bytes::new(), Bytes::from("hello"))])
                .await
                .unwrap();
            assert_eq!(seqs.len(), 1);
            assert_eq!(seqs[0], 1);

            let msgs = manager.read(name, 1, 100).await.unwrap();
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].payload, Bytes::from("hello"));
        }

        #[tokio::test]
        async fn test_batch_publish_multiple_items() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "batch-multi";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let items: Vec<PubItem> = (1..=5)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            let seqs = manager.publish_batch(name, items).await.unwrap();
            assert_eq!(seqs.len(), 5);
            assert_eq!(seqs, vec![1, 2, 3, 4, 5]);

            let msgs = manager.read(name, 1, 100).await.unwrap();
            assert_eq!(msgs.len(), 5);
            for (i, msg) in msgs.iter().enumerate() {
                assert_eq!(msg.payload, Bytes::from(format!("msg-{}", i + 1)));
            }
        }

        #[tokio::test]
        async fn test_batch_publish_with_keys() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "batch-keys";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let items = vec![
                item(Bytes::from("key-A"), Bytes::from("msg-1")),
                item(Bytes::from("key-B"), Bytes::from("msg-2")),
                item(Bytes::new(), Bytes::from("msg-3")),
            ];
            let seqs = manager.publish_batch(name, items).await.unwrap();
            assert_eq!(seqs.len(), 3);

            let msgs = manager.read(name, 1, 100).await.unwrap();
            assert_eq!(msgs[0].key, Bytes::from("key-A"));
            assert_eq!(msgs[1].key, Bytes::from("key-B"));
            assert!(msgs[2].key.is_empty());
        }

        #[tokio::test]
        async fn test_batch_publish_empty() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "batch-empty";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let seqs = manager.publish_batch(name, vec![]).await.unwrap();
            assert!(seqs.is_empty());

            let msgs = manager.read(name, 1, 100).await.unwrap();
            assert!(msgs.is_empty());
        }

        #[tokio::test]
        async fn test_batch_publish_nonexistent_stream() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;

            let result = manager
                .publish_batch("nonexistent", vec![item(Bytes::new(), Bytes::from("data"))])
                .await;
            assert!(result.is_err());
        }
    }

    mod regression {
        use super::*;

        // Concurrent publish must preserve contiguous seqs.
        #[tokio::test]
        async fn concurrent_publish_preserves_contiguous_seqs() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "reg-concurrent-pub";
            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            const PUBLISHERS: usize = 4;
            const MSGS_PER: usize = 50;
            const TOTAL: usize = PUBLISHERS * MSGS_PER;

            let mut handles = Vec::new();
            for _ in 0..PUBLISHERS {
                let m = manager.clone();
                let t = name.to_string();
                handles.push(tokio::spawn(async move {
                    let items: Vec<PubItem> = (0..MSGS_PER)
                        .map(|i| item(Bytes::new(), Bytes::from(format!("p-{}", i))))
                        .collect();
                    m.publish_batch(&t, items).await.unwrap()
                }));
            }

            let mut all_seqs = Vec::new();
            for h in handles {
                all_seqs.extend(h.await.unwrap());
            }

            all_seqs.sort();
            assert_eq!(all_seqs.len(), TOTAL);
            assert_eq!(all_seqs[0], 1);
            assert_eq!(all_seqs[TOTAL - 1], TOTAL as u64);
            let unique: std::collections::HashSet<u64> = all_seqs.iter().copied().collect();
            assert_eq!(
                unique.len(),
                TOTAL,
                "No duplicate seqs across concurrent publishers"
            );
            for i in 0..TOTAL {
                assert_eq!(all_seqs[i], (i + 1) as u64, "Seq gap at index {}", i);
            }

            let msgs = manager.read(name, 1, TOTAL as usize * 2).await.unwrap();
            assert_eq!(
                msgs.len(),
                TOTAL,
                "All messages must be readable after concurrent publish"
            );
        }

        #[tokio::test]
        async fn concurrent_reads_different_streams() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;

            const NUM_STREAMS: usize = 5;
            const MSGS_PER_STREAM: usize = 20;

            for t in 0..NUM_STREAMS {
                let name = format!("stream-{}", t);
                manager
                    .create_stream(name.clone(), StreamCreateOptions::default())
                    .await
                    .unwrap();
                for i in 0..MSGS_PER_STREAM {
                    manager
                        .publish(&name, Bytes::new(), Bytes::from(format!("t{}-m{}", t, i)))
                        .await
                        .unwrap();
                }
            }

            let mut handles = Vec::new();
            for t in 0..NUM_STREAMS {
                let m = manager.clone();
                let name = format!("stream-{}", t);
                handles.push(tokio::spawn(async move {
                    let msgs = m.read(&name, 1, MSGS_PER_STREAM * 2).await.unwrap();
                    assert_eq!(
                        msgs.len(),
                        MSGS_PER_STREAM,
                        "Stream {} should have {} messages",
                        t,
                        MSGS_PER_STREAM
                    );
                    for (i, msg) in msgs.iter().enumerate() {
                        assert_eq!(msg.seq, (i + 1) as u64);
                        assert_eq!(msg.payload, Bytes::from(format!("t{}-m{}", t, i)));
                    }
                }));
            }

            for h in handles {
                h.await.unwrap();
            }
        }

        /// A publish rejected at admission must not consume a sequence: the
        /// next valid publish still gets seq 1.
        #[tokio::test]
        async fn rejected_publish_does_not_commit_sequence() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "reg-append-failure";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            // Oversized key → rejected before reaching storage.
            let oversized_key = Bytes::from(vec![b'k'; nexo::protocol::STREAM_MAX_KEY_BYTES + 1]);
            let error = manager
                .publish(name, oversized_key, Bytes::from("failed"))
                .await
                .unwrap_err();
            assert_eq!(error.kind, BrokerErrorKind::InvalidArgument);

            let seq = manager
                .publish(name, Bytes::new(), Bytes::from("committed"))
                .await
                .unwrap();
            assert_eq!(seq, 1, "a rejected publish must not consume a sequence");
            let messages = manager.read(name, 1, 10).await.unwrap();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].payload, Bytes::from("committed"));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn bounded_storage_queue_waits_without_dropping_publishes() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.storage_queue_capacity = 1;
            let manager = build_manager(config).await;

            const STREAMS: usize = 32;
            let start = Arc::new(tokio::sync::Barrier::new(STREAMS));
            for name in 0..STREAMS {
                manager
                    .create_stream(format!("bounded-{}", name), StreamCreateOptions::default())
                    .await
                    .unwrap();
            }

            let mut handles = Vec::with_capacity(STREAMS);
            for name in 0..STREAMS {
                let manager = manager.clone();
                let start = start.clone();
                handles.push(tokio::spawn(async move {
                    start.wait().await;
                    let stream_name = format!("bounded-{}", name);
                    manager
                        .publish(&stream_name, Bytes::new(), Bytes::from(format!("message-{}", name)))
                        .await
                }));
            }

            for handle in handles {
                assert_eq!(handle.await.unwrap().unwrap(), 1);
            }

            for name in 0..STREAMS {
                let messages = manager
                    .read(&format!("bounded-{}", name), 1, 10)
                    .await
                    .unwrap();
                assert_eq!(messages.len(), 1);
                assert_eq!(
                    messages[0].payload,
                    Bytes::from(format!("message-{}", name))
                );
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_delete_and_publish_cannot_resurrect_stream() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;

            for round in 0..20 {
                let name = format!("delete-race-{}", round);
                manager
                    .create_stream(name.clone(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                let deleting_manager = manager.clone();
                let deleting_name = name.clone();
                let delete =
                    tokio::spawn(
                        async move { deleting_manager.delete_stream(deleting_name).await },
                    );

                let mut publishers = Vec::new();
                for publisher in 0..16 {
                    let publishing_manager = manager.clone();
                    let publishing_name = name.clone();
                    publishers.push(tokio::spawn(async move {
                        publishing_manager
                            .publish(
                                &publishing_name,
                                Bytes::new(),
                                Bytes::from(format!("publisher-{}", publisher)),
                            )
                            .await
                    }));
                }

                delete
                    .await
                    .unwrap_or_else(|error| panic!("delete task failed in round {round}: {error}"))
                    .unwrap_or_else(|error| panic!("delete failed in round {round}: {error}"));
                for publisher in publishers {
                    let _ = publisher.await.unwrap();
                }

                assert!(!manager.exists(&name).await.unwrap());
            }
        }

        // Keyless DLS messages must not be redelivered after restart.
        #[tokio::test]
        async fn keyless_dls_not_redelivered_after_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap();
            let mut config = get_test_config(Some(path));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;

            let name = "reg-keyless-dls";
            let group = "g-reg-keyless-dls";

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();
                manager
                    .publish(name, Bytes::new(), Bytes::from("poison"))
                    .await
                    .unwrap();

                let consumer = join_session(&manager, group, name, "client-A").await;
                fetch_messages(&manager, group, name, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(120)).await;
                fetch_messages(&manager, group, name, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(300)).await;

                let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
                assert_eq!(dls.len(), 1, "Message should be in DLS before restart");
                manager.shutdown().await;
            }

            {
                let manager2 = build_manager(config.clone()).await;
                let consumer = join_session(&manager2, group, name, "client-A").await;
                let batch = fetch_messages(&manager2, group, name, &consumer, 10, 0).await;
                assert!(
                    batch.is_empty(),
                    "Keyless DLS message must NOT be redelivered after restart"
                );

                let dls = manager2.peek_dls(name, group, 10, 0).await.unwrap();
                assert_eq!(dls.len(), 1, "DLS entry should survive restart");
            }
        }

        #[tokio::test]
        async fn dls_move_to_stream_survives_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap();
            let mut config = get_test_config(Some(path));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let name = "reg-dls-redrive-restart";
            let group = "g-reg-dls-redrive-restart";

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();
                manager
                    .publish(name, Bytes::new(), Bytes::from("poison"))
                    .await
                    .unwrap();

                let consumer = join_session(&manager, group, name, "client-A").await;
                fetch_messages(&manager, group, name, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(120)).await;
                fetch_messages(&manager, group, name, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(300)).await;

                assert_eq!(
                    manager.peek_dls(name, group, 10, 0).await.unwrap().len(),
                    1
                );
                manager.move_to_stream(name, group, 1).await.unwrap();
                manager.shutdown().await;
            }

            let recovered = build_manager(config).await;
            let consumer = join_session(&recovered, group, name, "client-B").await;
            let messages = fetch_messages(&recovered, group, name, &consumer, 1, 0).await;

            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].seq, 1);
            assert_eq!(messages[0].payload, Bytes::from("poison"));
        }

        // Ack must wake backpressured long-polling consumers.
        #[tokio::test]
        async fn ack_wakes_backpressured_long_poll() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 2;
            let manager = build_manager(config).await;
            let name = "reg-ack-wake";
            let group = "g-reg-ack-wake";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=4)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;

            let msgs = fetch_deliveries(&manager, group, name, &consumer, 2, 0).await;
            assert_eq!(msgs.len(), 2);

            let m = manager.clone();
            let name_clone = name.to_string();
            let group_clone = group.to_string();
            let identity = consumer.identity();
            let fetch_handle = tokio::spawn(async move {
                m.fetch(&name_clone, &group_clone, &identity, 10, 5000)
                    .await
                    .unwrap()
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            let start = Instant::now();
            manager
                .ack(
                    name,
                    group,
                    &consumer.identity(),
                    msgs[0].message.seq,
                    msgs[0].receipt,
                )
                .await
                .unwrap();

            let result = fetch_handle.await.unwrap();
            let elapsed = start.elapsed();

            assert!(
                !result.is_empty(),
                "Long-poll should deliver messages after ack frees a slot"
            );
            assert!(
                elapsed < Duration::from_millis(2000),
                "Long-poll should wake quickly after ack, took {:?}",
                elapsed
            );
        }

        // Publish during active long-poll must deliver to waiting consumer.
        #[tokio::test]
        async fn publish_during_fetch_interleaving() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "reg-interleave";
            let group = "g-reg-interleave";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let consumer = join_session(&manager, group, name, "client-A").await;

            let m = manager.clone();
            let name_clone = name.to_string();
            let group_clone = group.to_string();
            let identity = consumer.identity();
            let fetch_handle = tokio::spawn(async move {
                m.fetch(&name_clone, &group_clone, &identity, 10, 5000)
                    .await
                    .unwrap()
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            let start = Instant::now();
            manager
                .publish(name, Bytes::new(), Bytes::from("interleaved-msg"))
                .await
                .unwrap();

            let result = fetch_handle.await.unwrap();
            let elapsed = start.elapsed();

            assert_eq!(
                result.len(),
                1,
                "Long-poll should receive the published message"
            );
            assert_eq!(result[0].message.payload, Bytes::from("interleaved-msg"));
            assert!(
                elapsed < Duration::from_millis(2000),
                "Long-poll should wake quickly after publish, took {:?}",
                elapsed
            );
        }

        /// Read-after-write consistency through the store: every committed
        /// publish is immediately readable, ranges included.
        #[tokio::test]
        async fn read_after_write_consistent() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "read-consistency";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            for i in 1..=3 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let msgs1 = manager.read(name, 1, 100).await.unwrap();
            assert_eq!(msgs1.len(), 3, "first read should see all 3 messages");
            assert_eq!(msgs1[0].payload, Bytes::from("msg-1"));
            assert_eq!(msgs1[2].payload, Bytes::from("msg-3"));

            for i in 4..=6 {
                manager
                    .publish(name, Bytes::new(), Bytes::from(format!("msg-{}", i)))
                    .await
                    .unwrap();
            }

            let msgs2 = manager.read(name, 1, 100).await.unwrap();
            assert_eq!(msgs2.len(), 6, "second read should see all 6 messages");
            for (i, msg) in msgs2.iter().enumerate() {
                assert_eq!(msg.payload, Bytes::from(format!("msg-{}", i + 1)));
                assert_eq!(msg.seq, (i + 1) as u64);
            }

            let msgs3 = manager.read(name, 4, 2).await.unwrap();
            assert_eq!(msgs3.len(), 2, "partial read should see 2 messages");
            assert_eq!(msgs3[0].seq, 4);
            assert_eq!(msgs3[1].seq, 5);
        }
    }

    mod stress {
        use super::*;

        #[tokio::test]
        async fn test_high_pending_fetch_and_mass_ack() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 5000;
            let manager = build_manager(config).await;
            let name = "stress-high-pending";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=5000)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            let seqs = manager.publish_batch(name, batch).await.unwrap();
            assert_eq!(seqs.len(), 5000);

            let consumer = join_session(&manager, "grp1", name, "conn1").await;
            let msgs = fetch_deliveries(&manager, "grp1", name, &consumer, 5000, 100).await;
            assert_eq!(msgs.len(), 5000);

            for d in &msgs {
                ack_delivery(&manager, "grp1", name, &consumer, d).await;
            }

            let msgs2 = fetch_messages(&manager, "grp1", name, &consumer, 10, 50).await;
            assert!(msgs2.is_empty());
        }

        #[tokio::test]
        async fn test_high_pending_out_of_order_ack() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 1000;
            let manager = build_manager(config).await;
            let name = "stress-ooo-ack";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=1000)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", name, "conn1").await;
            let msgs = fetch_deliveries(&manager, "grp1", name, &consumer, 1000, 100).await;
            assert_eq!(msgs.len(), 1000);

            for d in msgs.iter().rev() {
                ack_delivery(&manager, "grp1", name, &consumer, d).await;
            }

            let msgs2 = fetch_messages(&manager, "grp1", name, &consumer, 10, 50).await;
            assert!(msgs2.is_empty());
        }

        #[tokio::test]
        async fn test_mass_redelivery_after_timeout() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 500;
            config.ack_wait_ms = 100;
            let manager = build_manager(config).await;
            let name = "stress-redelivery";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=200)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", name, "conn1").await;
            let msgs = fetch_messages(&manager, "grp1", name, &consumer, 200, 100).await;
            assert_eq!(msgs.len(), 200);

            tokio::time::sleep(Duration::from_millis(150)).await;

            let msgs2 = fetch_messages(&manager, "grp1", name, &consumer, 200, 200).await;
            assert_eq!(msgs2.len(), 200);
            let seqs: Vec<u64> = msgs2.iter().map(|m| m.seq).collect();
            assert_eq!(seqs[0], 1);
            assert_eq!(seqs[199], 200);
        }

        #[tokio::test]
        async fn test_clamp_head_with_high_pending() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 5000;
            let manager = build_manager(config).await;
            let name = "stress-clamp-head";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=3000)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", name, "conn1").await;
            let msgs = fetch_deliveries(&manager, "grp1", name, &consumer, 3000, 100).await;
            assert_eq!(msgs.len(), 3000);

            for d in msgs.iter().take(1000) {
                ack_delivery(&manager, "grp1", name, &consumer, d).await;
            }

            let read_msgs = manager.read(name, 1, 100).await.unwrap();
            assert!(!read_msgs.is_empty());
        }

        #[tokio::test]
        async fn test_dls_under_load_with_max_deliveries() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 100;
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let name = "stress-dls";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=50)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", name, "conn1").await;

            for _round in 0..3 {
                let msgs = fetch_messages(&manager, "grp1", name, &consumer, 50, 200).await;
                if msgs.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(80)).await;
            }

            let dls_entries = manager.peek_dls(name, "grp1", 100, 0).await.unwrap();
            assert!(
                !dls_entries.is_empty(),
                "DLS should have entries after max_deliveries exceeded"
            );
        }

        #[tokio::test]
        async fn test_multi_consumer_stress() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 5000;
            let manager = build_manager(config).await;
            let name = "stress-multi-consumer";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=1000)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let c1 = join_session(&manager, "grp1", name, "conn1").await;
            let c2 = join_session(&manager, "grp1", name, "conn2").await;

            let msgs1 = fetch_deliveries(&manager, "grp1", name, &c1, 500, 100).await;
            let msgs2 = fetch_deliveries(&manager, "grp1", name, &c2, 500, 100).await;

            let total = msgs1.len() + msgs2.len();
            assert_eq!(total, 1000);

            let seqs1: std::collections::HashSet<u64> =
                msgs1.iter().map(|d| d.message.seq).collect();
            let seqs2: std::collections::HashSet<u64> =
                msgs2.iter().map(|d| d.message.seq).collect();
            assert!(seqs1.is_disjoint(&seqs2));

            for d in &msgs1 {
                ack_delivery(&manager, "grp1", name, &c1, d).await;
            }
            for d in &msgs2 {
                ack_delivery(&manager, "grp1", name, &c2, d).await;
            }

            let msgs3 = fetch_messages(&manager, "grp1", name, &c1, 10, 50).await;
            assert!(msgs3.is_empty());
        }

        #[tokio::test]
        async fn test_persistent_state_after_mass_ack() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap();
            let mut config = get_test_config(Some(path));
            config.max_ack_pending = 2000;
            let name = "stress-persist";

            {
                let manager = build_manager(config.clone()).await;
                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                let batch: Vec<PubItem> = (1..=1000)
                    .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                    .collect();
                manager.publish_batch(name, batch).await.unwrap();

                let consumer = join_session(&manager, "grp1", name, "conn1").await;
                let msgs = fetch_deliveries(&manager, "grp1", name, &consumer, 1000, 100).await;
                assert_eq!(msgs.len(), 1000);

                for d in &msgs {
                    ack_delivery(&manager, "grp1", name, &consumer, d).await;
                }
                manager.shutdown().await;
            }

            let manager2 = build_manager(config).await;
            assert!(manager2.exists(name).await.unwrap());

            let consumer2 = join_session(&manager2, "grp1", name, "conn1").await;
            let msgs2 = fetch_messages(&manager2, "grp1", name, &consumer2, 10, 50).await;
            assert!(
                msgs2.is_empty(),
                "Should not re-deliver acked messages after restart"
            );
        }

        #[tokio::test]
        async fn test_per_key_ordering_stress() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 5000;
            let manager = build_manager(config).await;
            let name = "stress-per-key";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=100)
                .map(|i| {
                    let key = Bytes::from(format!("key-{}", i % 10));
                    item(key, Bytes::from(format!("msg-{}", i)))
                })
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", name, "conn1").await;

            let msgs = fetch_deliveries(&manager, "grp1", name, &consumer, 100, 100).await;
            assert_eq!(msgs.len(), 10);

            let keys: std::collections::HashSet<&Bytes> =
                msgs.iter().map(|d| &d.message.key).collect();
            assert_eq!(keys.len(), 10);

            for d in &msgs {
                ack_delivery(&manager, "grp1", name, &consumer, d).await;
            }

            let msgs2 = fetch_deliveries(&manager, "grp1", name, &consumer, 100, 100).await;
            assert_eq!(msgs2.len(), 10);

            for d in &msgs2 {
                ack_delivery(&manager, "grp1", name, &consumer, d).await;
            }

            for _round in 0..8 {
                let msgs_n = fetch_deliveries(&manager, "grp1", name, &consumer, 100, 100).await;
                assert_eq!(msgs_n.len(), 10, "Each round should deliver 10 messages");
                for d in &msgs_n {
                    ack_delivery(&manager, "grp1", name, &consumer, d).await;
                }
            }

            let final_msgs = fetch_messages(&manager, "grp1", name, &consumer, 10, 50).await;
            assert!(final_msgs.is_empty());
        }

        #[tokio::test]
        async fn test_disconnect_redelivery_stress() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 1000;
            let manager = build_manager(config).await;
            let name = "stress-disconnect";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=500)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let c1 = join_session(&manager, "grp1", name, "conn1").await;
            let msgs1 = fetch_messages(&manager, "grp1", name, &c1, 500, 100).await;
            assert_eq!(msgs1.len(), 500);

            manager.disconnect(&c1.connection_id).await;

            let c2 = join_session(&manager, "grp1", name, "conn2").await;
            let msgs2 = fetch_messages(&manager, "grp1", name, &c2, 500, 200).await;
            assert_eq!(msgs2.len(), 500);

            let seqs1: Vec<u64> = msgs1.iter().map(|m| m.seq).collect();
            let seqs2: Vec<u64> = msgs2.iter().map(|m| m.seq).collect();
            assert_eq!(seqs1, seqs2);
        }

        #[tokio::test]
        async fn test_fetch_partial_batch_does_not_block_next_deliver() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let name = "partial-batch";

            manager
                .create_stream(name.to_string(), StreamCreateOptions::default())
                .await
                .unwrap();

            let batch: Vec<PubItem> = (1..=10)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", name, "conn1").await;

            let msgs = fetch_deliveries(&manager, "grp1", name, &consumer, 20, 50).await;
            assert_eq!(
                msgs.len(),
                10,
                "should get all available messages, not block on missing seqs"
            );

            let seqs: Vec<u64> = msgs.iter().map(|d| d.message.seq).collect();
            assert_eq!(seqs, (1..=10).collect::<Vec<_>>());

            for d in &msgs {
                ack_delivery(&manager, "grp1", name, &consumer, d).await;
            }

            let batch2: Vec<PubItem> = (11..=15)
                .map(|i| item(Bytes::new(), Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(name, batch2).await.unwrap();

            let msgs2 = fetch_messages(&manager, "grp1", name, &consumer, 20, 50).await;
            assert_eq!(
                msgs2.len(),
                5,
                "should get only new messages, not retry old ones"
            );
            let seqs2: Vec<u64> = msgs2.iter().map(|m| m.seq).collect();
            assert_eq!(seqs2, (11..=15).collect::<Vec<_>>());
        }

        mod shutdown {
            use super::*;

            #[tokio::test]
            async fn test_shutdown_flushes_messages_and_group_state() {
                let temp_dir = tempfile::tempdir().unwrap();
                let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));

                let name = "shutdown-flush";
                let group = "g-shutdown";

                {
                    let manager = build_manager(config.clone()).await;
                    manager
                        .create_stream(name.to_string(), StreamCreateOptions::default())
                        .await
                        .unwrap();
                    manager
                        .publish(name, Bytes::new(), Bytes::from("msg1"))
                        .await
                        .unwrap();
                    manager
                        .publish(name, Bytes::new(), Bytes::from("msg2"))
                        .await
                        .unwrap();

                    let consumer = join_session(&manager, group, name, "client-A").await;
                    let msgs = fetch_deliveries(&manager, group, name, &consumer, 2, 0).await;
                    assert_eq!(msgs.len(), 2);
                    ack_delivery(&manager, group, name, &consumer, &msgs[0]).await;
                    ack_delivery(&manager, group, name, &consumer, &msgs[1]).await;

                    manager.shutdown().await;
                }

                {
                    let manager = build_manager(config).await;

                    let msgs = manager.read(name, 1, 10).await.unwrap();
                    assert_eq!(msgs.len(), 2, "Messages should survive shutdown flush");

                    let consumer = join_session(&manager, group, name, "client-A").await;
                    assert_eq!(
                        consumer.ack_floor, 2,
                        "Ack floor should survive shutdown flush"
                    );
                }
            }

            /// An ACK committed before LEAVE must not be redelivered to the
            /// next member of the group.
            #[tokio::test]
            async fn test_ack_before_leave_does_not_redeliver_acked() {
                let temp_dir = tempfile::tempdir().unwrap();
                let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
                let manager = build_manager(config).await;
                let name = "ack-before-leave";
                let group = "g-ack-leave";

                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();
                manager
                    .publish(name, Bytes::new(), Bytes::from("msg-1"))
                    .await
                    .unwrap();
                manager
                    .publish(name, Bytes::new(), Bytes::from("msg-2"))
                    .await
                    .unwrap();

                let consumer = join_session(&manager, group, name, "client-A").await;
                let msgs = fetch_deliveries(&manager, group, name, &consumer, 2, 0).await;
                assert_eq!(msgs.len(), 2);

                ack_delivery(&manager, group, name, &consumer, &msgs[0]).await;
                manager
                    .leave_group(name, group, &consumer.identity())
                    .await
                    .unwrap();

                let consumer2 = join_session(&manager, group, name, "client-B").await;
                let msgs2 = fetch_messages(&manager, group, name, &consumer2, 10, 0).await;
                assert_eq!(
                    msgs2.len(),
                    1,
                    "seq 1 was acked before LEAVE — must not be redelivered"
                );
                assert_eq!(msgs2[0].seq, 2);
            }

            /// After SEEK to beginning, a consumer that re-joins the group must
            /// see all messages from seq 1 again.
            #[tokio::test]
            async fn test_seek_to_beginning_then_refetch_returns_all_messages() {
                let temp_dir = tempfile::tempdir().unwrap();
                let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
                let manager = build_manager(config).await;
                let name = "seek-refetch";
                let group = "g-seek-refetch";

                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();
                for i in 1..=5 {
                    manager
                        .publish(name, Bytes::new(), Bytes::from(format!("msg-{i}")))
                        .await
                        .unwrap();
                }

                let consumer = join_session(&manager, group, name, "client-A").await;

                let msgs = fetch_deliveries(&manager, group, name, &consumer, 3, 0).await;
                assert_eq!(msgs.len(), 3);
                for m in &msgs {
                    ack_delivery(&manager, group, name, &consumer, m).await;
                }

                use nexo::brokers::stream::options::SeekTarget;
                manager
                    .seek(name, group, SeekTarget::Beginning)
                    .await
                    .unwrap();

                let consumer2 = join_session(&manager, group, name, "client-A").await;
                let msgs2 = fetch_messages(&manager, group, name, &consumer2, 10, 0).await;
                assert_eq!(
                    msgs2.len(),
                    5,
                    "SEEK to beginning should make all messages available"
                );
                assert_eq!(msgs2[0].seq, 1);
            }
        }

        mod delivery_guarantees {
            use super::*;
            use nexo::brokers::stream::options::{RetentionOptions, SeekTarget};

            /// SEEK must clear all DLS entries and parked keys — it is a full
            /// reset. After seek, a previously-poisoned key accepts messages.
            #[tokio::test]
            async fn seek_clears_dls_and_unblocks_parked_keys() {
                let temp_dir = tempfile::tempdir().unwrap();
                let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
                config.ack_wait_ms = 50;
                config.max_deliveries = 2;
                let manager = build_manager(config).await;
                let name = "seek-clears-dls";
                let group = "g-seek-clears-dls";
                let key = Bytes::from("poison-key");

                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                for i in 1..=3 {
                    manager
                        .publish(name, key.clone(), Bytes::from(format!("msg-{}", i)))
                        .await
                        .unwrap();
                }

                let consumer = join_session(&manager, group, name, "client-A").await;

                fetch_messages(&manager, group, name, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(120)).await;
                fetch_messages(&manager, group, name, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(300)).await;

                let batch_parked = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
                assert_eq!(
                    batch_parked.len(),
                    0,
                    "All same-key messages should be auto-parked"
                );

                let dls = manager.peek_dls(name, group, 10, 0).await.unwrap();
                assert_eq!(
                    dls.len(),
                    3,
                    "All 3 same-key messages should be in DLS before seek"
                );

                manager
                    .seek(name, group, SeekTarget::Beginning)
                    .await
                    .unwrap();

                let dls_after = manager.peek_dls(name, group, 10, 0).await.unwrap();
                assert_eq!(
                    dls_after.len(),
                    0,
                    "DLS should be empty after seek (full reset)"
                );

                let consumer2 = join_session(&manager, group, name, "client-A").await;
                manager
                    .publish(name, key.clone(), Bytes::from("msg-4"))
                    .await
                    .unwrap();

                let batch = fetch_deliveries(&manager, group, name, &consumer2, 10, 0).await;
                assert!(
                    !batch.is_empty(),
                    "Previously-parked key should be unblocked after seek"
                );
                assert_eq!(
                    batch[0].message.seq, 1,
                    "First message should be deliverable (key unblocked by seek)"
                );

                let mut current = batch[0].clone();
                for _ in 0..3 {
                    ack_delivery(&manager, group, name, &consumer2, &current).await;
                    let next = fetch_deliveries(&manager, group, name, &consumer2, 10, 0).await;
                    assert_eq!(next.len(), 1, "Next message should be unblocked after ack");
                    current = next.into_iter().next().unwrap();
                }
                assert_eq!(
                    current.message.seq, 4,
                    "Should reach msg-4 after acking predecessors"
                );
                assert_eq!(
                    fetch_messages(&manager, group, name, &consumer2, 10, 0)
                        .await
                        .len(),
                    0,
                    "No more messages after msg-4 is fetched"
                );
            }

            /// Retention options set via StreamCreateOptions must override
            /// system defaults.
            #[tokio::test]
            async fn retention_override_via_create_options_enforces_max_bytes() {
                let temp_dir = tempfile::tempdir().unwrap();
                let path_str = temp_dir.path().to_str().unwrap().to_string();

                let mut config = get_test_config(Some(&path_str));
                config.retention_check_interval_ms = 100;
                config.default_retention_bytes = 10_000_000;

                let manager = build_manager(config).await;
                let name = "retention-override";

                let options = StreamCreateOptions {
                    retention: Some(RetentionOptions {
                        max_age_ms: None,
                        max_bytes: Some(250),
                    }),
                };
                manager
                    .create_stream(name.to_string(), options)
                    .await
                    .unwrap();

                for i in 1..=7 {
                    manager
                        .publish(
                            name,
                            Bytes::new(),
                            Bytes::from(format!(
                                "msg{}-50bytes-payload-0000000000000000000000000000",
                                i
                            )),
                        )
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(60)).await;
                }

                tokio::time::sleep(Duration::from_millis(500)).await;

                let retained = manager.read(name, 1, 20).await.unwrap();
                assert!(!retained.is_empty(), "Should have retained messages");
                assert!(
                    retained[0].seq > 1,
                    "First message should be deleted by retention"
                );
            }

            /// Per-key ordering from the consumer's perspective: same-key
            /// messages are delivered one at a time in publication order while
            /// keyless messages flow in parallel.
            #[tokio::test]
            async fn per_key_ordering_consumer_receives_same_key_in_publication_order() {
                let temp_dir = tempfile::tempdir().unwrap();
                let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
                let manager = build_manager(config).await;
                let name = "per-key-e2e";
                let group = "g-per-key-e2e";
                let key_a = Bytes::from("user-A");
                let key_b = Bytes::from("user-B");

                manager
                    .create_stream(name.to_string(), StreamCreateOptions::default())
                    .await
                    .unwrap();

                manager
                    .publish(name, key_a.clone(), Bytes::from("A1"))
                    .await
                    .unwrap();
                manager
                    .publish(name, key_b.clone(), Bytes::from("B1"))
                    .await
                    .unwrap();
                manager
                    .publish(name, key_a.clone(), Bytes::from("A2"))
                    .await
                    .unwrap();
                manager
                    .publish(name, Bytes::new(), Bytes::from("no-key-1"))
                    .await
                    .unwrap();
                manager
                    .publish(name, key_b.clone(), Bytes::from("B2"))
                    .await
                    .unwrap();
                manager
                    .publish(name, key_a.clone(), Bytes::from("A3"))
                    .await
                    .unwrap();

                let consumer = join_session(&manager, group, name, "client-A").await;

                let batch1 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
                assert_eq!(batch1.len(), 3, "Should get 1 per key + 1 keyless");

                let payloads1: Vec<&str> = batch1
                    .iter()
                    .map(|d| std::str::from_utf8(&d.message.payload).unwrap())
                    .collect();
                assert!(payloads1.contains(&"A1"), "A1 should be delivered");
                assert!(payloads1.contains(&"B1"), "B1 should be delivered");
                assert!(payloads1.contains(&"no-key-1"), "Keyless message should be delivered");

                let a1 = batch1
                    .iter()
                    .find(|d| d.message.payload == Bytes::from("A1"))
                    .unwrap();
                ack_delivery(&manager, group, name, &consumer, a1).await;

                let batch2 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
                assert_eq!(batch2.len(), 1, "Only A2 should be unblocked after A1 ack");
                assert_eq!(
                    batch2[0].message.payload,
                    Bytes::from("A2"),
                    "A2 must be delivered after A1 is acked"
                );

                ack_delivery(&manager, group, name, &consumer, &batch2[0]).await;

                let batch3 = fetch_deliveries(&manager, group, name, &consumer, 10, 0).await;
                assert_eq!(batch3.len(), 1, "Only A3 should be unblocked after A2 ack");
                assert_eq!(
                    batch3[0].message.payload,
                    Bytes::from("A3"),
                    "A3 must be delivered after A2 is acked"
                );

                let b1 = batch1
                    .iter()
                    .find(|d| d.message.payload == Bytes::from("B1"))
                    .unwrap();
                ack_delivery(&manager, group, name, &consumer, b1).await;

                let batch4 = fetch_messages(&manager, group, name, &consumer, 10, 0).await;
                assert_eq!(batch4.len(), 1, "Only B2 should be unblocked after B1 ack");
                assert_eq!(
                    batch4[0].payload,
                    Bytes::from("B2"),
                    "B2 must be delivered after B1 is acked"
                );
            }
        }
    }
}

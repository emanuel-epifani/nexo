use nexo::brokers::stream::options::StreamCreateOptions;
use nexo::config::Config;
use bytes::Bytes;
use std::time::{Duration, Instant};
mod common;

#[cfg(test)]
mod stream_tests {
    use super::*;
    use nexo::brokers::stream::Message;
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

    async fn build_manager(config: nexo::brokers::stream::config::SystemStreamConfig) -> Arc<StreamManager> {
        Arc::new(StreamManager::new(Arc::new(config)).await)
    }

    async fn join_session(manager: &StreamManager, group: &str, topic: &str, client: &str) -> JoinGroupResult {
        manager.join_group(group, topic, client).await.unwrap()
    }

    async fn fetch_messages(manager: &StreamManager, group: &str, topic: &str, consumer: &JoinGroupResult, limit: usize, wait_ms: u64) -> Vec<Message> {
        manager.fetch(group, &consumer.consumer_id, consumer.generation, limit, topic, wait_ms).await.unwrap()
    }

    async fn ack_message(manager: &StreamManager, group: &str, topic: &str, consumer: &JoinGroupResult, seq: u64) {
        manager.ack(group, topic, &consumer.consumer_id, consumer.generation, seq).await.unwrap();
    }

    mod features {
        use super::*;

        #[tokio::test]
        async fn test_stream_basic_flow() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "test-basic-flow";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            assert!(manager.exists(topic).await);

            let payload = Bytes::from("hello world");
            let seq = manager.publish(topic, None, payload.clone()).await.unwrap();
            assert_eq!(seq, 1);

            let msgs = manager.read(topic, 1, 100).await;
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].payload, payload);
            assert_eq!(msgs[0].seq, 1);
        }

        #[tokio::test]
        async fn test_stream_ordering() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "ordering-topic";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            for i in 1..=3 {
                let payload = Bytes::from(format!("msg-{}", i));
                manager.publish(topic, None, payload).await.unwrap();
            }

            let msgs = manager.read(topic, 1, 10).await;
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
            let topic = "long-poll-wakeup";
            let group = "g-long-poll";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            let consumer = join_session(&manager, group, topic, "client-A").await;

            let publisher = manager.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                publisher.publish(topic, None, Bytes::from("wake-me")).await.unwrap();
            });

            let start = Instant::now();
            let msgs = fetch_messages(&manager, group, topic, &consumer, 10, 2_000).await;

            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].payload, Bytes::from("wake-me"));
            assert!(start.elapsed() < Duration::from_millis(1_000));
        }

        #[tokio::test]
        async fn test_long_poll_fetch_times_out() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "long-poll-timeout";
            let group = "g-timeout";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            let consumer = join_session(&manager, group, topic, "client-A").await;

            let start = Instant::now();
            let msgs = fetch_messages(&manager, group, topic, &consumer, 10, 250).await;
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
            let topic = "timeout-park-flow";
            let group = "g-timeout-park";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            manager.publish(topic, None, Bytes::from("msg-1")).await.unwrap();
            manager.publish(topic, None, Bytes::from("msg-2")).await.unwrap();

            let consumer = join_session(&manager, group, topic, "client-A").await;

            let first = fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            assert_eq!(first.len(), 1);
            assert_eq!(first[0].seq, 1);

            tokio::time::sleep(Duration::from_millis(120)).await;

            let second = fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            assert_eq!(second.len(), 1);
            assert_eq!(second[0].seq, 1);

            tokio::time::sleep(Duration::from_millis(120)).await;

            let third = fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            assert_eq!(third.len(), 1);
            assert_eq!(third[0].seq, 2);

            ack_message(&manager, group, topic, &consumer, 2).await;

            let probe = join_session(&manager, group, topic, "client-B").await;
            assert_eq!(probe.ack_floor, 2, "ack_floor must advance over DLT entries (msg-1 parked, msg-2 acked)");

            // Verify msg-1 is in DLT
            let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt.len(), 1);
            assert_eq!(dlt[0].0, 1, "msg-1 should be in DLT");
        }

        #[tokio::test]
        async fn test_ack_floor_advancement() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "ack-floor";
            let group = "g-floor";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            for i in 1..=5 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;
            let msgs = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(msgs.len(), 5);

            ack_message(&manager, group, topic, &consumer, 1).await;
            ack_message(&manager, group, topic, &consumer, 3).await;

            let ack_floor = join_session(&manager, group, topic, "client-B").await;
            assert_eq!(ack_floor.ack_floor, 1);

            ack_message(&manager, group, topic, &consumer, 2).await;
            let ack_floor = join_session(&manager, group, topic, "client-C").await;
            assert_eq!(ack_floor.ack_floor, 3);
        }

        #[tokio::test]
        async fn test_seek_beginning_end() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "seek-topic";
            let group = "g-seek";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            for i in 1..=10 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;
            let msgs = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            for msg in &msgs {
                ack_message(&manager, group, topic, &consumer, msg.seq).await;
            }

            let ack_floor = join_session(&manager, group, topic, "client-B").await;
            assert_eq!(ack_floor.ack_floor, 10);

            let empty = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert!(empty.is_empty());

            use nexo::brokers::stream::options::SeekTarget;
            manager.seek(group, topic, SeekTarget::Beginning).await.unwrap();

            let fenced = manager.fetch(group, &consumer.consumer_id, consumer.generation, 10, topic, 0).await;
            assert!(matches!(fenced, Err(ref e) if e == "FENCED"));

            let replay = join_session(&manager, group, topic, "client-A").await;
            let from_start = fetch_messages(&manager, group, topic, &replay, 10, 0).await;
            assert_eq!(from_start.len(), 10);
            assert_eq!(from_start[0].seq, 1);

            for msg in &from_start {
                ack_message(&manager, group, topic, &replay, msg.seq).await;
            }
            manager.seek(group, topic, SeekTarget::End).await.unwrap();

            let tail = join_session(&manager, group, topic, "client-A").await;
            let after_end = fetch_messages(&manager, group, topic, &tail, 10, 0).await;
            assert!(after_end.is_empty());
        }

        #[tokio::test]
        async fn test_seek_cancels_inflight_fetch() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "seek-cancel-fetch";
            let group = "g-seek-cancel";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            for i in 1..=5 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;
            let msgs = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            for msg in &msgs {
                ack_message(&manager, group, topic, &consumer, msg.seq).await;
            }

            let fetch_manager = manager.clone();
            let consumer_id = consumer.consumer_id.clone();
            let generation = consumer.generation;
            let fetch_handle = tokio::spawn(async move {
                let start = Instant::now();
                let result = fetch_manager.fetch(group, &consumer_id, generation, 10, topic, 5_000).await;
                (start.elapsed(), result)
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            use nexo::brokers::stream::options::SeekTarget;
            manager.seek(group, topic, SeekTarget::Beginning).await.unwrap();

            let (elapsed, result) = fetch_handle.await.unwrap();

            assert!(elapsed < Duration::from_millis(1_000), "Fetch should have been cancelled by seek, took {:?}", elapsed);
            assert!(result.unwrap().is_empty());

            let from_start_consumer = join_session(&manager, group, topic, "client-A").await;
            let from_start = fetch_messages(&manager, group, topic, &from_start_consumer, 10, 0).await;
            assert!(!from_start.is_empty(), "Should have messages from beginning after seek");
            assert_eq!(from_start[0].seq, 1);
        }

        #[tokio::test]
        async fn test_leave_cancels_inflight_fetch() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "leave-cancel-fetch";
            let group = "g-leave-cancel";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            let consumer = join_session(&manager, group, topic, "client-A").await;

            let fetch_manager = manager.clone();
            let consumer_id = consumer.consumer_id.clone();
            let generation = consumer.generation;
            let fetch_handle = tokio::spawn(async move {
                let start = Instant::now();
                let result = fetch_manager.fetch(group, &consumer_id, generation, 10, topic, 5_000).await;
                (start.elapsed(), result)
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            manager.leave_group(group, topic, &consumer.consumer_id, consumer.generation).await.unwrap();

            let (elapsed, result) = fetch_handle.await.unwrap();

            assert!(elapsed < Duration::from_millis(1_000), "Fetch should have been cancelled by leave, took {:?}", elapsed);
            assert!(result.unwrap().is_empty(), "Cancelled fetch should return empty");
        }

        #[tokio::test]
        async fn test_multi_consumer_parallel_fetch() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "multi-consumer";
            let group = "g-multi";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            for i in 1..=6 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer_a = join_session(&manager, group, topic, "client-A").await;
            let consumer_b = join_session(&manager, group, topic, "client-B").await;

            let msgs_a = fetch_messages(&manager, group, topic, &consumer_a, 3, 0).await;
            let msgs_b = fetch_messages(&manager, group, topic, &consumer_b, 3, 0).await;

            assert_eq!(msgs_a.len(), 3);
            assert_eq!(msgs_b.len(), 3);

            let seqs_a: Vec<u64> = msgs_a.iter().map(|m| m.seq).collect();
            let seqs_b: Vec<u64> = msgs_b.iter().map(|m| m.seq).collect();
            for seq in &seqs_a {
                assert!(!seqs_b.contains(seq), "Messages should not overlap between consumers");
            }
        }

        #[tokio::test]
        async fn test_delete_topic() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let manager = build_manager(config).await;
            let topic = "delete-topic-test";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            manager.publish(topic, None, Bytes::from("msg1")).await.unwrap();

            assert!(manager.exists(topic).await);
            let topic_path = temp_dir.path().join(topic);
            assert!(topic_path.exists());

            manager.delete_topic(topic.to_string()).await.unwrap();

            assert!(!manager.exists(topic).await);
            assert!(!topic_path.exists());

            let msgs = manager.read(topic, 1, 10).await;
            assert!(msgs.is_empty());
        }

        #[tokio::test]
        async fn test_disconnect_redelivers_inflight_to_another_consumer() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "disconnect-redeliver";
            let group = "g-disconnect-redeliver";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 3 messages
            for i in 1..=3 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            // Consumer A joins and fetches all 3 (without acking)
            let consumer_a = join_session(&manager, group, topic, "client-A").await;
            let msgs_a = fetch_messages(&manager, group, topic, &consumer_a, 10, 0).await;
            assert_eq!(msgs_a.len(), 3, "Consumer A should receive all 3 messages");

            // Consumer A leaves (simulates disconnect)
            manager.leave_group(group, topic, &consumer_a.consumer_id, consumer_a.generation).await.unwrap();

            // Consumer B joins and should get the 3 messages redelivered
            let consumer_b = join_session(&manager, group, topic, "client-B").await;
            let msgs_b = fetch_messages(&manager, group, topic, &consumer_b, 10, 0).await;
            assert_eq!(msgs_b.len(), 3, "Consumer B should receive redelivered messages");

            // Verify same seqs (redelivery, not new messages)
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
            let topic = "backpressure-test";
            let group = "g-backpressure";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 5 messages
            for i in 1..=5 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // Fetch with limit=10 but max_ack_pending=3 → only 3 delivered
            let batch1 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch1.len(), 3, "Should only get 3 messages (max_ack_pending=3)");

            // Fetch again → empty (backpressure: pending=3, max=3)
            let batch2 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 0, "Should be backpressured (no new messages)");

            // Ack 1 message → frees 1 slot
            ack_message(&manager, group, topic, &consumer, 1).await;

            // Fetch again → 1 more message (seq 4)
            let batch3 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1, "Should get 1 more after acking 1");
            assert_eq!(batch3[0].seq, 4);
        }
    }

    mod persistence {
        use super::*;
        use std::io::Write;

        #[tokio::test]
        async fn test_write_and_recover() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let topic = "persist-recover";

            {
                let manager = build_manager(config.clone()).await;
                manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

                manager.publish(topic, None, Bytes::from("msg1")).await.unwrap();
                manager.publish(topic, None, Bytes::from("msg2")).await.unwrap();
                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            {
                let manager = build_manager(config.clone()).await;

                let msgs = manager.read(topic, 1, 10).await;
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

            let topic = "persist-ack";
            let group = "g-persist";

            {
                let manager = build_manager(config.clone()).await;
                manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

                manager.publish(topic, None, Bytes::from("msg1")).await.unwrap();
                manager.publish(topic, None, Bytes::from("msg2")).await.unwrap();
                let consumer = join_session(&manager, group, topic, "client-A").await;
                let msgs = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
                for msg in &msgs {
                    ack_message(&manager, group, topic, &consumer, msg.seq).await;
                }

                manager.publish(topic, None, Bytes::from("msg3")).await.unwrap();
                tokio::time::sleep(Duration::from_millis(600)).await;
            }

            {
                let manager = build_manager(config.clone()).await;

                let ack_floor = join_session(&manager, group, topic, "client-A").await;
                assert_eq!(ack_floor.ack_floor, 2, "Ack floor should be recovered");
            }
        }

        #[tokio::test]
        async fn test_corruption_integrity() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let topic = "persist-corrupt";

            {
                let manager = build_manager(config.clone()).await;
                manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

                manager.publish(topic, None, Bytes::from("valid1")).await.unwrap();
                manager.publish(topic, None, Bytes::from("valid2")).await.unwrap();
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            let log_path = temp_dir.path().join(topic).join("1.log");
            let mut file = std::fs::OpenOptions::new().append(true).open(&log_path).expect("Log file should exist");
            file.write_all(b"GARBAGE_DATA_WITHOUT_HEADER").unwrap();

            {
                let manager = build_manager(config.clone()).await;

                let msgs = manager.read(topic, 1, 10).await;
                assert_eq!(msgs.len(), 2);
                assert_eq!(msgs[0].payload, Bytes::from("valid1"));
                assert_eq!(msgs[1].payload, Bytes::from("valid2"));
            }
        }

        #[tokio::test]
        async fn test_stream_log_segmentation() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            config.max_segment_size = 100;
            config.default_flush_ms = 50;

            let manager = build_manager(config).await;
            let topic = "topic_segmentation";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            manager.publish(topic, None, Bytes::from("msg1")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            manager.publish(topic, None, Bytes::from("msg2")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            manager.publish(topic, None, Bytes::from("msg3")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            manager.publish(topic, None, Bytes::from("msg4")).await.unwrap();

            tokio::time::sleep(Duration::from_millis(300)).await;

            let topic_path = temp_dir.path().join(topic);
            let mut files: Vec<String> = std::fs::read_dir(&topic_path)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .filter(|name| name.ends_with(".log") && name != "groups.log" && name != "state.log")
                .collect();
            files.sort();

            assert!(files.len() >= 2, "Should have at least 2 segments");

            drop(manager);

            let mut recover_config = Config::global().stream.clone();
            recover_config.persistence_path = path_str;
            let recovered_manager = build_manager(recover_config).await;

            let mut all_msgs = Vec::new();
            let mut next_seq = 1;

            while all_msgs.len() < 4 {
                let batch = recovered_manager.read(topic, next_seq, 10).await;
                if batch.is_empty() { break; }

                for msg in batch {
                    next_seq = msg.seq + 1;
                    all_msgs.push(msg);
                }
            }

            assert_eq!(all_msgs.len(), 4, "Should recover all 4 messages across segments");
            assert_eq!(all_msgs[0].payload, Bytes::from("msg1"));
            assert_eq!(all_msgs[3].payload, Bytes::from("msg4"));
        }

        #[tokio::test]
        async fn test_stream_log_retention() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            config.max_segment_size = 100;
            config.retention_check_interval_ms = 100;
            config.default_retention_bytes = 250;
            config.default_flush_ms = 50;

            let manager = build_manager(config).await;
            let topic = "topic_retention";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            for i in 1..=7 {
                manager.publish(topic, None, Bytes::from(format!("msg{}-50bytes-payload-0000000000000000000000000000", i))).await.unwrap();
                tokio::time::sleep(Duration::from_millis(60)).await;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;

            let topic_path = temp_dir.path().join(topic);
            let mut files: Vec<String> = std::fs::read_dir(&topic_path)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .filter(|name| name.ends_with(".log") && name != "groups.log" && name != "state.log")
                .collect();
            files.sort();

            assert!(!files.contains(&"1.log".to_string()), "Oldest segment should be deleted");

            let retained = manager.read(topic, 1, 20).await;
            assert!(!retained.is_empty());
            assert!(retained[0].seq > 1);

            let consumer = join_session(&manager, "g-retained", topic, "client-retained").await;
            let fetched = fetch_messages(&manager, "g-retained", topic, &consumer, 10, 0).await;
            assert_eq!(fetched.first().map(|msg| msg.seq), retained.first().map(|msg| msg.seq));
        }

        #[tokio::test]
        async fn test_warm_start_auto_restore() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let topic1 = "warm_topic1";
            let topic2 = "warm_topic2";
            let group = "warm_group";

            {
                let manager = build_manager(config.clone()).await;

                manager.create_topic(topic1.to_string(), StreamCreateOptions::default()).await.unwrap();
                manager.create_topic(topic2.to_string(), StreamCreateOptions::default()).await.unwrap();

                manager.publish(topic1, None, Bytes::from("msg1_t1")).await.unwrap();
                manager.publish(topic1, None, Bytes::from("msg2_t1")).await.unwrap();
                manager.publish(topic2, None, Bytes::from("msg1_t2")).await.unwrap();

                let consumer = join_session(&manager, group, topic1, "client-A").await;
                let msgs = fetch_messages(&manager, group, topic1, &consumer, 1, 0).await;
                if !msgs.is_empty() {
                    ack_message(&manager, group, topic1, &consumer, msgs[0].seq).await;
                }

                manager.publish(topic1, None, Bytes::from("msg3_t1")).await.unwrap();
                tokio::time::sleep(Duration::from_millis(600)).await;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;

            {
                let manager2 = build_manager(config.clone()).await;

                assert!(manager2.exists(topic1).await, "Topic 1 should be auto-restored");
                assert!(manager2.exists(topic2).await, "Topic 2 should be auto-restored");

                let msgs2 = manager2.read(topic2, 1, 10).await;
                assert_eq!(msgs2.len(), 1, "Should recover 1 message from topic2");
                assert_eq!(msgs2[0].payload, Bytes::from("msg1_t2"));
            }
        }

        #[tokio::test]
        async fn test_ram_eviction_under_load() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            config.default_flush_ms = 50;
            config.max_segment_size = 500;

            let manager = build_manager(config).await;
            let topic = "eviction_test";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            for i in 0..500 {
                let payload = Bytes::from(format!("msg_{:04}", i));
                manager.publish(topic, None, payload).await.unwrap();
            }

            tokio::time::sleep(Duration::from_millis(500)).await;

            let old_msgs = manager.read(topic, 1, 10).await;
            assert_eq!(old_msgs.len(), 10, "Cold read should work for evicted messages");
            assert_eq!(old_msgs[0].payload, Bytes::from("msg_0000"));
            assert_eq!(old_msgs[9].payload, Bytes::from("msg_0009"));

            let recent_msgs = manager.read(topic, 491, 10).await;
            assert_eq!(recent_msgs.len(), 10, "Hot read should work for recent messages");
            assert_eq!(recent_msgs[0].payload, Bytes::from("msg_0490"));
            assert_eq!(recent_msgs[9].payload, Bytes::from("msg_0499"));
        }

        #[tokio::test]
        async fn test_fetch_cold_read_from_disk() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            config.default_flush_ms = 50;

            let manager = build_manager(config).await;
            let topic = "cold-fetch-test";
            let group = "g-cold";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            for i in 1..=5 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            tokio::time::sleep(Duration::from_millis(500)).await;

            let consumer = join_session(&manager, group, topic, "client-A").await;
            let msgs = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;

            assert_eq!(msgs.len(), 5, "Should have fetched all 5 messages (3 from disk, 2 from RAM)");
            assert_eq!(msgs[0].seq, 1);
            assert_eq!(msgs[0].payload, Bytes::from("msg-1"));
            assert_eq!(msgs[4].seq, 5);
            assert_eq!(msgs[4].payload, Bytes::from("msg-5"));

            for msg in &msgs {
                ack_message(&manager, group, topic, &consumer, msg.seq).await;
            }
        }
    }

    mod error_handling {
        use super::*;

        #[tokio::test]
        async fn test_publish_nonexistent_topic() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();
            
            let manager = build_manager(config).await;
            
            let result = manager.publish("nonexistent_topic", None, Bytes::from("msg")).await;
            
            assert!(result.is_err(), "Publishing to nonexistent topic should fail");
            assert_eq!(result.err().unwrap(), "Topic not found");
        }

        #[tokio::test]
        async fn test_read_beyond_high_watermark() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();
            
            let manager = build_manager(config).await;
            manager.create_topic("test_topic".to_string(), StreamCreateOptions::default()).await.unwrap();
            
            // Publish 10 messages (seq 1-10)
            for i in 0..10 {
                manager.publish("test_topic", None, Bytes::from(format!("msg{}", i))).await.unwrap();
            }
            
            // Read seq 1000 (beyond) → should return empty
            let msgs = manager.read("test_topic", 1000, 10).await;
            assert!(msgs.is_empty(), "Reading beyond high watermark should return empty");
            
            // Read seq 1 (valid) → should return messages
            let msgs = manager.read("test_topic", 1, 100).await;
            assert!(!msgs.is_empty(), "Should read messages from RAM");
            assert_eq!(msgs[0].seq, 1, "First message should have seq 1");
            
            // Verify sequential
            for i in 1..msgs.len() {
                assert_eq!(msgs[i].seq, msgs[i-1].seq + 1, "Messages should be sequential");
            }
        }

        #[tokio::test]
        async fn test_fetch_without_join() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();
            
            let manager = build_manager(config).await;
            manager.create_topic("test_topic".to_string(), StreamCreateOptions::default()).await.unwrap();
            manager.publish("test_topic", None, Bytes::from("msg")).await.unwrap();
            
            let result = manager.fetch("test_group", "client-A", 1, 10, "test_topic", 0).await;
            assert!(result.is_err(), "Fetch without join should fail");
        }

        #[tokio::test]
        async fn test_per_key_ordering_same_key_serial() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "per-key-serial";
            let group = "g-pk-serial";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let key = Bytes::from("order-A");
            for i in 1..=3 {
                manager.publish(topic, Some(key.clone()), Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // First fetch: only msg-1 (key locked)
            let batch1 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].seq, 1);

            // Without ack, second fetch should NOT return msg-2 (same key, locked)
            let batch2 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 0, "Same-key messages must wait for ack");

            // Ack msg-1 → unlocks key → msg-2 deliverable
            ack_message(&manager, group, topic, &consumer, 1).await;
            let batch3 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1);
            assert_eq!(batch3[0].seq, 2);
        }

        #[tokio::test]
        async fn test_per_key_ordering_different_keys_parallel() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "per-key-parallel";
            let group = "g-pk-parallel";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let key_a = Bytes::from("key-A");
            let key_b = Bytes::from("key-B");
            manager.publish(topic, Some(key_a.clone()), Bytes::from("msg-1")).await.unwrap();
            manager.publish(topic, Some(key_b.clone()), Bytes::from("msg-2")).await.unwrap();
            manager.publish(topic, Some(key_a.clone()), Bytes::from("msg-3")).await.unwrap();

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // Should get msg-1 (key-A) and msg-2 (key-B) — different keys, parallel
            let batch = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch.len(), 2, "Different keys should be delivered in parallel");
            assert_eq!(batch[0].seq, 1);
            assert_eq!(batch[1].seq, 2);

            // msg-3 (key-A) should NOT be delivered (key-A locked by msg-1)
            let batch2 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 0, "Same-key msg-3 must wait for msg-1 ack");
        }

        #[tokio::test]
        async fn test_per_key_ordering_ack_unblocks_blocked() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "per-key-unblock";
            let group = "g-pk-unblock";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let key = Bytes::from("key-X");
            for i in 1..=3 {
                manager.publish(topic, Some(key.clone()), Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // Fetch msg-1 (locks key)
            let batch1 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].seq, 1);

            // Fetch again → empty (key locked, msg-2 and msg-3 blocked)
            let batch2 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 0);

            // Ack msg-1 → should unblock msg-2
            ack_message(&manager, group, topic, &consumer, 1).await;
            let batch3 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1);
            assert_eq!(batch3[0].seq, 2);

            // Ack msg-2 → should unblock msg-3
            ack_message(&manager, group, topic, &consumer, 2).await;
            let batch4 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch4.len(), 1);
            assert_eq!(batch4[0].seq, 3);
        }

        #[tokio::test]
        async fn test_per_key_ordering_no_key_unaffected() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "per-key-none";
            let group = "g-pk-none";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Messages with no key should behave as before — all delivered
            for i in 1..=5 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;
            let batch = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch.len(), 5, "No-key messages should all be delivered without ordering constraints");
        }

        #[tokio::test]
        async fn test_per_key_ordering_park_all_same_key() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let topic = "per-key-park";
            let group = "g-pk-park";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let key = Bytes::from("poison-key");
            for i in 1..=3 {
                manager.publish(topic, Some(key.clone()), Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // Fetch msg-1, let it timeout twice → parked
            let batch1 = fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].seq, 1);

            tokio::time::sleep(Duration::from_millis(120)).await;
            let batch2 = fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            assert_eq!(batch2.len(), 1);
            assert_eq!(batch2[0].seq, 1);

            // Wait for redelivery timer to fire and park msg-1 (max_deliveries=2)
            tokio::time::sleep(Duration::from_millis(300)).await;
            // msg-1 should now be parked (max_deliveries=2)
            // All same-key messages (msg-2, msg-3) should also be parked
            let batch3 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 0, "All same-key messages should be parked when one is parked");

            // Verify all 3 same-key messages are in DLT (ack_floor implicitly advanced since batch3 is empty)
            let dlt_entries = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt_entries.len(), 3, "all 3 same-key messages should be in DLT");
        }

        #[tokio::test]
        async fn test_per_key_ordering_timeout_does_not_break_ordering() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 10;
            let manager = build_manager(config).await;
            let topic = "per-key-timeout-order";
            let group = "g-pk-timeout-order";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let key = Bytes::from("key-K");
            manager.publish(topic, Some(key.clone()), Bytes::from("msg-1")).await.unwrap();
            manager.publish(topic, Some(key.clone()), Bytes::from("msg-2")).await.unwrap();

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // Fetch msg-1 only (limit=1). msg-2 stays fresh, not yet delivered.
            let batch1 = fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].seq, 1);

            // Wait for msg-1 to timeout (ack_wait=50ms). It goes back to redeliver.
            // The key K stays in keys_in_flight (fix), so msg-2 must NOT be delivered.
            tokio::time::sleep(Duration::from_millis(120)).await;

            // Fetch again: msg-1 should be redelivered, NOT msg-2.
            let batch2 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 1, "only msg-1 should be available (key K still locked)");
            assert_eq!(batch2[0].seq, 1, "msg-1 must be redelivered before msg-2");

            // Ack msg-1 → unblocks key K → msg-2 can now be delivered
            ack_message(&manager, group, topic, &consumer, 1).await;

            let batch3 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1);
            assert_eq!(batch3[0].seq, 2, "msg-2 delivered after msg-1 acked");
        }

        #[tokio::test]
        async fn test_dlt_peek_after_park() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let topic = "dlt-peek";
            let group = "g-dlt-peek";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            manager.publish(topic, None, Bytes::from("poison")).await.unwrap();

            let consumer = join_session(&manager, group, topic, "client-A").await;
            let batch = fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            assert_eq!(batch.len(), 1);

            // Let it timeout twice → parked in DLT
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt.len(), 1);
            assert_eq!(dlt[0].0, 1, "seq 1 should be in DLT");
            assert!(dlt[0].1.contains("max_deliveries"), "reason should mention max_deliveries");
            assert_eq!(dlt[0].2, 2, "attempts should be 2");
        }

        #[tokio::test]
        async fn test_dlt_move_to_stream_redelivers() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let topic = "dlt-move";
            let group = "g-dlt-move";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            manager.publish(topic, None, Bytes::from("poison")).await.unwrap();

            let consumer = join_session(&manager, group, topic, "client-A").await;
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Verify in DLT
            let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt.len(), 1);

            // Move back to stream
            manager.move_to_stream(topic, group, 1).await.unwrap();

            // DLT should be empty
            let dlt_after = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt_after.len(), 0, "DLT should be empty after moveToStream");

            // Should be redelivered
            let batch = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0].seq, 1, "msg should be redelivered after moveToStream");
        }

        #[tokio::test]
        async fn test_dlt_move_to_stream_preserves_order() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let topic = "dlt-order";
            let group = "g-dlt-order";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            // Publish 3 messages with same key
            let key = Bytes::from("order-key");
            for i in 1..=3 {
                manager.publish(topic, Some(key.clone()), Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;
            // Fetch msg-1, let it timeout twice → parked, all same-key auto-parked
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Trigger fetch to auto-park msg-2 and msg-3
            fetch_messages(&manager, group, topic, &consumer, 10, 0).await;

            // All 3 in DLT
            let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt.len(), 3);

            // Move back in REVERSE order: seq-3, then seq-2, then seq-1
            manager.move_to_stream(topic, group, 3).await.unwrap();
            manager.move_to_stream(topic, group, 2).await.unwrap();
            manager.move_to_stream(topic, group, 1).await.unwrap();

            // Fetch — per-key ordering delivers one at a time, must ack each
            let batch1 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch1.len(), 1);
            assert_eq!(batch1[0].seq, 1, "seq-1 must be first despite moveToStream(3) called first");
            ack_message(&manager, group, topic, &consumer, batch1[0].seq).await;

            let batch2 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch2.len(), 1);
            assert_eq!(batch2[0].seq, 2, "seq-2 must be second");
            ack_message(&manager, group, topic, &consumer, batch2[0].seq).await;

            let batch3 = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch3.len(), 1);
            assert_eq!(batch3[0].seq, 3, "seq-3 must be third");
        }

        #[tokio::test]
        async fn test_dlt_delete_removes_entry() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let topic = "dlt-delete";
            let group = "g-dlt-delete";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            manager.publish(topic, None, Bytes::from("poison")).await.unwrap();

            let consumer = join_session(&manager, group, topic, "client-A").await;
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Delete from DLT
            manager.delete_dlt(topic, group, 1).await.unwrap();

            let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt.len(), 0, "DLT should be empty after delete");

            // Should NOT be redelivered
            let batch = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch.len(), 0, "Deleted message should not be redelivered");
        }

        #[tokio::test]
        async fn test_dlt_purge_clears_all() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let topic = "dlt-purge";
            let group = "g-dlt-purge";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            for i in 1..=3 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;
            // Fetch all 3, let them all timeout and park
            fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert!(dlt.len() >= 1, "Should have entries in DLT");

            let count = manager.purge_dlt(topic, group).await.unwrap();
            assert!(count >= 1, "Purge should return count of removed entries");

            let dlt_after = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt_after.len(), 0, "DLT should be empty after purge");
        }

        #[tokio::test]
        async fn test_dlt_auto_unblock_on_last_entry() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let topic = "dlt-autounblock";
            let group = "g-dlt-autounblock";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let key = Bytes::from("block-key");
            for i in 1..=3 {
                manager.publish(topic, Some(key.clone()), Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            let consumer = join_session(&manager, group, topic, "client-A").await;
            // Fetch msg-1, let it timeout twice → parked, all same-key messages auto-parked
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Trigger fetch to auto-park msg-2 and msg-3 (same key is parked)
            let batch_park = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch_park.len(), 0, "All same-key messages should be auto-parked");

            // All 3 should be in DLT
            let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
            assert_eq!(dlt.len(), 3, "All 3 same-key messages should be in DLT");

            // Delete one — key should still be parked (other entries remain)
            manager.delete_dlt(topic, group, 1).await.unwrap();
            // Publish a new message with same key — should be auto-parked
            manager.publish(topic, Some(key.clone()), Bytes::from("msg-4")).await.unwrap();
            let batch = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch.len(), 0, "Key should still be parked with remaining DLT entries");

            // Delete all remaining original entries — but msg-4 is now in DLT too
            manager.delete_dlt(topic, group, 2).await.unwrap();
            manager.delete_dlt(topic, group, 3).await.unwrap();
            // msg-4 is also in DLT (auto-parked), key still parked
            let batch_still = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch_still.len(), 0, "Key still parked because msg-4 is in DLT");

            // Delete msg-4 from DLT — now key is fully unblocked
            manager.delete_dlt(topic, group, 4).await.unwrap();

            // Publish msg-5 with same key — should be delivered now
            manager.publish(topic, Some(key.clone()), Bytes::from("msg-5")).await.unwrap();
            let batch_final = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
            assert_eq!(batch_final.len(), 1);
            assert_eq!(batch_final[0].seq, 5, "msg-5 should be delivered after all DLT entries cleared");
        }

        #[tokio::test]
        async fn test_dlt_persistence_across_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;

            let topic = "dlt-persist";
            let group = "g-dlt-persist";
            let key = Bytes::from("persist-key");

            {
                let manager = build_manager(config.clone()).await;
                manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

                for i in 1..=2 {
                    manager.publish(topic, Some(key.clone()), Bytes::from(format!("msg-{}", i))).await.unwrap();
                }

                let consumer = join_session(&manager, group, topic, "client-A").await;
                fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(120)).await;
                fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(300)).await;

                // Verify DLT has entries
                let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
                assert!(dlt.len() >= 1, "Should have DLT entries before restart");

                // Wait for state to be saved
                tokio::time::sleep(Duration::from_millis(700)).await;
            }

            {
                let manager2 = build_manager(config.clone()).await;

                // DLT entries should survive restart
                let dlt = manager2.peek_dlt(topic, group, 10, 0).await.unwrap();
                assert!(dlt.len() >= 1, "DLT entries should persist across restart");

                // Parked key should survive — new message with same key should be auto-parked
                manager2.publish(topic, Some(key.clone()), Bytes::from("msg-3")).await.unwrap();
                let consumer = join_session(&manager2, group, topic, "client-A").await;
                let batch = fetch_messages(&manager2, group, topic, &consumer, 10, 0).await;
                assert_eq!(batch.len(), 0, "Parked key should survive restart — msg-3 auto-parked");
            }
        }

        #[tokio::test]
        async fn test_per_key_ordering_mixed_key_and_no_key() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "per-key-mixed";
            let group = "g-pk-mixed";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let key_a = Bytes::from("key-A");
            manager.publish(topic, Some(key_a.clone()), Bytes::from("msg-1")).await.unwrap(); // key-A
            manager.publish(topic, None, Bytes::from("msg-2")).await.unwrap();                 // no key
            manager.publish(topic, Some(key_a.clone()), Bytes::from("msg-3")).await.unwrap(); // key-A (blocked)
            manager.publish(topic, None, Bytes::from("msg-4")).await.unwrap();                 // no key

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // Should get msg-1 (key-A), msg-2 (no key), msg-4 (no key)
            // msg-3 is blocked by key-A held by msg-1
            let batch = fetch_messages(&manager, group, topic, &consumer, 10, 0).await;
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
            let topic = "fencing-test";
            let group = "g-fencing";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
            manager.publish(topic, None, Bytes::from("msg-1")).await.unwrap();

            // Consumer A joins → gets generation N
            let consumer_a = join_session(&manager, group, topic, "client-A").await;
            let old_gen = consumer_a.generation;

            // Fetch works with correct generation
            let msgs = fetch_messages(&manager, group, topic, &consumer_a, 10, 0).await;
            assert_eq!(msgs.len(), 1);

            // Seek triggers reset_runtime() → generation increments to N+1
            manager.seek(group, topic, nexo::brokers::stream::options::SeekTarget::Beginning).await.unwrap();

            // New consumer joins → gets generation N+1
            let consumer_b = join_session(&manager, group, topic, "client-B").await;
            assert!(consumer_b.generation > old_gen, "New generation should be greater after seek");

            // Fetch with old generation → FENCED
            let result = manager.fetch(group, &consumer_a.consumer_id, old_gen, 10, topic, 0).await;
            assert!(matches!(result, Err(ref e) if e == "FENCED"), "Old generation fetch should be FENCED");

            // Ack with old generation → FENCED
            let ack_result = manager.ack(group, topic, &consumer_a.consumer_id, old_gen, 1).await;
            assert!(matches!(ack_result, Err(ref e) if e == "FENCED"), "Old generation ack should be FENCED");

            // New consumer can fetch and ack normally
            let msgs_b = fetch_messages(&manager, group, topic, &consumer_b, 10, 0).await;
            assert_eq!(msgs_b.len(), 1, "New consumer should receive redelivered msg");
            assert_eq!(msgs_b[0].seq, 1);
            ack_message(&manager, group, topic, &consumer_b, 1).await;
        }
    }

    mod persistence_crc {
        use nexo::brokers::stream::{serialize_message, recover_topic};
        use std::path::PathBuf;

        #[tokio::test]
        async fn recover_topic_truncates_at_corrupted_record() {
            let tmp = tempfile::tempdir().unwrap();
            let topic_dir = tmp.path().join("test-topic");
            tokio::fs::create_dir_all(&topic_dir).await.unwrap();

            let seg_path = topic_dir.join("1.log");
            let mut buf = Vec::new();
            serialize_message(&mut buf, 1, 1000, None, b"msg1");
            serialize_message(&mut buf, 2, 2000, None, b"msg2");
            serialize_message(&mut buf, 3, 3000, None, b"msg3");
            tokio::fs::write(&seg_path, &buf).await.unwrap();

            // Corrupt CRC of msg2 (offset 34)
            let mut data = tokio::fs::read(&seg_path).await.unwrap();
            data[34] ^= 0xFF;
            tokio::fs::write(&seg_path, &data).await.unwrap();

            let file_size_before = tokio::fs::metadata(&seg_path).await.unwrap().len();

            // recover_topic should stop at msg2, index only msg1, and truncate the file
            let state = recover_topic("test-topic", tmp.path().to_path_buf()).await;
            assert_eq!(state.index.len(), 1, "Recovery should stop at corrupted record");
            assert!(state.index.contains_key(&1));
            assert_eq!(state.next_seq, 2);

            // File should be truncated to 30 bytes (only msg1)
            let file_size_after = tokio::fs::metadata(&seg_path).await.unwrap().len();
            assert_eq!(file_size_after, 30, "Segment file should be truncated to first record only");
            assert!(file_size_after < file_size_before, "File should be smaller after truncation");

            // Re-reading the truncated file should yield only msg1, no corruption
            let state2 = recover_topic("test-topic", tmp.path().to_path_buf()).await;
            assert_eq!(state2.index.len(), 1, "Re-reading truncated file should have no corruption");
            assert!(state2.index.contains_key(&1));
        }
    }

    mod batch {
        use super::*;

        #[tokio::test]
        async fn test_batch_publish_single_item() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "batch-single";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let seqs = manager.publish_batch(topic, vec![(None, Bytes::from("hello"))]).await.unwrap();
            assert_eq!(seqs.len(), 1);
            assert_eq!(seqs[0], 1);

            let msgs = manager.read(topic, 1, 100).await;
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].payload, Bytes::from("hello"));
        }

        #[tokio::test]
        async fn test_batch_publish_multiple_items() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "batch-multi";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let items: Vec<(Option<Bytes>, Bytes)> = (1..=5)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            let seqs = manager.publish_batch(topic, items).await.unwrap();
            assert_eq!(seqs.len(), 5);
            assert_eq!(seqs, vec![1, 2, 3, 4, 5]);

            let msgs = manager.read(topic, 1, 100).await;
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
            let topic = "batch-keys";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let items = vec![
                (Some(Bytes::from("key-A")), Bytes::from("msg-1")),
                (Some(Bytes::from("key-B")), Bytes::from("msg-2")),
                (None, Bytes::from("msg-3")),
            ];
            let seqs = manager.publish_batch(topic, items).await.unwrap();
            assert_eq!(seqs.len(), 3);

            let msgs = manager.read(topic, 1, 100).await;
            assert_eq!(msgs[0].key, Some(Bytes::from("key-A")));
            assert_eq!(msgs[1].key, Some(Bytes::from("key-B")));
            assert_eq!(msgs[2].key, None);
        }

        #[tokio::test]
        async fn test_batch_publish_empty() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "batch-empty";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let seqs = manager.publish_batch(topic, vec![]).await.unwrap();
            assert!(seqs.is_empty());

            let msgs = manager.read(topic, 1, 100).await;
            assert!(msgs.is_empty());
        }

        #[tokio::test]
        async fn test_batch_publish_nonexistent_topic() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;

            let result = manager.publish_batch("nonexistent", vec![(None, Bytes::from("data"))]).await;
            assert!(result.is_err());
        }
    }

    mod regression {
        use super::*;
        use nexo::brokers::stream::{serialize_message, recover_topic};
        use std::path::PathBuf;

        // Regression #2: concurrent publish must preserve contiguous seqs and segment invariant
        #[tokio::test]
        async fn concurrent_publish_preserves_segment_invariant() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "reg-concurrent-pub";
            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let manager = Arc::new(manager);
            const PUBLISHERS: usize = 4;
            const MSGS_PER: usize = 50;
            const TOTAL: usize = PUBLISHERS * MSGS_PER;

            let mut handles = Vec::new();
            for _ in 0..PUBLISHERS {
                let m = manager.clone();
                let t = topic.to_string();
                handles.push(tokio::spawn(async move {
                    let items: Vec<(Option<Bytes>, Bytes)> = (0..MSGS_PER)
                        .map(|i| (None, Bytes::from(format!("p-{}", i))))
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
            // No duplicates
            let unique: std::collections::HashSet<u64> = all_seqs.iter().copied().collect();
            assert_eq!(unique.len(), TOTAL, "No duplicate seqs across concurrent publishers");
            // No gaps
            for i in 0..TOTAL {
                assert_eq!(all_seqs[i], (i + 1) as u64, "Seq gap at index {}", i);
            }

            // Verify all messages are readable via read()
            let msgs = manager.read(topic, 1, TOTAL as usize * 2).await;
            assert_eq!(msgs.len(), TOTAL, "All messages must be readable after concurrent publish");
        }

        // Regression: concurrent reads from different topics must all return correct data
        // (verifies that spawning ReadRange as independent tasks is safe)
        #[tokio::test]
        async fn concurrent_reads_different_topics() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;

            const NUM_TOPICS: usize = 5;
            const MSGS_PER_TOPIC: usize = 20;

            for t in 0..NUM_TOPICS {
                let topic = format!("topic-{}", t);
                manager.create_topic(topic.clone(), StreamCreateOptions::default()).await.unwrap();
                for i in 0..MSGS_PER_TOPIC {
                    manager.publish(&topic, None, Bytes::from(format!("t{}-m{}", t, i))).await.unwrap();
                }
            }

            let manager = Arc::new(manager);
            let mut handles = Vec::new();
            for t in 0..NUM_TOPICS {
                let m = manager.clone();
                let topic = format!("topic-{}", t);
                handles.push(tokio::spawn(async move {
                    let msgs = m.read(&topic, 1, MSGS_PER_TOPIC * 2).await;
                    assert_eq!(msgs.len(), MSGS_PER_TOPIC, "Topic {} should have {} messages", t, MSGS_PER_TOPIC);
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

        // Regression #3: keyless DLT messages must not be redelivered after restart
        #[tokio::test]
        async fn keyless_dlt_not_redelivered_after_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap();
            let mut config = get_test_config(Some(path));
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            config.default_flush_ms = 50;

            let topic = "reg-keyless-dlt";
            let group = "g-reg-keyless-dlt";

            {
                let manager = build_manager(config.clone()).await;
                manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
                manager.publish(topic, None, Bytes::from("poison")).await.unwrap();

                let consumer = join_session(&manager, group, topic, "client-A").await;
                // Fetch, timeout, fetch, timeout → DLT (max_deliveries=2)
                fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(120)).await;
                fetch_messages(&manager, group, topic, &consumer, 1, 0).await;
                tokio::time::sleep(Duration::from_millis(300)).await;

                let dlt = manager.peek_dlt(topic, group, 10, 0).await.unwrap();
                assert_eq!(dlt.len(), 1, "Message should be in DLT before restart");

                // Wait for state save (group_save_interval = default_flush_ms * 10 = 500ms)
                tokio::time::sleep(Duration::from_millis(600)).await;
            }

            {
                let manager2 = build_manager(config.clone()).await;
                let consumer = join_session(&manager2, group, topic, "client-A").await;
                let batch = fetch_messages(&manager2, group, topic, &consumer, 10, 0).await;
                assert!(batch.is_empty(), "Keyless DLT message must NOT be redelivered after restart");

                let dlt = manager2.peek_dlt(topic, group, 10, 0).await.unwrap();
                assert_eq!(dlt.len(), 1, "DLT entry should survive restart");
            }
        }

        // Regression #4: recovery must truncate partial trailing records
        #[tokio::test]
        async fn recovery_truncates_partial_trailing_record() {
            let tmp = tempfile::tempdir().unwrap();
            let topic_dir = tmp.path().join("reg-partial");
            tokio::fs::create_dir_all(&topic_dir).await.unwrap();

            let seg_path = topic_dir.join("1.log");
            let mut buf = Vec::new();
            serialize_message(&mut buf, 1, 1000, None, b"msg1");
            serialize_message(&mut buf, 2, 2000, None, b"msg2");
            let valid_len = buf.len() as u64;
            tokio::fs::write(&seg_path, &buf).await.unwrap();

            // Append partial record (just 10 bytes of garbage — not a complete record)
            let partial = vec![0xABu8; 10];
            let mut file_content = tokio::fs::read(&seg_path).await.unwrap();
            file_content.extend_from_slice(&partial);
            tokio::fs::write(&seg_path, &file_content).await.unwrap();

            let file_size_before = tokio::fs::metadata(&seg_path).await.unwrap().len();
            assert!(file_size_before > valid_len, "File should have partial bytes at end");

            // recover_topic should truncate the partial bytes
            let state = recover_topic("reg-partial", tmp.path().to_path_buf()).await;
            assert_eq!(state.index.len(), 2, "Should index both valid records");
            assert!(state.index.contains_key(&1));
            assert!(state.index.contains_key(&2));

            let file_size_after = tokio::fs::metadata(&seg_path).await.unwrap().len();
            assert_eq!(file_size_after, valid_len, "Partial trailing bytes must be truncated");
            assert!(file_size_after < file_size_before, "File must be smaller after truncation");

            // Re-read should be clean
            let state2 = recover_topic("reg-partial", tmp.path().to_path_buf()).await;
            assert_eq!(state2.index.len(), 2, "Re-recovery should find no corruption");
        }

        // Regression #5: ack must wake backpressured long-polling consumers
        #[tokio::test]
        async fn ack_wakes_backpressured_long_poll() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 2;
            let manager = build_manager(config).await;
            let topic = "reg-ack-wake";
            let group = "g-reg-ack-wake";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 4 messages
            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=4)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // Fetch 2 messages (fills pending to max_ack_pending=2)
            let msgs = fetch_messages(&manager, group, topic, &consumer, 2, 0).await;
            assert_eq!(msgs.len(), 2);

            // Start long-poll in a separate task — should be backpressured
            let m = Arc::new(manager);
            let m_clone = m.clone();
            let topic_clone = topic.to_string();
            let group_clone = group.to_string();
            let consumer_id = consumer.consumer_id.clone();
            let generation = consumer.generation;
            let fetch_handle = tokio::spawn(async move {
                m_clone.fetch(&group_clone, &consumer_id, generation, 10, &topic_clone, 5000).await.unwrap()
            });

            // Give the long-poll time to enter the waiting state
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Ack one message — frees a slot and wakes the consumer
            let start = Instant::now();
            m.ack(group, topic, &consumer.consumer_id, consumer.generation, msgs[0].seq).await.unwrap();

            // The long-poll should return quickly (not wait 5000ms)
            let result = fetch_handle.await.unwrap();
            let elapsed = start.elapsed();

            assert!(!result.is_empty(), "Long-poll should deliver messages after ack frees a slot");
            assert!(
                elapsed < Duration::from_millis(2000),
                "Long-poll should wake quickly after ack, took {:?}",
                elapsed
            );
        }

        // Regression #6: publish during active long-poll must deliver to waiting consumer
        #[tokio::test]
        async fn publish_during_fetch_interleaving() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "reg-interleave";
            let group = "g-reg-interleave";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let consumer = join_session(&manager, group, topic, "client-A").await;

            // Start long-poll with 5000ms wait — no messages available yet
            let m = Arc::new(manager);
            let m_clone = m.clone();
            let topic_clone = topic.to_string();
            let group_clone = group.to_string();
            let consumer_id = consumer.consumer_id.clone();
            let generation = consumer.generation;
            let fetch_handle = tokio::spawn(async move {
                m_clone.fetch(&group_clone, &consumer_id, generation, 10, &topic_clone, 5000).await.unwrap()
            });

            // Give the long-poll time to enter waiting state
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Publish a message — should wake the long-poll
            let start = Instant::now();
            m.publish(topic, None, Bytes::from("interleaved-msg")).await.unwrap();

            let result = fetch_handle.await.unwrap();
            let elapsed = start.elapsed();

            assert_eq!(result.len(), 1, "Long-poll should receive the published message");
            assert_eq!(result[0].payload, Bytes::from("interleaved-msg"));
            assert!(
                elapsed < Duration::from_millis(2000),
                "Long-poll should wake quickly after publish, took {:?}",
                elapsed
            );
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
            let topic = "stress-high-pending";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 5000 messages
            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=5000)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            let seqs = manager.publish_batch(topic, batch).await.unwrap();
            assert_eq!(seqs.len(), 5000);

            // Join group and fetch all 5000
            let consumer = join_session(&manager, "grp1", topic, "conn1").await;
            let msgs = fetch_messages(&manager, "grp1", topic, &consumer, 5000, 100).await;
            assert_eq!(msgs.len(), 5000);

            // Mass ack all
            for seq in 1..=5000 {
                ack_message(&manager, "grp1", topic, &consumer, seq).await;
            }

            // ack_floor should be 5000
            let msgs2 = fetch_messages(&manager, "grp1", topic, &consumer, 10, 50).await;
            assert!(msgs2.is_empty());
        }

        #[tokio::test]
        async fn test_high_pending_out_of_order_ack() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 1000;
            let manager = build_manager(config).await;
            let topic = "stress-ooo-ack";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=1000)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", topic, "conn1").await;
            let msgs = fetch_messages(&manager, "grp1", topic, &consumer, 1000, 100).await;
            assert_eq!(msgs.len(), 1000);

            // Ack in reverse order
            for seq in (1..=1000).rev() {
                ack_message(&manager, "grp1", topic, &consumer, seq).await;
            }

            // After all acked, fetch should return empty
            let msgs2 = fetch_messages(&manager, "grp1", topic, &consumer, 10, 50).await;
            assert!(msgs2.is_empty());
        }

        #[tokio::test]
        async fn test_mass_redelivery_after_timeout() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 500;
            config.ack_wait_ms = 100;
            let manager = build_manager(config).await;
            let topic = "stress-redelivery";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=200)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", topic, "conn1").await;
            let msgs = fetch_messages(&manager, "grp1", topic, &consumer, 200, 100).await;
            assert_eq!(msgs.len(), 200);

            // Wait for timeout
            tokio::time::sleep(Duration::from_millis(150)).await;

            // Fetch again — should get redelivered messages
            let msgs2 = fetch_messages(&manager, "grp1", topic, &consumer, 200, 200).await;
            assert_eq!(msgs2.len(), 200);
            // Verify same seqs
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
            let topic = "stress-clamp-head";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 3000 messages
            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=3000)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", topic, "conn1").await;
            let msgs = fetch_messages(&manager, "grp1", topic, &consumer, 3000, 100).await;
            assert_eq!(msgs.len(), 3000);

            // Ack first 1000 to advance floor
            for seq in 1..=1000 {
                ack_message(&manager, "grp1", topic, &consumer, seq).await;
            }

            // Now seek to beginning which resets runtime, then re-fetch
            // This tests clamp_head indirectly through the retention path
            // Verify we can still read messages
            let read_msgs = manager.read(topic, 1, 100).await;
            assert!(!read_msgs.is_empty());
        }

        #[tokio::test]
        async fn test_dlt_under_load_with_max_deliveries() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 100;
            config.ack_wait_ms = 50;
            config.max_deliveries = 2;
            let manager = build_manager(config).await;
            let topic = "stress-dlt";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 50 messages
            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=50)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", topic, "conn1").await;

            // Fetch, wait for timeout, re-fetch — repeat until messages hit DLT
            // max_deliveries = 2, so after 2 deliveries without ack → DLT
            for _round in 0..3 {
                let msgs = fetch_messages(&manager, "grp1", topic, &consumer, 50, 200).await;
                if msgs.is_empty() {
                    break;
                }
                // Don't ack — let them timeout
                tokio::time::sleep(Duration::from_millis(80)).await;
            }

            // Check DLT has entries
            let dlt_entries = manager.peek_dlt(topic, "grp1", 100, 0).await.unwrap();
            assert!(!dlt_entries.is_empty(), "DLT should have entries after max_deliveries exceeded");
        }

        #[tokio::test]
        async fn test_multi_consumer_stress() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 5000;
            let manager = build_manager(config).await;
            let topic = "stress-multi-consumer";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 1000 messages
            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=1000)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            // Two consumers in same group
            let c1 = join_session(&manager, "grp1", topic, "conn1").await;
            let c2 = join_session(&manager, "grp1", topic, "conn2").await;

            // Both fetch — messages should be distributed (not duplicated)
            let msgs1 = fetch_messages(&manager, "grp1", topic, &c1, 500, 100).await;
            let msgs2 = fetch_messages(&manager, "grp1", topic, &c2, 500, 100).await;

            let total = msgs1.len() + msgs2.len();
            assert_eq!(total, 1000);

            // Verify no overlap
            let seqs1: std::collections::HashSet<u64> = msgs1.iter().map(|m| m.seq).collect();
            let seqs2: std::collections::HashSet<u64> = msgs2.iter().map(|m| m.seq).collect();
            assert!(seqs1.is_disjoint(&seqs2));

            // Ack all
            for msg in &msgs1 {
                ack_message(&manager, "grp1", topic, &c1, msg.seq).await;
            }
            for msg in &msgs2 {
                ack_message(&manager, "grp1", topic, &c2, msg.seq).await;
            }

            // Verify no more messages
            let msgs3 = fetch_messages(&manager, "grp1", topic, &c1, 10, 50).await;
            assert!(msgs3.is_empty());
        }

        #[tokio::test]
        async fn test_persistent_state_after_mass_ack() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap();
            let mut config = get_test_config(Some(path));
            config.max_ack_pending = 2000;
            config.default_flush_ms = 50;
            let manager = build_manager(config).await;
            let topic = "stress-persist";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=1000)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", topic, "conn1").await;
            let msgs = fetch_messages(&manager, "grp1", topic, &consumer, 1000, 100).await;
            assert_eq!(msgs.len(), 1000);

            // Ack all
            for seq in 1..=1000 {
                ack_message(&manager, "grp1", topic, &consumer, seq).await;
            }

            // Wait for flush (group save interval = default_flush_ms * 10 = 500ms)
            tokio::time::sleep(Duration::from_millis(600)).await;

            // Drop manager and recover
            drop(manager);
            let config2 = get_test_config(Some(path));
            let manager2 = build_manager(config2).await;

            // Verify topic still exists
            assert!(manager2.exists(topic).await);

            // Join group and verify ack_floor is preserved
            let consumer2 = join_session(&manager2, "grp1", topic, "conn1").await;
            let msgs2 = fetch_messages(&manager2, "grp1", topic, &consumer2, 10, 50).await;
            assert!(msgs2.is_empty(), "Should not re-deliver acked messages after restart");
        }

        #[tokio::test]
        async fn test_per_key_ordering_stress() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 5000;
            let manager = build_manager(config).await;
            let topic = "stress-per-key";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 100 messages with 10 keys (10 messages per key)
            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=100)
                .map(|i| {
                    let key = Bytes::from(format!("key-{}", i % 10));
                    (Some(key), Bytes::from(format!("msg-{}", i)))
                })
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", topic, "conn1").await;

            // Fetch — should get 10 messages (one per key, the rest blocked)
            let msgs = fetch_messages(&manager, "grp1", topic, &consumer, 100, 100).await;
            assert_eq!(msgs.len(), 10);

            // Verify each key appears exactly once
            let keys: std::collections::HashSet<&Bytes> = msgs.iter().filter_map(|m| m.key.as_ref()).collect();
            assert_eq!(keys.len(), 10);

            // Ack all 10 → should unblock next 10
            for msg in &msgs {
                ack_message(&manager, "grp1", topic, &consumer, msg.seq).await;
            }

            let msgs2 = fetch_messages(&manager, "grp1", topic, &consumer, 100, 100).await;
            assert_eq!(msgs2.len(), 10);

            // Continue until all 100 are delivered and acked
            for msg in &msgs2 {
                ack_message(&manager, "grp1", topic, &consumer, msg.seq).await;
            }

            // Repeat for remaining rounds
            for _round in 0..8 {
                let msgs_n = fetch_messages(&manager, "grp1", topic, &consumer, 100, 100).await;
                assert_eq!(msgs_n.len(), 10, "Each round should deliver 10 messages");
                for msg in &msgs_n {
                    ack_message(&manager, "grp1", topic, &consumer, msg.seq).await;
                }
            }

            // All 100 should be delivered and acked
            let final_msgs = fetch_messages(&manager, "grp1", topic, &consumer, 10, 50).await;
            assert!(final_msgs.is_empty());
        }

        #[tokio::test]
        async fn test_disconnect_redelivery_stress() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            config.max_ack_pending = 1000;
            let manager = build_manager(config).await;
            let topic = "stress-disconnect";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=500)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            // Consumer 1 fetches but doesn't ack
            let c1 = join_session(&manager, "grp1", topic, "conn1").await;
            let msgs1 = fetch_messages(&manager, "grp1", topic, &c1, 500, 100).await;
            assert_eq!(msgs1.len(), 500);

            // Consumer 1 disconnects
            manager.leave_group("grp1", topic, &c1.consumer_id, c1.generation).await.unwrap();

            // Consumer 2 joins and fetches — should get all 500 redelivered
            let c2 = join_session(&manager, "grp1", topic, "conn2").await;
            let msgs2 = fetch_messages(&manager, "grp1", topic, &c2, 500, 200).await;
            assert_eq!(msgs2.len(), 500);

            // Verify same seqs
            let seqs1: Vec<u64> = msgs1.iter().map(|m| m.seq).collect();
            let seqs2: Vec<u64> = msgs2.iter().map(|m| m.seq).collect();
            assert_eq!(seqs1, seqs2);
        }

        #[tokio::test]
        async fn test_fetch_partial_batch_does_not_block_next_deliver() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "partial-batch";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish seq 1..=10
            let batch: Vec<(Option<Bytes>, Bytes)> = (1..=10)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch).await.unwrap();

            let consumer = join_session(&manager, "grp1", topic, "conn1").await;

            // Fetch with limit=20 — only 10 exist, should get 10
            let msgs = fetch_messages(&manager, "grp1", topic, &consumer, 20, 50).await;
            assert_eq!(msgs.len(), 10, "should get all available messages, not block on missing seqs");

            // Verify seqs 1..=10
            let seqs: Vec<u64> = msgs.iter().map(|m| m.seq).collect();
            assert_eq!(seqs, (1..=10).collect::<Vec<_>>());

            // Ack all
            for seq in 1..=10 {
                ack_message(&manager, "grp1", topic, &consumer, seq).await;
            }

            // Publish 5 more (seq 11..=15)
            let batch2: Vec<(Option<Bytes>, Bytes)> = (11..=15)
                .map(|i| (None, Bytes::from(format!("msg-{}", i))))
                .collect();
            manager.publish_batch(topic, batch2).await.unwrap();

            // Fetch with limit=20 again — should get only 11..=15
            let msgs2 = fetch_messages(&manager, "grp1", topic, &consumer, 20, 50).await;
            assert_eq!(msgs2.len(), 5, "should get only new messages, not retry old ones");
            let seqs2: Vec<u64> = msgs2.iter().map(|m| m.seq).collect();
            assert_eq!(seqs2, (11..=15).collect::<Vec<_>>());
        }

    mod fd_cache_regression {
        use super::*;

        // Regression: read-after-write on same fd (O_APPEND + seek + read)
        // Publish, read, publish more, read again — verify all messages visible.
        #[tokio::test]
        async fn read_after_write_same_fd() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = build_manager(config).await;
            let topic = "fd-cache-rw";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Round 1: publish 3 messages
            for i in 1..=3 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            // Read 1: should see all 3
            let msgs1 = manager.read(topic, 1, 100).await;
            assert_eq!(msgs1.len(), 3, "first read should see all 3 messages");
            assert_eq!(msgs1[0].payload, Bytes::from("msg-1"));
            assert_eq!(msgs1[2].payload, Bytes::from("msg-3"));

            // Round 2: publish 3 more (same segment, same fd in cache)
            for i in 4..=6 {
                manager.publish(topic, None, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            // Read 2: should see all 6
            let msgs2 = manager.read(topic, 1, 100).await;
            assert_eq!(msgs2.len(), 6, "second read should see all 6 messages");
            for (i, msg) in msgs2.iter().enumerate() {
                assert_eq!(msg.payload, Bytes::from(format!("msg-{}", i + 1)));
                assert_eq!(msg.seq, (i + 1) as u64);
            }

            // Read 3: partial range
            let msgs3 = manager.read(topic, 4, 2).await;
            assert_eq!(msgs3.len(), 2, "partial read should see 2 messages");
            assert_eq!(msgs3[0].seq, 4);
            assert_eq!(msgs3[1].seq, 5);
        }
    }

    mod shutdown {
        use super::*;

        #[tokio::test]
        async fn test_shutdown_flushes_messages_and_group_state() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));

            let topic = "shutdown-flush";
            let group = "g-shutdown";

            {
                let manager = build_manager(config.clone()).await;
                manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();
                manager.publish(topic, None, Bytes::from("msg1")).await.unwrap();
                manager.publish(topic, None, Bytes::from("msg2")).await.unwrap();

                let consumer = join_session(&manager, group, topic, "client-A").await;
                let msgs = fetch_messages(&manager, group, topic, &consumer, 2, 0).await;
                assert_eq!(msgs.len(), 2);
                ack_message(&manager, group, topic, &consumer, msgs[0].seq).await;
                ack_message(&manager, group, topic, &consumer, msgs[1].seq).await;

                // Shutdown immediately — no sleep, no waiting for flush timer
                manager.shutdown().await;
            }

            // Recover with a new manager
            {
                let manager = build_manager(config).await;

                let msgs = manager.read(topic, 1, 10).await;
                assert_eq!(msgs.len(), 2, "Messages should survive shutdown flush");

                let consumer = join_session(&manager, group, topic, "client-A").await;
                assert_eq!(consumer.ack_floor, 2, "Ack floor should survive shutdown flush");
            }
        }
    }
    }
}
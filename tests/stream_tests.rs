use nexo::brokers::stream::options::StreamCreateOptions;
use nexo::config::Config;
use bytes::Bytes;
use std::time::{Duration, Instant};
mod common;
use common::Benchmark;

#[cfg(test)]
mod stream_tests {
    use super::*;
    use nexo::brokers::stream::domain::message::Message;
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

    async fn build_manager(config: nexo::brokers::stream::config::SystemStreamConfig) -> StreamManager {
        StreamManager::new(Arc::new(config)).await
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

                let snapshot = manager2.get_snapshot().await;
                let topic_names: Vec<String> = snapshot.topics.iter().map(|t| t.name.clone()).collect();
                assert!(topic_names.contains(&topic1.to_string()), "Snapshot should include topic1");
                assert!(topic_names.contains(&topic2.to_string()), "Snapshot should include topic2");
                assert_eq!(snapshot.topics.len(), 2, "Should have 2 topics restored");
            }
        }

        #[tokio::test]
        async fn test_ram_eviction_under_load() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();
            config.ram_soft_limit = 100;
            config.ram_hard_limit = 200;
            config.eviction_interval_ms = 100;
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
            config.ram_soft_limit = 2; // Only 2 messages allowed in RAM
            config.eviction_interval_ms = 50;
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

    mod performance {
        use super::*;

        const COUNT: usize = 500_000;


        #[tokio::test]
        async fn bench_stream_publish() {
            // cargo test --release bench_stream_publish -- --test-threads=1 --nocapture
            let temp_dir = tempfile::tempdir().unwrap();

            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();

            let manager = build_manager(config).await;
            let topic = "bench-async";
            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            let mut bench = Benchmark::start("STREAM PUSH (Async Background)", COUNT);
            for _ in 0..COUNT {
                let start = Instant::now();
                manager.publish(topic, None, Bytes::from("data")).await.unwrap();
                bench.record(start.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            bench.stop();
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

            // Verify ack_floor advanced (DLT entries don't block floor)
            let snapshot = manager.get_snapshot().await;
            let topic_snap = snapshot.topics.iter().find(|t| t.name == topic).unwrap();
            let group_snap = topic_snap.groups.iter().find(|g| g.id == group).unwrap();
            assert_eq!(group_snap.ack_floor, 3, "ack_floor must advance over DLT entries");
            assert_eq!(group_snap.dlt_count, 3, "all 3 same-key messages should be in DLT");
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
    }
}
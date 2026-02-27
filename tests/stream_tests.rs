use nexo::brokers::stream::commands::StreamCreateOptions;
use nexo::config::Config;
use bytes::Bytes;
use std::time::{Duration, Instant};
mod helpers;
use helpers::Benchmark;

#[cfg(test)]
mod stream_tests {
    use super::*;

    mod features {
        use super::*;
        use nexo::brokers::stream::StreamManager;

        fn get_test_config(path: Option<&str>) -> nexo::config::SystemStreamConfig {
            let mut config = Config::global().stream.clone();
            if let Some(p) = path {
                config.persistence_path = p.to_string();
            }
            config
        }

        #[tokio::test]
        async fn test_stream_basic_flow() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = StreamManager::new(config);
            let topic = "test-basic-flow";

            // 1. Create
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                ..Default::default()
            }).await.unwrap();

            assert!(manager.exists(topic).await);

            // 2. Publish
            let payload = Bytes::from("hello world");
            let seq = manager.publish(topic, payload.clone()).await.unwrap();
            assert_eq!(seq, 1); // sequences start at 1

            // 3. Read
            let msgs = manager.read(topic, 1, 100).await;
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].payload, payload);
            assert_eq!(msgs[0].seq, 1);
        }

        #[tokio::test]
        async fn test_stream_ordering() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = StreamManager::new(config);
            let topic = "ordering-topic";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 1, 2, 3
            for i in 1..=3 {
                let payload = Bytes::from(format!("msg-{}", i));
                manager.publish(topic, payload).await.unwrap();
            }

            // Read back
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
        async fn test_ack_nack_flow() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = StreamManager::new(config);
            let topic = "ack-nack-flow";
            let group = "g-ack";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 3 messages
            for i in 1..=3 {
                manager.publish(topic, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            // Join group
            manager.join_group(group, topic, "client-A").await.unwrap();

            // Fetch
            let msgs = manager.fetch(group, "client-A", 10, topic).await.unwrap();
            assert_eq!(msgs.len(), 3);
            assert_eq!(msgs[0].seq, 1);

            // Ack first two
            manager.ack(group, topic, 1).await.unwrap();
            manager.ack(group, topic, 2).await.unwrap();

            // Nack the third — should be redelivered
            manager.nack(group, topic, 3).await.unwrap();

            // Fetch again — should get the nacked message
            let msgs2 = manager.fetch(group, "client-A", 10, topic).await.unwrap();
            assert_eq!(msgs2.len(), 1);
            assert_eq!(msgs2[0].seq, 3);
            assert_eq!(msgs2[0].payload, Bytes::from("msg-3"));
        }

        #[tokio::test]
        async fn test_ack_floor_advancement() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = StreamManager::new(config);
            let topic = "ack-floor";
            let group = "g-floor";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 5 messages
            for i in 1..=5 {
                manager.publish(topic, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            // Join and fetch all
            manager.join_group(group, topic, "client-A").await.unwrap();
            let msgs = manager.fetch(group, "client-A", 10, topic).await.unwrap();
            assert_eq!(msgs.len(), 5);

            // Ack out of order: 1, 3, 2 — floor should advance to 3 after acking 2
            manager.ack(group, topic, 1).await.unwrap();
            manager.ack(group, topic, 3).await.unwrap();

            // Floor should be at 1 (can't skip 2)
            let ack_floor = manager.join_group(group, topic, "client-A").await.unwrap();
            assert_eq!(ack_floor, 1);

            // Now ack 2 — floor should jump to 3
            manager.ack(group, topic, 2).await.unwrap();
            let ack_floor = manager.join_group(group, topic, "client-A").await.unwrap();
            assert_eq!(ack_floor, 3);
        }

        #[tokio::test]
        async fn test_seek_beginning_end() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = StreamManager::new(config);
            let topic = "seek-topic";
            let group = "g-seek";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 10 messages
            for i in 1..=10 {
                manager.publish(topic, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            // Join, fetch all, ack all
            manager.join_group(group, topic, "client-A").await.unwrap();
            let msgs = manager.fetch(group, "client-A", 10, topic).await.unwrap();
            for msg in &msgs {
                manager.ack(group, topic, msg.seq).await.unwrap();
            }

            // Verify floor is at 10
            let ack_floor = manager.join_group(group, topic, "client-A").await.unwrap();
            assert_eq!(ack_floor, 10);

            // Fetch should return empty (all consumed)
            let empty = manager.fetch(group, "client-A", 10, topic).await.unwrap();
            assert!(empty.is_empty());

            // Seek to beginning
            use nexo::brokers::stream::commands::SeekTarget;
            manager.seek(group, topic, SeekTarget::Beginning).await.unwrap();

            // Fetch should return messages from beginning
            let from_start = manager.fetch(group, "client-A", 10, topic).await.unwrap();
            assert_eq!(from_start.len(), 10);
            assert_eq!(from_start[0].seq, 1);

            // Ack all again, then seek to end
            for msg in &from_start {
                manager.ack(group, topic, msg.seq).await.unwrap();
            }
            manager.seek(group, topic, SeekTarget::End).await.unwrap();

            // Fetch should return empty
            let after_end = manager.fetch(group, "client-A", 10, topic).await.unwrap();
            assert!(after_end.is_empty());
        }

        #[tokio::test]
        async fn test_multi_consumer_parallel_fetch() {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
            let manager = StreamManager::new(config);
            let topic = "multi-consumer";
            let group = "g-multi";

            manager.create_topic(topic.to_string(), StreamCreateOptions::default()).await.unwrap();

            // Publish 6 messages
            for i in 1..=6 {
                manager.publish(topic, Bytes::from(format!("msg-{}", i))).await.unwrap();
            }

            // Two clients join the same group
            manager.join_group(group, topic, "client-A").await.unwrap();
            manager.join_group(group, topic, "client-B").await.unwrap();

            // Client A fetches 3
            let msgs_a = manager.fetch(group, "client-A", 3, topic).await.unwrap();
            // Client B fetches 3 — should get *different* messages
            let msgs_b = manager.fetch(group, "client-B", 3, topic).await.unwrap();

            assert_eq!(msgs_a.len(), 3);
            assert_eq!(msgs_b.len(), 3);

            // Verify no overlap
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

            let manager = StreamManager::new(config);
            let topic = "delete-topic-test";

            // 1. Create
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                ..Default::default()
            }).await.unwrap();

            // 2. Publish
            manager.publish(topic, Bytes::from("msg1")).await.unwrap();

            assert!(manager.exists(topic).await);
            let topic_path = temp_dir.path().join(topic);
            assert!(topic_path.exists());

            // 3. Delete
            manager.delete_topic(topic.to_string()).await.unwrap();

            // 4. Verify
            assert!(!manager.exists(topic).await);
            assert!(!topic_path.exists());

            // 5. Read should be empty
            let msgs = manager.read(topic, 1, 10).await;
            assert!(msgs.is_empty());
        }
    }

    mod persistence {
        use super::*;
        use std::io::Write;
        use nexo::brokers::stream::StreamManager;

        #[tokio::test]
        async fn test_write_and_recover() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let topic = "persist-recover";

            // 1. Create & Publish (Sync)
            {
                let manager = StreamManager::new(config.clone());
                manager.create_topic(topic.to_string(), StreamCreateOptions {
                    ..Default::default()
                }).await.unwrap();

                manager.publish(topic, Bytes::from("msg1")).await.unwrap();
                manager.publish(topic, Bytes::from("msg2")).await.unwrap();
                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // 2. Recovery
            {
                let manager = StreamManager::new(config.clone());
                manager.create_topic(topic.to_string(), StreamCreateOptions {
                    ..Default::default()
                }).await.unwrap();

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
                let manager = StreamManager::new(config.clone());
                manager.create_topic(topic.to_string(), StreamCreateOptions {
                    ..Default::default()
                }).await.unwrap();

                // Publish, join, fetch, ack
                manager.publish(topic, Bytes::from("msg1")).await.unwrap();
                manager.publish(topic, Bytes::from("msg2")).await.unwrap();
                manager.join_group(group, topic, "client-A").await.unwrap();
                let msgs = manager.fetch(group, "client-A", 10, topic).await.unwrap();
                for msg in &msgs {
                    manager.ack(group, topic, msg.seq).await.unwrap();
                }

                // Force a flush by publishing another message (sync mode)
                manager.publish(topic, Bytes::from("msg3")).await.unwrap();
                // Give time for groups to be saved (requires > default_flush_ms * 10)
                tokio::time::sleep(Duration::from_millis(600)).await;
            }

            // Recover
            {
                let manager = StreamManager::new(config.clone());
                manager.create_topic(topic.to_string(), StreamCreateOptions {
                    ..Default::default()
                }).await.unwrap();

                let ack_floor = manager.join_group(group, topic, "client-A").await.unwrap();
                assert_eq!(ack_floor, 2, "Ack floor should be recovered");
            }
        }

        #[tokio::test]
        async fn test_corruption_integrity() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path_str = temp_dir.path().to_str().unwrap().to_string();

            let mut config = Config::global().stream.clone();
            config.persistence_path = path_str.clone();

            let topic = "persist-corrupt";

            // 1. Write Data
            {
                let manager = StreamManager::new(config.clone());
                manager.create_topic(topic.to_string(), StreamCreateOptions {
                    ..Default::default()
                }).await.unwrap();

                manager.publish(topic, Bytes::from("valid1")).await.unwrap();
                manager.publish(topic, Bytes::from("valid2")).await.unwrap();
            }

            // 2. Corrupt Data (Append garbage)
            tokio::time::sleep(Duration::from_millis(500)).await;

            let log_path = temp_dir.path().join(topic).join("1.log");
            let mut file = std::fs::OpenOptions::new().append(true).open(&log_path).expect("Log file should exist");
            file.write_all(b"GARBAGE_DATA_WITHOUT_HEADER").unwrap();

            // 3. Recover
            {
                let manager = StreamManager::new(config.clone());
                manager.create_topic(topic.to_string(), StreamCreateOptions {
                    ..Default::default()
                }).await.unwrap();

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

            let manager = StreamManager::new(config);
            let topic = "topic_segmentation";

            manager.create_topic(topic.to_string(), StreamCreateOptions {
                ..Default::default()
            }).await.unwrap();

            // Write messages with enough delay to trigger asynchronous flush batches
            manager.publish(topic, Bytes::from("msg1")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            manager.publish(topic, Bytes::from("msg2")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            manager.publish(topic, Bytes::from("msg3")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            manager.publish(topic, Bytes::from("msg4")).await.unwrap();

            // Wait for flush/rotation (flush timer is 50ms)
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Verify Filesystem
            let topic_path = temp_dir.path().join(topic);
            let mut files: Vec<String> = std::fs::read_dir(&topic_path)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .filter(|name| name.ends_with(".log") && name != "groups.log")
                .collect();
            files.sort();

            println!("Segment files found: {:?}", files);
            assert!(files.len() >= 2, "Should have at least 2 segments");

            // Restart and verify all messages are readable
            drop(manager);

            let mut recover_config = Config::global().stream.clone();
            recover_config.persistence_path = path_str;
            let recovered_manager = StreamManager::new(recover_config);

            recovered_manager.create_topic(topic.to_string(), StreamCreateOptions {
                ..Default::default()
            }).await.unwrap();

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

            let manager = StreamManager::new(config);
            let topic = "topic_retention";

            manager.create_topic(topic.to_string(), StreamCreateOptions {
                ..Default::default()
            }).await.unwrap();

            // Write large messages to create multiple segments, with delay to ensure flush
            for i in 1..=7 {
                manager.publish(topic, Bytes::from(format!("msg{}-50bytes-payload-0000000000000000000000000000", i))).await.unwrap();
                tokio::time::sleep(Duration::from_millis(60)).await;
            }

            // Wait for flush + retention check
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Verify filesystem
            let topic_path = temp_dir.path().join(topic);
            let mut files: Vec<String> = std::fs::read_dir(&topic_path)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .filter(|name| name.ends_with(".log") && name != "groups.log")
                .collect();
            files.sort();

            println!("Files after retention: {:?}", files);
            // Oldest segment should be deleted
            assert!(!files.contains(&"1.log".to_string()), "Oldest segment should be deleted");
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

            // Phase 1: Create topics, publish messages
            {
                let manager = StreamManager::new(config.clone());
                
                manager.create_topic(topic1.to_string(), StreamCreateOptions {
                    ..Default::default()
                }).await.unwrap();

                manager.create_topic(topic2.to_string(), StreamCreateOptions {
                    ..Default::default()
                }).await.unwrap();

                manager.publish(topic1, Bytes::from("msg1_t1")).await.unwrap();
                manager.publish(topic1, Bytes::from("msg2_t1")).await.unwrap();
                manager.publish(topic2, Bytes::from("msg1_t2")).await.unwrap();

                // Join group, fetch and ack
                manager.join_group(group, topic1, "client-A").await.unwrap();
                let msgs = manager.fetch(group, "client-A", 1, topic1).await.unwrap();
                if !msgs.is_empty() {
                    manager.ack(group, topic1, msgs[0].seq).await.unwrap();
                }

                // Force flush
                manager.publish(topic1, Bytes::from("msg3_t1")).await.unwrap();
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            // Small delay
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Phase 2: Warm restart
            {
                let manager2 = StreamManager::new(config.clone());
                tokio::time::sleep(Duration::from_millis(200)).await;

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

            let manager = StreamManager::new(config);
            let topic = "eviction_test";

            manager.create_topic(topic.to_string(), StreamCreateOptions {
                ..Default::default()
            }).await.unwrap();

            // Publish 500 messages
            for i in 0..500 {
                let payload = Bytes::from(format!("msg_{:04}", i));
                manager.publish(topic, payload).await.unwrap();
            }

            // Wait for flush + eviction
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Cold read: old messages from disk
            let old_msgs = manager.read(topic, 1, 10).await;
            assert_eq!(old_msgs.len(), 10, "Cold read should work for evicted messages");
            assert_eq!(old_msgs[0].payload, Bytes::from("msg_0000"));
            assert_eq!(old_msgs[9].payload, Bytes::from("msg_0009"));

            // Hot read: recent messages from RAM
            let recent_msgs = manager.read(topic, 491, 10).await;
            assert_eq!(recent_msgs.len(), 10, "Hot read should work for recent messages");
            assert_eq!(recent_msgs[0].payload, Bytes::from("msg_0490"));
            assert_eq!(recent_msgs[9].payload, Bytes::from("msg_0499"));

            println!("✅ RAM eviction test passed");
        }
    }

    mod performance {
        use super::*;
        use nexo::brokers::stream::StreamManager;

        const COUNT: usize = 500_000;


        #[tokio::test]
        async fn bench_stream_publish() {
            let temp_dir = tempfile::tempdir().unwrap();

            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();

            let manager = StreamManager::new(config);
            let topic = "bench-async";
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                ..Default::default()
            }).await.unwrap();

            let mut bench = Benchmark::start("STREAM PUSH (Async Background)", COUNT);
            for _ in 0..COUNT {
                let start = Instant::now();
                manager.publish(topic, Bytes::from("data")).await.unwrap();
                bench.record(start.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            bench.stop();
        }
    }

    mod error_handling {
        use super::*;
        use nexo::brokers::stream::StreamManager;

        #[tokio::test]
        async fn test_publish_nonexistent_topic() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();
            
            let manager = StreamManager::new(config);
            
            let result = manager.publish("nonexistent_topic", Bytes::from("msg")).await;
            
            assert!(result.is_err(), "Publishing to nonexistent topic should fail");
            assert_eq!(result.err().unwrap(), "Topic not found");
        }

        #[tokio::test]
        async fn test_read_beyond_high_watermark() {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut config = Config::global().stream.clone();
            config.persistence_path = temp_dir.path().to_str().unwrap().to_string();
            
            let manager = StreamManager::new(config);
            manager.create_topic("test_topic".to_string(), StreamCreateOptions::default()).await.unwrap();
            
            // Publish 10 messages (seq 1-10)
            for i in 0..10 {
                manager.publish("test_topic", Bytes::from(format!("msg{}", i))).await.unwrap();
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
            
            let manager = StreamManager::new(config);
            manager.create_topic("test_topic".to_string(), StreamCreateOptions::default()).await.unwrap();
            manager.publish("test_topic", Bytes::from("msg")).await.unwrap();
            
            // Fetch without joining should fail
            let result = manager.fetch("test_group", "client-A", 10, "test_topic").await;
            assert!(result.is_err(), "Fetch without join should fail");
        }
    }
}
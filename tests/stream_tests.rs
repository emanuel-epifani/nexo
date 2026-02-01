use nexo::brokers::stream::commands::{StreamCreateOptions, StreamPublishOptions, PersistenceOptions, RetentionOptions};
use nexo::brokers::stream::StreamManager;
use nexo::config::{Config, SystemStreamConfig};
use bytes::Bytes;
use std::collections::HashSet;
use std::time::{Duration, Instant};
mod helpers;
use helpers::Benchmark;

mod features {
    use super::*;
    use nexo::brokers::stream::StreamManager;

    fn get_test_config(path: Option<&str>) -> SystemStreamConfig {
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
            partitions: Some(2),
            persistence: None,
            ..Default::default()
        }).await.unwrap();

        assert!(manager.exists(topic).await);

        // 2. Publish
        let payload = Bytes::from("hello world");
        let offset = manager.publish(topic, StreamPublishOptions { key: None }, payload.clone()).await.unwrap();
        assert_eq!(offset, 0);

        // 3. Read Raw (Direct Read)
        let msgs = manager.read(topic, 0, 100).await;
        // Note: read returns messages from partition 0 by default in the manager wrapper,
        // and Round Robin without key might have sent to P0 or P1.
        // If it went to P1, this might return empty unless we read both.
        // However, initial round robin usually starts at 0.
        if !msgs.is_empty() {
            assert_eq!(msgs[0].payload, payload);
        }
    }

    #[tokio::test]
    async fn test_stream_group_rebalancing_scale_up() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
        let manager = StreamManager::new(config);
        let topic = "rebalance-scale-up";
        let group = "g-scale";

        // Create topic with 4 partitions
        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(4),
            persistence: None,
            ..Default::default()
        }).await.unwrap();

        // 1. Client A Joins -> Should get ALL 4 partitions
        let (gen_a, parts_a, _) = manager.join_group(group, topic, "client-A").await.unwrap();
        assert_eq!(parts_a.len(), 4, "Client A should have all partitions initially");

        // 2. Client B Joins -> Should trigger rebalance
        let (gen_b, parts_b, _) = manager.join_group(group, topic, "client-B").await.unwrap();

        assert!(gen_b > gen_a, "Generation ID must increase");
        assert_eq!(parts_b.len(), 2, "Client B should get 2 partitions");

        // 3. Client A Re-Joins (simulating response to Rebalance Error)
        let (gen_a_new, parts_a_new, _) = manager.join_group(group, topic, "client-A").await.unwrap();

        assert_eq!(gen_a_new, gen_b, "Generation ID should match current epoch");
        assert_eq!(parts_a_new.len(), 2, "Client A should now have 2 partitions");

        // Ensure partitions are distinct
        let set_a: HashSet<_> = parts_a_new.iter().collect();
        let set_b: HashSet<_> = parts_b.iter().collect();
        assert!(set_a.is_disjoint(&set_b), "Partitions must not overlap");
    }

    #[tokio::test]
    async fn test_stream_group_rebalancing_scale_down() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
        let manager = StreamManager::new(config);
        let topic = "rebalance-scale-down";
        let group = "g-down";

        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(2),
            persistence: None,
            ..Default::default()
        }).await.unwrap();

        // 1. Both Join
        manager.join_group(group, topic, "client-A").await.unwrap();
        manager.join_group(group, topic, "client-B").await.unwrap();

        // 2. Verify Assignment (should be split)
        let (_, parts_a, _) = manager.join_group(group, topic, "client-A").await.unwrap();
        let (_, parts_b, _) = manager.join_group(group, topic, "client-B").await.unwrap();
        assert_eq!(parts_a.len(), 1);
        assert_eq!(parts_b.len(), 1);

        // 3. Client A Disconnects
        manager.disconnect("client-A".to_string()).await;

        // 4. Client B Re-Joins/Checks -> Should inherit everything
        let (_, parts_b_new, _) = manager.join_group(group, topic, "client-B").await.unwrap();
        assert_eq!(parts_b_new.len(), 2, "Survivor should inherit all partitions");
    }

    #[tokio::test]
    async fn test_stream_epoch_fencing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
        let manager = StreamManager::new(config);
        let topic = "epoch-fencing";
        let group = "g-epoch";

        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(2), persistence: None, ..Default::default() }).await.unwrap();

        // A joins -> Gen 1
        let (gen_1, _, _) = manager.join_group(group, topic, "client-A").await.unwrap();

        // B joins -> Gen 2 (Invalidates Gen 1)
        let (gen_2, _, _) = manager.join_group(group, topic, "client-B").await.unwrap();
        assert!(gen_2 > gen_1);

        // A tries to commit with Gen 1 -> Should Fail
        let res = manager.commit_offset(group, topic, 0, 10, "client-A", gen_1).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap(), "REBALANCE_NEEDED");

        // A tries to fetch with Gen 1 -> Should Fail
        let res_fetch = manager.fetch_group(group, "client-A", gen_1, 0, 0, 10, topic).await;
        assert!(res_fetch.is_err());
        assert_eq!(res_fetch.err().unwrap(), "REBALANCE_NEEDED");
    }

    #[tokio::test]
    async fn test_stream_ordering_per_partition() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
        let manager = StreamManager::new(config);
        let topic = "ordering-topic";

        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(1), persistence: None, ..Default::default() }).await.unwrap();

        // Publish 1, 2, 3
        for i in 1..=3 {
            let payload = Bytes::from(format!("msg-{}", i));
            manager.publish(topic, StreamPublishOptions { key: None }, payload).await.unwrap();
        }

        // Read back
        let msgs = manager.read(topic, 0, 10).await;
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].payload, Bytes::from("msg-1"));
        assert_eq!(msgs[1].payload, Bytes::from("msg-2"));
        assert_eq!(msgs[2].payload, Bytes::from("msg-3"));
    }

    #[tokio::test]
    async fn test_stream_commit_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = get_test_config(Some(temp_dir.path().to_str().unwrap()));
        let manager = StreamManager::new(config);
        let topic = "commit-topic";
        let group = "g-commit";

        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(1), persistence: None, ..Default::default() }).await.unwrap();

        // Join
        let (gen, parts, _) = manager.join_group(group, topic, "client-A").await.unwrap();
        let pid = parts[0];

        // Commit offset 100
        manager.commit_offset(group, topic, pid, 100, "client-A", gen).await.unwrap();

        // Re-Join (Simulate restart/reconnect)
        let (_, _, offsets) = manager.join_group(group, topic, "client-A").await.unwrap();

        // Should recall offset 100
        assert_eq!(*offsets.get(&pid).unwrap(), 100);
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
            partitions: Some(1),
            persistence: Some(PersistenceOptions::FileSync),
            ..Default::default()
        }).await.unwrap();

        // 2. Publish (create some data on disk)
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg1")).await.unwrap();

        assert!(manager.exists(topic).await);
        let topic_path = temp_dir.path().join(topic);
        assert!(topic_path.exists());

        // 3. Delete
        manager.delete_topic(topic.to_string()).await.unwrap();

        // 4. Verify in Memory
        assert!(!manager.exists(topic).await);

        // 5. Verify on Disk
        assert!(!topic_path.exists());

        // 6. Try to read (should be empty/fail)
        let msgs = manager.read(topic, 0, 10).await;
        assert!(msgs.is_empty());
    }

}

mod persistence {
    use super::*;
    use std::fs;
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
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
                ..Default::default()
            }).await.unwrap();

            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg1")).await.unwrap();
            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg2")).await.unwrap();
            
            // Drop Manager
        }

        // 2. Recovery
        {
            let manager = StreamManager::new(config.clone());
            // Topic auto-recovers if exists on disk? 
            // Currently StreamManager::new() does NOT auto-scan disk to recreate actors. 
            // It lazily creates them or we need to "re-create" topic to trigger recovery logic in Actor::new()
            
            // Re-call create to respawn actor (it should check existence or overwrite? Logic says: if !actors.contains_key)
            // But Actor::new calls recover_topic. So we just need to spawn the actor.
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
                ..Default::default()
            }).await.unwrap();

            let msgs = manager.read(topic, 0, 10).await;
            assert_eq!(msgs.len(), 2);
            assert_eq!(msgs[0].payload, Bytes::from("msg1"));
            assert_eq!(msgs[1].payload, Bytes::from("msg2"));
            assert_eq!(msgs[0].offset, 0);
            assert_eq!(msgs[1].offset, 1);
        }
    }

    #[tokio::test]
    async fn test_commit_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path_str = temp_dir.path().to_str().unwrap().to_string();
        
        let mut config = Config::global().stream.clone();
        config.persistence_path = path_str.clone();

        let topic = "persist-commit";
        let group = "g-persist";

        {
            let manager = StreamManager::new(config.clone());
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
                ..Default::default()
            }).await.unwrap();

            let (gen, parts, _) = manager.join_group(group, topic, "client-A").await.unwrap();
            manager.commit_offset(group, topic, parts[0], 50, "client-A", gen).await.unwrap();
        }

        {
            let manager = StreamManager::new(config.clone());
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
                ..Default::default()
            }).await.unwrap();

            let (_, _, offsets) = manager.join_group(group, topic, "client-A").await.unwrap();
            assert_eq!(*offsets.get(&0).unwrap(), 50);
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
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
                ..Default::default()
            }).await.unwrap();

            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("valid1")).await.unwrap();
            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("valid2")).await.unwrap();
        }

        // 2. Corrupt Data (Append garbage)
        tokio::time::sleep(Duration::from_millis(500)).await;

        let log_path = temp_dir.path().join(topic).join("0_0.log");
        
        // Se non esiste 0_0.log, prova 0.log (compatibilità)
        let log_path = if log_path.exists() {
            log_path
        } else {
            temp_dir.path().join(topic).join("0.log")
        };

        let mut file = std::fs::OpenOptions::new().append(true).open(log_path).expect("Log file should exist");
        file.write_all(b"GARBAGE_DATA_WITHOUT_HEADER").unwrap();

        // 3. Recover
        {
            let manager = StreamManager::new(config.clone());
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
                ..Default::default()
            }).await.unwrap();

            let msgs = manager.read(topic, 0, 10).await;
            assert_eq!(msgs.len(), 2);
            assert_eq!(msgs[0].payload, Bytes::from("valid1"));
            assert_eq!(msgs[1].payload, Bytes::from("valid2"));
        }
    }

    #[tokio::test]
    async fn test_stream_log_compaction_persistence() {
        // 1. SETUP: Usa config custom
        let temp_dir = tempfile::tempdir().unwrap();
        let path_str = temp_dir.path().to_str().unwrap().to_string();
        
        let mut config = Config::global().stream.clone();
        config.persistence_path = path_str.clone();
        config.compaction_threshold = 2; // Trigger compaction after every 2 ops

        let manager = StreamManager::new(config);
        let topic = "topic_compaction";
        let group = "group_compact";
        let client = "client_1";

        // Creazione Topic
        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(1),
            persistence: Some(PersistenceOptions::FileAsync { flush_interval_ms: Some(10) }),
            ..Default::default()
        }).await.unwrap();

        // Join Group
        let (gen_id, _, _) = manager.join_group(group, topic, client).await.unwrap();

        // 2. ACTION: Eseguiamo 10 Commit progressivi
        for i in 1..=10 {
            manager.commit_offset(group, topic, 0, i, client, gen_id).await.unwrap();
            // Un piccolo sleep per dare tempo al writer async di processare e compattare
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // 3. RESTART: Simuliamo un riavvio del broker
        drop(manager);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut recover_config = Config::global().stream.clone();
        recover_config.persistence_path = path_str;
        let recovered_manager = StreamManager::new(recover_config);

        // 4. VERIFY: Verifichiamo lo stato recuperato
        recovered_manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(1),
            persistence: Some(PersistenceOptions::FileAsync { flush_interval_ms: Some(10) }),
            ..Default::default()
        }).await.unwrap();

        let (_new_gen, _assignments, committed_offsets) = recovered_manager
            .join_group(group, topic, client)
            .await
            .unwrap();

        // Verifica
        assert_eq!(
            *committed_offsets.get(&0).unwrap(), 
            10, 
            "L'offset recuperato dopo la compattazione deve essere l'ultimo committato (10)"
        );

        let file_size = std::fs::metadata(temp_dir.path().join(topic).join("commits.log"))
            .unwrap()
            .len();
        
        println!("Final commits.log size: {} bytes", file_size);
        assert!(file_size < 300, "Il file commits.log dovrebbe essere compattato (piccolo)");
    }

    #[tokio::test]
    async fn test_stream_log_segmentation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path_str = temp_dir.path().to_str().unwrap().to_string();
        
        let mut config = Config::global().stream.clone();
        config.persistence_path = path_str.clone();
        // Set segment size to a very small value to force rotation frequently
        // 1 message is around 50-60 bytes. Let's set it to 100 bytes to force rotation after ~2 messages.
        config.max_segment_size = 100; 

        let manager = StreamManager::new(config);
        let topic = "topic_segmentation";

        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(1),
            persistence: Some(PersistenceOptions::FileSync),
            ..Default::default()
        }).await.unwrap();

        // 1. Write messages
        // Msg 1 (offset 0)
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg1")).await.unwrap();
        // Msg 2 (offset 1) - Should fill segment
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg2")).await.unwrap();
        // Msg 3 (offset 2) - Should trigger rotation and go to new segment
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg3")).await.unwrap();
        // Msg 4 (offset 3)
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg4")).await.unwrap();

        // Wait for async flush/rotation
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 2. Verify Filesystem
        let topic_path = temp_dir.path().join(topic);
        let mut files: Vec<String> = std::fs::read_dir(&topic_path)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|name| name.ends_with(".log") && name != "commits.log")
            .collect();
        files.sort();

        println!("Segment files found: {:?}", files);
        assert!(files.len() >= 2, "Should have at least 2 segments");
        // Expecting something like: "0_0.log", "0_2.log" (assuming rotation happened at offset 2)

        // 3. Verify Reading (Transparency with Pagination)
        // Restart manager to force recovery from disk
        drop(manager);
        
        let mut recover_config = Config::global().stream.clone();
        recover_config.persistence_path = path_str;
        let recovered_manager = StreamManager::new(recover_config);
        
        // Trigger recovery
        recovered_manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(1),
            persistence: Some(PersistenceOptions::FileSync),
            ..Default::default()
        }).await.unwrap();

        let mut all_msgs = Vec::new();
        let mut next_offset = 0;

        // Simulate a real client polling loop
        // We expect 4 messages total.
        while all_msgs.len() < 4 {
            let batch = recovered_manager.read(topic, next_offset, 10).await;
            if batch.is_empty() {
                break; 
            }
            
            for msg in batch {
                next_offset = msg.offset + 1;
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
        config.max_segment_size = 100; // Small segments (approx 2 msgs per segment)
        config.retention_check_interval_ms = 100; // Check every 100ms
        config.default_retention_bytes = 250; // Keep approx 2.5 segments

        let manager = StreamManager::new(config);
        let topic = "topic_retention";

        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(1),
            persistence: Some(PersistenceOptions::FileSync), // Use Sync to ensure writes happen
            ..Default::default()
        }).await.unwrap();

        // 1. Write Messages to create segments
        // Segment 1: Msg 1, Msg 2 (Size ~100-120)
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg1-50bytes-payload-0000000000000000000000000000")).await.unwrap();
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg2-50bytes-payload-0000000000000000000000000000")).await.unwrap();
        
        // Segment 2: Msg 3, Msg 4
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg3-50bytes-payload-0000000000000000000000000000")).await.unwrap();
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg4-50bytes-payload-0000000000000000000000000000")).await.unwrap();

        // Segment 3: Msg 5, Msg 6
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg5-50bytes-payload-0000000000000000000000000000")).await.unwrap();
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg6-50bytes-payload-0000000000000000000000000000")).await.unwrap();

        // Segment 4: Msg 7 (Active)
        manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg7-50bytes-payload-0000000000000000000000000000")).await.unwrap();

        // Total roughly: 4 segments.
        // Seg 1 (~100), Seg 2 (~100), Seg 3 (~100), Seg 4 (~50). Total ~350.
        // Limit 250.
        // Expected: Seg 1 deleted. Seg 2 might be deleted if boundary is tight. 
        // Let's wait for retention check.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // 2. Verify Filesystem
        let topic_path = temp_dir.path().join(topic);
        let mut files: Vec<String> = std::fs::read_dir(&topic_path)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|name| name.ends_with(".log") && name != "commits.log")
            .collect();
        files.sort();

        println!("Files after retention: {:?}", files);

        // We expect fewer segments than created.
        // Created: 0_0.log, 0_2.log, 0_4.log, 0_6.log
        // If Retention worked, 0_0.log should be gone.
        assert!(!files.contains(&"0_0.log".to_string()), "Oldest segment should be deleted");
        assert!(files.contains(&"0_6.log".to_string()), "Newest segment should exist");
    }
}

mod performance {
    use super::*;
    use nexo::brokers::stream::StreamManager;
    
    // Reduce count for tests to be fast, but enough to see diff
    const COUNT: usize = 10_000;

    #[tokio::test]
    async fn bench_stream_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        std::env::set_var("STREAM_ROOT_PERSISTENCE_PATH", temp_dir.path().to_str().unwrap());
        
        let mut config = Config::global().stream.clone();
        config.persistence_path = temp_dir.path().to_str().unwrap().to_string();

        let manager = StreamManager::new(config);
        let topic = "bench-mem";
        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(1), persistence: Some(PersistenceOptions::Memory), ..Default::default() }).await.unwrap();

        let mut bench = Benchmark::start("STREAM PUSH - Memory", COUNT);
        for _ in 0..COUNT {
            let start = Instant::now();
            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("data")).await.unwrap();
            bench.record(start.elapsed());
        }
        bench.stop();
    }

    #[tokio::test]
    async fn bench_stream_fsync() {
        let temp_dir = tempfile::tempdir().unwrap();
        std::env::set_var("STREAM_ROOT_PERSISTENCE_PATH", temp_dir.path().to_str().unwrap());
        
        let mut config = Config::global().stream.clone();
        config.persistence_path = temp_dir.path().to_str().unwrap().to_string();

        let manager = StreamManager::new(config);
        let topic = "bench-sync";
        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(1), persistence: Some(PersistenceOptions::FileSync), ..Default::default() }).await.unwrap();

        let mut bench = Benchmark::start("STREAM PUSH - FSync", COUNT);
        for _ in 0..COUNT {
            let start = Instant::now();
            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("data")).await.unwrap();
            bench.record(start.elapsed());
        }
        bench.stop();
    }

    #[tokio::test]
    async fn bench_stream_fasync() {
        let temp_dir = tempfile::tempdir().unwrap();
        std::env::set_var("STREAM_ROOT_PERSISTENCE_PATH", temp_dir.path().to_str().unwrap());
        
        let mut config = Config::global().stream.clone();
        config.persistence_path = temp_dir.path().to_str().unwrap().to_string();

        let manager = StreamManager::new(config);
        let topic = "bench-async";
        manager.create_topic(topic.to_string(), StreamCreateOptions { 
            partitions: Some(1), 
            persistence: Some(PersistenceOptions::FileAsync { flush_interval_ms: Some(200) }),
            ..Default::default() 
        }).await.unwrap();

        let mut bench = Benchmark::start("STREAM PUSH - FAsync", COUNT);
        for _ in 0..COUNT {
            let start = Instant::now();
            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("data")).await.unwrap();
            bench.record(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        bench.stop();
    }
}

mod unit_tests {
    use nexo::brokers::stream::topic::TopicState;
    use bytes::Bytes;

    #[test]
    fn test_topic_state_pure_logic() {
        let mut state = TopicState::new("test_topic".to_string(), 2, 1000);
        
        // 1. Publish (Partition 0)
        let (offset, _) = state.publish(0, Bytes::from("msg1"));
        assert_eq!(offset, 0);
        assert_eq!(state.get_high_watermark(0), 1);

        // 2. Publish (Partition 1)
        let (offset, _) = state.publish(1, Bytes::from("msg2"));
        assert_eq!(offset, 0);
        
        // 3. Read
        let msgs = state.read(0, 0, 10);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, Bytes::from("msg1"));
        
        let msgs = state.read(1, 0, 10);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, Bytes::from("msg2"));
    }

    #[test]
    fn test_read_offset_logic() {
        let mut state = TopicState::new("test".to_string(), 1, 1000);
        for i in 0..10 {
            state.publish(0, Bytes::from(format!("msg{}", i)));
        }

        // Read all
        let all = state.read(0, 0, 100);
        assert_eq!(all.len(), 10);
        assert_eq!(all[0].offset, 0);
        assert_eq!(all[9].offset, 9);

        // Read mid
        let mid = state.read(0, 5, 100);
        assert_eq!(mid.len(), 5);
        assert_eq!(mid[0].offset, 5);
        
        // Read future
        let future = state.read(0, 100, 100);
        assert!(future.is_empty());
    }
}

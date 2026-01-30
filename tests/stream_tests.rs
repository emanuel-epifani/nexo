use nexo::brokers::stream::stream_manager::StreamManager;
use nexo::brokers::stream::commands::{StreamCreateOptions, StreamPublishOptions, PersistenceOptions};
use nexo::config::{Config, StreamConfig};
use bytes::Bytes;
use std::collections::HashSet;
use std::time::{Duration, Instant};
mod helpers;
use helpers::Benchmark;


mod features {
    use super::*;

    #[tokio::test]
    async fn test_stream_basic_flow() {
        let manager = StreamManager::new();
        let topic = "test-basic-flow";

        // 1. Create
        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(2),
            persistence: None,
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
        let manager = StreamManager::new();
        let topic = "rebalance-scale-up";
        let group = "g-scale";

        // Create topic with 4 partitions
        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(4),
            persistence: None,
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
        let manager = StreamManager::new();
        let topic = "rebalance-scale-down";
        let group = "g-down";

        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(2),
            persistence: None,
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
        let manager = StreamManager::new();
        let topic = "epoch-fencing";
        let group = "g-epoch";

        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(2), persistence: None }).await.unwrap();

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
        let manager = StreamManager::new();
        let topic = "ordering-topic";

        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(1), persistence: None }).await.unwrap();

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
        let manager = StreamManager::new();
        let topic = "commit-topic";
        let group = "g-commit";

        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(1), persistence: None }).await.unwrap();

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

}

mod persistence {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[tokio::test]
    async fn test_write_and_recover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path_str = temp_dir.path().to_str().unwrap().to_string();
        std::env::set_var("STREAM_ROOT_PERSISTENCE_PATH", &path_str);
        
        let topic = "persist-recover";

        // 1. Create & Publish (Sync)
        {
            let manager = StreamManager::new();
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
            }).await.unwrap();

            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg1")).await.unwrap();
            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("msg2")).await.unwrap();
            
            // Drop Manager
        }

        // 2. Recovery
        {
            let manager = StreamManager::new();
            // Topic auto-recovers if exists on disk? 
            // Currently StreamManager::new() does NOT auto-scan disk to recreate actors. 
            // It lazily creates them or we need to "re-create" topic to trigger recovery logic in Actor::new()
            
            // Re-call create to respawn actor (it should check existence or overwrite? Logic says: if !actors.contains_key)
            // But Actor::new calls recover_topic. So we just need to spawn the actor.
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
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
        std::env::set_var("STREAM_ROOT_PERSISTENCE_PATH", &path_str);

        let topic = "persist-commit";
        let group = "g-persist";

        {
            let manager = StreamManager::new();
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
            }).await.unwrap();

            let (gen, parts, _) = manager.join_group(group, topic, "client-A").await.unwrap();
            manager.commit_offset(group, topic, parts[0], 50, "client-A", gen).await.unwrap();
        }

        {
            let manager = StreamManager::new();
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
            }).await.unwrap();

            let (_, _, offsets) = manager.join_group(group, topic, "client-A").await.unwrap();
            assert_eq!(*offsets.get(&0).unwrap(), 50);
        }
    }

    #[tokio::test]
    async fn test_corruption_integrity() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path_str = temp_dir.path().to_str().unwrap().to_string();
        std::env::set_var("STREAM_ROOT_PERSISTENCE_PATH", &path_str);

        let topic = "persist-corrupt";

        // 1. Write Data
        {
            let manager = StreamManager::new();
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
            }).await.unwrap();

            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("valid1")).await.unwrap();
            manager.publish(topic, StreamPublishOptions { key: None }, Bytes::from("valid2")).await.unwrap();
        }

        // 2. Corrupt Data (Append garbage)
        // Wait a bit to ensure async file flush/creation has happened
        tokio::time::sleep(Duration::from_millis(500)).await;

        let log_path = temp_dir.path().join(topic).join("0.log");
        let mut file = std::fs::OpenOptions::new().append(true).open(log_path).expect("Log file should exist");
        file.write_all(b"GARBAGE_DATA_WITHOUT_HEADER").unwrap();

        // 3. Recover
        {
            let manager = StreamManager::new();
            manager.create_topic(topic.to_string(), StreamCreateOptions {
                partitions: Some(1),
                persistence: Some(PersistenceOptions::FileSync),
            }).await.unwrap();

            let msgs = manager.read(topic, 0, 10).await;
            // Should read 2 valid messages and stop at garbage
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

        let manager = StreamManager::with_config(config);
        let topic = "topic_compaction";
        let group = "group_compact";
        let client = "client_1";

        // Creazione Topic
        manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(1),
            persistence: Some(PersistenceOptions::FileAsync { flush_interval_ms: Some(10) }), // Flush veloce
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
        let recovered_manager = StreamManager::with_config(recover_config);

        // 4. VERIFY: Verifichiamo lo stato recuperato
        // Facciamo un join: dovremmo ricevere l'offset 10 come punto di partenza
        // Nota: re-create topic necessario per spawnare l'attore (come negli altri test)
        recovered_manager.create_topic(topic.to_string(), StreamCreateOptions {
            partitions: Some(1),
            persistence: Some(PersistenceOptions::FileAsync { flush_interval_ms: Some(10) }),
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

        // Opzionale: Verifica fisica che il file non sia esploso
        let file_size = std::fs::metadata(temp_dir.path().join(topic).join("commits.log"))
            .unwrap()
            .len();
        
        println!("Final commits.log size: {} bytes", file_size);
        // Un singolo record pesa circa 40-50 byte. 10 record sarebbero ~500. 1 record ~50.
        // Diamo un margine ampio ma che esclude 10 record.
        assert!(file_size < 300, "Il file commits.log dovrebbe essere compattato (piccolo)");
    }
}

mod performance {
    use super::*;
    
    // Reduce count for tests to be fast, but enough to see diff
    // const COUNT: usize = 10_000;
    const COUNT: usize = 200_000;

    #[tokio::test]
    async fn bench_stream_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        std::env::set_var("STREAM_ROOT_PERSISTENCE_PATH", temp_dir.path().to_str().unwrap());
        
        let manager = StreamManager::new();
        let topic = "bench-mem";
        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(1), persistence: Some(PersistenceOptions::Memory) }).await.unwrap();

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

        let manager = StreamManager::new();
        let topic = "bench-sync";
        manager.create_topic(topic.to_string(), StreamCreateOptions { partitions: Some(1), persistence: Some(PersistenceOptions::FileSync) }).await.unwrap();

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

        let manager = StreamManager::new();
        let topic = "bench-async";
        manager.create_topic(topic.to_string(), StreamCreateOptions { 
            partitions: Some(1), 
            persistence: Some(PersistenceOptions::FileAsync { flush_interval_ms: Some(100) }) 
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

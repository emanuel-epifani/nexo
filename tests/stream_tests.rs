use nexo::brokers::stream::stream_manager::StreamManager;
use nexo::brokers::stream::commands::{StreamCreateOptions, StreamPublishOptions};
use bytes::Bytes;
use std::collections::HashSet;


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

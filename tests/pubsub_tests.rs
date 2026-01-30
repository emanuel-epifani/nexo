use nexo::brokers::pub_sub::{PubSubManager, ClientId};
use std::sync::Arc;
use tokio::sync::mpsc;
use bytes::{Bytes, Buf};
use std::time::{Duration, Instant};
use uuid::Uuid;

mod helpers;
use helpers::{setup_pubsub_manager, Benchmark};

// =========================================================================================
// 1. FEATURE TESTS (Happy Path + Wildcards + Retained)
// =========================================================================================

mod features {
    use super::*;

    #[tokio::test]
    async fn test_basic_pub_sub() {
        let (manager, _tmp) = setup_pubsub_manager().await;
        let client_id = ClientId("sub1".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        
        // 1. Connect
        let _session = manager.connect(client_id.clone(), tx);
        
        // 2. Subscribe
        let topic = "sensors/temp";
        manager.subscribe(topic, client_id.clone()).await;
        
        // 3. Publish
        let payload = Bytes::from("24.5");
        let count = manager.publish(topic, payload.clone(), false).await;
        assert_eq!(count, 1, "Should deliver to 1 subscriber");
        
        // 4. Verify Receipt
        let msg = rx.recv().await.expect("Should receive message");
        assert_eq!(msg.topic, topic);
        assert_eq!(msg.payload, payload);
    }

    #[tokio::test]
    async fn test_wildcard_plus_single_level() {
        let (manager, _tmp) = setup_pubsub_manager().await;
        let client_id = ClientId("wild_plus".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _session = manager.connect(client_id.clone(), tx);

        // Subscribe to "home/+/status"
        manager.subscribe("home/+/status", client_id.clone()).await;

        // MATCH: "home/kitchen/status"
        manager.publish("home/kitchen/status", Bytes::from("on"), false).await;
        let msg = rx.recv().await.expect("Should match + wildcard");
        assert_eq!(msg.topic, "home/kitchen/status");

        // NO MATCH: "home/kitchen/fridge/status" (too deep)
        let count = manager.publish("home/kitchen/fridge/status", Bytes::from("off"), false).await;
        assert_eq!(count, 0, "Should not match nested levels");

        // NO MATCH: "home/status" (too shallow)
        let count = manager.publish("home/status", Bytes::from("err"), false).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_wildcard_hash_multi_level() {
        let (manager, _tmp) = setup_pubsub_manager().await;
        let client_id = ClientId("wild_hash".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _session = manager.connect(client_id.clone(), tx);

        // Subscribe to "logs/#"
        manager.subscribe("logs/#", client_id.clone()).await;

        // MATCH: "logs/error"
        manager.publish("logs/error", Bytes::from("e1"), false).await;
        assert_eq!(rx.recv().await.unwrap().topic, "logs/error");

        // MATCH: "logs/app/backend/error" (deep)
        manager.publish("logs/app/backend/error", Bytes::from("e2"), false).await;
        assert_eq!(rx.recv().await.unwrap().topic, "logs/app/backend/error");
    }

    #[tokio::test]
    async fn test_retained_messages() {
        let (manager, _tmp) = setup_pubsub_manager().await;
        let topic = "config/settings";
        
        // 1. Publish Retained (No subscribers yet)
        manager.publish(topic, Bytes::from("dark_mode"), true).await;

        // 2. New Client Connects & Subscribes
        let client_id = ClientId("late_joiner".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _session = manager.connect(client_id.clone(), tx);

        manager.subscribe(topic, client_id.clone()).await;

        // 3. Should receive retained message immediately
        let msg = rx.recv().await.expect("Should receive retained message");
        assert_eq!(msg.topic, topic);
        assert_eq!(msg.payload, Bytes::from("dark_mode"));
    }

    #[tokio::test]
    async fn test_cleanup_on_disconnect() {
        let (manager, _tmp) = setup_pubsub_manager().await;
        let client_id = ClientId("leaver".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        
        // Scope to drop session
        {
            let _session = manager.connect(client_id.clone(), tx);
            manager.subscribe("chat/room1", client_id.clone()).await;
            
            // Verify subscription exists (indirectly via publish count)
            let count = manager.publish("chat/room1", Bytes::from("hi"), false).await;
            assert_eq!(count, 1);
        } // _session dropped here -> Disconnect triggered

        // Wait for async cleanup (tokio::spawn in Drop)
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish again -> Should be 0 subscribers
        let count = manager.publish("chat/room1", Bytes::from("anyone?"), false).await;
        assert_eq!(count, 0, "Client should be unsubscribed after disconnect");
    }
}

// =========================================================================================
// 2. PERFORMANCE BENCHMARKS
// =========================================================================================

mod performance {
    use super::*;

    const MSG_COUNT: usize = 100_000;

    #[tokio::test]
    async fn bench_pubsub_throughput_exact_match() {
        let (manager, _tmp) = setup_pubsub_manager().await;
        let client_id = ClientId("bench_sub".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _session = manager.connect(client_id.clone(), tx);

        let topic = "bench/speed";
        manager.subscribe(topic, client_id.clone()).await;

        let payload = Bytes::from("fast_data");

        // Spawn consumer to drain channel
        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {}
        });

        let mut bench = Benchmark::start("PUBSUB - Exact Match Throughput", MSG_COUNT);
        
        for _ in 0..MSG_COUNT {
            let start = Instant::now();
            manager.publish(topic, payload.clone(), false).await;
            bench.record(start.elapsed());
        }
        
        bench.stop();
    }

    #[tokio::test]
    async fn bench_pubsub_throughput_wildcard_match() {
        let (manager, _tmp) = setup_pubsub_manager().await;
        let client_id = ClientId("bench_wild".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _session = manager.connect(client_id.clone(), tx);

        // Subscribe with wildcard
        manager.subscribe("bench/+/metric", client_id.clone()).await;
        let payload = Bytes::from("data");

        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {}
        });

        let mut bench = Benchmark::start("PUBSUB - Wildcard Match Throughput", MSG_COUNT);
        
        for _ in 0..MSG_COUNT {
            let start = Instant::now();
            manager.publish("bench/server1/metric", payload.clone(), false).await;
            bench.record(start.elapsed());
        }
        
        bench.stop();
    }

    #[tokio::test]
    async fn bench_pubsub_fanout() {
        let (manager, _tmp) = setup_pubsub_manager().await;
        let topic = "fanout/global";
        let num_subs = 100;
        
        // Create 100 subscribers
        for i in 0..num_subs {
            let client_id = ClientId(format!("sub_{}", i));
            let (tx, mut rx) = mpsc::unbounded_channel();
            let _session = manager.connect(client_id.clone(), tx);
            manager.subscribe(topic, client_id).await;
            
            tokio::spawn(async move {
                while let Some(_) = rx.recv().await {}
            });
        }

        let payload = Bytes::from("broadcast");
        let count = 10_000; 

        let mut bench = Benchmark::start(&format!("PUBSUB - Fanout 1->{}", num_subs), count);
        
        for _ in 0..count {
            let start = Instant::now();
            manager.publish(topic, payload.clone(), false).await;
            bench.record(start.elapsed());
        }
        
        bench.stop();
    }
}
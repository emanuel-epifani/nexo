use nexo::brokers::pub_sub::{PubSubManager, ClientId};
use std::sync::Arc;
use tokio::sync::mpsc;
use bytes::Bytes;
use std::time::{Duration, Instant};

mod helpers;
use helpers::{setup_pubsub_manager, Benchmark};



#[cfg(test)]
mod pubsub_tests {
    use super::*;

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
            manager.connect(client_id.clone(), tx);

            // 2. Subscribe
            let topic = "sensors/temp";
            manager.subscribe(topic, client_id.clone()).await;

            // 3. Publish
            let payload = Bytes::from("24.5");
            let count = manager.publish(topic, payload.clone(), false, None).await;
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
            manager.connect(client_id.clone(), tx);

            // Subscribe to "home/+/status"
            manager.subscribe("home/+/status", client_id.clone()).await;

            // MATCH: "home/kitchen/status"
            manager.publish("home/kitchen/status", Bytes::from("on"), false, None).await;
            let msg = rx.recv().await.expect("Should match + wildcard");
            assert_eq!(msg.topic, "home/kitchen/status");

            // NO MATCH: "home/kitchen/fridge/status" (too deep)
            let count = manager.publish("home/kitchen/fridge/status", Bytes::from("off"), false, None).await;
            assert_eq!(count, 0, "Should not match nested levels");

            // NO MATCH: "home/status" (too shallow)
            let count = manager.publish("home/status", Bytes::from("err"), false, None).await;
            assert_eq!(count, 0);
        }

        #[tokio::test]
        async fn test_wildcard_hash_multi_level() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = ClientId("wild_hash".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);

            // Subscribe to "logs/#"
            manager.subscribe("logs/#", client_id.clone()).await;

            // MATCH: "logs/error"
            manager.publish("logs/error", Bytes::from("e1"), false, None).await;
            assert_eq!(rx.recv().await.unwrap().topic, "logs/error");

            // MATCH: "logs/app/backend/error" (deep)
            manager.publish("logs/app/backend/error", Bytes::from("e2"), false, None).await;
            assert_eq!(rx.recv().await.unwrap().topic, "logs/app/backend/error");
        }

        #[tokio::test]
        async fn test_retained_messages() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "config/settings";

            // 1. Publish Retained (No subscribers yet)
            manager.publish(topic, Bytes::from("dark_mode"), true, None).await;

            // 2. New Client Connects & Subscribes
            let client_id = ClientId("late_joiner".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);

            manager.subscribe(topic, client_id.clone()).await;

            // 3. Should receive retained message immediately
            let msg = rx.recv().await.expect("Should receive retained message");
            assert_eq!(msg.topic, topic);
            assert_eq!(msg.payload, Bytes::from("dark_mode"));
        }

        #[tokio::test]
        async fn test_cleanup_on_disconnect() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            
            let mut config = nexo::config::Config::global().pubsub.clone();
            config.persistence_path = path;
            let manager = Arc::new(nexo::brokers::pub_sub::PubSubManager::new(config));
            
            let client_id = ClientId("leaver".to_string());
            let (tx, _rx) = mpsc::unbounded_channel();

            manager.connect(client_id.clone(), tx);
            manager.subscribe("chat/room1", client_id.clone()).await;

            // Verify subscription exists (indirectly via publish count)
            let count = manager.publish("chat/room1", Bytes::from("hi"), false, None).await;
            assert_eq!(count, 1);

            // Explicit disconnect (simulates socket close)
            manager.disconnect(&client_id).await;

            // Publish again -> Should be 0 subscribers
            let count = manager.publish("chat/room1", Bytes::from("anyone?"), false, None).await;
            assert_eq!(count, 0, "Client should be unsubscribed after disconnect");
        }

        #[tokio::test]
        async fn test_retained_with_custom_ttl() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "sensors/temp";

            // Publish retained with custom TTL (2 seconds)
            manager.publish(topic, Bytes::from("23.5"), true, Some(2)).await;

            // Subscribe immediately - should receive retained
            let client_id = ClientId("sub1".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);
            manager.subscribe(topic, client_id.clone()).await;

            let msg = rx.recv().await.expect("Should receive retained message");
            assert_eq!(msg.payload, Bytes::from("23.5"));

            // Wait for TTL to expire (2s + buffer)
            tokio::time::sleep(Duration::from_secs(3)).await;

            // New subscriber should NOT receive expired retained
            let client_id2 = ClientId("sub2".to_string());
            let (tx2, mut rx2) = mpsc::unbounded_channel();
            manager.connect(client_id2.clone(), tx2);
            manager.subscribe(topic, client_id2.clone()).await;

            // Should timeout (no retained message)
            let result = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await;
            assert!(result.is_err(), "Should not receive expired retained message");
        }

        #[tokio::test]
        async fn test_clear_retained_with_empty_payload() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "config/theme";

            // 1. Publish retained
            manager.publish(topic, Bytes::from("dark"), true, None).await;

            // 2. Verify retained exists
            let client_id = ClientId("sub1".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);
            manager.subscribe(topic, client_id.clone()).await;

            let msg = rx.recv().await.expect("Should receive retained");
            assert_eq!(msg.payload, Bytes::from("dark"));

            // 3. Clear retained with empty payload (MQTT standard)
            manager.publish(topic, Bytes::from(""), true, None).await;

            // 4. New subscriber should NOT receive retained
            let client_id2 = ClientId("sub2".to_string());
            let (tx2, mut rx2) = mpsc::unbounded_channel();
            manager.connect(client_id2.clone(), tx2);
            manager.subscribe(topic, client_id2.clone()).await;

            let result = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await;
            assert!(result.is_err(), "Should not receive cleared retained message");
        }

        #[tokio::test]
        async fn test_retained_persistence_across_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            
            let topic = "persistent/data";
            let payload = Bytes::from("important_value");

            // Create manager and publish retained
            {
                let mut config = nexo::config::Config::global().pubsub.clone();
                config.persistence_path = path.clone();
                let manager = Arc::new(nexo::brokers::pub_sub::PubSubManager::new(config));
                
                manager.publish(topic, payload.clone(), true, None).await;

                // Wait for async save to disk
                tokio::time::sleep(Duration::from_millis(200)).await;

                // Drop manager (simulates restart)
                drop(manager);
            }

            // Wait for cleanup
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Create new manager (simulates restart) with SAME path - should load from disk
            {
                let mut config = nexo::config::Config::global().pubsub.clone();
                config.persistence_path = path.clone();
                let manager2 = Arc::new(nexo::brokers::pub_sub::PubSubManager::new(config));

                // Subscribe - should receive retained from disk
                let client_id = ClientId("after_restart".to_string());
                let (tx, mut rx) = mpsc::unbounded_channel();
                manager2.connect(client_id.clone(), tx);
                manager2.subscribe(topic, client_id.clone()).await;

                let msg = rx.recv().await.expect("Should receive retained after restart");
                assert_eq!(msg.payload, payload);
            }
            
            // temp_dir gets dropped here at end of test
        }

        #[tokio::test]
        async fn test_cleanup_expired_retained_background() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "temp/sensor";

            // Publish retained with 1 second TTL
            manager.publish(topic, Bytes::from("old_value"), true, Some(1)).await;

            // Verify retained exists
            let client_id = ClientId("sub1".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);
            manager.subscribe(topic, client_id.clone()).await;

            let msg = rx.recv().await.expect("Should receive retained");
            assert_eq!(msg.payload, Bytes::from("old_value"));

            // Wait for TTL expiration + background cleanup cycle (60s is too long for test)
            // Note: Background cleanup runs every 60s, but expired check happens on subscribe
            tokio::time::sleep(Duration::from_secs(2)).await;

            // New subscriber should NOT receive expired retained
            let client_id2 = ClientId("sub2".to_string());
            let (tx2, mut rx2) = mpsc::unbounded_channel();
            manager.connect(client_id2.clone(), tx2);
            manager.subscribe(topic, client_id2.clone()).await;

            let result = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await;
            assert!(result.is_err(), "Should not receive expired retained");
        }
    }

    // =========================================================================================
    // 2. ROBUSTNESS TESTS (Concurrency, Edge Cases, Stress)
    // =========================================================================================

    mod robustness {
        use super::*;

        #[tokio::test]
        async fn test_concurrent_publish_disconnect() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = ClientId("concurrent".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            
            manager.connect(client_id.clone(), tx);
            manager.subscribe("test/topic", client_id.clone()).await;
            
            // Spawn consumer to drain channel
            tokio::spawn(async move {
                while let Some(_) = rx.recv().await {}
            });
            
            // Concurrent operations: publish while disconnecting
            let manager_clone = manager.clone();
            let client_clone = client_id.clone();
            let disconnect_task = tokio::spawn(async move {
                manager_clone.disconnect(&client_clone).await;
            });
            
            // Publish 100 messages while disconnect is happening
            for _ in 0..100 {
                manager.publish("test/topic", Bytes::from("data"), false, None).await;
            }
            
            // Should complete without deadlock
            disconnect_task.await.unwrap();
        }

        #[tokio::test]
        async fn test_global_hash_subscriber() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = ClientId("global".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            
            manager.connect(client_id.clone(), tx);
            manager.subscribe("#", client_id.clone()).await;
            
            // Should receive ALL messages from any topic
            manager.publish("sensors/temp", Bytes::from("1"), false, None).await;
            manager.publish("logs/error", Bytes::from("2"), false, None).await;
            manager.publish("any/random/topic", Bytes::from("3"), false, None).await;
            
            let msg1 = rx.recv().await.expect("Should receive message 1");
            let msg2 = rx.recv().await.expect("Should receive message 2");
            let msg3 = rx.recv().await.expect("Should receive message 3");
            
            // Verify all topics received
            let topics: Vec<String> = vec![msg1.topic.clone(), msg2.topic.clone(), msg3.topic.clone()];
            assert!(topics.contains(&"sensors/temp".to_string()));
            assert!(topics.contains(&"logs/error".to_string()));
            assert!(topics.contains(&"any/random/topic".to_string()));
        }

        #[tokio::test]
        async fn test_retained_with_wildcard_subscribe() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            
            // Publish retained on specific topics
            manager.publish("sensors/temp", Bytes::from("20"), true, None).await;
            manager.publish("sensors/humidity", Bytes::from("60"), true, None).await;
            manager.publish("sensors/pressure", Bytes::from("1013"), true, None).await;
            
            // Subscribe with wildcard AFTER retained messages exist
            let client_id = ClientId("wildcard_late".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);
            manager.subscribe("sensors/+", client_id.clone()).await;
            
            // Should receive ALL 3 retained messages
            let mut received = vec![
                rx.recv().await.expect("Should receive retained 1"),
                rx.recv().await.expect("Should receive retained 2"),
                rx.recv().await.expect("Should receive retained 3"),
            ];
            
            // Sort by topic for deterministic comparison
            received.sort_by(|a, b| a.topic.cmp(&b.topic));
            
            assert_eq!(received[0].topic, "sensors/humidity");
            assert_eq!(received[0].payload, Bytes::from("60"));
            assert_eq!(received[1].topic, "sensors/pressure");
            assert_eq!(received[1].payload, Bytes::from("1013"));
            assert_eq!(received[2].topic, "sensors/temp");
            assert_eq!(received[2].payload, Bytes::from("20"));
        }

        #[tokio::test]
        async fn test_multiple_clients_same_topic() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "broadcast/news";
            
            // Create 3 clients subscribing to same topic
            let mut receivers = Vec::new();
            for i in 0..3 {
                let client_id = ClientId(format!("client_{}", i));
                let (tx, rx) = mpsc::unbounded_channel();
                manager.connect(client_id.clone(), tx);
                manager.subscribe(topic, client_id).await;
                receivers.push(rx);
            }
            
            // Publish one message
            let count = manager.publish(topic, Bytes::from("breaking_news"), false, None).await;
            assert_eq!(count, 3, "Should deliver to all 3 subscribers");
            
            // All 3 clients should receive the message
            for mut rx in receivers {
                let msg = rx.recv().await.expect("Should receive message");
                assert_eq!(msg.topic, topic);
                assert_eq!(msg.payload, Bytes::from("breaking_news"));
            }
        }

        #[tokio::test]
        async fn test_same_client_multiple_subscriptions() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = ClientId("multi_sub".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            
            manager.connect(client_id.clone(), tx);
            
            // Subscribe to same topic twice (should deduplicate)
            manager.subscribe("sensors/temp", client_id.clone()).await;
            manager.subscribe("sensors/temp", client_id.clone()).await;
            
            // Publish
            manager.publish("sensors/temp", Bytes::from("data"), false, None).await;
            
            // Should receive only 1 message (not 2)
            let msg1 = rx.recv().await.expect("Should receive message");
            assert_eq!(msg1.payload, Bytes::from("data"));
            
            // Timeout - should not receive duplicate
            let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
            assert!(result.is_err(), "Should not receive duplicate message");
        }

        #[tokio::test]
        async fn test_unsubscribe_without_subscribe() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = ClientId("never_subbed".to_string());
            let (tx, _rx) = mpsc::unbounded_channel();
            
            manager.connect(client_id.clone(), tx);
            
            // Unsubscribe from topic never subscribed to (should not panic)
            manager.unsubscribe("sensors/temp", &client_id).await;
            
            // Publish should work normally
            let count = manager.publish("sensors/temp", Bytes::from("data"), false, None).await;
            assert_eq!(count, 0, "Should have no subscribers");
        }

        #[tokio::test]
        async fn test_disconnect_during_subscribe() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = ClientId("race".to_string());
            let (tx, _rx) = mpsc::unbounded_channel();
            
            manager.connect(client_id.clone(), tx);
            
            // Subscribe and disconnect in parallel (race condition test)
            let manager_clone = manager.clone();
            let client_clone = client_id.clone();
            let subscribe_task = tokio::spawn(async move {
                for _ in 0..10 {
                    manager_clone.subscribe("test/topic", client_clone.clone()).await;
                }
            });
            
            let manager_clone2 = manager.clone();
            let client_clone2 = client_id.clone();
            let disconnect_task = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                manager_clone2.disconnect(&client_clone2).await;
            });
            
            // Should complete without deadlock or panic
            let _ = subscribe_task.await;
            disconnect_task.await.unwrap();
        }

        #[tokio::test]
        async fn test_retained_overwrite_same_topic() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "config/setting";
            
            // Publish retained 3 times on same topic
            manager.publish(topic, Bytes::from("v1"), true, None).await;
            manager.publish(topic, Bytes::from("v2"), true, None).await;
            manager.publish(topic, Bytes::from("v3"), true, None).await;
            
            // New subscriber should receive only latest (v3)
            let client_id = ClientId("late".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);
            manager.subscribe(topic, client_id).await;
            
            let msg = rx.recv().await.expect("Should receive retained");
            assert_eq!(msg.payload, Bytes::from("v3"), "Should receive only latest retained");
            
            // Should not receive v1 or v2
            let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
            assert!(result.is_err(), "Should not receive old retained messages");
        }

        #[tokio::test]
        async fn test_multiple_wildcards_same_message() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            
            // Client subscribes to multiple overlapping patterns
            let client_id = ClientId("multi_pattern".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);
            
            manager.subscribe("sensors/+/temp", client_id.clone()).await;
            manager.subscribe("sensors/kitchen/+", client_id.clone()).await;
            
            // Publish to topic that matches BOTH patterns
            manager.publish("sensors/kitchen/temp", Bytes::from("data"), false, None).await;
            
            // Should receive message only once (deduplicated by client_id)
            let msg1 = rx.recv().await.expect("Should receive message");
            assert_eq!(msg1.topic, "sensors/kitchen/temp");
            
            // Should not receive duplicate
            let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
            assert!(result.is_err(), "Should not receive duplicate from overlapping patterns");
        }
    }

    // =========================================================================================
    // 3. PERFORMANCE BENCHMARKS
    // =========================================================================================

    mod performance {
        use super::*;

        const MSG_COUNT: usize = 100_000;

        #[tokio::test]
        async fn bench_pubsub_throughput_exact_match() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = ClientId("bench_sub".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);

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
                manager.publish(topic, payload.clone(), false, None).await;
                bench.record(start.elapsed());
            }

            bench.stop();
        }

        #[tokio::test]
        async fn bench_pubsub_throughput_wildcard_match() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = ClientId("bench_wild".to_string());
            let (tx, mut rx) = mpsc::unbounded_channel();
            manager.connect(client_id.clone(), tx);

            // Subscribe with wildcard
            manager.subscribe("bench/+/metric", client_id.clone()).await;
            let payload = Bytes::from("data");

            tokio::spawn(async move {
                while let Some(_) = rx.recv().await {}
            });

            let mut bench = Benchmark::start("PUBSUB - Wildcard Match Throughput", MSG_COUNT);

            for _ in 0..MSG_COUNT {
                let start = Instant::now();
                manager.publish("bench/server1/metric", payload.clone(), false, None).await;
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
                manager.connect(client_id.clone(), tx);
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
                manager.publish(topic, payload.clone(), false, None).await;
                bench.record(start.elapsed());
            }

            bench.stop();
        }
    }
}
use bytes::Bytes;
use nexo::brokers::pub_sub::PubSubManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

mod common;
use common::setup_pubsub_manager;

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
            let client_id = "sub1".to_string();
            let (tx, mut rx) = mpsc::channel(8192);

            // 1. Connect
            manager.connect(&client_id, tx);

            // 2. Subscribe
            let topic = "sensors/temp";
            manager.subscribe(&client_id, topic).unwrap();

            // 3. Publish
            let payload = Bytes::from("24.5");
            let count = manager.publish(topic, payload.clone(), false, false, None);
            assert_eq!(count, Ok(1), "Should deliver to 1 subscriber");

            // 4. Verify Receipt
            let msg = rx.recv().await.expect("Should receive message");
            assert_eq!(msg.topic, topic);
            assert_eq!(msg.payload, payload);
        }

        #[tokio::test]
        async fn test_wildcard_plus_single_level() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "wild_plus".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            // Subscribe to "home/+/status"
            manager.subscribe(&client_id, "home/+/status").unwrap();

            // MATCH: "home/kitchen/status"
            manager
                .publish("home/kitchen/status", Bytes::from("on"), false, false, None)
                .unwrap();
            let msg = rx.recv().await.expect("Should match + wildcard");
            assert_eq!(msg.topic, "home/kitchen/status");

            // NO MATCH: "home/kitchen/fridge/status" (too deep)
            let count = manager.publish(
                "home/kitchen/fridge/status",
                Bytes::from("off"),
                false,
                false,
                None,
            );
            assert_eq!(count, Ok(0), "Should not match nested levels");

            // NO MATCH: "home/status" (too shallow)
            let count = manager.publish("home/status", Bytes::from("err"), false, false, None);
            assert_eq!(count, Ok(0));
        }

        #[tokio::test]
        async fn test_wildcard_hash_multi_level() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "wild_hash".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            // Subscribe to "logs/#"
            manager.subscribe(&client_id, "logs/#").unwrap();

            // MATCH: "logs/error"
            manager
                .publish("logs/error", Bytes::from("e1"), false, false, None)
                .unwrap();
            assert_eq!(rx.recv().await.unwrap().topic, "logs/error");

            // MATCH: "logs/app/backend/error" (deep)
            manager
                .publish(
                    "logs/app/backend/error",
                    Bytes::from("e2"),
                    false,
                    false,
                    None,
                )
                .unwrap();
            assert_eq!(rx.recv().await.unwrap().topic, "logs/app/backend/error");
        }

        #[tokio::test]
        async fn test_retained_messages() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "config/settings";

            // 1. Publish Retained (No subscribers yet)
            manager
                .publish(topic, Bytes::from("dark_mode"), true, false, None)
                .unwrap();

            // 2. New Client Connects & Subscribes
            let client_id = "late_joiner".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            manager.subscribe(&client_id, topic).unwrap();

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
            let manager = Arc::new(PubSubManager::new(Arc::new(config)));

            let client_id = "leaver".to_string();
            let (tx, _rx) = mpsc::channel(8192);

            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, "chat/room1").unwrap();

            // Verify subscription exists (indirectly via publish count)
            let count = manager.publish("chat/room1", Bytes::from("hi"), false, false, None);
            assert_eq!(count, Ok(1));

            // Explicit disconnect (simulates socket close)
            manager.disconnect(&client_id);

            // Publish again -> Should be 0 subscribers
            let count = manager.publish("chat/room1", Bytes::from("anyone?"), false, false, None);
            assert_eq!(
                count,
                Ok(0),
                "Client should be unsubscribed after disconnect"
            );
        }

        #[tokio::test]
        async fn test_retained_with_custom_ttl() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "sensors/temp";

            // Publish retained with custom TTL (2 seconds)
            manager
                .publish(topic, Bytes::from("23.5"), true, false, Some(2))
                .unwrap();

            // Subscribe immediately - should receive retained
            let client_id = "sub1".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, topic).unwrap();

            let msg = rx.recv().await.expect("Should receive retained message");
            assert_eq!(msg.payload, Bytes::from("23.5"));

            // Wait for TTL to expire (2s + buffer)
            tokio::time::sleep(Duration::from_secs(3)).await;

            // New subscriber should NOT receive expired retained
            let client_id2 = "sub2".to_string();
            let (tx2, mut rx2) = mpsc::channel(8192);
            manager.connect(&client_id2, tx2);
            manager.subscribe(&client_id2, topic).unwrap();

            // Should timeout (no retained message)
            let result = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await;
            assert!(
                result.is_err(),
                "Should not receive expired retained message"
            );
        }

        #[tokio::test]
        async fn test_clear_retained_with_empty_payload() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "config/theme";

            // 1. Publish retained
            manager
                .publish(topic, Bytes::from("dark"), true, false, None)
                .unwrap();

            // 2. Verify retained exists
            let client_id = "sub1".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, topic).unwrap();

            let msg = rx.recv().await.expect("Should receive retained");
            assert_eq!(msg.payload, Bytes::from("dark"));

            // 3. Clear retained with empty payload (MQTT standard)
            manager
                .publish(topic, Bytes::from(""), true, true, None)
                .unwrap();

            // 4. New subscriber should NOT receive retained
            let client_id2 = "sub2".to_string();
            let (tx2, mut rx2) = mpsc::channel(8192);
            manager.connect(&client_id2, tx2);
            manager.subscribe(&client_id2, topic).unwrap();

            let result = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await;
            assert!(
                result.is_err(),
                "Should not receive cleared retained message"
            );
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
                let manager = Arc::new(PubSubManager::new(Arc::new(config)));

                manager
                    .publish(topic, payload.clone(), true, false, None)
                    .unwrap();

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
                let manager2 = Arc::new(PubSubManager::new(Arc::new(config)));

                // Subscribe - should receive retained from disk
                let client_id = "after_restart".to_string();
                let (tx, mut rx) = mpsc::channel(8192);
                manager2.connect(&client_id, tx);
                manager2.subscribe(&client_id, topic).unwrap();

                let msg = rx
                    .recv()
                    .await
                    .expect("Should receive retained after restart");
                assert_eq!(msg.payload, payload);
            }

            // temp_dir gets dropped here at end of test
        }

        #[tokio::test]
        async fn test_expired_retained_not_loaded_after_restart() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();

            let topic = "persistent/expired";
            let payload = Bytes::from("old_value");

            {
                let mut config = nexo::config::Config::global().pubsub.clone();
                config.persistence_path = path.clone();
                let manager = Arc::new(PubSubManager::new(Arc::new(config)));

                // Publish retained with 1 second TTL
                manager
                    .publish(topic, payload.clone(), true, false, Some(1))
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(200)).await;
                drop(manager);
            }

            // Wait for retained to expire
            tokio::time::sleep(Duration::from_secs(2)).await;

            {
                let mut config = nexo::config::Config::global().pubsub.clone();
                config.persistence_path = path.clone();
                let manager2 = Arc::new(PubSubManager::new(Arc::new(config)));

                let client_id = "after_restart".to_string();
                let (tx, mut rx) = mpsc::channel(8192);
                manager2.connect(&client_id, tx);
                manager2.subscribe(&client_id, topic).unwrap();

                let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
                assert!(
                    result.is_err(),
                    "Expired retained should not be loaded after restart"
                );
            }
        }

        #[tokio::test]
        async fn test_cleanup_expired_retained_background() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let topic = "temp/sensor";

            // Publish retained with 1 second TTL
            manager
                .publish(topic, Bytes::from("old_value"), true, false, Some(1))
                .unwrap();

            // Verify retained exists
            let client_id = "sub1".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, topic).unwrap();

            let msg = rx.recv().await.expect("Should receive retained");
            assert_eq!(msg.payload, Bytes::from("old_value"));

            // Wait for TTL expiration + background cleanup cycle (60s is too long for test)
            // Note: Background cleanup runs every 60s, but expired check happens on subscribe
            tokio::time::sleep(Duration::from_secs(2)).await;

            // New subscriber should NOT receive expired retained
            let client_id2 = "sub2".to_string();
            let (tx2, mut rx2) = mpsc::channel(8192);
            manager.connect(&client_id2, tx2);
            manager.subscribe(&client_id2, topic).unwrap();

            let result = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await;
            assert!(result.is_err(), "Should not receive expired retained");
        }
    }

    // =========================================================================================
    // 2. VALIDATION TESTS
    // =========================================================================================

    mod validation {
        use super::*;

        #[tokio::test]
        async fn test_subscribe_hash_in_middle_rejected() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "bad1".to_string();
            let (tx, _rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            assert!(manager.subscribe(&client_id, "sensors/#/temp").is_err());
            assert!(manager.subscribe(&client_id, "#/temp").is_err());
            assert!(manager.subscribe(&client_id, "a/#/b").is_err());
        }

        #[tokio::test]
        async fn test_subscribe_hash_at_end_ok() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "ok1".to_string();
            let (tx, _rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            assert!(manager.subscribe(&client_id, "sensors/#").is_ok());
            assert!(manager.subscribe(&client_id, "#").is_ok());
            assert!(manager.subscribe(&client_id, "a/b/#").is_ok());
        }

        #[tokio::test]
        async fn test_subscribe_empty_pattern_rejected() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "bad2".to_string();
            let (tx, _rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            assert!(manager.subscribe(&client_id, "").is_err());
        }

        #[tokio::test]
        async fn test_subscribe_empty_segment_rejected() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "bad3".to_string();
            let (tx, _rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            assert!(manager.subscribe(&client_id, "sensors//temp").is_err());
            assert!(manager.subscribe(&client_id, "/temp").is_err());
            assert!(manager.subscribe(&client_id, "temp/").is_err());
        }

        #[tokio::test]
        async fn test_subscribe_plus_anywhere_ok() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "ok2".to_string();
            let (tx, _rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            assert!(manager.subscribe(&client_id, "sensors/+/temp").is_ok());
            assert!(manager.subscribe(&client_id, "+").is_ok());
            assert!(manager.subscribe(&client_id, "a/+/b/+/c").is_ok());
            assert!(manager.subscribe(&client_id, "sensors/temp+").is_err());
            assert!(manager.subscribe(&client_id, "sensors/#suffix").is_err());
        }

        #[tokio::test]
        async fn test_publish_with_plus_rejected() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            assert!(manager
                .publish("sensors/+/temp", Bytes::from("x"), false, false, None)
                .is_err());
            assert!(manager
                .publish("+", Bytes::from("x"), false, false, None)
                .is_err());
            assert!(manager
                .publish("sensors/temp+", Bytes::from("x"), false, false, None)
                .is_err());
            assert!(manager
                .publish("a/+/b", Bytes::from("x"), false, false, None)
                .is_err());
        }

        #[tokio::test]
        async fn test_publish_with_hash_rejected() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            assert!(manager
                .publish("sensors/#", Bytes::from("x"), false, false, None)
                .is_err());
            assert!(manager
                .publish("#", Bytes::from("x"), false, false, None)
                .is_err());
            assert!(manager
                .publish("a/b/#", Bytes::from("x"), false, false, None)
                .is_err());
        }

        #[tokio::test]
        async fn test_publish_empty_topic_rejected() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            assert!(manager
                .publish("", Bytes::from("x"), false, false, None)
                .is_err());
        }

        #[tokio::test]
        async fn test_publish_empty_segment_rejected() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            assert!(manager
                .publish("sensors//temp", Bytes::from("x"), false, false, None)
                .is_err());
            assert!(manager
                .publish("/temp", Bytes::from("x"), false, false, None)
                .is_err());
            assert!(manager
                .publish("temp/", Bytes::from("x"), false, false, None)
                .is_err());
        }

        #[tokio::test]
        async fn test_publish_concrete_topic_ok() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            assert!(manager
                .publish("sensors/temp", Bytes::from("x"), false, false, None)
                .is_ok());
            assert!(manager
                .publish("a", Bytes::from("x"), false, false, None)
                .is_ok());
            assert!(manager
                .publish("a/b/c/d", Bytes::from("x"), false, false, None)
                .is_ok());
        }

        #[tokio::test]
        async fn test_subscribe_invalid_pattern_no_delivery() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "bad_sub".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            assert!(manager.subscribe(&client_id, "sensors/#/temp").is_err());
            let count =
                manager.publish("sensors/real/temp", Bytes::from("data"), false, false, None);
            assert_eq!(
                count,
                Ok(0),
                "Invalid subscription should not receive messages"
            );

            let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
            assert!(result.is_err(), "Should not receive any message");
        }

        #[tokio::test]
        async fn test_publish_with_wildcard_no_delivery() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "sub1".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, "sensors/+/temp").unwrap();

            assert!(manager
                .publish("sensors/+/temp", Bytes::from("data"), false, false, None)
                .is_err());

            let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
            assert!(result.is_err(), "Should not receive any message");
        }
    }

    // =========================================================================================
    // 3. ROBUSTNESS TESTS (Concurrency, Edge Cases, Stress)
    // =========================================================================================

    mod robustness {
        use super::*;

        #[tokio::test]
        async fn test_concurrent_publish_disconnect() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "concurrent".to_string();
            let (tx, mut rx) = mpsc::channel(8192);

            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, "test/topic").unwrap();

            // Spawn consumer to drain channel
            tokio::spawn(async move { while let Some(_) = rx.recv().await {} });

            // Concurrent operations: publish while disconnecting
            let manager_clone = manager.clone();
            let client_clone = client_id.clone();
            let disconnect_task = tokio::spawn(async move {
                manager_clone.disconnect(&client_clone);
            });

            // Publish 100 messages while disconnect is happening
            for _ in 0..100 {
                let _ = manager.publish("test/topic", Bytes::from("data"), false, false, None);
            }

            // Should complete without deadlock
            disconnect_task.await.unwrap();
        }

        #[tokio::test]
        async fn test_global_hash_subscriber() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "global".to_string();
            let (tx, mut rx) = mpsc::channel(8192);

            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, "#").unwrap();

            // Should receive ALL messages from any topic
            manager
                .publish("sensors/temp", Bytes::from("1"), false, false, None)
                .unwrap();
            manager
                .publish("logs/error", Bytes::from("2"), false, false, None)
                .unwrap();
            manager
                .publish("any/random/topic", Bytes::from("3"), false, false, None)
                .unwrap();

            let msg1 = rx.recv().await.expect("Should receive message 1");
            let msg2 = rx.recv().await.expect("Should receive message 2");
            let msg3 = rx.recv().await.expect("Should receive message 3");

            // Verify all topics received
            let topics: Vec<String> =
                vec![msg1.topic.clone(), msg2.topic.clone(), msg3.topic.clone()];
            assert!(topics.contains(&"sensors/temp".to_string()));
            assert!(topics.contains(&"logs/error".to_string()));
            assert!(topics.contains(&"any/random/topic".to_string()));
        }

        #[tokio::test]
        async fn test_retained_with_wildcard_subscribe() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            // Publish retained on specific topics
            manager
                .publish("sensors/temp", Bytes::from("20"), true, false, None)
                .unwrap();
            manager
                .publish("sensors/humidity", Bytes::from("60"), true, false, None)
                .unwrap();
            manager
                .publish("sensors/pressure", Bytes::from("1013"), true, false, None)
                .unwrap();

            // Subscribe with wildcard AFTER retained messages exist
            let client_id = "wildcard_late".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, "sensors/+").unwrap();

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
                let client_id = format!("client_{}", i);
                let (tx, rx) = mpsc::channel(8192);
                manager.connect(&client_id, tx);
                manager.subscribe(&client_id, topic).unwrap();
                receivers.push(rx);
            }

            // Publish one message
            let count = manager.publish(topic, Bytes::from("breaking_news"), false, false, None);
            assert_eq!(count, Ok(3), "Should deliver to all 3 subscribers");

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
            let client_id = "multi_sub".to_string();
            let (tx, mut rx) = mpsc::channel(8192);

            manager.connect(&client_id, tx);

            // Subscribe to same topic twice (should deduplicate)
            manager.subscribe(&client_id, "sensors/temp").unwrap();
            manager.subscribe(&client_id, "sensors/temp").unwrap();

            // Publish
            manager
                .publish("sensors/temp", Bytes::from("data"), false, false, None)
                .unwrap();

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
            let client_id = "never_subbed".to_string();
            let (tx, _rx) = mpsc::channel(8192);

            manager.connect(&client_id, tx);

            // Unsubscribe from topic never subscribed to (should not panic)
            manager.unsubscribe(&client_id, "sensors/temp");

            // Publish should work normally
            let count = manager.publish("sensors/temp", Bytes::from("data"), false, false, None);
            assert_eq!(count, Ok(0), "Should have no subscribers");
        }

        #[tokio::test]
        async fn test_disconnect_during_subscribe() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let client_id = "race".to_string();
            let (tx, _rx) = mpsc::channel(8192);

            manager.connect(&client_id, tx);

            // Subscribe and disconnect in parallel (race condition test)
            let manager_clone = manager.clone();
            let client_clone = client_id.clone();
            let subscribe_task = tokio::spawn(async move {
                for _ in 0..10 {
                    let _ = manager_clone.subscribe(&client_clone, "test/topic");
                }
            });

            let manager_clone2 = manager.clone();
            let client_clone2 = client_id.clone();
            let disconnect_task = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                manager_clone2.disconnect(&client_clone2);
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
            manager
                .publish(topic, Bytes::from("v1"), true, false, None)
                .unwrap();
            manager
                .publish(topic, Bytes::from("v2"), true, false, None)
                .unwrap();
            manager
                .publish(topic, Bytes::from("v3"), true, false, None)
                .unwrap();

            // New subscriber should receive only latest (v3)
            let client_id = "late".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);
            manager.subscribe(&client_id, topic).unwrap();

            let msg = rx.recv().await.expect("Should receive retained");
            assert_eq!(
                msg.payload,
                Bytes::from("v3"),
                "Should receive only latest retained"
            );

            // Should not receive v1 or v2
            let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
            assert!(result.is_err(), "Should not receive old retained messages");
        }

        #[tokio::test]
        async fn test_multiple_wildcards_same_message() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            // Client subscribes to multiple overlapping patterns
            let client_id = "multi_pattern".to_string();
            let (tx, mut rx) = mpsc::channel(8192);
            manager.connect(&client_id, tx);

            manager.subscribe(&client_id, "sensors/+/temp").unwrap();
            manager.subscribe(&client_id, "sensors/kitchen/+").unwrap();

            // Publish to topic that matches BOTH patterns
            manager
                .publish(
                    "sensors/kitchen/temp",
                    Bytes::from("data"),
                    false,
                    false,
                    None,
                )
                .unwrap();

            // Should receive message only once (deduplicated by client_id)
            let msg1 = rx.recv().await.expect("Should receive message");
            assert_eq!(msg1.topic, "sensors/kitchen/temp");

            // Should not receive duplicate
            let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
            assert!(
                result.is_err(),
                "Should not receive duplicate from overlapping patterns"
            );
        }
    }

    mod concurrency {
        use super::*;

        #[tokio::test]
        async fn concurrent_subscribe_and_publish_retained() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let manager = Arc::new(manager);

            const TOPICS: usize = 10;
            const CLIENTS: usize = 50;

            for t in 0..TOPICS {
                let topic = format!("stress/retained/{}", t);
                manager
                    .publish(&topic, Bytes::from(format!("v-{}", t)), true, false, None)
                    .unwrap();
            }

            let mut handles = Vec::new();

            for t in 0..TOPICS {
                let m = manager.clone();
                let topic = format!("stress/retained/{}", t);
                handles.push(tokio::spawn(async move {
                    for _ in 0..10 {
                        m.publish(&topic, Bytes::from("burst"), true, false, None)
                            .unwrap();
                        tokio::task::yield_now().await;
                    }
                }));
            }

            for c in 0..CLIENTS {
                let m = manager.clone();
                let client_id = format!("concurrent-client-{}", c);
                let topic_idx = c % TOPICS;
                let topic = format!("stress/retained/{}", topic_idx);
                handles.push(tokio::spawn(async move {
                    let (tx, mut rx) = mpsc::channel(8192);
                    m.connect(&client_id, tx);
                    m.subscribe(&client_id, &topic).unwrap();
                    let _ = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
                }));
            }

            for h in handles {
                h.await.unwrap();
            }
        }

        #[tokio::test]
        async fn concurrent_disconnect_and_publish() {
            let (manager, _tmp) = setup_pubsub_manager().await;
            let manager = Arc::new(manager);

            let topic = "disconnect/stress";
            for i in 0..100 {
                let client_id = format!("client-{}", i);
                let (tx, _rx) = mpsc::channel(8192);
                manager.connect(&client_id, tx);
                manager.subscribe(&client_id, topic).unwrap();
            }

            let m1 = manager.clone();
            let disconnect_handle = tokio::spawn(async move {
                for i in 0..100 {
                    m1.disconnect(&format!("client-{}", i));
                }
            });

            let m2 = manager.clone();
            let publish_handle = tokio::spawn(async move {
                for i in 0..100 {
                    m2.publish(topic, Bytes::from(format!("msg-{}", i)), false, false, None)
                        .unwrap();
                }
            });

            let (d, p) = tokio::join!(disconnect_handle, publish_handle);
            d.unwrap();
            p.unwrap();
        }
    }

    mod slow_consumer {
        use super::*;

        #[tokio::test]
        async fn slow_subscriber_gets_disconnected() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            let (tx, _rx) = mpsc::channel(2);
            manager.connect("slow", tx);
            manager.subscribe("slow", "topic").unwrap();

            for i in 0..4 {
                manager
                    .publish("topic", Bytes::from(format!("{}", i)), false, false, None)
                    .unwrap();
            }

            assert!(
                !manager.exists("slow"),
                "Slow subscriber should be disconnected"
            );
        }

        #[tokio::test]
        async fn slow_subscriber_subscription_removed_from_tree() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            let (tx, _rx) = mpsc::channel(2);
            manager.connect("slow", tx);
            manager.subscribe("slow", "topic").unwrap();

            for i in 0..4 {
                manager
                    .publish("topic", Bytes::from(format!("{}", i)), false, false, None)
                    .unwrap();
            }

            assert!(!manager.exists("slow"));

            let count = manager.publish("topic", Bytes::from("after"), false, false, None);
            assert_eq!(
                count,
                Ok(0),
                "Disconnected subscriber should not match in tree"
            );
        }

        #[tokio::test]
        async fn slow_subscriber_does_not_block_others() {
            let (manager, _tmp) = setup_pubsub_manager().await;

            let (slow_tx, _slow_rx) = mpsc::channel(2);
            manager.connect("slow", slow_tx);
            manager.subscribe("slow", "topic").unwrap();

            let (fast_tx, mut fast_rx) = mpsc::channel(8192);
            manager.connect("fast", fast_tx);
            manager.subscribe("fast", "topic").unwrap();

            for i in 0..4 {
                manager
                    .publish("topic", Bytes::from(format!("{}", i)), false, false, None)
                    .unwrap();
            }

            assert!(!manager.exists("slow"), "Slow subscriber disconnected");
            assert!(manager.exists("fast"), "Fast subscriber still connected");

            let mut received = 0;
            while let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(100), fast_rx.recv()).await
            {
                assert_eq!(msg.topic, "topic");
                received += 1;
            }
            assert!(
                received > 0,
                "Fast subscriber should have received messages"
            );
        }
    }

    mod shutdown {
        use super::*;

        #[tokio::test]
        async fn test_shutdown_flushes_retained_messages() {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();

            let topic = "shutdown/retained";
            let payload = Bytes::from("survivor");

            {
                let mut config = nexo::config::Config::global().pubsub.clone();
                config.persistence_path = path.clone();
                let manager = Arc::new(PubSubManager::new(Arc::new(config)));

                manager
                    .publish(topic, payload.clone(), true, false, None)
                    .unwrap();

                // Shutdown immediately — no sleep, no waiting for flush timer
                manager.shutdown();
            }

            // Recover with a new manager
            {
                let mut config = nexo::config::Config::global().pubsub.clone();
                config.persistence_path = path.clone();
                let manager2 = Arc::new(PubSubManager::new(Arc::new(config)));

                let client_id = "after_shutdown".to_string();
                let (tx, mut rx) = mpsc::channel(8192);
                manager2.connect(&client_id, tx);
                manager2.subscribe(&client_id, topic).unwrap();

                let msg = rx
                    .recv()
                    .await
                    .expect("Should receive retained after shutdown flush");
                assert_eq!(msg.payload, payload);
            }
        }
    }
}

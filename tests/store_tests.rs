mod common;
use common::setup_store_manager;
use bytes::Bytes;
use std::time::Duration;
use uuid::Uuid;


#[cfg(test)]
mod store_tests {
    use super::*;

    // =========================================================================================
    // 1. FEATURE TESTS (Happy Path + Logic)
    // =========================================================================================

    mod features {
        use super::*;

        #[tokio::test]
        async fn test_basic_crud() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("key_{}", Uuid::new_v4());
            let val = Bytes::from("value");

            // PUT (no TTL = persistent)
            manager.map.set(key.clone(), val.clone(), None).unwrap();

            // GET
            let retrieved = manager.map.get(&key).expect("Key should exist");
            assert_eq!(retrieved, val);

            // DEL
            let deleted = manager.map.del(&key);
            assert!(deleted, "Should return true for deleted key");

            // GET -> None
            let after_del = manager.map.get(&key);
            assert!(after_del.is_none(), "Key should be gone");
        }

        #[tokio::test]
        async fn test_overwrite_value() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("key_ovr_{}", Uuid::new_v4());

            manager.map.set(key.clone(), Bytes::from("v1"), None).unwrap();
            manager.map.set(key.clone(), Bytes::from("v2"), None).unwrap();

            let val = manager.map.get(&key).unwrap();
            assert_eq!(val, Bytes::from("v2"));
        }

        #[tokio::test]
        async fn test_no_ttl_is_persistent() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("key_persist_{}", Uuid::new_v4());

            manager.map.set(key.clone(), Bytes::from("forever"), None).unwrap();

            // Should still exist after a short wait
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(manager.map.get(&key).is_some(), "Key without TTL should persist");
        }

        #[tokio::test]
        async fn test_ttl_expiration() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("key_ttl_{}", Uuid::new_v4());

            let ttl_sec = 1;
            manager.map.set(key.clone(), Bytes::from("temp"), Some(ttl_sec)).unwrap();

            let retrieved = manager.map.get(&key);
            assert!(retrieved.is_some());

            // Wait > TTL
            tokio::time::sleep(Duration::from_millis((ttl_sec * 1000) + 100)).await;

            let after_ttl = manager.map.get(&key);
            assert!(after_ttl.is_none(), "Key should have expired");
        }

        #[tokio::test]
        async fn test_ttl_zero_is_error() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("key_zero_{}", Uuid::new_v4());

            let result = manager.map.set(key.clone(), Bytes::from("val"), Some(0));
            assert!(result.is_err(), "ttl=0 should return an error");
            assert!(manager.map.get(&key).is_none(), "Key should not exist after failed set");
        }
    }

    // =========================================================================================
    // 2. INCR TESTS
    // =========================================================================================

    mod incr {
        use super::*;

        fn encode_int(val: i64) -> Bytes {
            let mut buf = vec![0x03u8];
            buf.extend_from_slice(&val.to_be_bytes());
            Bytes::from(buf)
        }

        #[tokio::test]
        async fn test_incr_new_key_starts_from_zero() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("incr_new_{}", Uuid::new_v4());

            let result = manager.map.incr(&key, 1).unwrap();
            assert_eq!(result, encode_int(1));
        }

        #[tokio::test]
        async fn test_incr_existing_integer() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("incr_existing_{}", Uuid::new_v4());

            manager.map.set(key.clone(), encode_int(10), None).unwrap();
            let result = manager.map.incr(&key, 5).unwrap();
            assert_eq!(result, encode_int(15));

            // Verify the value was actually written
            let stored = manager.map.get(&key).unwrap();
            assert_eq!(stored, encode_int(15));
        }

        #[tokio::test]
        async fn test_incr_negative_delta() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("incr_neg_{}", Uuid::new_v4());

            manager.map.set(key.clone(), encode_int(10), None).unwrap();
            let result = manager.map.incr(&key, -3).unwrap();
            assert_eq!(result, encode_int(7));
        }

        #[tokio::test]
        async fn test_incr_non_integer_value_errors() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("incr_str_{}", Uuid::new_v4());

            manager.map.set(key.clone(), Bytes::from("hello"), None).unwrap();
            let result = manager.map.incr(&key, 1);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), "value is not an integer or out of range");
        }

        #[tokio::test]
        async fn test_incr_overflow_errors() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("incr_overflow_{}", Uuid::new_v4());

            manager.map.set(key.clone(), encode_int(i64::MAX), None).unwrap();
            let result = manager.map.incr(&key, 1);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), "increment would overflow");
        }

        #[tokio::test]
        async fn test_incr_preserves_ttl() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("incr_ttl_{}", Uuid::new_v4());

            manager.map.set(key.clone(), encode_int(5), Some(60)).unwrap();
            manager.map.incr(&key, 1).unwrap();

            // Verify value updated
            let stored = manager.map.get(&key).unwrap();
            assert_eq!(stored, encode_int(6));

            // Verify TTL preserved: wait 200ms, should still exist (TTL=60)
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(manager.map.get(&key).is_some(), "Key should still exist with TTL=60");
        }

        #[tokio::test]
        async fn test_incr_on_expired_key_starts_from_zero() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("incr_expired_{}", Uuid::new_v4());

            manager.map.set(key.clone(), encode_int(99), Some(1)).unwrap();

            // Wait for expiry
            tokio::time::sleep(Duration::from_millis(1100)).await;

            // INCR on expired key should start from 0
            let result = manager.map.incr(&key, 1).unwrap();
            assert_eq!(result, encode_int(1));
        }

        #[tokio::test]
        async fn test_incr_negative_on_new_key() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("incr_neg_new_{}", Uuid::new_v4());

            let result = manager.map.incr(&key, -5).unwrap();
            assert_eq!(result, encode_int(-5));
        }
    }




}
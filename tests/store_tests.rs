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

            // PUT
            manager.map.set(key.clone(), val.clone(), None);

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

            manager.map.set(key.clone(), Bytes::from("v1"), None);
            manager.map.set(key.clone(), Bytes::from("v2"), None);

            let val = manager.map.get(&key).unwrap();
            assert_eq!(val, Bytes::from("v2"));
        }

        #[tokio::test]
        async fn test_ttl_expiration() {
            let (manager, _tmp) = setup_store_manager().await;
            let key = format!("key_ttl_{}", Uuid::new_v4());

            let ttl_sec = 1;
            manager.map.set(key.clone(), Bytes::from("temp"), Some(ttl_sec));

            let retrieved = manager.map.get(&key);
            assert!(retrieved.is_some());

            // Wait > TTL
            tokio::time::sleep(Duration::from_millis((ttl_sec * 1000) + 100)).await;

            let after_ttl = manager.map.get(&key);
            // Should expire (either by lazy check or background, new implementation has lazy check!)
            assert!(after_ttl.is_none(), "Key should have expired");
        }
    }




}
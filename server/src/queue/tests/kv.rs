//! Key-Value storage tests

use super::*;

// ============== Basic Operations ==============

#[tokio::test]
async fn test_kv_set_get() {
    let qm = setup();

    // Set a value
    let result = qm.kv_set("key1".to_string(), json!({"foo": "bar"}), None);
    assert!(result.is_ok());

    // Get the value
    let value = qm.kv_get("key1");
    assert!(value.is_some());
    assert_eq!(value.unwrap(), json!({"foo": "bar"}));
}

#[tokio::test]
async fn test_kv_get_nonexistent() {
    let qm = setup();
    let value = qm.kv_get("nonexistent");
    assert!(value.is_none());
}

#[tokio::test]
async fn test_kv_del() {
    let qm = setup();

    qm.kv_set("key1".to_string(), json!("value"), None).unwrap();

    // Delete existing key
    let deleted = qm.kv_del("key1");
    assert!(deleted);

    // Verify it's gone
    let value = qm.kv_get("key1");
    assert!(value.is_none());

    // Delete non-existent key
    let deleted = qm.kv_del("nonexistent");
    assert!(!deleted);
}

#[tokio::test]
async fn test_kv_exists() {
    let qm = setup();

    // Non-existent key
    assert!(!qm.kv_exists("key1"));

    // Set and check
    qm.kv_set("key1".to_string(), json!("value"), None).unwrap();
    assert!(qm.kv_exists("key1"));

    // Delete and check
    qm.kv_del("key1");
    assert!(!qm.kv_exists("key1"));
}

// ============== TTL Operations ==============

#[tokio::test]
async fn test_kv_set_with_ttl() {
    let qm = setup();

    // Set with 100ms TTL
    qm.kv_set("key1".to_string(), json!("value"), Some(100)).unwrap();

    // Should exist immediately
    assert!(qm.kv_exists("key1"));

    // Wait for expiration
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Should be expired now
    assert!(!qm.kv_exists("key1"));
    assert!(qm.kv_get("key1").is_none());
}

#[tokio::test]
async fn test_kv_expire() {
    let qm = setup();

    // Set without TTL
    qm.kv_set("key1".to_string(), json!("value"), None).unwrap();

    // TTL should be -1 (no expiration)
    assert_eq!(qm.kv_ttl("key1"), -1);

    // Set TTL
    let success = qm.kv_expire("key1", 100);
    assert!(success);

    // TTL should now be positive
    let ttl = qm.kv_ttl("key1");
    assert!(ttl > 0 && ttl <= 100);

    // Expire non-existent key
    let success = qm.kv_expire("nonexistent", 100);
    assert!(!success);
}

#[tokio::test]
async fn test_kv_ttl() {
    let qm = setup();

    // Non-existent key returns -2
    assert_eq!(qm.kv_ttl("nonexistent"), -2);

    // Key without TTL returns -1
    qm.kv_set("key1".to_string(), json!("value"), None).unwrap();
    assert_eq!(qm.kv_ttl("key1"), -1);

    // Key with TTL returns remaining time
    qm.kv_set("key2".to_string(), json!("value"), Some(5000)).unwrap();
    let ttl = qm.kv_ttl("key2");
    assert!(ttl > 0 && ttl <= 5000);
}

// ============== Batch Operations ==============

#[tokio::test]
async fn test_kv_mget() {
    let qm = setup();

    qm.kv_set("key1".to_string(), json!("value1"), None).unwrap();
    qm.kv_set("key2".to_string(), json!("value2"), None).unwrap();

    let values = qm.kv_mget(&["key1".to_string(), "nonexistent".to_string(), "key2".to_string()]);

    assert_eq!(values.len(), 3);
    assert_eq!(values[0], Some(json!("value1")));
    assert_eq!(values[1], None);
    assert_eq!(values[2], Some(json!("value2")));
}

#[tokio::test]
async fn test_kv_mset() {
    let qm = setup();

    let entries = vec![
        ("key1".to_string(), json!("value1"), None),
        ("key2".to_string(), json!("value2"), Some(5000)),
        ("key3".to_string(), json!(123), None),
    ];

    let count = qm.kv_mset(entries).unwrap();
    assert_eq!(count, 3);

    assert_eq!(qm.kv_get("key1"), Some(json!("value1")));
    assert_eq!(qm.kv_get("key2"), Some(json!("value2")));
    assert_eq!(qm.kv_get("key3"), Some(json!(123)));
}

// ============== Keys Pattern Matching ==============

#[tokio::test]
async fn test_kv_keys_all() {
    let qm = setup();

    qm.kv_set("user:1".to_string(), json!("a"), None).unwrap();
    qm.kv_set("user:2".to_string(), json!("b"), None).unwrap();
    qm.kv_set("session:1".to_string(), json!("c"), None).unwrap();

    let keys = qm.kv_keys(None);
    assert_eq!(keys.len(), 3);
}

#[tokio::test]
async fn test_kv_keys_pattern_star() {
    let qm = setup();

    qm.kv_set("user:1".to_string(), json!("a"), None).unwrap();
    qm.kv_set("user:2".to_string(), json!("b"), None).unwrap();
    qm.kv_set("session:1".to_string(), json!("c"), None).unwrap();

    let keys = qm.kv_keys(Some("user:*"));
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"user:1".to_string()));
    assert!(keys.contains(&"user:2".to_string()));
}

#[tokio::test]
async fn test_kv_keys_pattern_question() {
    let qm = setup();

    qm.kv_set("key1".to_string(), json!("a"), None).unwrap();
    qm.kv_set("key2".to_string(), json!("b"), None).unwrap();
    qm.kv_set("key10".to_string(), json!("c"), None).unwrap();

    let keys = qm.kv_keys(Some("key?"));
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"key1".to_string()));
    assert!(keys.contains(&"key2".to_string()));
    assert!(!keys.contains(&"key10".to_string()));
}

// ============== Increment ==============

#[tokio::test]
async fn test_kv_incr_new_key() {
    let qm = setup();

    // Increment non-existent key creates it
    let value = qm.kv_incr("counter", 1).unwrap();
    assert_eq!(value, 1);

    let value = qm.kv_incr("counter", 1).unwrap();
    assert_eq!(value, 2);
}

#[tokio::test]
async fn test_kv_incr_existing() {
    let qm = setup();

    qm.kv_set("counter".to_string(), json!(10), None).unwrap();

    let value = qm.kv_incr("counter", 5).unwrap();
    assert_eq!(value, 15);

    let value = qm.kv_incr("counter", -3).unwrap();
    assert_eq!(value, 12);
}

#[tokio::test]
async fn test_kv_incr_non_numeric() {
    let qm = setup();

    qm.kv_set("key".to_string(), json!("not a number"), None).unwrap();

    let result = qm.kv_incr("key", 1);
    assert!(result.is_err());
}

// ============== Validation ==============

#[tokio::test]
async fn test_kv_empty_key() {
    let qm = setup();

    let result = qm.kv_set("".to_string(), json!("value"), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("empty"));
}

#[tokio::test]
async fn test_kv_key_too_long() {
    let qm = setup();

    let long_key = "x".repeat(2000);
    let result = qm.kv_set(long_key, json!("value"), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("too long"));
}

#[tokio::test]
async fn test_kv_value_too_large() {
    let qm = setup();

    // Create a very large JSON value
    let large_value: Vec<u8> = vec![0; 2_000_000];
    let result = qm.kv_set("key".to_string(), json!(large_value), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("too large"));
}

// ============== Cleanup ==============

#[tokio::test]
async fn test_kv_cleanup_expired() {
    let qm = setup();

    // Set some keys with TTL
    qm.kv_set("key1".to_string(), json!("a"), Some(50)).unwrap();
    qm.kv_set("key2".to_string(), json!("b"), Some(50)).unwrap();
    qm.kv_set("key3".to_string(), json!("c"), None).unwrap(); // No TTL

    // Wait for expiration
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Run cleanup
    qm.cleanup_expired_kv();

    // Expired keys should be removed
    let keys = qm.kv_keys(None);
    assert_eq!(keys.len(), 1);
    assert!(keys.contains(&"key3".to_string()));
}

// ============== Different Value Types ==============

#[tokio::test]
async fn test_kv_different_value_types() {
    let qm = setup();

    // String
    qm.kv_set("str".to_string(), json!("hello"), None).unwrap();
    assert_eq!(qm.kv_get("str"), Some(json!("hello")));

    // Number
    qm.kv_set("num".to_string(), json!(42), None).unwrap();
    assert_eq!(qm.kv_get("num"), Some(json!(42)));

    // Float
    qm.kv_set("float".to_string(), json!(3.14), None).unwrap();
    assert_eq!(qm.kv_get("float"), Some(json!(3.14)));

    // Boolean
    qm.kv_set("bool".to_string(), json!(true), None).unwrap();
    assert_eq!(qm.kv_get("bool"), Some(json!(true)));

    // Null
    qm.kv_set("null".to_string(), json!(null), None).unwrap();
    assert_eq!(qm.kv_get("null"), Some(json!(null)));

    // Array
    qm.kv_set("arr".to_string(), json!([1, 2, 3]), None).unwrap();
    assert_eq!(qm.kv_get("arr"), Some(json!([1, 2, 3])));

    // Object
    qm.kv_set("obj".to_string(), json!({"a": 1, "b": 2}), None).unwrap();
    assert_eq!(qm.kv_get("obj"), Some(json!({"a": 1, "b": 2})));
}

// ============== Overwrite ==============

#[tokio::test]
async fn test_kv_overwrite() {
    let qm = setup();

    qm.kv_set("key".to_string(), json!("old"), None).unwrap();
    assert_eq!(qm.kv_get("key"), Some(json!("old")));

    qm.kv_set("key".to_string(), json!("new"), None).unwrap();
    assert_eq!(qm.kv_get("key"), Some(json!("new")));
}

#[tokio::test]
async fn test_kv_overwrite_changes_ttl() {
    let qm = setup();

    // Set with TTL
    qm.kv_set("key".to_string(), json!("v1"), Some(5000)).unwrap();
    assert!(qm.kv_ttl("key") > 0);

    // Overwrite without TTL
    qm.kv_set("key".to_string(), json!("v2"), None).unwrap();
    assert_eq!(qm.kv_ttl("key"), -1);
}

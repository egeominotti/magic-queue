//! Edge cases and validation tests.

use super::*;

#[tokio::test]
async fn test_empty_queue_name() {
    let qm = setup();

    // Empty queue names should be rejected
    let result = qm
        .push(
            "".to_string(),
            json!({}),
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("cannot be empty"));
}

#[tokio::test]
async fn test_large_payload() {
    let qm = setup();

    // Create a large payload (but within limits)
    let large_data: Vec<i32> = (0..10000).collect();
    let job = qm
        .push(
            "test".to_string(),
            json!({"data": large_data}),
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let pulled = qm.pull("test").await;
    assert_eq!(pulled.id, job.id);
}

#[tokio::test]
async fn test_special_characters_in_queue_name() {
    let qm = setup();

    // Valid queue names: alphanumeric, dash, underscore, dot
    let valid_queues = vec![
        "queue-with-dash",
        "queue_with_underscore",
        "queue.with.dots",
        "Queue123",
    ];

    for name in valid_queues {
        let job = qm
            .push(
                name.to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
                false,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(job.queue, name);
        let pulled = qm.pull(name).await;
        qm.ack(pulled.id, None).await.unwrap();
    }

    // Invalid queue names: colons, slashes, spaces, etc.
    let invalid_queues = vec![
        "queue:with:colons",
        "queue/with/slashes",
        "queue with spaces",
        "queue@special",
    ];

    for name in invalid_queues {
        let result = qm
            .push(
                name.to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
                false,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await;
        assert!(result.is_err(), "Queue name '{}' should be rejected", name);
    }
}

#[tokio::test]
async fn test_unicode_queue_name() {
    let qm = setup();

    let job = qm
        .push(
            "队列名称".to_string(),
            json!({"emoji": "🚀"}),
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(job.queue, "队列名称");

    let pulled = qm.pull("队列名称").await;
    assert_eq!(pulled.id, job.id);
}

#[tokio::test]
async fn test_null_json_data() {
    let qm = setup();

    qm.push(
        "test".to_string(),
        json!(null),
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let pulled = qm.pull("test").await;
    assert_eq!(pulled.data, json!(null));
}

#[tokio::test]
async fn test_nested_json_data() {
    let qm = setup();

    let nested = json!({
        "level1": {
            "level2": {
                "level3": {
                    "value": 42
                }
            }
        },
        "array": [1, 2, {"nested": true}]
    });

    qm.push(
        "test".to_string(),
        nested.clone(),
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let pulled = qm.pull("test").await;
    assert_eq!(pulled.data, nested);
}

// ==================== TTL ====================

#[tokio::test]
async fn test_job_ttl_expired_not_processed() {
    let qm = setup();

    // Push job with very short TTL
    let job = qm
        .push(
            "test".to_string(),
            json!({"ttl_test": true}),
            0,
            None,
            Some(1), // 1ms TTL
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Wait for TTL to expire
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // The job's is_expired method should return true
    let now = crate::queue::types::now_ms();
    let (fetched, _) = qm.get_job(job.id);
    if let Some(j) = fetched {
        assert!(j.is_expired(now));
    }
}

#[tokio::test]
async fn test_job_ttl_not_expired() {
    let qm = setup();

    // Push job with long TTL
    let job = qm
        .push(
            "test".to_string(),
            json!({}),
            0,
            None,
            Some(60000), // 60 seconds TTL
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Should not be expired
    let now = crate::queue::types::now_ms();
    let (fetched, _) = qm.get_job(job.id);
    assert!(!fetched.unwrap().is_expired(now));
}

// ==================== PAYLOAD SIZE LIMIT ====================

#[tokio::test]
async fn test_payload_too_large_rejected() {
    let qm = setup();

    // Create a very large payload (over 10MB limit)
    let large_string = "x".repeat(11_000_000); // ~11MB
    let result = qm
        .push(
            "test".to_string(),
            json!({"data": large_string}),
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("too large"));
}

// ==================== GET JOB UNKNOWN ====================

#[tokio::test]
async fn test_get_job_unknown_id() {
    let qm = setup();

    let (job, state) = qm.get_job(999999);
    assert!(job.is_none());
    assert_eq!(state, crate::protocol::JobState::Unknown);
}

#[tokio::test]
async fn test_get_state_unknown_id() {
    let qm = setup();

    let state = qm.get_state(999999);
    assert_eq!(state, crate::protocol::JobState::Unknown);
}

// ==================== DOUBLE ACK/FAIL ====================

#[tokio::test]
async fn test_double_ack_fails() {
    let qm = setup();

    let job = qm
        .push(
            "test".to_string(),
            json!({}),
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let pulled = qm.pull("test").await;

    // First ack succeeds
    let result1 = qm.ack(pulled.id, None).await;
    assert!(result1.is_ok());

    // Second ack fails
    let result2 = qm.ack(job.id, None).await;
    assert!(result2.is_err());
}

#[tokio::test]
async fn test_double_fail_fails() {
    let qm = setup();

    let job = qm
        .push(
            "test".to_string(),
            json!({}),
            0,
            None,
            None,
            None,
            Some(3), // Allow retries
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let pulled = qm.pull("test").await;

    // First fail succeeds (job goes back to queue)
    let result1 = qm.fail(pulled.id, None).await;
    assert!(result1.is_ok());

    // Second fail without pulling again fails
    let result2 = qm.fail(job.id, None).await;
    assert!(result2.is_err());
}

// ==================== CUSTOM JOB ID ====================

#[tokio::test]
async fn test_custom_job_id() {
    let qm = setup();

    let job = qm
        .push(
            "test".to_string(),
            json!({"custom": true}),
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            Some("my-custom-id-123".to_string()),
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(job.custom_id, Some("my-custom-id-123".to_string()));

    // Lookup by custom ID
    let found = qm.get_job_by_custom_id("my-custom-id-123");
    assert!(found.is_some());
    assert_eq!(found.unwrap().0.id, job.id);
}

#[tokio::test]
async fn test_custom_job_id_not_found() {
    let qm = setup();

    let found = qm.get_job_by_custom_id("nonexistent-id");
    assert!(found.is_none());
}

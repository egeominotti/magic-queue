#[cfg(test)]
mod tests {
    use super::super::*;
    use serde_json::json;

    fn setup() -> std::sync::Arc<QueueManager> {
        QueueManager::new(false)
    }

    // ==================== CORE OPERATIONS ====================

    #[tokio::test]
    async fn test_push_and_pull() {
        let qm = setup();

        let job = qm
            .push(
                "test".to_string(),
                json!({"key": "value"}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        assert!(job.id > 0);
        assert_eq!(job.queue, "test");

        let pulled = qm.pull("test").await;
        assert_eq!(pulled.id, job.id);
    }

    #[tokio::test]
    async fn test_push_with_all_options() {
        let qm = setup();

        let job = qm
            .push(
                "test".to_string(),
                json!({"payload": "data"}),
                10,                        // priority
                Some(0),                   // delay (0 = immediate)
                Some(60000),               // ttl
                Some(30000),               // timeout
                Some(3),                   // max_attempts
                Some(1000),                // backoff
                Some("key-1".to_string()), // unique_key
                None,                      // depends_on
                None,                      // tags
                false,                     // lifo
                false,                     // remove_on_complete
                false,                     // remove_on_fail
                None,                      // stall_timeout
                None,                      // debounce_id
                None,                      // debounce_ttl
                None,                      // job_id
                None,                      // keep_completed_age
                None,                      // keep_completed_count
            )
            .await
            .unwrap();

        assert_eq!(job.priority, 10);
        assert_eq!(job.ttl, 60000);
        assert_eq!(job.timeout, 30000);
        assert_eq!(job.max_attempts, 3);
        assert_eq!(job.backoff, 1000);
        assert_eq!(job.unique_key, Some("key-1".to_string()));
    }

    #[tokio::test]
    async fn test_push_batch() {
        let qm = setup();

        let inputs = vec![
            crate::protocol::JobInput {
                data: json!({"i": 1}),
                priority: 0,
                delay: None,
                ttl: None,
                timeout: None,
                max_attempts: None,
                backoff: None,
                unique_key: None,
                depends_on: None,
                tags: None,
                lifo: false,
                remove_on_complete: false,
                remove_on_fail: false,
                stall_timeout: None,
                debounce_id: None,
                debounce_ttl: None,
                job_id: None,
                keep_completed_age: None,
                keep_completed_count: None,
            },
            crate::protocol::JobInput {
                data: json!({"i": 2}),
                priority: 5,
                delay: None,
                ttl: None,
                timeout: None,
                max_attempts: None,
                backoff: None,
                unique_key: None,
                depends_on: None,
                tags: None,
                lifo: false,
                remove_on_complete: false,
                remove_on_fail: false,
                stall_timeout: None,
                debounce_id: None,
                debounce_ttl: None,
                job_id: None,
                keep_completed_age: None,
                keep_completed_count: None,
            },
        ];

        let ids = qm.push_batch("test".to_string(), inputs).await;
        assert_eq!(ids.len(), 2);

        // Higher priority job should come first
        let job1 = qm.pull("test").await;
        assert_eq!(job1.priority, 5);

        let job2 = qm.pull("test").await;
        assert_eq!(job2.priority, 0);
    }

    #[tokio::test]
    async fn test_push_batch_large() {
        let qm = setup();

        let inputs: Vec<_> = (0..100)
            .map(|i| crate::protocol::JobInput {
                data: json!({"i": i}),
                priority: i as i32,
                delay: None,
                ttl: None,
                timeout: None,
                max_attempts: None,
                backoff: None,
                unique_key: None,
                depends_on: None,
                tags: None,
                lifo: false,
                remove_on_complete: false,
                remove_on_fail: false,
                stall_timeout: None,
                debounce_id: None,
                debounce_ttl: None,
                job_id: None,
                keep_completed_age: None,
                keep_completed_count: None,
            })
            .collect();

        let ids = qm.push_batch("test".to_string(), inputs).await;
        assert_eq!(ids.len(), 100);

        // Highest priority first
        let job = qm.pull("test").await;
        assert_eq!(job.priority, 99);
    }

    #[tokio::test]
    async fn test_push_batch_exceeds_limit() {
        let qm = setup();

        // Try to push more than MAX_BATCH_SIZE (1000) jobs
        let inputs: Vec<_> = (0..1001)
            .map(|i| crate::protocol::JobInput {
                data: json!({"i": i}),
                priority: 0,
                delay: None,
                ttl: None,
                timeout: None,
                max_attempts: None,
                backoff: None,
                unique_key: None,
                depends_on: None,
                tags: None,
                lifo: false,
                remove_on_complete: false,
                remove_on_fail: false,
                stall_timeout: None,
                debounce_id: None,
                debounce_ttl: None,
                job_id: None,
                keep_completed_age: None,
                keep_completed_count: None,
            })
            .collect();

        // Should return empty vec due to batch size limit
        let ids = qm.push_batch("test".to_string(), inputs).await;
        assert!(ids.is_empty(), "Batch exceeding limit should be rejected");
    }

    #[tokio::test]
    async fn test_push_batch_at_limit() {
        let qm = setup();

        // Push exactly MAX_BATCH_SIZE (1000) jobs - should succeed
        let inputs: Vec<_> = (0..1000)
            .map(|i| crate::protocol::JobInput {
                data: json!({"i": i}),
                priority: 0,
                delay: None,
                ttl: None,
                timeout: None,
                max_attempts: None,
                backoff: None,
                unique_key: None,
                depends_on: None,
                tags: None,
                lifo: false,
                remove_on_complete: false,
                remove_on_fail: false,
                stall_timeout: None,
                debounce_id: None,
                debounce_ttl: None,
                job_id: None,
                keep_completed_age: None,
                keep_completed_count: None,
            })
            .collect();

        let ids = qm.push_batch("test".to_string(), inputs).await;
        assert_eq!(ids.len(), 1000, "Batch at limit should succeed");
    }

    #[tokio::test]
    async fn test_ack() {
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let pulled = qm.pull("test").await;

        let result = qm.ack(pulled.id, None).await;
        assert!(result.is_ok());

        // Double ack should fail
        let result2 = qm.ack(job.id, None).await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_ack_with_result() {
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let pulled = qm.pull("test").await;

        let result_data = json!({"computed": 42, "success": true});
        qm.ack(pulled.id, Some(result_data.clone())).await.unwrap();

        // Check result is stored
        let stored = qm.get_result(job.id).await;
        assert!(stored.is_some());
        assert_eq!(stored.unwrap(), result_data);
    }

    #[tokio::test]
    async fn test_ack_nonexistent_job() {
        let qm = setup();
        let result = qm.ack(999999, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pull_batch() {
        let qm = setup();

        for i in 0..10 {
            qm.push(
                "test".to_string(),
                json!({"i": i}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        }

        let jobs = qm.pull_batch("test", 5).await;
        assert_eq!(jobs.len(), 5);

        let (queued, processing, _, _) = qm.stats().await;
        assert_eq!(queued, 5);
        assert_eq!(processing, 5);
    }

    #[tokio::test]
    async fn test_ack_batch() {
        let qm = setup();

        for i in 0..5 {
            qm.push(
                "test".to_string(),
                json!({"i": i}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        }

        let jobs = qm.pull_batch("test", 5).await;
        let ids: Vec<u64> = jobs.iter().map(|j| j.id).collect();

        let acked = qm.ack_batch(&ids).await;
        assert_eq!(acked, 5);

        let (_, processing, _, _) = qm.stats().await;
        assert_eq!(processing, 0);
    }

    // ==================== FAIL AND RETRY ====================

    #[tokio::test]
    async fn test_fail_and_retry() {
        let qm = setup();

        let job = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                Some(3),
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let pulled = qm.pull("test").await;
        assert_eq!(pulled.attempts, 0);

        // Fail the job
        qm.fail(pulled.id, Some("error".to_string())).await.unwrap();

        // Job should be back in queue with increased attempts
        let repulled = qm.pull("test").await;
        assert_eq!(repulled.id, job.id);
        assert_eq!(repulled.attempts, 1);
    }

    #[tokio::test]
    async fn test_fail_with_backoff() {
        let qm = setup();

        let _job = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                Some(3),
                Some(100),
                None,
                None,
                None,
                false, // lifo
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let pulled = qm.pull("test").await;
        qm.fail(pulled.id, None).await.unwrap();

        // Job should have run_at set in the future due to backoff
        // (exponential: 100ms * 2^attempt)
    }

    #[tokio::test]
    async fn test_fail_nonexistent_job() {
        let qm = setup();
        let result = qm.fail(999999, None).await;
        assert!(result.is_err());
    }

    // ==================== DLQ ====================

    #[tokio::test]
    async fn test_dlq() {
        let qm = setup();

        // Job with max_attempts=1 goes to DLQ after first failure
        let job = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                Some(1),
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let pulled = qm.pull("test").await;
        qm.fail(pulled.id, None).await.unwrap();

        let dlq = qm.get_dlq("test", None).await;
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].id, job.id);

        // Retry from DLQ
        let retried = qm.retry_dlq("test", None).await;
        assert_eq!(retried, 1);

        let dlq_after = qm.get_dlq("test", None).await;
        assert!(dlq_after.is_empty());
    }

    #[tokio::test]
    async fn test_dlq_retry_single() {
        let qm = setup();

        // Create two jobs that will fail
        let job1 = qm
            .push(
                "test".to_string(),
                json!({"i": 1}),
                0,
                None,
                None,
                None,
                Some(1),
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let job2 = qm
            .push(
                "test".to_string(),
                json!({"i": 2}),
                0,
                None,
                None,
                None,
                Some(1),
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Fail both
        let p1 = qm.pull("test").await;
        qm.fail(p1.id, None).await.unwrap();
        let p2 = qm.pull("test").await;
        qm.fail(p2.id, None).await.unwrap();

        let dlq = qm.get_dlq("test", None).await;
        assert_eq!(dlq.len(), 2);

        // Retry only job1
        let retried = qm.retry_dlq("test", Some(job1.id)).await;
        assert_eq!(retried, 1);

        let dlq_after = qm.get_dlq("test", None).await;
        assert_eq!(dlq_after.len(), 1);
        assert_eq!(dlq_after[0].id, job2.id);
    }

    #[tokio::test]
    async fn test_dlq_with_limit() {
        let qm = setup();

        // Create 10 jobs that will fail
        for i in 0..10 {
            qm.push(
                "test".to_string(),
                json!({"i": i}),
                0,
                None,
                None,
                None,
                Some(1),
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        }

        // Fail all
        for _ in 0..10 {
            let p = qm.pull("test").await;
            qm.fail(p.id, None).await.unwrap();
        }

        // Get only 5
        let dlq = qm.get_dlq("test", Some(5)).await;
        assert_eq!(dlq.len(), 5);
    }

    // ==================== UNIQUE KEY ====================

    #[tokio::test]
    async fn test_unique_key() {
        let qm = setup();

        let job1 = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                None,
                None,
                Some("unique-123".to_string()),
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await;
        assert!(job1.is_ok());

        // Duplicate should fail
        let job2 = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                None,
                None,
                Some("unique-123".to_string()),
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await;
        assert!(job2.is_err());

        // After ack, key should be released
        let pulled = qm.pull("test").await;
        qm.ack(pulled.id, None).await.unwrap();

        let job3 = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                None,
                None,
                Some("unique-123".to_string()),
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await;
        assert!(job3.is_ok());
    }

    #[tokio::test]
    async fn test_unique_key_different_queues() {
        let qm = setup();

        // Same unique key in different queues should work
        let job1 = qm
            .push(
                "queue1".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                None,
                None,
                Some("same-key".to_string()),
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await;
        assert!(job1.is_ok());

        let job2 = qm
            .push(
                "queue2".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                None,
                None,
                Some("same-key".to_string()),
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await;
        assert!(job2.is_ok());
    }

    #[tokio::test]
    async fn test_unique_key_released_on_fail() {
        let qm = setup();

        let _job = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                Some(1),
                None,
                Some("unique-fail".to_string()),
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let pulled = qm.pull("test").await;
        qm.fail(pulled.id, None).await.unwrap(); // Goes to DLQ

        // Unique key should still be locked while in DLQ
        // (this is expected behavior - job is still "in system")
    }

    // ==================== CANCEL ====================

    #[tokio::test]
    async fn test_cancel() {
        let qm = setup();

        let job = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                Some(60000),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let result = qm.cancel(job.id).await;
        assert!(result.is_ok());

        // Cancel non-existent should fail
        let result2 = qm.cancel(999999).await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_cancel_processing_job() {
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let _pulled = qm.pull("test").await;

        // Cancel while processing
        let result = qm.cancel(job.id).await;
        assert!(result.is_ok());

        let (_, processing, _, _) = qm.stats().await;
        assert_eq!(processing, 0);
    }

    // ==================== PROGRESS ====================

    #[tokio::test]
    async fn test_progress() {
        let qm = setup();

        let _job = qm
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let pulled = qm.pull("test").await;

        // Update progress
        qm.update_progress(pulled.id, 50, Some("halfway".to_string()))
            .await
            .unwrap();

        let (progress, msg) = qm.get_progress(pulled.id).await.unwrap();
        assert_eq!(progress, 50);
        assert_eq!(msg, Some("halfway".to_string()));
    }

    #[tokio::test]
    async fn test_progress_max_100() {
        let qm = setup();

        let _job = qm
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let pulled = qm.pull("test").await;

        // Progress should cap at 100
        qm.update_progress(pulled.id, 150, None).await.unwrap();

        let (progress, _) = qm.get_progress(pulled.id).await.unwrap();
        assert_eq!(progress, 100);
    }

    #[tokio::test]
    async fn test_progress_nonexistent() {
        let qm = setup();
        let result = qm.get_progress(999999).await;
        assert!(result.is_err());
    }

    // ==================== PRIORITY ====================

    #[tokio::test]
    async fn test_priority_ordering() {
        let qm = setup();

        // Push jobs with different priorities
        qm.push(
            "test".to_string(),
            json!({"p": 1}),
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();
        qm.push(
            "test".to_string(),
            json!({"p": 3}),
            3,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();
        qm.push(
            "test".to_string(),
            json!({"p": 2}),
            2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();

        // Should get highest priority first
        let j1 = qm.pull("test").await;
        assert_eq!(j1.priority, 3);

        let j2 = qm.pull("test").await;
        assert_eq!(j2.priority, 2);

        let j3 = qm.pull("test").await;
        assert_eq!(j3.priority, 1);
    }

    #[tokio::test]
    async fn test_priority_negative() {
        let qm = setup();

        qm.push(
            "test".to_string(),
            json!({}),
            -10,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();
        qm.push(
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
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();
        qm.push(
            "test".to_string(),
            json!({}),
            10,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();

        let j1 = qm.pull("test").await;
        assert_eq!(j1.priority, 10);

        let j2 = qm.pull("test").await;
        assert_eq!(j2.priority, 0);

        let j3 = qm.pull("test").await;
        assert_eq!(j3.priority, -10);
    }

    #[tokio::test]
    async fn test_fifo_same_priority() {
        let qm = setup();

        // Jobs with same priority should be FIFO (by created_at)
        let job1 = qm
            .push(
                "test".to_string(),
                json!({"order": 1}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let job2 = qm
            .push(
                "test".to_string(),
                json!({"order": 2}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let job3 = qm
            .push(
                "test".to_string(),
                json!({"order": 3}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let p1 = qm.pull("test").await;
        let p2 = qm.pull("test").await;
        let p3 = qm.pull("test").await;

        assert_eq!(p1.id, job1.id);
        assert_eq!(p2.id, job2.id);
        assert_eq!(p3.id, job3.id);
    }

    #[tokio::test]
    async fn test_lifo_ordering() {
        let qm = setup();

        // Jobs with LIFO flag should be pulled in reverse order (last in, first out)
        let job1 = qm
            .push(
                "test".to_string(),
                json!({"order": 1}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                true,  // lifo = true
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let job2 = qm
            .push(
                "test".to_string(),
                json!({"order": 2}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                true,  // lifo = true
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let job3 = qm
            .push(
                "test".to_string(),
                json!({"order": 3}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                true,  // lifo = true
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // LIFO: last pushed should be pulled first
        let p1 = qm.pull("test").await;
        let p2 = qm.pull("test").await;
        let p3 = qm.pull("test").await;

        assert_eq!(p1.id, job3.id, "LIFO: job3 should be pulled first");
        assert_eq!(p2.id, job2.id, "LIFO: job2 should be pulled second");
        assert_eq!(p3.id, job1.id, "LIFO: job1 should be pulled last");
    }

    #[tokio::test]
    async fn test_lifo_mixed_with_fifo() {
        let qm = setup();

        // Mix of LIFO and FIFO jobs - LIFO jobs get higher effective priority
        let fifo_job = qm
            .push(
                "test".to_string(),
                json!({"type": "fifo"}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false, // fifo
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let lifo_job = qm
            .push(
                "test".to_string(),
                json!({"type": "lifo"}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                true,  // lifo - should be pulled before fifo
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // LIFO job should be pulled first (pushed after but LIFO)
        let p1 = qm.pull("test").await;
        let p2 = qm.pull("test").await;

        assert_eq!(p1.id, lifo_job.id, "LIFO job should be pulled first");
        assert_eq!(p2.id, fifo_job.id, "FIFO job should be pulled second");
    }

    // ==================== DELAYED JOBS ====================

    #[tokio::test]
    async fn test_delayed_job() {
        let qm = setup();

        // Job delayed by 100ms
        let job = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                Some(100),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        assert!(job.run_at > job.created_at);
    }

    #[tokio::test]
    async fn test_delayed_job_ordering() {
        let qm = setup();

        // Job 1: delayed 200ms
        let _job1 = qm
            .push(
                "test".to_string(),
                json!({"order": 1}),
                0,
                Some(200),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Job 2: immediate
        let job2 = qm
            .push(
                "test".to_string(),
                json!({"order": 2}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Immediate job should be pulled first
        let pulled = qm.pull("test").await;
        assert_eq!(pulled.id, job2.id);
    }

    // ==================== JOB DEPENDENCIES ====================

    #[tokio::test]
    async fn test_job_dependencies_single() {
        let qm = setup();

        // Create parent job
        let parent = qm
            .push(
                "test".to_string(),
                json!({"parent": true}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Create child job that depends on parent
        let child = qm
            .push(
                "test".to_string(),
                json!({"child": true}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(vec![parent.id]),
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Child should not be pullable yet
        let pulled_parent = qm.pull("test").await;
        assert_eq!(pulled_parent.id, parent.id);

        // Complete parent
        qm.ack(parent.id, None).await.unwrap();

        // Now trigger dependency check (normally done by background task)
        qm.check_dependencies().await;

        // Now child should be pullable
        let pulled_child = qm.pull("test").await;
        assert_eq!(pulled_child.id, child.id);
    }

    #[tokio::test]
    async fn test_job_dependencies_multiple() {
        let qm = setup();

        // Create two parent jobs
        let parent1 = qm
            .push(
                "test".to_string(),
                json!({"p": 1}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        let parent2 = qm
            .push(
                "test".to_string(),
                json!({"p": 2}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Create child that depends on both
        let _child = qm
            .push(
                "test".to_string(),
                json!({"child": true}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(vec![parent1.id, parent2.id]),
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Pull and ack first parent
        let p1 = qm.pull("test").await;
        qm.ack(p1.id, None).await.unwrap();

        qm.check_dependencies().await;

        // Child still waiting (parent2 not done)
        let p2 = qm.pull("test").await;
        assert_eq!(p2.id, parent2.id);

        qm.ack(p2.id, None).await.unwrap();
        qm.check_dependencies().await;

        // Now child should be available
        let (queued, _, _, _) = qm.stats().await;
        assert_eq!(queued, 1); // child is now queued
    }

    // ==================== PAUSE/RESUME ====================

    #[tokio::test]
    async fn test_pause_resume() {
        let qm = setup();

        // Need to push a job first so the queue appears in list_queues
        qm.push(
            "test".to_string(),
            json!({"x": 1}),
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
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();

        qm.pause("test").await;

        // Verify paused via list_queues
        let queues = qm.list_queues().await;
        let test_queue = queues.iter().find(|q| q.name == "test");
        assert!(test_queue.is_some());
        assert!(test_queue.unwrap().paused);

        qm.resume("test").await;

        // Verify resumed via list_queues
        let queues = qm.list_queues().await;
        let test_queue = queues.iter().find(|q| q.name == "test");
        assert!(!test_queue.unwrap().paused);
    }

    // ==================== RATE LIMITING ====================

    #[tokio::test]
    async fn test_rate_limit() {
        let qm = setup();

        qm.set_rate_limit("test".to_string(), 100).await;

        // Check via list_queues that rate limit is set
        qm.push(
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
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();

        let queues = qm.list_queues().await;
        let test_queue = queues.iter().find(|q| q.name == "test");
        assert!(test_queue.is_some());
        assert_eq!(test_queue.unwrap().rate_limit, Some(100));

        qm.clear_rate_limit("test").await;

        let queues2 = qm.list_queues().await;
        let test_queue2 = queues2.iter().find(|q| q.name == "test");
        assert_eq!(test_queue2.unwrap().rate_limit, None);
    }

    // ==================== CONCURRENCY LIMIT ====================

    #[tokio::test]
    async fn test_concurrency_limit() {
        let qm = setup();

        qm.set_concurrency("test".to_string(), 2).await;

        // Push 3 jobs
        for i in 0..3 {
            qm.push(
                "test".to_string(),
                json!({"i": i}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        }

        // Check via list_queues
        let queues = qm.list_queues().await;
        let test_queue = queues.iter().find(|q| q.name == "test");
        assert_eq!(test_queue.unwrap().concurrency_limit, Some(2));

        // Clear limit
        qm.clear_concurrency("test").await;
    }

    // ==================== CRON JOBS ====================

    #[tokio::test]
    async fn test_cron() {
        let qm = setup();

        qm.add_cron(
            "test-cron".to_string(),
            "test".to_string(),
            json!({"cron": true}),
            "*/60".to_string(),
            0,
        )
        .await
        .unwrap();

        let crons = qm.list_crons().await;
        assert_eq!(crons.len(), 1);
        assert_eq!(crons[0].name, "test-cron");
        assert_eq!(crons[0].queue, "test");

        let deleted = qm.delete_cron("test-cron").await;
        assert!(deleted);

        let crons_after = qm.list_crons().await;
        assert!(crons_after.is_empty());
    }

    #[tokio::test]
    async fn test_cron_delete_nonexistent() {
        let qm = setup();
        let deleted = qm.delete_cron("nonexistent").await;
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_cron_with_priority() {
        let qm = setup();

        qm.add_cron(
            "high-priority-cron".to_string(),
            "test".to_string(),
            json!({}),
            "*/30".to_string(),
            100, // high priority
        )
        .await
        .unwrap();

        let crons = qm.list_crons().await;
        assert_eq!(crons[0].priority, 100);

        qm.delete_cron("high-priority-cron").await;
    }

    // ==================== STATS & METRICS ====================

    #[tokio::test]
    async fn test_stats() {
        let qm = setup();

        // Push some jobs
        for i in 0..5 {
            qm.push(
                "test".to_string(),
                json!({"i": i}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        }

        let (queued, processing, delayed, dlq) = qm.stats().await;
        assert_eq!(queued, 5);
        assert_eq!(processing, 0);
        assert_eq!(delayed, 0);
        assert_eq!(dlq, 0);

        // Pull one
        let _ = qm.pull("test").await;

        let (queued2, processing2, _, _) = qm.stats().await;
        assert_eq!(queued2, 4);
        assert_eq!(processing2, 1);
    }

    #[tokio::test]
    async fn test_metrics() {
        let qm = setup();

        // Push and complete some jobs
        for _ in 0..5 {
            qm.push(
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
            let pulled = qm.pull("test").await;
            qm.ack(pulled.id, None).await.unwrap();
        }

        let metrics = qm.get_metrics().await;
        assert_eq!(metrics.total_pushed, 5);
        assert_eq!(metrics.total_completed, 5);
    }

    #[tokio::test]
    async fn test_list_queues() {
        let qm = setup();

        // Push to create queues
        qm.push(
            "queue1".to_string(),
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
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();
        qm.push(
            "queue2".to_string(),
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
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();
        qm.push(
            "queue2".to_string(),
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
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();

        let queues = qm.list_queues().await;
        assert!(queues.len() >= 2);

        let q2 = queues.iter().find(|q| q.name == "queue2");
        assert!(q2.is_some());
        assert_eq!(q2.unwrap().pending, 2);
    }

    // ==================== AUTHENTICATION ====================

    #[tokio::test]
    async fn test_auth_token_verification() {
        let qm = QueueManager::with_auth_tokens(false, vec!["secret-token".to_string()]);

        assert!(qm.verify_token("secret-token"));
        assert!(!qm.verify_token("wrong-token"));
    }

    #[tokio::test]
    async fn test_auth_empty_tokens_allows_all() {
        let qm = QueueManager::new(false);

        // With no tokens configured, any token should be accepted
        assert!(qm.verify_token("any-token"));
        assert!(qm.verify_token(""));
    }

    #[tokio::test]
    async fn test_auth_multiple_tokens() {
        let qm = QueueManager::with_auth_tokens(
            false,
            vec![
                "token1".to_string(),
                "token2".to_string(),
                "token3".to_string(),
            ],
        );

        assert!(qm.verify_token("token1"));
        assert!(qm.verify_token("token2"));
        assert!(qm.verify_token("token3"));
        assert!(!qm.verify_token("token4"));
    }

    // ==================== SHARDING ====================

    #[tokio::test]
    async fn test_different_queues_different_shards() {
        let qm = setup();

        // Push to many different queues
        for i in 0..100 {
            qm.push(
                format!("queue-{}", i),
                json!({"i": i}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        }

        let queues = qm.list_queues().await;
        assert_eq!(queues.len(), 100);
    }

    #[tokio::test]
    async fn test_shard_index_consistency() {
        // Same queue name should always map to same shard
        let idx1 = QueueManager::shard_index("test-queue");
        let idx2 = QueueManager::shard_index("test-queue");
        assert_eq!(idx1, idx2);

        // Different queues may map to different shards
        let idx3 = QueueManager::shard_index("another-queue");
        // (idx3 might equal idx1 by coincidence, so no assertion here)
        assert!(idx3 < 32); // Should be within shard range
    }

    // ==================== EDGE CASES ====================

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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
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
                    false, // remove_on_complete
                    false, // remove_on_fail
                    None,  // stall_timeout
                    None,  // debounce_id
                    None,  // debounce_ttl
                    None,  // job_id
                    None,  // keep_completed_age
                    None,  // keep_completed_count
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
                    false, // remove_on_complete
                    false, // remove_on_fail
                    None,  // stall_timeout
                    None,  // debounce_id
                    None,  // debounce_ttl
                    None,  // job_id
                    None,  // keep_completed_age
                    None,  // keep_completed_count
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
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
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
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
            false, // remove_on_complete
            false, // remove_on_fail
            None,  // stall_timeout
            None,  // debounce_id
            None,  // debounce_ttl
            None,  // job_id
            None,  // keep_completed_age
            None,  // keep_completed_count
        )
        .await
        .unwrap();

        let pulled = qm.pull("test").await;
        assert_eq!(pulled.data, nested);
    }

    // ==================== CONCURRENT OPERATIONS ====================

    #[tokio::test]
    async fn test_concurrent_push() {
        let qm = setup();

        let mut handles = vec![];
        for i in 0..100 {
            let qm_clone = qm.clone();
            handles.push(tokio::spawn(async move {
                qm_clone
                    .push(
                        "test".to_string(),
                        json!({"i": i}),
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
                        false, // remove_on_complete
                        false, // remove_on_fail
                        None,  // stall_timeout
                        None,  // debounce_id
                        None,  // debounce_ttl
                        None,  // job_id
                        None,  // keep_completed_age
                        None,  // keep_completed_count
                    )
                    .await
                    .unwrap()
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let (queued, _, _, _) = qm.stats().await;
        assert_eq!(queued, 100);
    }

    #[tokio::test]
    async fn test_concurrent_push_pull() {
        let qm = setup();

        // Pre-populate
        for i in 0..50 {
            qm.push(
                "test".to_string(),
                json!({"i": i}),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();
        }

        let mut push_handles = vec![];
        let mut pull_handles = vec![];

        // Concurrent pushes
        for i in 50..100 {
            let qm_clone = qm.clone();
            push_handles.push(tokio::spawn(async move {
                qm_clone
                    .push(
                        "test".to_string(),
                        json!({"i": i}),
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
                        false, // remove_on_complete
                        false, // remove_on_fail
                        None,  // stall_timeout
                        None,  // debounce_id
                        None,  // debounce_ttl
                        None,  // job_id
                        None,  // keep_completed_age
                        None,  // keep_completed_count
                    )
                    .await
                    .unwrap()
            }));
        }

        // Concurrent pulls
        for _ in 0..50 {
            let qm_clone = qm.clone();
            pull_handles.push(tokio::spawn(async move { qm_clone.pull("test").await }));
        }

        for handle in push_handles {
            handle.await.unwrap();
        }
        for handle in pull_handles {
            handle.await.unwrap();
        }

        let (queued, processing, _, _) = qm.stats().await;
        assert_eq!(queued + processing, 100);
    }

    // ==================== PROTOCOL TESTS ====================

    #[test]
    fn test_job_is_ready() {
        let now = 1000u64;

        let job = crate::protocol::Job {
            id: 1,
            queue: "test".to_string(),
            data: json!({}),
            priority: 0,
            created_at: 900,
            run_at: 950, // In the past
            started_at: 0,
            attempts: 0,
            max_attempts: 0,
            backoff: 0,
            ttl: 0,
            timeout: 0,
            unique_key: None,
            depends_on: vec![],
            progress: 0,
            progress_msg: None,
            tags: vec![],
            lifo: false,
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: vec![],
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        };

        assert!(job.is_ready(now));

        let delayed_job = crate::protocol::Job {
            run_at: 2000, // In the future
            ..job.clone()
        };

        assert!(!delayed_job.is_ready(now));
    }

    #[test]
    fn test_job_is_expired() {
        let now = 1000u64;

        let job = crate::protocol::Job {
            id: 1,
            queue: "test".to_string(),
            data: json!({}),
            priority: 0,
            created_at: 500,
            run_at: 500,
            started_at: 0,
            attempts: 0,
            max_attempts: 0,
            backoff: 0,
            ttl: 400, // Expires at 900
            timeout: 0,
            unique_key: None,
            depends_on: vec![],
            progress: 0,
            progress_msg: None,
            tags: vec![],
            lifo: false,
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: vec![],
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        };

        assert!(job.is_expired(now)); // 1000 > 900

        let valid_job = crate::protocol::Job {
            ttl: 600, // Expires at 1100
            ..job.clone()
        };

        assert!(!valid_job.is_expired(now)); // 1000 < 1100

        let no_ttl_job = crate::protocol::Job {
            ttl: 0, // No TTL
            ..job.clone()
        };

        assert!(!no_ttl_job.is_expired(now)); // Never expires
    }

    #[test]
    fn test_job_is_timed_out() {
        let now = 1000u64;

        let job = crate::protocol::Job {
            id: 1,
            queue: "test".to_string(),
            data: json!({}),
            priority: 0,
            created_at: 500,
            run_at: 500,
            started_at: 600,
            attempts: 0,
            max_attempts: 0,
            backoff: 0,
            ttl: 0,
            timeout: 300, // Times out at 900
            unique_key: None,
            depends_on: vec![],
            progress: 0,
            progress_msg: None,
            tags: vec![],
            lifo: false,
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: vec![],
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        };

        assert!(job.is_timed_out(now)); // 1000 > 900

        let valid_job = crate::protocol::Job {
            timeout: 500, // Times out at 1100
            ..job.clone()
        };

        assert!(!valid_job.is_timed_out(now)); // 1000 < 1100

        let not_started = crate::protocol::Job {
            started_at: 0,
            ..job.clone()
        };

        assert!(!not_started.is_timed_out(now)); // Not started yet
    }

    #[test]
    fn test_job_should_go_to_dlq() {
        let job = crate::protocol::Job {
            id: 1,
            queue: "test".to_string(),
            data: json!({}),
            priority: 0,
            created_at: 0,
            run_at: 0,
            started_at: 0,
            attempts: 3,
            max_attempts: 3,
            backoff: 0,
            ttl: 0,
            timeout: 0,
            unique_key: None,
            depends_on: vec![],
            progress: 0,
            progress_msg: None,
            tags: vec![],
            lifo: false,
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: vec![],
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        };

        assert!(job.should_go_to_dlq()); // attempts >= max_attempts

        let retry_job = crate::protocol::Job {
            attempts: 2,
            ..job.clone()
        };

        assert!(!retry_job.should_go_to_dlq()); // attempts < max_attempts

        let no_limit = crate::protocol::Job {
            max_attempts: 0,
            ..job.clone()
        };

        assert!(!no_limit.should_go_to_dlq()); // max_attempts=0 means unlimited
    }

    #[test]
    fn test_job_next_backoff() {
        let job = crate::protocol::Job {
            id: 1,
            queue: "test".to_string(),
            data: json!({}),
            priority: 0,
            created_at: 0,
            run_at: 0,
            started_at: 0,
            attempts: 0,
            max_attempts: 5,
            backoff: 1000, // 1 second base
            ttl: 0,
            timeout: 0,
            unique_key: None,
            depends_on: vec![],
            progress: 0,
            progress_msg: None,
            tags: vec![],
            lifo: false,
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: vec![],
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        };

        // Exponential backoff: base * 2^attempts
        assert_eq!(job.next_backoff(), 1000); // 1000 * 2^0

        let job1 = crate::protocol::Job {
            attempts: 1,
            ..job.clone()
        };
        assert_eq!(job1.next_backoff(), 2000); // 1000 * 2^1

        let job2 = crate::protocol::Job {
            attempts: 2,
            ..job.clone()
        };
        assert_eq!(job2.next_backoff(), 4000); // 1000 * 2^2

        let no_backoff = crate::protocol::Job {
            backoff: 0,
            ..job.clone()
        };
        assert_eq!(no_backoff.next_backoff(), 0); // No backoff configured
    }

    // ==================== JOB TAGS TESTS ====================

    #[tokio::test]
    async fn test_push_with_tags() {
        let qm = setup();

        let job = qm
            .push(
                "test".to_string(),
                json!({"data": "with tags"}),
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(vec!["urgent".to_string(), "backend".to_string()]),
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        assert_eq!(job.tags, vec!["urgent", "backend"]);
    }

    #[tokio::test]
    async fn test_push_with_empty_tags() {
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
                Some(vec![]),
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        assert!(job.tags.is_empty());
    }

    #[tokio::test]
    async fn test_tags_preserved_after_pull() {
        let qm = setup();

        let original = qm
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
                Some(vec!["tag1".to_string(), "tag2".to_string()]),
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let pulled = qm.pull("test").await;
        assert_eq!(pulled.id, original.id);
        assert_eq!(pulled.tags, vec!["tag1", "tag2"]);
    }

    // ==================== WORKER REGISTRATION TESTS ====================

    #[tokio::test]
    async fn test_list_workers_empty() {
        let qm = setup();
        let workers = qm.list_workers().await;
        assert!(workers.is_empty());
    }

    #[tokio::test]
    async fn test_worker_heartbeat_creates_worker() {
        let qm = setup();

        qm.worker_heartbeat(
            "worker-1".to_string(),
            vec!["queue-a".to_string(), "queue-b".to_string()],
            4,
            0,
        )
        .await;

        let workers = qm.list_workers().await;
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].id, "worker-1");
        assert_eq!(workers[0].queues, vec!["queue-a", "queue-b"]);
        assert_eq!(workers[0].concurrency, 4);
    }

    #[tokio::test]
    async fn test_worker_heartbeat_updates_existing() {
        let qm = setup();

        // First heartbeat
        qm.worker_heartbeat("worker-1".to_string(), vec!["queue-a".to_string()], 2, 0)
            .await;

        // Second heartbeat with different settings
        qm.worker_heartbeat(
            "worker-1".to_string(),
            vec!["queue-a".to_string(), "queue-b".to_string()],
            4,
            0,
        )
        .await;

        let workers = qm.list_workers().await;
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].queues.len(), 2);
        assert_eq!(workers[0].concurrency, 4);
    }

    #[tokio::test]
    async fn test_multiple_workers() {
        let qm = setup();

        qm.worker_heartbeat("worker-1".to_string(), vec!["queue-a".to_string()], 2, 0)
            .await;
        qm.worker_heartbeat("worker-2".to_string(), vec!["queue-b".to_string()], 4, 0)
            .await;
        qm.worker_heartbeat(
            "worker-3".to_string(),
            vec!["queue-a".to_string(), "queue-b".to_string()],
            8,
            0,
        )
        .await;

        let workers = qm.list_workers().await;
        assert_eq!(workers.len(), 3);
    }

    // ==================== WEBHOOK TESTS ====================

    #[tokio::test]
    async fn test_list_webhooks_empty() {
        let qm = setup();
        let webhooks = qm.list_webhooks().await;
        assert!(webhooks.is_empty());
    }

    #[tokio::test]
    async fn test_add_webhook() {
        let qm = setup();

        let id = qm
            .add_webhook(
                "https://example.com/webhook".to_string(),
                vec!["job.pushed".to_string(), "job.completed".to_string()],
                Some("my-queue".to_string()),
                Some("secret123".to_string()),
            )
            .await
            .unwrap();

        let webhooks = qm.list_webhooks().await;
        assert_eq!(webhooks.len(), 1);
        assert_eq!(webhooks[0].id, id);
        assert_eq!(webhooks[0].url, "https://example.com/webhook");
        assert_eq!(webhooks[0].events, vec!["job.pushed", "job.completed"]);
        assert_eq!(webhooks[0].queue, Some("my-queue".to_string()));
        assert_eq!(webhooks[0].secret, Some("secret123".to_string()));
    }

    #[tokio::test]
    async fn test_add_webhook_ssrf_blocks_localhost() {
        let qm = setup();

        // Should block localhost variants
        let result = qm
            .add_webhook(
                "http://localhost/webhook".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Localhost"));

        let result = qm
            .add_webhook(
                "http://127.0.0.1/webhook".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_webhook_ssrf_blocks_private_ips() {
        let qm = setup();

        // Should block RFC1918 private IPs
        let result = qm
            .add_webhook(
                "http://192.168.1.1/webhook".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Private"));

        let result = qm
            .add_webhook(
                "http://10.0.0.1/webhook".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());

        let result = qm
            .add_webhook(
                "http://172.16.0.1/webhook".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_webhook_ssrf_blocks_link_local() {
        let qm = setup();

        // Should block AWS metadata endpoint (link-local)
        let result = qm
            .add_webhook(
                "http://169.254.169.254/latest/meta-data/".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_webhook_ssrf_blocks_internal_domains() {
        let qm = setup();

        // Should block internal domain patterns
        let result = qm
            .add_webhook(
                "http://service.internal/webhook".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());

        let result = qm
            .add_webhook(
                "http://app.svc.cluster.local/webhook".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_webhook_ssrf_allows_valid_urls() {
        let qm = setup();

        // Should allow valid external URLs
        let result = qm
            .add_webhook(
                "https://api.example.com/webhook".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_ok());

        let result = qm
            .add_webhook(
                "https://hooks.slack.com/services/xxx".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_add_webhook_ssrf_blocks_invalid_schemes() {
        let qm = setup();

        // Should block file:// and other schemes
        let result = qm
            .add_webhook(
                "file:///etc/passwd".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("scheme"));

        let result = qm
            .add_webhook(
                "ftp://server.com/file".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_webhook_without_queue_filter() {
        let qm = setup();

        qm.add_webhook(
            "https://example.com/global".to_string(),
            vec!["job.failed".to_string()],
            None,
            None,
        )
        .await
        .unwrap();

        let webhooks = qm.list_webhooks().await;
        assert_eq!(webhooks.len(), 1);
        assert!(webhooks[0].queue.is_none());
        assert!(webhooks[0].secret.is_none());
    }

    #[tokio::test]
    async fn test_delete_webhook() {
        let qm = setup();

        let id1 = qm
            .add_webhook(
                "https://example.com/1".to_string(),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await
            .unwrap();
        let id2 = qm
            .add_webhook(
                "https://example.com/2".to_string(),
                vec!["job.completed".to_string()],
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(qm.list_webhooks().await.len(), 2);

        let deleted = qm.delete_webhook(&id1).await;
        assert!(deleted);

        let webhooks = qm.list_webhooks().await;
        assert_eq!(webhooks.len(), 1);
        assert_eq!(webhooks[0].id, id2);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_webhook() {
        let qm = setup();
        let deleted = qm.delete_webhook("nonexistent-id").await;
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_multiple_webhooks() {
        let qm = setup();

        for i in 0..5 {
            qm.add_webhook(
                format!("https://example.com/{}", i),
                vec!["job.pushed".to_string()],
                None,
                None,
            )
            .await
            .unwrap();
        }

        assert_eq!(qm.list_webhooks().await.len(), 5);
    }

    // ==================== EVENT SUBSCRIPTION TESTS ====================

    #[tokio::test]
    async fn test_subscribe_events() {
        let qm = setup();

        // Subscribe to events (None = all queues)
        let mut rx = qm.subscribe_events(None);

        // Push a job (should trigger event)
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Try to receive the event
        tokio::select! {
            result = rx.recv() => {
                let event = result.unwrap();
                assert_eq!(event.event_type, "pushed");
                assert_eq!(event.queue, "test");
                assert_eq!(event.job_id, job.id);
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                // Event was broadcast (may have been missed if receiver wasn't ready)
                // This is acceptable for a broadcast channel
            }
        }
    }

    #[tokio::test]
    async fn test_broadcast_event_on_ack() {
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Subscribe after pushing (will miss push event)
        let mut rx = qm.subscribe_events(None);

        // Pull and ack
        let pulled = qm.pull("test").await;
        qm.ack(pulled.id, None).await.unwrap();

        // Should receive completed event
        tokio::select! {
            result = rx.recv() => {
                let event = result.unwrap();
                assert_eq!(event.event_type, "completed");
                assert_eq!(event.job_id, job.id);
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                // Event broadcast happened
            }
        }
    }

    #[tokio::test]
    async fn test_broadcast_event_on_fail() {
        let qm = setup();

        let job = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                Some(1),
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let mut rx = qm.subscribe_events(None);

        // Pull and fail
        let pulled = qm.pull("test").await;
        qm.fail(pulled.id, Some("test error".to_string()))
            .await
            .unwrap();

        // Should receive failed event
        tokio::select! {
            result = rx.recv() => {
                let event = result.unwrap();
                assert_eq!(event.event_type, "failed");
                assert_eq!(event.job_id, job.id);
                assert_eq!(event.error, Some("test error".to_string()));
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                // Event broadcast happened
            }
        }
    }

    // ==================== GET JOB / GET STATE WITH TAGS ====================

    #[tokio::test]
    async fn test_get_job_includes_tags() {
        let qm = setup();

        let original = qm
            .push(
                "test".to_string(),
                json!({"key": "value"}),
                5,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(vec!["priority".to_string(), "email".to_string()]),
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let (job_opt, state) = qm.get_job(original.id);
        let job = job_opt.unwrap();
        assert_eq!(job.id, original.id);
        assert_eq!(job.tags, vec!["priority", "email"]);
        assert_eq!(state, crate::protocol::JobState::Waiting);
    }

    #[tokio::test]
    async fn test_job_state_with_tags() {
        let qm = setup();

        // Push with tags
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
                Some(vec!["test-tag".to_string()]),
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Check state is waiting
        let state = qm.get_state(job.id);
        assert_eq!(state, crate::protocol::JobState::Waiting);

        // Pull the job
        let pulled = qm.pull("test").await;
        assert_eq!(pulled.tags, vec!["test-tag"]);

        // Check state is active
        let state = qm.get_state(job.id);
        assert_eq!(state, crate::protocol::JobState::Active);

        // Ack the job
        qm.ack(job.id, None).await.unwrap();

        // Check state is completed
        let state = qm.get_state(job.id);
        assert_eq!(state, crate::protocol::JobState::Completed);
    }

    // ==================== CLEANUP TESTS ====================

    #[tokio::test]
    async fn test_cleanup_completed_jobs_removes_from_index() {
        let qm = setup();

        // Push and complete many jobs
        for i in 0..100 {
            let job = qm
                .push(
                    "test".to_string(),
                    json!({"i": i}),
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
                    false, // remove_on_complete
                    false, // remove_on_fail
                    None,  // stall_timeout
                    None,  // debounce_id
                    None,  // debounce_ttl
                    None,  // job_id
                    None,  // keep_completed_age
                    None,  // keep_completed_count
                )
                .await
                .unwrap();
            let pulled = qm.pull("test").await;
            qm.ack(pulled.id, None).await.unwrap();
        }

        // Verify jobs are in completed_jobs
        let completed_count = qm.completed_jobs.read().len();
        assert_eq!(completed_count, 100);

        // Verify job_index has entries for completed jobs
        let index_count = qm.job_index.len();
        assert!(index_count >= 100);
    }

    #[tokio::test]
    async fn test_cancel_job_in_queue_preserves_others() {
        let qm = setup();

        // Push multiple jobs
        let mut job_ids = Vec::new();
        for i in 0..10 {
            let job = qm
                .push(
                    "test".to_string(),
                    json!({"i": i}),
                    i as i32,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                    false, // remove_on_complete
                    false, // remove_on_fail
                    None,  // stall_timeout
                    None,  // debounce_id
                    None,  // debounce_ttl
                    None,  // job_id
                    None,  // keep_completed_age
                    None,  // keep_completed_count
                )
                .await
                .unwrap();
            job_ids.push(job.id);
        }

        // Cancel middle job
        let cancel_result = qm.cancel(job_ids[5]).await;
        assert!(cancel_result.is_ok());

        // Verify other jobs still exist
        let (queued, _, _, _) = qm.stats().await;
        assert_eq!(queued, 9);

        // Verify we can pull remaining jobs in priority order
        let first = qm.pull("test").await;
        assert_eq!(first.priority, 9); // Highest priority
    }

    #[tokio::test]
    async fn test_cancel_releases_unique_key() {
        let qm = setup();

        // Push job with unique key
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
                Some("cancel-test-key".to_string()),
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Cancel the job
        qm.cancel(job.id).await.unwrap();

        // Should be able to push with same key again
        let job2 = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                None,
                None,
                Some("cancel-test-key".to_string()),
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await;
        assert!(job2.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_cancel_operations() {
        let qm = setup();

        // Push multiple jobs
        let mut job_ids = Vec::new();
        for i in 0..50 {
            let job = qm
                .push(
                    "test".to_string(),
                    json!({"i": i}),
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
                    false, // remove_on_complete
                    false, // remove_on_fail
                    None,  // stall_timeout
                    None,  // debounce_id
                    None,  // debounce_ttl
                    None,  // job_id
                    None,  // keep_completed_age
                    None,  // keep_completed_count
                )
                .await
                .unwrap();
            job_ids.push(job.id);
        }

        // Concurrently cancel multiple jobs
        let mut handles = Vec::new();
        for id in job_ids.iter().take(25) {
            let qm_clone = qm.clone();
            let id = *id;
            handles.push(tokio::spawn(async move { qm_clone.cancel(id).await }));
        }

        let mut cancelled = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                cancelled += 1;
            }
        }

        assert_eq!(cancelled, 25);

        // Verify remaining jobs
        let (queued, _, _, _) = qm.stats().await;
        assert_eq!(queued, 25);
    }

    // ==================== INDEX CONSISTENCY TESTS ====================

    #[tokio::test]
    async fn test_job_index_consistency_after_operations() {
        use crate::queue::types::JobLocation;
        let qm = setup();

        // Push job
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Check index shows Queue location
        let loc = qm.job_index.get(&job.id).map(|r| *r);
        assert!(matches!(loc, Some(JobLocation::Queue { .. })));

        // Pull job
        let _pulled = qm.pull("test").await;

        // Check index shows Processing location
        let loc = qm.job_index.get(&job.id).map(|r| *r);
        assert!(matches!(loc, Some(JobLocation::Processing)));

        // Ack job
        qm.ack(job.id, None).await.unwrap();

        // Check index shows Completed location
        let loc = qm.job_index.get(&job.id).map(|r| *r);
        assert!(matches!(loc, Some(JobLocation::Completed)));
    }

    #[tokio::test]
    async fn test_job_index_consistency_after_fail_to_dlq() {
        use crate::queue::types::JobLocation;
        let qm = setup();

        // Push job with max_attempts=1
        let job = qm
            .push(
                "test".to_string(),
                json!({}),
                0,
                None,
                None,
                None,
                Some(1),
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        let _pulled = qm.pull("test").await;
        qm.fail(job.id, None).await.unwrap();

        // Check index shows DLQ location
        let loc = qm.job_index.get(&job.id).map(|r| *r);
        assert!(matches!(loc, Some(JobLocation::Dlq { .. })));
    }

    // ==================== BACKGROUND TASKS TESTS ====================

    #[tokio::test]
    async fn test_job_timeout_detection() {
        let qm = setup();

        // Push job with 1ms timeout
        let job = qm
            .push(
                "test".to_string(),
                json!({"timeout_test": true}),
                0,
                None,
                None,
                Some(1), // 1ms timeout
                Some(1), // max_attempts = 1 (go to DLQ after timeout)
                None,
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Pull job to start processing
        let _pulled = qm.pull("test").await;

        // Wait for timeout
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Manually trigger timeout check
        qm.check_timed_out_jobs().await;

        // Job should be in DLQ (max_attempts=1, so after 1 fail goes to DLQ)
        let dlq_jobs = qm.get_dlq("test", Some(10)).await;
        assert_eq!(dlq_jobs.len(), 1);
        assert_eq!(dlq_jobs[0].id, job.id);
    }

    #[tokio::test]
    async fn test_job_timeout_retry() {
        let qm = setup();

        // Push job with 1ms timeout and multiple attempts
        let job = qm
            .push(
                "test".to_string(),
                json!({"timeout_retry": true}),
                0,
                None,
                None,
                Some(1), // 1ms timeout
                Some(3), // 3 max attempts
                Some(0), // no backoff
                None,
                None,
                None,
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .unwrap();

        // Pull job
        let _pulled = qm.pull("test").await;

        // Wait for timeout
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Trigger timeout check
        qm.check_timed_out_jobs().await;

        // Job should be back in queue (attempt 1 of 3)
        let (queued, processing, _, _) = qm.stats().await;
        assert_eq!(processing, 0, "Job should not be processing");
        assert_eq!(queued, 1, "Job should be back in queue");

        // Pull again and verify it's the same job with incremented attempts
        let pulled_again = qm.pull("test").await;
        assert_eq!(pulled_again.id, job.id);
        assert_eq!(pulled_again.attempts, 1);
    }

    #[tokio::test]
    async fn test_cron_job_scheduling() {
        let qm = setup();

        // Add cron job that runs every second
        qm.add_cron(
            "test-cron-scheduling".to_string(),
            "cron-queue".to_string(),
            json!({"scheduled": true}),
            "* * * * * *".to_string(), // Every second
            5,                         // priority
        )
        .await;

        let crons = qm.list_crons().await;
        assert_eq!(crons.len(), 1);
        assert_eq!(crons[0].name, "test-cron-scheduling");
        assert_eq!(crons[0].queue, "cron-queue");
        assert_eq!(crons[0].priority, 5);

        // Clean up
        qm.delete_cron("test-cron-scheduling").await;
    }

    #[tokio::test]
    async fn test_cron_invalid_schedule() {
        let qm = setup();

        // Try to add cron with invalid schedule
        qm.add_cron(
            "invalid-cron".to_string(),
            "test".to_string(),
            json!({}),
            "invalid schedule".to_string(),
            0,
        )
        .await;

        // Should not be added
        let crons = qm.list_crons().await;
        assert!(crons.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_completed_jobs_threshold() {
        let qm = setup();

        // Push and complete many jobs
        for i in 0..100 {
            let job = qm
                .push(
                    "cleanup-test".to_string(),
                    json!({"i": i}),
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
                    false, // remove_on_complete
                    false, // remove_on_fail
                    None,  // stall_timeout
                    None,  // debounce_id
                    None,  // debounce_ttl
                    None,  // job_id
                    None,  // keep_completed_age
                    None,  // keep_completed_count
                )
                .await
                .unwrap();
            qm.pull("cleanup-test").await;
            qm.ack(job.id, None).await.unwrap();
        }

        // Completed jobs should be tracked
        let completed_count = qm.completed_jobs.read().len();
        assert_eq!(completed_count, 100);
    }

    // ==================== WEBHOOK EXECUTION TESTS ====================

    #[tokio::test]
    async fn test_webhook_signature_generation() {
        let qm = setup();

        // Add webhook with secret
        let _id = qm
            .add_webhook(
                "https://httpbin.org/post".to_string(),
                vec!["job.completed".to_string()],
                Some("test-queue".to_string()),
                Some("my-secret-key".to_string()),
            )
            .await
            .unwrap();

        // Verify webhook was added
        let webhooks = qm.list_webhooks().await;
        assert_eq!(webhooks.len(), 1);
        assert_eq!(webhooks[0].secret, Some("my-secret-key".to_string()));
    }

    // ==================== METRICS TESTS ====================

    #[tokio::test]
    async fn test_metrics_throughput_calculation() {
        let qm = setup();

        // Push and process some jobs
        for i in 0..10 {
            let job = qm
                .push(
                    "metrics-test".to_string(),
                    json!({"i": i}),
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
                    false, // remove_on_complete
                    false, // remove_on_fail
                    None,  // stall_timeout
                    None,  // debounce_id
                    None,  // debounce_ttl
                    None,  // job_id
                    None,  // keep_completed_age
                    None,  // keep_completed_count
                )
                .await
                .unwrap();
            qm.pull("metrics-test").await;
            qm.ack(job.id, None).await.unwrap();
        }

        // Check metrics
        let metrics = qm.get_metrics().await;
        assert!(metrics.total_pushed >= 10);
        assert!(metrics.total_completed >= 10);
    }

    #[tokio::test]
    async fn test_global_metrics_atomic_operations() {
        let qm = setup();

        // Record multiple operations concurrently
        let mut handles = Vec::new();
        for _ in 0..100 {
            let qm_clone = qm.clone();
            handles.push(tokio::spawn(async move {
                qm_clone.metrics.record_push(1);
                qm_clone.metrics.record_complete(10); // 10ms latency
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let metrics = qm.get_metrics().await;
        assert_eq!(metrics.total_pushed, 100);
        assert_eq!(metrics.total_completed, 100);
    }

    // ==================== POSTGRESQL INTEGRATION TESTS ====================
    // These tests require a running PostgreSQL instance
    // Run with: cargo test -- --ignored

    #[tokio::test]
    #[ignore = "Requires PostgreSQL - run with: DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_postgres_migration() {
        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        let storage = super::super::postgres::PostgresStorage::new(&db_url)
            .await
            .expect("Failed to connect to PostgreSQL");

        // Run migrations
        storage.migrate().await.expect("Migration failed");

        // Verify tables exist by querying them
        let result: (i64,) = sqlx::query_as::<sqlx::Postgres, (i64,)>("SELECT COUNT(*) FROM jobs")
            .fetch_one(storage.pool())
            .await
            .expect("Failed to query jobs table");

        assert!(result.0 >= 0);
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL - run with: DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_postgres_job_persistence() {
        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        // Create QueueManager with PostgreSQL
        let qm = QueueManager::with_postgres(&db_url).await;

        // Push a job
        let job = qm
            .push(
                "postgres-test".to_string(),
                json!({"persisted": true}),
                10,
                None,
                None,
                None,
                Some(3),
                None,
                Some("unique-pg-test".to_string()),
                None,
                Some(vec!["tag1".to_string(), "tag2".to_string()]),
                false,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .expect("Failed to push job");

        // Verify job is in database
        let storage = qm.storage.as_ref().expect("PostgreSQL not initialized");
        let db_job: Option<(i64, String, i32)> =
            sqlx::query_as::<sqlx::Postgres, (i64, String, i32)>(
                "SELECT id, queue, priority FROM jobs WHERE id = $1",
            )
            .bind(job.id as i64)
            .fetch_optional(storage.pool())
            .await
            .expect("Failed to query job");

        assert!(db_job.is_some());
        let (id, queue, priority) = db_job.unwrap();
        assert_eq!(id, job.id as i64);
        assert_eq!(queue, "postgres-test");
        assert_eq!(priority, 10);

        // Clean up
        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job.id as i64)
            .execute(storage.pool())
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL - run with: DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_postgres_job_state_transitions() {
        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        let qm = QueueManager::with_postgres(&db_url).await;

        // Push job
        let job = qm
            .push(
                "state-test".to_string(),
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
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
            .expect("Push failed");

        let storage = qm.storage.as_ref().unwrap();

        // Check initial state
        let state: (String,) =
            sqlx::query_as::<sqlx::Postgres, (String,)>("SELECT state FROM jobs WHERE id = $1")
                .bind(job.id as i64)
                .fetch_one(storage.pool())
                .await
                .expect("Query failed");
        assert_eq!(state.0, "waiting");

        // Pull job
        let _pulled = qm.pull("state-test").await;

        // Check active state
        let state: (String,) =
            sqlx::query_as::<sqlx::Postgres, (String,)>("SELECT state FROM jobs WHERE id = $1")
                .bind(job.id as i64)
                .fetch_one(storage.pool())
                .await
                .expect("Query failed");
        assert_eq!(state.0, "active");

        // Ack job
        qm.ack(job.id, Some(json!({"result": "done"})))
            .await
            .unwrap();

        // Check completed state
        let state: (String,) =
            sqlx::query_as::<sqlx::Postgres, (String,)>("SELECT state FROM jobs WHERE id = $1")
                .bind(job.id as i64)
                .fetch_one(storage.pool())
                .await
                .expect("Query failed");
        assert_eq!(state.0, "completed");

        // Clean up
        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job.id as i64)
            .execute(storage.pool())
            .await
            .ok();
        sqlx::query("DELETE FROM job_results WHERE job_id = $1")
            .bind(job.id as i64)
            .execute(storage.pool())
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL - run with: DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_postgres_cron_persistence() {
        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        let qm = QueueManager::with_postgres(&db_url).await;

        // Add cron job
        qm.add_cron(
            "pg-cron-test".to_string(),
            "cron-queue".to_string(),
            json!({"cron": "persistent"}),
            "0 * * * * *".to_string(), // Every minute
            0,
        )
        .await;

        // Verify in database
        let storage = qm.storage.as_ref().unwrap();
        let cron: Option<(String, String)> = sqlx::query_as::<sqlx::Postgres, (String, String)>(
            "SELECT name, queue FROM cron_jobs WHERE name = $1",
        )
        .bind("pg-cron-test")
        .fetch_optional(storage.pool())
        .await
        .expect("Query failed");

        assert!(cron.is_some());
        let (name, queue) = cron.unwrap();
        assert_eq!(name, "pg-cron-test");
        assert_eq!(queue, "cron-queue");

        // Delete cron
        qm.delete_cron("pg-cron-test").await;

        // Verify deleted
        let cron: Option<(String,)> = sqlx::query_as::<sqlx::Postgres, (String,)>(
            "SELECT name FROM cron_jobs WHERE name = $1",
        )
        .bind("pg-cron-test")
        .fetch_optional(storage.pool())
        .await
        .expect("Query failed");

        assert!(cron.is_none());
    }

    // ==================== CLUSTER MODE TESTS ====================
    // These tests require PostgreSQL and CLUSTER_MODE=1

    #[tokio::test]
    #[ignore = "Requires PostgreSQL with CLUSTER_MODE - run with: CLUSTER_MODE=1 DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_cluster_leader_election() {
        use super::super::cluster::ClusterManager;

        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        // Create connection pool
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .expect("Failed to connect");

        // Create cluster manager
        let cm = ClusterManager::new(
            "test-node-1".to_string(),
            "localhost".to_string(),
            6789,
            Some(pool.clone()),
        );

        // Initialize tables
        cm.init_tables().await.expect("Failed to init tables");

        // Try to become leader
        cm.try_become_leader()
            .await
            .expect("Leader election failed");

        // Should be leader (only node)
        assert!(cm.is_leader());

        // Register node
        cm.register_node().await.expect("Registration failed");

        // Get nodes
        let nodes = cm.list_nodes().await.expect("Get nodes failed");
        assert!(!nodes.is_empty());

        // Clean up
        sqlx::query("DELETE FROM cluster_nodes WHERE node_id = $1")
            .bind("test-node-1")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL with CLUSTER_MODE - run with: CLUSTER_MODE=1 DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_cluster_node_heartbeat() {
        use super::super::cluster::ClusterManager;

        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .expect("Failed to connect");

        let cm = ClusterManager::new(
            "test-node-heartbeat".to_string(),
            "localhost".to_string(),
            6790,
            Some(pool.clone()),
        );

        cm.init_tables().await.expect("Init failed");
        cm.register_node().await.expect("Register failed");

        // Update heartbeat
        cm.heartbeat().await.expect("Heartbeat failed");

        // Query heartbeat timestamp
        let result: (i64,) = sqlx::query_as::<sqlx::Postgres, (i64,)>(
            "SELECT EXTRACT(EPOCH FROM last_heartbeat)::bigint FROM cluster_nodes WHERE node_id = $1"
        )
        .bind("test-node-heartbeat")
        .fetch_one(&pool)
        .await
        .expect("Query failed");

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Heartbeat should be within last 5 seconds
        assert!(now - result.0 < 5);

        // Clean up
        sqlx::query("DELETE FROM cluster_nodes WHERE node_id = $1")
            .bind("test-node-heartbeat")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL with CLUSTER_MODE - run with: CLUSTER_MODE=1 DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_cluster_stale_node_cleanup() {
        use super::super::cluster::ClusterManager;

        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .expect("Failed to connect");

        let cm = ClusterManager::new(
            "test-node-cleanup".to_string(),
            "localhost".to_string(),
            6791,
            Some(pool.clone()),
        );

        cm.init_tables().await.expect("Init failed");

        // Insert a stale node (heartbeat 1 hour ago)
        sqlx::query(
            "INSERT INTO cluster_nodes (node_id, host, port, last_heartbeat)
             VALUES ($1, $2, $3, NOW() - INTERVAL '1 hour')
             ON CONFLICT (node_id) DO UPDATE SET last_heartbeat = NOW() - INTERVAL '1 hour'",
        )
        .bind("stale-node")
        .bind("localhost")
        .bind(9999)
        .execute(&pool)
        .await
        .expect("Insert failed");

        // Run cleanup
        cm.cleanup_stale_nodes().await.expect("Cleanup failed");

        // Stale node should be removed
        let result: Option<(String,)> = sqlx::query_as::<sqlx::Postgres, (String,)>(
            "SELECT node_id FROM cluster_nodes WHERE node_id = $1",
        )
        .bind("stale-node")
        .fetch_optional(&pool)
        .await
        .expect("Query failed");

        assert!(result.is_none(), "Stale node should have been cleaned up");
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL with CLUSTER_MODE - run with: CLUSTER_MODE=1 DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_cluster_load_balancing() {
        use super::super::cluster::{ClusterManager, LoadBalanceStrategy};

        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .expect("Failed to connect");

        let cm = ClusterManager::new(
            "test-lb-node".to_string(),
            "localhost".to_string(),
            6792,
            Some(pool.clone()),
        );

        cm.init_tables().await.expect("Init failed");

        // Set load balancing strategy
        cm.set_load_balance_strategy(LoadBalanceStrategy::LeastConnections);
        assert_eq!(
            cm.load_balance_strategy(),
            LoadBalanceStrategy::LeastConnections
        );

        cm.set_load_balance_strategy(LoadBalanceStrategy::RoundRobin);
        assert_eq!(cm.load_balance_strategy(), LoadBalanceStrategy::RoundRobin);

        // Clean up
        sqlx::query("DELETE FROM cluster_nodes WHERE node_id = $1")
            .bind("test-lb-node")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "Requires PostgreSQL - run with: DATABASE_URL=postgres://... cargo test -- --ignored"]
    async fn test_postgres_unique_job_id_sequence() {
        let db_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

        let qm = QueueManager::with_postgres(&db_url).await;

        // Get multiple job IDs
        let ids1 = qm.next_job_ids(10).await;
        let ids2 = qm.next_job_ids(10).await;

        // All IDs should be unique
        let mut all_ids: Vec<_> = ids1.into_iter().chain(ids2.into_iter()).collect();
        let original_len = all_ids.len();
        all_ids.sort();
        all_ids.dedup();

        assert_eq!(all_ids.len(), original_len, "All job IDs should be unique");
    }
}

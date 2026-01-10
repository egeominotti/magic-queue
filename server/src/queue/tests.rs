#[cfg(test)]
mod tests {
    use super::super::*;
    use serde_json::json;

    fn setup() -> std::sync::Arc<QueueManager> {
        QueueManager::new(false)
    }

    #[tokio::test]
    async fn test_push_and_pull() {
        let qm = setup();

        let job = qm.push(
            "test".to_string(),
            json!({"key": "value"}),
            0, None, None, None, None, None, None, None
        ).await.unwrap();

        assert!(job.id > 0);
        assert_eq!(job.queue, "test");

        let pulled = qm.pull("test").await;
        assert_eq!(pulled.id, job.id);
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
    async fn test_ack() {
        let qm = setup();

        let job = qm.push("test".to_string(), json!({}), 0, None, None, None, None, None, None, None)
            .await.unwrap();
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

        let job = qm.push("test".to_string(), json!({}), 0, None, None, None, None, None, None, None)
            .await.unwrap();
        let pulled = qm.pull("test").await;

        let result_data = json!({"computed": 42});
        qm.ack(pulled.id, Some(result_data.clone())).await.unwrap();

        // Check result is stored
        let stored = qm.get_result(job.id).await;
        assert!(stored.is_some());
        assert_eq!(stored.unwrap(), result_data);
    }

    #[tokio::test]
    async fn test_fail_and_retry() {
        let qm = setup();

        let job = qm.push("test".to_string(), json!({}), 0, None, None, None, Some(3), None, None, None)
            .await.unwrap();

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
    async fn test_dlq() {
        let qm = setup();

        // Job with max_attempts=1 goes to DLQ after first failure
        let job = qm.push("test".to_string(), json!({}), 0, None, None, None, Some(1), None, None, None)
            .await.unwrap();

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
    async fn test_unique_key() {
        let qm = setup();

        let job1 = qm.push(
            "test".to_string(), json!({}), 0, None, None, None, None, None,
            Some("unique-123".to_string()), None
        ).await;
        assert!(job1.is_ok());

        // Duplicate should fail
        let job2 = qm.push(
            "test".to_string(), json!({}), 0, None, None, None, None, None,
            Some("unique-123".to_string()), None
        ).await;
        assert!(job2.is_err());

        // After ack, key should be released
        let pulled = qm.pull("test").await;
        qm.ack(pulled.id, None).await.unwrap();

        let job3 = qm.push(
            "test".to_string(), json!({}), 0, None, None, None, None, None,
            Some("unique-123".to_string()), None
        ).await;
        assert!(job3.is_ok());
    }

    #[tokio::test]
    async fn test_cancel() {
        let qm = setup();

        let job = qm.push("test".to_string(), json!({}), 0, Some(60000), None, None, None, None, None, None)
            .await.unwrap();

        let result = qm.cancel(job.id).await;
        assert!(result.is_ok());

        // Cancel non-existent should fail
        let result2 = qm.cancel(999999).await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_progress() {
        let qm = setup();

        let _job = qm.push("test".to_string(), json!({}), 0, None, None, None, None, None, None, None)
            .await.unwrap();
        let pulled = qm.pull("test").await;

        // Update progress
        qm.update_progress(pulled.id, 50, Some("halfway".to_string())).await.unwrap();

        let (progress, msg) = qm.get_progress(pulled.id).await.unwrap();
        assert_eq!(progress, 50);
        assert_eq!(msg, Some("halfway".to_string()));
    }

    #[tokio::test]
    async fn test_stats() {
        let qm = setup();

        // Push some jobs
        for i in 0..5 {
            qm.push("test".to_string(), json!({"i": i}), 0, None, None, None, None, None, None, None)
                .await.unwrap();
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
    async fn test_cron() {
        let qm = setup();

        qm.add_cron(
            "test-cron".to_string(),
            "test".to_string(),
            json!({"cron": true}),
            "*/60".to_string(),
            0
        ).await;

        let crons = qm.list_crons().await;
        assert_eq!(crons.len(), 1);
        assert_eq!(crons[0].name, "test-cron");

        let deleted = qm.delete_cron("test-cron").await;
        assert!(deleted);

        let crons_after = qm.list_crons().await;
        assert!(crons_after.is_empty());
    }

    #[tokio::test]
    async fn test_rate_limit() {
        let qm = setup();

        qm.set_rate_limit("test".to_string(), 100).await;

        let _metrics = qm.get_metrics().await;
        // Rate limit should be set (will show in queue metrics once queue exists)

        qm.clear_rate_limit("test").await;
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let qm = setup();

        // Push jobs with different priorities
        qm.push("test".to_string(), json!({"p": 1}), 1, None, None, None, None, None, None, None).await.unwrap();
        qm.push("test".to_string(), json!({"p": 3}), 3, None, None, None, None, None, None, None).await.unwrap();
        qm.push("test".to_string(), json!({"p": 2}), 2, None, None, None, None, None, None, None).await.unwrap();

        // Should get highest priority first
        let j1 = qm.pull("test").await;
        assert_eq!(j1.priority, 3);

        let j2 = qm.pull("test").await;
        assert_eq!(j2.priority, 2);

        let j3 = qm.pull("test").await;
        assert_eq!(j3.priority, 1);
    }

    #[tokio::test]
    async fn test_pull_batch() {
        let qm = setup();

        for i in 0..10 {
            qm.push("test".to_string(), json!({"i": i}), 0, None, None, None, None, None, None, None)
                .await.unwrap();
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
            qm.push("test".to_string(), json!({"i": i}), 0, None, None, None, None, None, None, None)
                .await.unwrap();
        }

        let jobs = qm.pull_batch("test", 5).await;
        let ids: Vec<u64> = jobs.iter().map(|j| j.id).collect();

        let acked = qm.ack_batch(&ids).await;
        assert_eq!(acked, 5);

        let (_, processing, _, _) = qm.stats().await;
        assert_eq!(processing, 0);
    }

    #[tokio::test]
    async fn test_pause_resume() {
        let qm = setup();

        qm.pause("test").await;
        assert!(qm.paused_queues.read().contains("test"));

        qm.resume("test").await;
        assert!(!qm.paused_queues.read().contains("test"));
    }

    #[tokio::test]
    async fn test_concurrency_limit() {
        let qm = setup();

        qm.set_concurrency("test".to_string(), 2).await;

        // Push 3 jobs
        for i in 0..3 {
            qm.push("test".to_string(), json!({"i": i}), 0, None, None, None, None, None, None, None)
                .await.unwrap();
        }

        // Clear limit for cleanup
        qm.clear_concurrency("test").await;
    }

    #[tokio::test]
    async fn test_list_queues() {
        let qm = setup();

        // Push to create queue
        qm.push("queue1".to_string(), json!({}), 0, None, None, None, None, None, None, None)
            .await.unwrap();
        qm.push("queue2".to_string(), json!({}), 0, None, None, None, None, None, None, None)
            .await.unwrap();

        let queues = qm.list_queues().await;
        assert!(queues.len() >= 2);
    }
}

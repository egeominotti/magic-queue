//! Admin and settings operations.
//!
//! Server management, webhooks, workers, metrics, and runtime settings.

use serde_json::Value;
use tokio::sync::broadcast;

use super::manager::{CleanupSettings, QueueDefaults, QueueManager};
use super::types::{now_ms, Webhook, Worker};
use crate::protocol::{
    JobBrowserItem, JobEvent, JobState, MetricsHistoryPoint, WebhookConfig, WorkerInfo,
};

impl QueueManager {
    // ============== Job Browser ==============

    /// List all jobs with filtering options.
    /// Returns jobs sorted by created_at descending (newest first).
    pub fn list_jobs(
        &self,
        queue_filter: Option<&str>,
        state_filter: Option<JobState>,
        limit: usize,
        offset: usize,
    ) -> Vec<JobBrowserItem> {
        let now = now_ms();
        let mut jobs: Vec<JobBrowserItem> = Vec::new();

        // Collect jobs from all shards
        for shard in &self.shards {
            let shard = shard.read();

            // Jobs in queues (waiting/delayed)
            for (queue_name, heap) in &shard.queues {
                if let Some(filter) = queue_filter {
                    if &**queue_name != filter {
                        continue;
                    }
                }
                for job in heap.iter() {
                    let state = if job.run_at > now {
                        JobState::Delayed
                    } else {
                        JobState::Waiting
                    };
                    if let Some(sf) = state_filter {
                        if sf != state {
                            continue;
                        }
                    }
                    jobs.push(JobBrowserItem {
                        job: job.clone(),
                        state,
                    });
                }
            }

            // Jobs in DLQ (failed)
            for (queue_name, dlq) in &shard.dlq {
                if let Some(filter) = queue_filter {
                    if &**queue_name != filter {
                        continue;
                    }
                }
                if let Some(sf) = state_filter {
                    if sf != JobState::Failed {
                        continue;
                    }
                }
                for job in dlq.iter() {
                    jobs.push(JobBrowserItem {
                        job: job.clone(),
                        state: JobState::Failed,
                    });
                }
            }

            // Jobs waiting for dependencies
            for job in shard.waiting_deps.values() {
                if let Some(filter) = queue_filter {
                    if job.queue != filter {
                        continue;
                    }
                }
                if let Some(sf) = state_filter {
                    if sf != JobState::WaitingChildren {
                        continue;
                    }
                }
                jobs.push(JobBrowserItem {
                    job: job.clone(),
                    state: JobState::WaitingChildren,
                });
            }
        }

        // Add jobs in processing (active) - iterate all shards
        for shard in &self.processing_shards {
            let processing = shard.read();
            for job in processing.values() {
                if let Some(filter) = queue_filter {
                    if job.queue != filter {
                        continue;
                    }
                }
                if let Some(sf) = state_filter {
                    if sf != JobState::Active {
                        continue;
                    }
                }
                jobs.push(JobBrowserItem {
                    job: job.clone(),
                    state: JobState::Active,
                });
            }
        }

        // Sort by created_at descending (newest first)
        jobs.sort_by(|a, b| b.job.created_at.cmp(&a.job.created_at));

        // Apply offset and limit
        jobs.into_iter().skip(offset).take(limit).collect()
    }

    // ============== Metrics History ==============

    /// Get metrics history for charts.
    pub fn get_metrics_history(&self) -> Vec<MetricsHistoryPoint> {
        self.metrics_history.read().clone()
    }

    /// Collect and store a metrics history point.
    pub(crate) fn collect_metrics_history(&self) {
        use std::sync::atomic::Ordering;

        let now = now_ms();
        let (queued, processing, _delayed, _dlq) = self.stats_sync();

        let total_completed = self.metrics.total_completed.load(Ordering::Relaxed);
        let total_failed = self.metrics.total_failed.load(Ordering::Relaxed);
        let latency_count = self.metrics.latency_count.load(Ordering::Relaxed);
        let avg_latency = if latency_count > 0 {
            self.metrics.latency_sum.load(Ordering::Relaxed) as f64 / latency_count as f64
        } else {
            0.0
        };

        // Calculate throughput from history
        let throughput = {
            let history = self.metrics_history.read();
            if history.len() >= 2 {
                let prev = &history[history.len() - 1];
                let time_diff = (now - prev.timestamp) as f64 / 1000.0;
                if time_diff > 0.0 {
                    (total_completed - prev.completed) as f64 / time_diff
                } else {
                    0.0
                }
            } else {
                0.0
            }
        };

        let point = MetricsHistoryPoint {
            timestamp: now,
            queued,
            processing,
            completed: total_completed,
            failed: total_failed,
            throughput,
            latency_ms: avg_latency,
        };

        let mut history = self.metrics_history.write();
        history.push(point);

        // Keep only last 60 points (5 minutes at 5s intervals)
        if history.len() > 60 {
            history.remove(0);
        }
    }

    /// Synchronous stats helper for internal use.
    fn stats_sync(&self) -> (usize, usize, usize, usize) {
        let now = now_ms();
        let mut queued = 0;
        let mut delayed = 0;
        let mut dlq_count = 0;

        for shard in &self.shards {
            let shard = shard.read();
            for heap in shard.queues.values() {
                for job in heap.iter() {
                    if job.run_at > now {
                        delayed += 1;
                    } else {
                        queued += 1;
                    }
                }
            }
            for dlq in shard.dlq.values() {
                dlq_count += dlq.len();
            }
        }

        let processing = self.processing_len();
        (queued, processing, delayed, dlq_count)
    }

    // ============== Worker Registration ==============

    /// List active workers.
    pub async fn list_workers(&self) -> Vec<WorkerInfo> {
        let now = now_ms();
        let workers = self.workers.read();
        workers
            .values()
            .filter(|w| now - w.last_heartbeat < 30_000) // Active in last 30s
            .map(|w| WorkerInfo {
                id: w.id.clone(),
                queues: w.queues.clone(),
                concurrency: w.concurrency,
                last_heartbeat: w.last_heartbeat,
                jobs_processed: w.jobs_processed,
            })
            .collect()
    }

    /// Register worker heartbeat.
    pub async fn worker_heartbeat(
        &self,
        id: String,
        queues: Vec<String>,
        concurrency: u32,
        jobs_processed: u64,
    ) {
        let mut workers = self.workers.write();
        let worker = workers
            .entry(id.clone())
            .or_insert_with(|| Worker::new(id, queues.clone(), concurrency));
        worker.queues = queues;
        worker.concurrency = concurrency;
        worker.jobs_processed = jobs_processed;
        worker.last_heartbeat = now_ms();
    }

    /// Increment worker job count.
    #[allow(dead_code)]
    pub(crate) fn increment_worker_jobs(&self, worker_id: &str) {
        if let Some(worker) = self.workers.write().get_mut(worker_id) {
            worker.jobs_processed += 1;
        }
    }

    // ============== Server Management ==============

    /// Reset all server memory - clears all queues, jobs, DLQ, metrics, etc.
    pub async fn reset(&self) {
        // Clear all shards
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            shard.queues.clear();
            shard.dlq.clear();
            shard.unique_keys.clear();
            shard.waiting_deps.clear();
            shard.waiting_children.clear();
            shard.queue_state.clear();
        }

        // Clear global structures (sharded processing)
        for shard in &self.processing_shards {
            shard.write().clear();
        }
        self.cron_jobs.write().clear();
        self.completed_jobs.write().clear();
        self.job_results.write().clear();

        // Clear job index (DashMap)
        self.job_index.clear();

        // Clear workers
        self.workers.write().clear();

        // Clear metrics history
        self.metrics_history.write().clear();

        // Clear job logs
        self.job_logs.write().clear();

        // Clear stalled count
        self.stalled_count.write().clear();

        // Clear debounce cache
        self.debounce_cache.write().clear();

        // Clear custom ID map
        self.custom_id_map.write().clear();

        // Clear job waiters
        self.job_waiters.write().clear();

        // Clear completed retention
        self.completed_retention.write().clear();

        // Reset metrics counters
        self.metrics
            .total_pushed
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .total_completed
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .total_failed
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .total_timed_out
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Clear all queues (waiting jobs only).
    pub async fn clear_all_queues(&self) -> u64 {
        let mut total = 0u64;
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            for queue in shard.queues.values_mut() {
                total += queue.len() as u64;
                queue.clear();
            }
        }
        self.job_index.clear();
        total
    }

    /// Clear all DLQ.
    pub async fn clear_all_dlq(&self) -> u64 {
        let mut total = 0u64;
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            for dlq in shard.dlq.values_mut() {
                total += dlq.len() as u64;
                dlq.clear();
            }
        }
        total
    }

    /// Clear completed jobs.
    pub async fn clear_completed_jobs(&self) -> u64 {
        let total = self.completed_jobs.read().len() as u64;
        self.completed_jobs.write().clear();
        self.job_results.write().clear();
        self.completed_retention.write().clear();
        total
    }

    /// Reset metrics.
    pub async fn reset_metrics(&self) {
        self.metrics
            .total_pushed
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .total_completed
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .total_failed
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .total_timed_out
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics_history.write().clear();
    }

    // ============== Webhooks ==============

    /// List all webhooks.
    pub async fn list_webhooks(&self) -> Vec<WebhookConfig> {
        let webhooks = self.webhooks.read();
        webhooks
            .values()
            .map(|w| WebhookConfig {
                id: w.id.clone(),
                url: w.url.clone(),
                events: w.events.clone(),
                queue: w.queue.clone(),
                secret: w.secret.clone(),
                created_at: w.created_at,
            })
            .collect()
    }

    /// Add a webhook.
    pub async fn add_webhook(
        &self,
        url: String,
        events: Vec<String>,
        queue: Option<String>,
        secret: Option<String>,
    ) -> String {
        let id = format!("wh_{}", crate::protocol::next_id());
        let webhook = Webhook::new(id.clone(), url, events, queue, secret);
        self.webhooks.write().insert(id.clone(), webhook);
        id
    }

    /// Delete a webhook.
    pub async fn delete_webhook(&self, id: &str) -> bool {
        self.webhooks.write().remove(id).is_some()
    }

    /// Fire webhooks for an event.
    pub(crate) fn fire_webhooks(
        &self,
        event_type: &str,
        queue: &str,
        job_id: u64,
        data: Option<&Value>,
        error: Option<&str>,
    ) {
        let webhooks = self.webhooks.read();
        for webhook in webhooks.values() {
            // Check event type matches
            if !webhook.events.iter().any(|e| e == event_type || e == "*") {
                continue;
            }
            // Check queue filter
            if let Some(ref wq) = webhook.queue {
                if wq != queue {
                    continue;
                }
            }

            let url = webhook.url.clone();
            let secret = webhook.secret.clone();
            let payload = serde_json::json!({
                "event": event_type,
                "queue": queue,
                "job_id": job_id,
                "timestamp": now_ms(),
                "data": data,
                "error": error,
            });

            // Fire webhook in background (non-blocking)
            let webhook_url = url.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new());

                let mut req = client.post(&url).json(&payload);

                if let Some(secret) = secret {
                    // Add HMAC signature header
                    let body = serde_json::to_string(&payload).unwrap_or_default();
                    let signature = hmac_sha256(&secret, &body);
                    req = req.header("X-FlashQ-Signature", signature);
                }

                match req.send().await {
                    Ok(response) => {
                        if !response.status().is_success() {
                            eprintln!(
                                "Webhook failed: {} returned status {}",
                                webhook_url,
                                response.status()
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Webhook error: {} - {}", webhook_url, e);
                    }
                }
            });
        }
    }

    // ============== Event Broadcasting (SSE/WebSocket) ==============

    /// Subscribe to job events.
    pub fn subscribe_events(&self, _queue: Option<String>) -> broadcast::Receiver<JobEvent> {
        self.event_tx.subscribe()
    }

    /// Broadcast a job event.
    pub(crate) fn broadcast_event(&self, event: JobEvent) {
        // Fire webhooks first (uses references, no clone needed)
        self.fire_webhooks(
            &event.event_type,
            &event.queue,
            event.job_id,
            event.data.as_ref(),
            event.error.as_deref(),
        );

        // Then send to broadcast channel (consumes ownership)
        let _ = self.event_tx.send(event);
    }

    // ============== Runtime Settings ==============

    /// Set auth tokens at runtime.
    pub fn set_auth_tokens(&self, tokens: Vec<String>) {
        let mut auth = self.auth_tokens.write();
        auth.clear();
        for token in tokens {
            if !token.is_empty() {
                auth.insert(token);
            }
        }
    }

    /// Set queue defaults at runtime.
    pub fn set_queue_defaults(
        &self,
        timeout: Option<u64>,
        max_attempts: Option<u32>,
        backoff: Option<u64>,
        ttl: Option<u64>,
    ) {
        let mut defaults = self.queue_defaults.write();
        defaults.timeout = timeout;
        defaults.max_attempts = max_attempts;
        defaults.backoff = backoff;
        defaults.ttl = ttl;
    }

    /// Get queue defaults.
    #[allow(dead_code)]
    pub fn get_queue_defaults(&self) -> QueueDefaults {
        self.queue_defaults.read().clone()
    }

    /// Set cleanup settings at runtime.
    pub fn set_cleanup_settings(
        &self,
        max_completed_jobs: Option<usize>,
        max_job_results: Option<usize>,
        cleanup_interval_secs: Option<u64>,
        metrics_history_size: Option<usize>,
    ) {
        let mut settings = self.cleanup_settings.write();
        if let Some(v) = max_completed_jobs {
            settings.max_completed_jobs = v;
        }
        if let Some(v) = max_job_results {
            settings.max_job_results = v;
        }
        if let Some(v) = cleanup_interval_secs {
            settings.cleanup_interval_secs = v;
        }
        if let Some(v) = metrics_history_size {
            settings.metrics_history_size = v;
        }
    }

    /// Get cleanup settings.
    #[allow(dead_code)]
    pub fn get_cleanup_settings(&self) -> CleanupSettings {
        self.cleanup_settings.read().clone()
    }

    /// Run cleanup immediately.
    pub fn run_cleanup(&self) {
        let settings = self.cleanup_settings.read().clone();

        // Cleanup completed jobs
        let mut completed = self.completed_jobs.write();
        if completed.len() > settings.max_completed_jobs {
            let to_remove = completed.len() - settings.max_completed_jobs / 2;
            let ids: Vec<_> = completed.iter().take(to_remove).copied().collect();
            for id in ids {
                completed.remove(&id);
            }
        }

        // Cleanup job results
        let mut results = self.job_results.write();
        if results.len() > settings.max_job_results {
            let to_remove = results.len() - settings.max_job_results / 2;
            let ids: Vec<_> = results.keys().take(to_remove).copied().collect();
            for id in ids {
                results.remove(&id);
            }
        }

        // Cleanup job index (DashMap - iterate and remove)
        let index_len = self.job_index.len();
        if index_len > 100000 {
            let to_remove = index_len - 50000;
            let ids: Vec<_> = self
                .job_index
                .iter()
                .take(to_remove)
                .map(|r| *r.key())
                .collect();
            for id in ids {
                self.job_index.remove(&id);
            }
        }
    }

    /// Get TCP connection count.
    pub fn connection_count(&self) -> usize {
        self.tcp_connection_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Increment TCP connection count.
    #[allow(dead_code)]
    pub fn increment_connections(&self) {
        self.tcp_connection_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Decrement TCP connection count.
    #[allow(dead_code)]
    pub fn decrement_connections(&self) {
        self.tcp_connection_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// HMAC-SHA256 for webhook signatures using proper crypto libraries.
fn hmac_sha256(key: &str, message: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let mut mac =
        HmacSha256::new_from_slice(key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());

    let result = mac.finalize();
    let bytes = result.into_bytes();

    // Convert to hex string
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

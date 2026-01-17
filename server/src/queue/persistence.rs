//! PostgreSQL persistence operations.
//!
//! All persist_* methods for storing job state changes to PostgreSQL.

use std::sync::Arc;

use serde_json::Value;

use super::manager::QueueManager;
use crate::protocol::{CronJob, Job, WebhookConfig};

impl QueueManager {
    // ============== Persistence Methods (PostgreSQL) ==============

    /// Persist a pushed job to PostgreSQL and notify cluster.
    /// In snapshot mode, only records the change (actual persistence happens in background snapshot).
    #[inline]
    pub(crate) fn persist_push(&self, job: &Job, state: &str) {
        // In snapshot mode, just record the change
        if self.is_snapshot_mode() {
            self.record_change();
            return;
        }

        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let job = job.clone();
            let state = state.to_string();
            let node_id = self.node_id();
            let is_cluster = self.is_cluster_enabled();
            tokio::spawn(async move {
                // First persist to PostgreSQL
                if let Err(e) = storage.insert_job(&job, &state).await {
                    eprintln!("Failed to persist job {}: {}", job.id, e);
                    return;
                }
                // Then notify cluster (only after INSERT succeeds)
                if is_cluster {
                    storage
                        .notify_job_pushed(job.id, &job.queue, &node_id.unwrap_or_default())
                        .await;
                }
            });
        }
    }

    /// Persist a batch of jobs to PostgreSQL and notify cluster.
    /// In snapshot mode, only records the change.
    #[inline]
    pub(crate) fn persist_push_batch(&self, jobs: &[Job], state: &str) {
        if jobs.is_empty() {
            return;
        }

        // In snapshot mode, just record the changes
        if self.is_snapshot_mode() {
            self.snapshot_changes
                .fetch_add(jobs.len() as u64, std::sync::atomic::Ordering::Relaxed);
            return;
        }

        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let jobs = jobs.to_vec();
            let state = state.to_string();
            let node_id = self.node_id();
            let is_cluster = self.is_cluster_enabled();
            // Capture queue name before moving jobs
            let queue = jobs.first().map(|j| j.queue.clone()).unwrap_or_default();
            let job_ids: Vec<u64> = jobs.iter().map(|j| j.id).collect();
            tokio::spawn(async move {
                // First persist to PostgreSQL
                if let Err(e) = storage.insert_jobs_batch(&jobs, &state).await {
                    eprintln!("Failed to persist batch: {}", e);
                    return;
                }
                // Then notify cluster (only after INSERT succeeds)
                if is_cluster {
                    storage
                        .notify_jobs_pushed(&job_ids, &queue, &node_id.unwrap_or_default())
                        .await;
                }
            });
        }
    }

    /// Persist job acknowledgment to PostgreSQL.
    /// In snapshot mode, only records the change.
    #[inline]
    pub(crate) fn persist_ack(&self, job_id: u64, result: Option<Value>) {
        if self.is_snapshot_mode() {
            self.record_change();
            // Still store results in memory (they'll be available until cleanup)
            if let Some(res) = result {
                self.job_results.write().insert(job_id, res);
            }
            return;
        }

        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            tokio::spawn(async move {
                if let Err(e) = storage.ack_job(job_id, result).await {
                    eprintln!("Failed to persist ack {}: {}", job_id, e);
                }
            });
        }
    }

    /// Persist batch acknowledgments to PostgreSQL.
    /// In snapshot mode, only records the change.
    #[inline]
    pub(crate) fn persist_ack_batch(&self, ids: &[u64]) {
        if ids.is_empty() {
            return;
        }

        if self.is_snapshot_mode() {
            self.snapshot_changes
                .fetch_add(ids.len() as u64, std::sync::atomic::Ordering::Relaxed);
            return;
        }

        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let ids = ids.to_vec();
            tokio::spawn(async move {
                if let Err(e) = storage.ack_jobs_batch(&ids).await {
                    eprintln!("Failed to persist ack batch: {}", e);
                }
            });
        }
    }

    /// Persist job failure (retry) to PostgreSQL.
    /// In snapshot mode, only records the change.
    #[inline]
    pub(crate) fn persist_fail(&self, job_id: u64, _new_run_at: u64, _attempts: u32) {
        if self.is_snapshot_mode() {
            self.record_change();
            return;
        }

        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            tokio::spawn(async move {
                if let Err(e) = storage.fail_job(job_id, _new_run_at, _attempts).await {
                    eprintln!("Failed to persist fail {}: {}", job_id, e);
                }
            });
        }
    }

    /// Persist job moved to DLQ.
    /// In snapshot mode, only records the change.
    #[inline]
    pub(crate) fn persist_dlq(&self, job: &Job, error: Option<&str>) {
        if self.is_snapshot_mode() {
            self.record_change();
            return;
        }

        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let job = job.clone();
            let error = error.map(|s| s.to_string());
            tokio::spawn(async move {
                if let Err(e) = storage.move_to_dlq(&job, error.as_deref()).await {
                    eprintln!("Failed to persist DLQ {}: {}", job.id, e);
                }
            });
        }
    }

    /// Persist job cancellation.
    /// In snapshot mode, only records the change.
    #[inline]
    pub(crate) fn persist_cancel(&self, job_id: u64) {
        if self.is_snapshot_mode() {
            self.record_change();
            return;
        }

        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            tokio::spawn(async move {
                if let Err(e) = storage.cancel_job(job_id).await {
                    eprintln!("Failed to persist cancel {}: {}", job_id, e);
                }
            });
        }
    }

    /// Persist cron job.
    #[inline]
    pub(crate) fn persist_cron(&self, cron: &CronJob) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let cron = cron.clone();
            tokio::spawn(async move {
                if let Err(e) = storage.save_cron(&cron).await {
                    eprintln!("Failed to persist cron {}: {}", cron.name, e);
                }
            });
        }
    }

    /// Persist cron job deletion.
    #[inline]
    pub(crate) fn persist_cron_delete(&self, name: &str) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let name = name.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage.delete_cron(&name).await {
                    eprintln!("Failed to persist cron delete {}: {}", name, e);
                }
            });
        }
    }

    /// Persist cron next_run update.
    #[inline]
    pub(crate) fn persist_cron_next_run(&self, name: &str, next_run: u64) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let name = name.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage.update_cron_next_run(&name, next_run).await {
                    eprintln!("Failed to update cron next_run {}: {}", name, e);
                }
            });
        }
    }

    /// Persist webhook.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn persist_webhook(&self, webhook: &WebhookConfig) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let webhook = webhook.clone();
            tokio::spawn(async move {
                if let Err(e) = storage.save_webhook(&webhook).await {
                    eprintln!("Failed to persist webhook {}: {}", webhook.id, e);
                }
            });
        }
    }

    /// Persist webhook deletion.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn persist_webhook_delete(&self, id: &str) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let id = id.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage.delete_webhook(&id).await {
                    eprintln!("Failed to persist webhook delete {}: {}", id, e);
                }
            });
        }
    }

    /// Notify event subscribers.
    pub(crate) fn notify_subscribers(&self, event: &str, queue: &str, job: &Job) {
        let subs = self.subscribers.read();
        for sub in subs.iter() {
            if sub.queue.as_str() == queue && sub.events.contains(&event.to_string()) {
                let msg = serde_json::json!({
                    "event": event,
                    "queue": queue,
                    "job": job
                })
                .to_string();
                let _ = sub.tx.send(msg);
            }
        }
    }

    /// Notify shard's waiting workers.
    #[inline]
    pub(crate) fn notify_shard(&self, idx: usize) {
        self.shards[idx].read().notify.notify_waiters();
    }

    /// Notify all shards - wakes up workers that may have missed push notifications.
    #[inline]
    pub(crate) fn notify_all(&self) {
        for shard in &self.shards {
            shard.read().notify.notify_waiters();
        }
    }
}

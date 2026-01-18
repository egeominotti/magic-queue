//! Dead Letter Queue (DLQ) operations.
//!
//! Handles jobs that have exceeded max_attempts and need manual intervention.

use tracing::error;

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation};
use crate::protocol::Job;

impl QueueManager {
    /// Get jobs from the dead letter queue for a specific queue.
    pub async fn get_dlq(&self, queue_name: &str, count: Option<usize>) -> Vec<Job> {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let shard = self.shards[idx].read();
        shard.dlq.get(&queue_arc).map_or(Vec::new(), |dlq| {
            dlq.iter().take(count.unwrap_or(100)).cloned().collect()
        })
    }

    /// Retry jobs from the dead letter queue.
    /// If job_id is provided, retry only that job. Otherwise, retry all jobs.
    pub async fn retry_dlq(&self, queue_name: &str, job_id: Option<u64>) -> usize {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut shard = self.shards[idx].write();
        let now = now_ms();
        let mut jobs_to_retry = Vec::new();

        if let Some(dlq) = shard.dlq.get_mut(&queue_arc) {
            if let Some(id) = job_id {
                if let Some(pos) = dlq.iter().position(|j| j.id == id) {
                    // Safe: position() just found this index
                    let mut job = dlq
                        .remove(pos)
                        .expect("position valid after iter().position()");
                    job.attempts = 0;
                    job.run_at = now;
                    jobs_to_retry.push(job);
                }
            } else {
                while let Some(mut job) = dlq.pop_front() {
                    job.attempts = 0;
                    job.run_at = now;
                    jobs_to_retry.push(job);
                }
            }
        }

        let retried = jobs_to_retry.len();
        if retried > 0 {
            let heap = shard.queues.entry(queue_arc.clone()).or_default();
            for job in jobs_to_retry {
                self.index_job(job.id, JobLocation::Queue { shard_idx: idx });
                heap.push(job);
            }
            drop(shard);
            self.notify_shard(idx);
        }
        retried
    }

    /// Purge all jobs from the dead letter queue for a specific queue.
    pub async fn purge_dlq(&self, queue_name: &str) -> usize {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut shard = self.shards[idx].write();

        if let Some(dlq) = shard.dlq.get_mut(&queue_arc) {
            // Collect job IDs to remove from index
            let job_ids: Vec<u64> = dlq.iter().map(|j| j.id).collect();
            let count = dlq.len();
            dlq.clear();
            drop(shard); // Release shard lock

            // Remove job index entries (lock-free DashMap)
            for id in &job_ids {
                self.job_index.remove(id);
            }

            // Persist to PostgreSQL
            if let Some(ref storage) = self.storage {
                let storage = std::sync::Arc::clone(storage);
                let queue = queue_name.to_string();
                tokio::spawn(async move {
                    if let Err(e) = storage.purge_dlq(&queue).await {
                        error!(queue = %queue, error = %e, "Failed to persist purge_dlq");
                    }
                });
            }

            count
        } else {
            0
        }
    }
}

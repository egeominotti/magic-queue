//! Job query operations.
//!
//! Get job state, lookup by ID, wait for completion.

use serde_json::Value;

use super::manager::QueueManager;
use super::types::{now_ms, JobLocation};
use crate::protocol::JobState;

impl QueueManager {
    /// Get job by ID with its current state - O(1) lookup via job_index (lock-free).
    pub fn get_job(&self, id: u64) -> (Option<crate::protocol::Job>, JobState) {
        let now = now_ms();

        // O(1) lock-free lookup in DashMap
        let location = match self.job_index.get(&id) {
            Some(loc) => *loc,
            None => return (None, JobState::Unknown),
        };

        match location {
            JobLocation::Processing => {
                let job = self.processing_get(id);
                let state = job
                    .as_ref()
                    .map(|j| location.to_state(j.run_at, now))
                    .unwrap_or(JobState::Active);
                (job, state)
            }
            JobLocation::Queue { shard_idx } => {
                let shard = self.shards[shard_idx].read();
                for heap in shard.queues.values() {
                    if let Some(job) = heap.iter().find(|j| j.id == id) {
                        return (Some(job.clone()), location.to_state(job.run_at, now));
                    }
                }
                (None, JobState::Unknown)
            }
            JobLocation::Dlq { shard_idx } => {
                let shard = self.shards[shard_idx].read();
                for dlq in shard.dlq.values() {
                    if let Some(job) = dlq.iter().find(|j| j.id == id) {
                        return (Some(job.clone()), JobState::Failed);
                    }
                }
                (None, JobState::Unknown)
            }
            JobLocation::WaitingDeps { shard_idx } => {
                let shard = self.shards[shard_idx].read();
                let job = shard.waiting_deps.get(&id).cloned();
                (job, JobState::WaitingChildren)
            }
            JobLocation::WaitingChildren { shard_idx } => {
                let shard = self.shards[shard_idx].read();
                let job = shard.waiting_children.get(&id).cloned();
                (job, JobState::WaitingParent)
            }
            JobLocation::Completed => {
                // Job completed, no data stored (only result if any)
                (None, JobState::Completed)
            }
        }
    }

    /// Get only the state of a job by ID - O(1) lock-free lookup.
    #[inline]
    pub fn get_state(&self, id: u64) -> JobState {
        let now = now_ms();

        match self.job_index.get(&id) {
            Some(location) => {
                let location = *location;
                // For Queue state, we need run_at to determine Waiting vs Delayed
                if let JobLocation::Queue { shard_idx } = location {
                    let shard = self.shards[shard_idx].read();
                    for heap in shard.queues.values() {
                        if let Some(job) = heap.iter().find(|j| j.id == id) {
                            return location.to_state(job.run_at, now);
                        }
                    }
                    JobState::Unknown
                } else {
                    location.to_state(0, now)
                }
            }
            None => JobState::Unknown,
        }
    }

    /// Get job by internal ID - returns just the Job (for idempotency check).
    pub fn get_job_by_internal_id(&self, id: u64) -> Option<crate::protocol::Job> {
        self.get_job(id).0
    }

    /// Get job by custom ID.
    pub fn get_job_by_custom_id(
        &self,
        custom_id: &str,
    ) -> Option<(crate::protocol::Job, JobState)> {
        let internal_id = self.custom_id_map.read().get(custom_id).copied()?;
        let (job, state) = self.get_job(internal_id);
        job.map(|j| (j, state))
    }

    /// Get multiple jobs by IDs in a single call (batch status).
    pub async fn get_jobs_batch(&self, ids: &[u64]) -> Vec<crate::protocol::JobBrowserItem> {
        ids.iter()
            .filter_map(|&id| {
                let (job, state) = self.get_job(id);
                job.map(|j| crate::protocol::JobBrowserItem { job: j, state })
            })
            .collect()
    }

    /// Wait for job to complete and return result (finished() promise).
    pub async fn wait_for_job(
        &self,
        id: u64,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Value>, String> {
        let timeout = timeout_ms.unwrap_or(30000);

        // First check if job is already completed
        let state = self.get_state(id);
        if state == JobState::Completed {
            return Ok(self.job_results.read().get(&id).cloned());
        }
        if state == JobState::Failed {
            return Err("Job failed".to_string());
        }
        if state == JobState::Unknown {
            return Err("Job not found".to_string());
        }

        // Create a oneshot channel to wait for completion
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Register the waiter
        self.job_waiters.write().entry(id).or_default().push(tx);

        // Wait with timeout
        match tokio::time::timeout(std::time::Duration::from_millis(timeout), rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err("Waiter channel closed".to_string()),
            Err(_) => Err(format!(
                "Timeout waiting for job {} after {}ms",
                id, timeout
            )),
        }
    }

    /// Notify all waiters when a job completes.
    pub fn notify_job_waiters(&self, job_id: u64, result: Option<Value>) {
        let waiters = self.job_waiters.write().remove(&job_id);
        if let Some(waiters) = waiters {
            for tx in waiters {
                let _ = tx.send(result.clone());
            }
        }
    }

    /// Track job location in index (lock-free DashMap).
    #[inline]
    pub(crate) fn index_job(&self, id: u64, location: JobLocation) {
        self.job_index.insert(id, location);
    }

    /// Remove job from index (lock-free DashMap).
    #[inline]
    pub(crate) fn unindex_job(&self, id: u64) {
        self.job_index.remove(&id);
    }

    /// Remove job from index - alias for compatibility.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn remove_job_index(&self, id: u64) {
        self.job_index.remove(&id);
    }
}

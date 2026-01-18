//! Pull operations for retrieving jobs from the queue.
//!
//! Contains `pull`, `pull_batch`, and distributed pull implementations.

use std::sync::Arc;

use tokio::time::Duration;
use tracing::warn;

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation};
use crate::protocol::Job;

impl QueueManager {
    /// Pull a job from the queue. Blocks until a job is available.
    pub async fn pull(&self, queue_name: &str) -> Job {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);

        enum PullResult {
            Job(Box<Job>),
            Wait,
            SleepPaused,
            SleepRateLimit,
            SleepConcurrency,
        }

        loop {
            let now = now_ms();

            let pull_result = {
                let mut shard = self.shards[idx].write();
                let state = shard.get_state(&queue_arc);

                if state.paused {
                    PullResult::SleepPaused
                } else if state
                    .rate_limiter
                    .as_mut()
                    .is_some_and(|l| !l.try_acquire())
                {
                    PullResult::SleepRateLimit
                } else if state.concurrency.as_mut().is_some_and(|c| !c.try_acquire()) {
                    PullResult::SleepConcurrency
                } else {
                    let mut result = None;
                    if let Some(heap) = shard.queues.get_mut(&queue_arc) {
                        loop {
                            // Safe pattern: pop directly and check, avoiding peek+expect race
                            match heap.pop() {
                                Some(job) if job.is_expired(now) => {
                                    // Expired job, continue to next
                                    continue;
                                }
                                Some(mut job) if job.is_ready(now) => {
                                    job.started_at = now;
                                    job.last_heartbeat = now;
                                    result = Some(job);
                                    break;
                                }
                                Some(job) => {
                                    // Job not ready yet (delayed), put it back and stop
                                    heap.push(job);
                                    break;
                                }
                                None => break,
                            }
                        }
                    }

                    if let Some(job) = result {
                        PullResult::Job(Box::new(job))
                    } else {
                        let state = shard.get_state(&queue_arc);
                        if let Some(ref mut conc) = state.concurrency {
                            conc.release();
                        }
                        PullResult::Wait
                    }
                }
            };

            match pull_result {
                PullResult::Job(job) => {
                    self.index_job(job.id, JobLocation::Processing);
                    self.processing_insert((*job).clone());
                    self.metrics.record_pull();
                    return *job;
                }
                PullResult::SleepPaused => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                PullResult::SleepRateLimit | PullResult::SleepConcurrency => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                PullResult::Wait => {
                    // Simple polling - wait for notify with timeout
                    let notify = {
                        let shard = self.shards[idx].read();
                        Arc::clone(&shard.notify)
                    };
                    // Wait max 100ms then retry
                    let _ =
                        tokio::time::timeout(Duration::from_millis(100), notify.notified()).await;
                }
            }
        }
    }

    /// Pull multiple jobs from the queue. Blocks until at least one job is available.
    pub async fn pull_batch(&self, queue_name: &str, count: usize) -> Vec<Job> {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut result = Vec::with_capacity(count);

        enum BatchResult {
            Jobs(Vec<Job>),
            Paused,
            Wait,
        }

        loop {
            let now = now_ms();

            let batch_result = {
                let mut shard = self.shards[idx].write();
                let state = shard.get_state(&queue_arc);

                if state.paused {
                    BatchResult::Paused
                } else {
                    let has_concurrency = state.concurrency.is_some();
                    let mut jobs = Vec::new();
                    let mut slots_acquired = 0usize;

                    if has_concurrency {
                        let state = shard.get_state(&queue_arc);
                        if let Some(ref mut conc) = state.concurrency {
                            while slots_acquired < count && conc.try_acquire() {
                                slots_acquired += 1;
                            }
                        }
                    } else {
                        slots_acquired = count;
                    }

                    if slots_acquired > 0 {
                        if let Some(heap) = shard.queues.get_mut(&queue_arc) {
                            while jobs.len() < slots_acquired {
                                // Safe pattern: pop directly and check, avoiding peek+expect race
                                match heap.pop() {
                                    Some(job) if job.is_expired(now) => {
                                        // Expired job, continue to next
                                        continue;
                                    }
                                    Some(mut job) if job.is_ready(now) => {
                                        job.started_at = now;
                                        job.last_heartbeat = now;
                                        jobs.push(job);
                                    }
                                    Some(job) => {
                                        // Job not ready yet (delayed), put it back and stop
                                        heap.push(job);
                                        break;
                                    }
                                    None => break,
                                }
                            }
                        }

                        if has_concurrency && jobs.len() < slots_acquired {
                            let state = shard.get_state(&queue_arc);
                            if let Some(ref mut conc) = state.concurrency {
                                for _ in 0..(slots_acquired - jobs.len()) {
                                    conc.release();
                                }
                            }
                        }
                    }

                    if jobs.is_empty() {
                        BatchResult::Wait
                    } else {
                        BatchResult::Jobs(jobs)
                    }
                }
            };

            match batch_result {
                BatchResult::Jobs(jobs) => {
                    for job in jobs {
                        self.index_job(job.id, JobLocation::Processing);
                        self.processing_insert(job.clone());
                        self.metrics.record_pull();
                        result.push(job);
                    }
                    return result;
                }
                BatchResult::Paused => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                BatchResult::Wait => {
                    // Simple polling - wait for notify with timeout
                    let notify = {
                        let shard = self.shards[idx].read();
                        Arc::clone(&shard.notify)
                    };
                    let _ =
                        tokio::time::timeout(Duration::from_millis(100), notify.notified()).await;
                }
            }
        }
    }

    // ============== Distributed Pull (Cluster Mode) ==============

    /// Pull a job using PostgreSQL's SELECT FOR UPDATE SKIP LOCKED.
    /// This prevents duplicate job processing across cluster nodes.
    /// Use this in cluster mode for consistency at the cost of performance.
    /// Uses exponential backoff on errors to avoid hammering a failed database.
    pub async fn pull_distributed(&self, queue_name: &str, timeout_ms: u64) -> Option<Job> {
        let storage = self.storage.as_ref()?;
        let deadline = now_ms() + timeout_ms;
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut backoff_ms = 50u64; // Initial backoff

        loop {
            let now = now_ms();

            // Check if queue is paused
            let is_paused = {
                let shard = self.shards[idx].read();
                let state = shard.queue_state.get(&queue_arc);
                state.map(|s| s.paused).unwrap_or(false)
            };
            if is_paused {
                if now >= deadline {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Try to claim a job atomically from PostgreSQL
            match storage.pull_job_distributed(queue_name, now as i64).await {
                Ok(Some(job)) => {
                    // Update local state for consistency
                    self.processing_insert(job.clone());
                    self.index_job(job.id, JobLocation::Processing);

                    // Remove from local queue if present (sync)
                    {
                        let mut shard = self.shards[idx].write();
                        if let Some(heap) = shard.queues.get_mut(&queue_arc) {
                            heap.retain(|j| j.id != job.id);
                        }
                    }

                    return Some(job);
                }
                Ok(None) => {
                    // No jobs available - reset backoff
                    backoff_ms = 50;
                    if now >= deadline {
                        return None;
                    }
                    // Wait a bit before retrying
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    warn!(queue = %queue_name, error = %e, "Distributed pull error");
                    if now >= deadline {
                        return None;
                    }
                    // Exponential backoff on errors (max 5 seconds)
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(5000);
                }
            }
        }
    }

    /// Pull multiple jobs using PostgreSQL's SELECT FOR UPDATE SKIP LOCKED.
    /// Batch version of pull_distributed for better performance.
    /// Uses exponential backoff on errors to avoid hammering a failed database.
    pub async fn pull_distributed_batch(
        &self,
        queue_name: &str,
        count: usize,
        timeout_ms: u64,
    ) -> Vec<Job> {
        let storage = match self.storage.as_ref() {
            Some(s) => s,
            None => return Vec::new(),
        };
        let deadline = now_ms() + timeout_ms;
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut backoff_ms = 50u64; // Initial backoff

        loop {
            let now = now_ms();

            // Check if queue is paused
            let is_paused = {
                let shard = self.shards[idx].read();
                let state = shard.queue_state.get(&queue_arc);
                state.map(|s| s.paused).unwrap_or(false)
            };
            if is_paused {
                if now >= deadline {
                    return Vec::new();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Try to claim jobs atomically from PostgreSQL
            // Clamp count to i32::MAX to prevent overflow
            let count_i32 = count.min(i32::MAX as usize) as i32;
            match storage
                .pull_jobs_distributed_batch(queue_name, count_i32, now as i64)
                .await
            {
                Ok(jobs) if !jobs.is_empty() => {
                    // Update local state (sharded processing)
                    for job in &jobs {
                        self.processing_insert(job.clone());
                        self.index_job(job.id, JobLocation::Processing);
                    }

                    // Remove from local queue if present
                    {
                        let job_ids: std::collections::HashSet<u64> =
                            jobs.iter().map(|j| j.id).collect();
                        let mut shard = self.shards[idx].write();
                        if let Some(heap) = shard.queues.get_mut(&queue_arc) {
                            heap.retain(|j| !job_ids.contains(&j.id));
                        }
                    }

                    return jobs;
                }
                Ok(_) => {
                    // No jobs available - reset backoff
                    backoff_ms = 50;
                    if now >= deadline {
                        return Vec::new();
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    warn!(queue = %queue_name, error = %e, "Distributed batch pull error");
                    if now >= deadline {
                        return Vec::new();
                    }
                    // Exponential backoff on errors (max 5 seconds)
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(5000);
                }
            }
        }
    }
}

//! Pull operations for retrieving jobs from the queue.
//!
//! Contains `pull`, `pull_batch`, and distributed pull implementations.
//!
//! ## Performance Optimizations
//!
//! - **Reduced lock contention**: Read-only checks (paused, rate limit peek) use read lock
//! - **Exponential backoff**: Prevents thundering herd when no jobs available
//! - **Avoid cloning**: Jobs moved directly to processing without intermediate clone

use std::sync::Arc;

use tokio::time::Duration;
use tracing::warn;

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation};
use crate::protocol::Job;

impl QueueManager {
    /// Optimized pull with reduced lock contention and exponential backoff.
    ///
    /// Performance improvements:
    /// 1. Read-only checks use read lock first (paused check)
    /// 2. Exponential backoff prevents thundering herd (10ms -> 20ms -> ... -> 200ms max)
    /// 3. Only falls back to Notify after max backoff reached
    pub async fn pull(&self, queue_name: &str) -> Job {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);

        // Result of lock acquisition: Job found, need to wait, or need to sleep
        enum PullResult {
            Job(Box<Job>),
            Wait,
            SleepPaused,
            SleepRateLimit,
            SleepConcurrency,
        }

        // Exponential backoff state to prevent thundering herd
        let mut backoff_ms = 10u64;
        const MAX_BACKOFF_MS: u64 = 200;

        loop {
            let now = now_ms();

            // OPTIMIZATION: Quick read-only check for paused state first
            // This avoids acquiring write lock when queue is paused
            let is_paused = {
                let shard = self.shards[idx].read();
                shard
                    .queue_state
                    .get(&queue_arc)
                    .map(|s| s.paused)
                    .unwrap_or(false)
            }; // Lock is dropped here before any await
            if is_paused {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Now acquire write lock for actual job retrieval
            let pull_result = {
                let mut shard = self.shards[idx].write();
                let state = shard.get_state(&queue_arc);

                // Re-check paused (could have changed)
                if state.paused {
                    PullResult::SleepPaused
                }
                // Check rate limit
                else if state
                    .rate_limiter
                    .as_mut()
                    .is_some_and(|l| !l.try_acquire())
                {
                    PullResult::SleepRateLimit
                }
                // Check concurrency limit
                else if state.concurrency.as_mut().is_some_and(|c| !c.try_acquire()) {
                    PullResult::SleepConcurrency
                } else {
                    // Try to get a job (with lazy TTL deletion)
                    let mut result = None;
                    if let Some(heap) = shard.queues.get_mut(&queue_arc) {
                        // IPQ.peek() cleans stale entries and returns valid job
                        while let Some(job) = heap.peek() {
                            if job.is_expired(now) {
                                heap.pop();
                                continue;
                            }
                            if job.is_ready(now) {
                                // Safe: peek() returned Some above
                                let mut job = heap.pop().expect("heap non-empty after peek");
                                job.started_at = now;
                                job.last_heartbeat = now; // Initialize heartbeat for stall detection
                                result = Some(job);
                                break;
                            }
                            break; // Job not ready yet (delayed)
                        }
                    }

                    // If no job found, release concurrency slot
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
                    // OPTIMIZATION: Update atomic counter for O(1) stats
                    self.metrics.record_pull();
                    return *job;
                }
                PullResult::SleepPaused => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    backoff_ms = 10; // Reset backoff
                }
                PullResult::SleepRateLimit | PullResult::SleepConcurrency => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    backoff_ms = 10; // Reset backoff
                }
                PullResult::Wait => {
                    // OPTIMIZATION: Exponential backoff to prevent thundering herd
                    // Instead of all workers waking on Notify, they wake at staggered times
                    if backoff_ms < MAX_BACKOFF_MS {
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    } else {
                        // After max backoff, wait for notification (job pushed)
                        let notify = {
                            let shard = self.shards[idx].read();
                            Arc::clone(&shard.notify)
                        };
                        notify.notified().await;
                        backoff_ms = 10; // Reset after notification
                    }
                }
            }
        }
    }

    /// Optimized batch pull with reduced lock contention and exponential backoff.
    pub async fn pull_batch(&self, queue_name: &str, count: usize) -> Vec<Job> {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut result = Vec::with_capacity(count);

        enum BatchResult {
            Jobs(Vec<Job>),
            Paused,
            Wait,
        }

        // Exponential backoff state
        let mut backoff_ms = 10u64;
        const MAX_BACKOFF_MS: u64 = 200;

        loop {
            let now = now_ms();

            // OPTIMIZATION: Quick read-only check for paused state
            let is_paused = {
                let shard = self.shards[idx].read();
                shard
                    .queue_state
                    .get(&queue_arc)
                    .map(|s| s.paused)
                    .unwrap_or(false)
            }; // Lock is dropped here before any await
            if is_paused {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let batch_result = {
                let mut shard = self.shards[idx].write();
                let state = shard.get_state(&queue_arc);

                // Re-check paused
                if state.paused {
                    BatchResult::Paused
                } else {
                    let has_concurrency = state.concurrency.is_some();
                    let mut jobs = Vec::new();
                    let mut slots_acquired = 0usize;

                    // Try to acquire concurrency slots upfront
                    if has_concurrency {
                        let state = shard.get_state(&queue_arc);
                        if let Some(ref mut conc) = state.concurrency {
                            while slots_acquired < count && conc.try_acquire() {
                                slots_acquired += 1;
                            }
                        }
                    } else {
                        slots_acquired = count; // No limit
                    }

                    if slots_acquired > 0 {
                        if let Some(heap) = shard.queues.get_mut(&queue_arc) {
                            while jobs.len() < slots_acquired {
                                match heap.peek() {
                                    Some(job) if job.is_expired(now) => {
                                        heap.pop();
                                    }
                                    Some(job) if job.is_ready(now) => {
                                        // Safe: peek() returned Some above
                                        let mut job =
                                            heap.pop().expect("heap non-empty after peek");
                                        job.started_at = now;
                                        job.last_heartbeat = now; // Initialize heartbeat for stall detection
                                        jobs.push(job);
                                    }
                                    _ => break,
                                }
                            }
                        }

                        // Release unused slots
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
                        // OPTIMIZATION: Update atomic counter for O(1) stats
                        self.metrics.record_pull();
                        result.push(job);
                    }
                    return result;
                }
                BatchResult::Paused => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    backoff_ms = 10;
                }
                BatchResult::Wait => {
                    // OPTIMIZATION: Exponential backoff to prevent thundering herd
                    if backoff_ms < MAX_BACKOFF_MS {
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    } else {
                        let notify = {
                            let shard = self.shards[idx].read();
                            Arc::clone(&shard.notify)
                        };
                        notify.notified().await;
                        backoff_ms = 10;
                    }
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

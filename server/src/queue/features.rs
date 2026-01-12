use std::collections::BinaryHeap;
use std::sync::Arc;

use serde_json::Value;
use std::sync::atomic::Ordering;

use super::manager::QueueManager;
use super::types::{intern, ConcurrencyLimiter, JobLocation, RateLimiter, Subscriber};
use crate::protocol::{CronJob, Job, MetricsData, QueueInfo, QueueMetrics};

impl QueueManager {
    pub async fn cancel(&self, job_id: u64) -> Result<(), String> {
        // Use job_index for O(1) lookup of location
        let location = self.job_index.read().get(&job_id).copied();

        match location {
            Some(JobLocation::Processing) => {
                if let Some(job) = self.processing.write().remove(&job_id) {
                    let idx = Self::shard_index(&job.queue);
                    let queue_arc = intern(&job.queue);
                    {
                        let mut shard = self.shards[idx].write();
                        let state = shard.get_state(&queue_arc);
                        if let Some(ref mut conc) = state.concurrency {
                            conc.release();
                        }
                        if let Some(ref key) = job.unique_key {
                            if let Some(keys) = shard.unique_keys.get_mut(&queue_arc) {
                                keys.remove(key);
                            }
                        }
                    }
                    self.unindex_job(job_id);
                    self.persist_cancel(job_id);
                    return Ok(());
                }
            }
            Some(JobLocation::WaitingDeps { shard_idx }) => {
                if self.shards[shard_idx]
                    .write()
                    .waiting_deps
                    .remove(&job_id)
                    .is_some()
                {
                    self.unindex_job(job_id);
                    self.persist_cancel(job_id);
                    return Ok(());
                }
            }
            Some(JobLocation::Queue { shard_idx }) => {
                let mut shard = self.shards[shard_idx].write();
                let mut found_key: Option<(Arc<str>, Option<String>)> = None;

                // First pass: find the job and its queue without modifying the heap
                let mut target_queue: Option<Arc<str>> = None;
                for (queue_name, heap) in shard.queues.iter() {
                    if heap.iter().any(|j| j.id == job_id) {
                        target_queue = Some(Arc::clone(queue_name));
                        break;
                    }
                }

                // Second pass: only modify the heap if we found the job
                if let Some(queue_name) = target_queue {
                    if let Some(heap) = shard.queues.get_mut(&queue_name) {
                        // Collect all jobs, filter out the target, rebuild heap atomically
                        let mut jobs: Vec<_> = std::mem::take(heap).into_vec();
                        let original_len = jobs.len();

                        // Find and remove the target job
                        if let Some(pos) = jobs.iter().position(|j| j.id == job_id) {
                            let removed_job = jobs.swap_remove(pos);
                            found_key = Some((Arc::clone(&queue_name), removed_job.unique_key));

                            // Rebuild the heap with remaining jobs
                            *heap = jobs.into_iter().collect();
                        } else {
                            // Job not found (race condition), restore all jobs
                            *heap = jobs.into_iter().collect();
                        }

                        // Verify we didn't lose jobs (except the cancelled one)
                        debug_assert!(
                            heap.len() == original_len - 1 || found_key.is_none(),
                            "Job cancellation should only remove one job"
                        );
                    }
                }

                if let Some((queue_name, unique_key)) = found_key {
                    if let Some(key) = unique_key {
                        if let Some(keys) = shard.unique_keys.get_mut(&queue_name) {
                            keys.remove(&key);
                        }
                    }
                    drop(shard);
                    self.unindex_job(job_id);
                    self.persist_cancel(job_id);
                    return Ok(());
                }
            }
            Some(JobLocation::Dlq { .. }) | Some(JobLocation::Completed) => {
                return Err(format!(
                    "Job {} cannot be cancelled (already completed/failed)",
                    job_id
                ));
            }
            None => {}
        }

        Err(format!("Job {} not found", job_id))
    }

    pub async fn update_progress(
        &self,
        job_id: u64,
        progress: u8,
        message: Option<String>,
    ) -> Result<(), String> {
        let mut proc = self.processing.write();
        if let Some(job) = proc.get_mut(&job_id) {
            job.progress = progress.min(100);
            job.progress_msg = message.clone();
            self.notify_subscribers("progress", &job.queue, job);
            return Ok(());
        }
        Err(format!("Job {} not found in processing", job_id))
    }

    pub async fn get_progress(&self, job_id: u64) -> Result<(u8, Option<String>), String> {
        let proc = self.processing.read();
        if let Some(job) = proc.get(&job_id) {
            return Ok((job.progress, job.progress_msg.clone()));
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn get_dlq(&self, queue_name: &str, count: Option<usize>) -> Vec<Job> {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let shard = self.shards[idx].read();
        shard.dlq.get(&queue_arc).map_or(Vec::new(), |dlq| {
            dlq.iter().take(count.unwrap_or(100)).cloned().collect()
        })
    }

    pub async fn retry_dlq(&self, queue_name: &str, job_id: Option<u64>) -> usize {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut shard = self.shards[idx].write();
        let now = Self::now_ms();
        let mut jobs_to_retry = Vec::new();

        if let Some(dlq) = shard.dlq.get_mut(&queue_arc) {
            if let Some(id) = job_id {
                if let Some(pos) = dlq.iter().position(|j| j.id == id) {
                    let mut job = dlq.remove(pos).unwrap();
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
            let heap = shard
                .queues
                .entry(Arc::clone(&queue_arc))
                .or_insert_with(BinaryHeap::new);
            for job in jobs_to_retry {
                self.index_job(job.id, JobLocation::Queue { shard_idx: idx });
                heap.push(job);
            }
            drop(shard);
            self.notify_shard(idx);
        }
        retried
    }

    // === Rate Limiting ===
    pub async fn set_rate_limit(&self, queue: String, limit: u32) {
        let idx = Self::shard_index(&queue);
        let queue_arc = intern(&queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.rate_limiter = Some(RateLimiter::new(limit));
    }

    pub async fn clear_rate_limit(&self, queue: &str) {
        let idx = Self::shard_index(queue);
        let queue_arc = intern(queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.rate_limiter = None;
    }

    // === Concurrency Limiting ===
    pub async fn set_concurrency(&self, queue: String, limit: u32) {
        let idx = Self::shard_index(&queue);
        let queue_arc = intern(&queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.concurrency = Some(ConcurrencyLimiter::new(limit));
    }

    pub async fn clear_concurrency(&self, queue: &str) {
        let idx = Self::shard_index(queue);
        let queue_arc = intern(queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.concurrency = None;
    }

    // === Queue Control ===
    pub async fn pause(&self, queue: &str) {
        let idx = Self::shard_index(queue);
        let queue_arc = intern(queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.paused = true;
    }

    pub async fn resume(&self, queue: &str) {
        let idx = Self::shard_index(queue);
        let queue_arc = intern(queue);
        {
            let mut shard = self.shards[idx].write();
            let state = shard.get_state(&queue_arc);
            state.paused = false;
        }
        self.notify_shard(idx);
    }

    pub async fn list_queues(&self) -> Vec<QueueInfo> {
        let mut queues = Vec::new();
        let proc = self.processing.read();

        for shard in &self.shards {
            // Use read lock instead of write lock - this is a read-only operation
            let s = shard.read();
            for (name, heap) in &s.queues {
                let state = s.queue_state.get(name);
                queues.push(QueueInfo {
                    name: name.to_string(),
                    pending: heap.len(),
                    processing: proc
                        .values()
                        .filter(|j| j.queue.as_str() == name.as_ref())
                        .count(),
                    dlq: s.dlq.get(name).map_or(0, |d| d.len()),
                    paused: state.map_or(false, |s| s.paused),
                    rate_limit: state.and_then(|s| s.rate_limiter.as_ref().map(|r| r.limit)),
                    concurrency_limit: state.and_then(|s| s.concurrency.as_ref().map(|c| c.limit)),
                });
            }
        }
        queues
    }

    // === Pub/Sub ===
    pub fn subscribe(
        &self,
        queue: String,
        events: Vec<String>,
        tx: tokio::sync::mpsc::UnboundedSender<String>,
    ) {
        let queue_arc = intern(&queue);
        self.subscribers.write().push(Subscriber {
            queue: queue_arc,
            events,
            tx,
        });
    }

    pub fn unsubscribe(&self, queue: &str) {
        let queue_arc = intern(queue);
        self.subscribers.write().retain(|s| s.queue != queue_arc);
    }

    // === Cron Jobs ===
    pub async fn add_cron(
        &self,
        name: String,
        queue: String,
        data: Value,
        schedule: String,
        priority: i32,
    ) -> Result<(), String> {
        // Validate cron expression first
        Self::validate_cron(&schedule)?;

        let now = Self::now_ms();
        let next_run = Self::parse_next_cron_run(&schedule, now);
        let cron = CronJob {
            name: name.clone(),
            queue,
            data,
            schedule,
            priority,
            next_run,
        };

        // Persist to PostgreSQL
        self.persist_cron(&cron);

        self.cron_jobs.write().insert(name, cron);
        Ok(())
    }

    pub async fn delete_cron(&self, name: &str) -> bool {
        let removed = self.cron_jobs.write().remove(name).is_some();
        if removed {
            // Persist deletion to PostgreSQL
            self.persist_cron_delete(name);
        }
        removed
    }

    pub async fn list_crons(&self) -> Vec<CronJob> {
        self.cron_jobs.read().values().cloned().collect()
    }

    // === Metrics ===
    pub async fn get_metrics(&self) -> MetricsData {
        let latency_count = self.metrics.latency_count.load(Ordering::Relaxed);
        let avg_latency = if latency_count > 0 {
            self.metrics.latency_sum.load(Ordering::Relaxed) as f64 / latency_count as f64
        } else {
            0.0
        };

        let mut queues = Vec::new();
        let proc = self.processing.read();

        for shard in &self.shards {
            let s = shard.write();
            for (name, heap) in &s.queues {
                let state = s.queue_state.get(name);
                queues.push(QueueMetrics {
                    name: name.to_string(),
                    pending: heap.len(),
                    processing: proc
                        .values()
                        .filter(|j| j.queue.as_str() == name.as_ref())
                        .count(),
                    dlq: s.dlq.get(name).map_or(0, |d| d.len()),
                    rate_limit: state.and_then(|s| s.rate_limiter.as_ref().map(|r| r.limit)),
                });
            }
        }

        MetricsData {
            total_pushed: self.metrics.total_pushed.load(Ordering::Relaxed),
            total_completed: self.metrics.total_completed.load(Ordering::Relaxed),
            total_failed: self.metrics.total_failed.load(Ordering::Relaxed),
            jobs_per_second: 0.0,
            avg_latency_ms: avg_latency,
            queues,
        }
    }

    pub async fn stats(&self) -> (usize, usize, usize, usize) {
        let now = Self::now_ms();
        let (mut ready, mut delayed, mut dlq) = (0, 0, 0);
        let processing = self.processing.read().len();

        for shard in &self.shards {
            let s = shard.read();
            for d in s.dlq.values() {
                dlq += d.len();
            }
            for heap in s.queues.values() {
                for job in heap.iter() {
                    if job.is_ready(now) {
                        ready += 1;
                    } else {
                        delayed += 1;
                    }
                }
            }
        }
        (ready, processing, delayed, dlq)
    }
}

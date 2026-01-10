use std::collections::BinaryHeap;

use serde_json::Value;
use std::sync::atomic::Ordering;

use crate::protocol::{CronJob, Job, MetricsData, QueueInfo, QueueMetrics};
use super::types::{ConcurrencyLimiter, RateLimiter, Subscriber, WalEvent};
use super::manager::QueueManager;

impl QueueManager {
    pub async fn cancel(&self, job_id: u64) -> Result<(), String> {
        // Try global processing map first
        if let Some(job) = self.processing.write().remove(&job_id) {
            // Release concurrency
            {
                let mut conc = self.concurrency_limiters.write();
                if let Some(limiter) = conc.get_mut(&job.queue) {
                    limiter.release();
                }
            }

            if let Some(ref key) = job.unique_key {
                let idx = Self::shard_index(&job.queue);
                if let Some(keys) = self.shards[idx].write().unique_keys.get_mut(&job.queue) {
                    keys.remove(key);
                }
            }
            self.write_wal(&WalEvent::Cancel(job_id));
            return Ok(());
        }

        // Try waiting deps in all shards
        for shard in &self.shards {
            if shard.write().waiting_deps.remove(&job_id).is_some() {
                self.write_wal(&WalEvent::Cancel(job_id));
                return Ok(());
            }
        }

        // Search in queues (expensive)
        for shard in &self.shards {
            let mut shard_w = shard.write();
            let mut found_job: Option<(String, Option<String>)> = None;

            for (queue_name, heap) in shard_w.queues.iter_mut() {
                let jobs: Vec<_> = std::mem::take(heap).into_vec();
                for job in jobs {
                    if job.id == job_id {
                        found_job = Some((queue_name.clone(), job.unique_key.clone()));
                    } else {
                        heap.push(job);
                    }
                }
                if found_job.is_some() { break; }
            }

            if let Some((queue_name, unique_key)) = found_job {
                if let Some(key) = unique_key {
                    if let Some(keys) = shard_w.unique_keys.get_mut(&queue_name) {
                        keys.remove(&key);
                    }
                }
                self.write_wal(&WalEvent::Cancel(job_id));
                return Ok(());
            }
        }

        Err(format!("Job {} not found", job_id))
    }

    pub async fn update_progress(&self, job_id: u64, progress: u8, message: Option<String>) -> Result<(), String> {
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
        let shard = self.shards[idx].read();
        shard.dlq.get(queue_name).map_or(Vec::new(), |dlq| {
            dlq.iter().take(count.unwrap_or(100)).cloned().collect()
        })
    }

    pub async fn retry_dlq(&self, queue_name: &str, job_id: Option<u64>) -> usize {
        let idx = Self::shard_index(queue_name);
        let mut shard = self.shards[idx].write();
        let now = Self::now_ms();
        let mut jobs_to_retry = Vec::new();

        if let Some(dlq) = shard.dlq.get_mut(queue_name) {
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
            let heap = shard.queues.entry(queue_name.to_string()).or_insert_with(BinaryHeap::new);
            for job in jobs_to_retry {
                heap.push(job);
            }
            self.notify.notify_waiters();
        }
        retried
    }

    // === Rate Limiting ===
    pub async fn set_rate_limit(&self, queue: String, limit: u32) {
        self.rate_limiters.write().insert(queue, RateLimiter::new(limit));
    }

    pub async fn clear_rate_limit(&self, queue: &str) {
        self.rate_limiters.write().remove(queue);
    }

    // === Concurrency Limiting ===
    pub async fn set_concurrency(&self, queue: String, limit: u32) {
        self.concurrency_limiters.write().insert(queue, ConcurrencyLimiter::new(limit));
    }

    pub async fn clear_concurrency(&self, queue: &str) {
        self.concurrency_limiters.write().remove(queue);
    }

    // === Queue Control ===
    pub async fn pause(&self, queue: &str) {
        self.paused_queues.write().insert(queue.to_string());
    }

    pub async fn resume(&self, queue: &str) {
        self.paused_queues.write().remove(queue);
        self.notify.notify_waiters();
    }

    pub async fn is_paused(&self, queue: &str) -> bool {
        self.paused_queues.read().contains(queue)
    }

    pub async fn list_queues(&self) -> Vec<QueueInfo> {
        let mut queues = Vec::new();
        let rate_limiters = self.rate_limiters.read();
        let conc_limiters = self.concurrency_limiters.read();
        let paused = self.paused_queues.read();
        let proc = self.processing.read();

        for shard in &self.shards {
            let s = shard.read();
            for (name, heap) in &s.queues {
                queues.push(QueueInfo {
                    name: name.clone(),
                    pending: heap.len(),
                    processing: proc.values().filter(|j| &j.queue == name).count(),
                    dlq: s.dlq.get(name).map_or(0, |d| d.len()),
                    paused: paused.contains(name),
                    rate_limit: rate_limiters.get(name).map(|r| r.limit),
                    concurrency_limit: conc_limiters.get(name).map(|c| c.limit),
                });
            }
        }
        queues
    }

    // === Pub/Sub ===
    pub fn subscribe(&self, queue: String, events: Vec<String>, tx: tokio::sync::mpsc::UnboundedSender<String>) {
        self.subscribers.write().push(Subscriber { queue, events, tx });
    }

    pub fn unsubscribe(&self, queue: &str) {
        self.subscribers.write().retain(|s| s.queue != queue);
    }

    // === Cron Jobs ===
    pub async fn add_cron(&self, name: String, queue: String, data: Value, schedule: String, priority: i32) {
        let now = Self::now_ms();
        let next_run = Self::parse_next_cron_run(&schedule, now);
        self.cron_jobs.write().insert(name.clone(), CronJob {
            name, queue, data, schedule, priority, next_run,
        });
    }

    pub async fn delete_cron(&self, name: &str) -> bool {
        self.cron_jobs.write().remove(name).is_some()
    }

    pub async fn list_crons(&self) -> Vec<CronJob> {
        self.cron_jobs.read().values().cloned().collect()
    }

    // === Metrics ===
    pub async fn get_metrics(&self) -> MetricsData {
        let latency_count = self.metrics.latency_count.load(Ordering::Relaxed);
        let avg_latency = if latency_count > 0 {
            self.metrics.latency_sum.load(Ordering::Relaxed) as f64 / latency_count as f64
        } else { 0.0 };

        let mut queues = Vec::new();
        let rate_limiters = self.rate_limiters.read();
        let proc = self.processing.read();

        for shard in &self.shards {
            let s = shard.read();
            for (name, heap) in &s.queues {
                queues.push(QueueMetrics {
                    name: name.clone(),
                    pending: heap.len(),
                    processing: proc.values().filter(|j| &j.queue == name).count(),
                    dlq: s.dlq.get(name).map_or(0, |d| d.len()),
                    rate_limit: rate_limiters.get(name).map(|r| r.limit),
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
            for d in s.dlq.values() { dlq += d.len(); }
            for heap in s.queues.values() {
                for job in heap.iter() {
                    if job.is_ready(now) { ready += 1; } else { delayed += 1; }
                }
            }
        }
        (ready, processing, delayed, dlq)
    }
}

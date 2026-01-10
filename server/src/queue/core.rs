use std::collections::BinaryHeap;
use std::sync::atomic::Ordering;

use serde_json::Value;
use tokio::time::Duration;

use crate::protocol::{next_id, Job, JobInput};
use super::types::WalEvent;
use super::manager::QueueManager;

impl QueueManager {
    #[inline(always)]
    pub fn create_job(
        &self,
        queue: String,
        data: Value,
        priority: i32,
        delay: Option<u64>,
        ttl: Option<u64>,
        timeout: Option<u64>,
        max_attempts: Option<u32>,
        backoff: Option<u64>,
        unique_key: Option<String>,
        depends_on: Option<Vec<u64>>,
    ) -> Job {
        let now = Self::now_ms();
        Job {
            id: next_id(),
            queue,
            data,
            priority,
            created_at: now,
            run_at: delay.map_or(now, |d| now + d),
            started_at: 0,
            attempts: 0,
            max_attempts: max_attempts.unwrap_or(0),
            backoff: backoff.unwrap_or(0),
            ttl: ttl.unwrap_or(0),
            timeout: timeout.unwrap_or(0),
            unique_key,
            depends_on: depends_on.unwrap_or_default(),
            progress: 0,
            progress_msg: None,
        }
    }

    pub async fn push(
        &self,
        queue: String,
        data: Value,
        priority: i32,
        delay: Option<u64>,
        ttl: Option<u64>,
        timeout: Option<u64>,
        max_attempts: Option<u32>,
        backoff: Option<u64>,
        unique_key: Option<String>,
        depends_on: Option<Vec<u64>>,
    ) -> Result<Job, String> {
        let job = self.create_job(
            queue.clone(), data, priority, delay, ttl, timeout, max_attempts, backoff,
            unique_key.clone(), depends_on.clone()
        );

        let idx = Self::shard_index(&queue);

        {
            let mut shard = self.shards[idx].write();

            // Check unique key
            if let Some(ref key) = unique_key {
                let keys = shard.unique_keys.entry(queue.clone()).or_default();
                if keys.contains(key) {
                    return Err(format!("Duplicate job with key: {}", key));
                }
                keys.insert(key.clone());
            }

            self.write_wal(&WalEvent::Push(job.clone()));

            // Check dependencies
            if !job.depends_on.is_empty() {
                let completed = self.completed_jobs.read();
                let deps_satisfied = job.depends_on.iter().all(|dep| completed.contains(dep));

                if !deps_satisfied {
                    shard.waiting_deps.insert(job.id, job.clone());
                    return Ok(job);
                }
            }

            shard.queues.entry(queue).or_insert_with(BinaryHeap::new).push(job.clone());
        }

        self.metrics.record_push(1);
        self.notify.notify_waiters();
        Ok(job)
    }

    pub async fn push_batch(&self, queue: String, jobs: Vec<JobInput>) -> Vec<u64> {
        let mut ids = Vec::with_capacity(jobs.len());
        let mut created_jobs = Vec::with_capacity(jobs.len());
        let mut waiting_jobs = Vec::new();

        let idx = Self::shard_index(&queue);
        let completed = self.completed_jobs.read().clone();

        for input in jobs {
            let job = self.create_job(
                queue.clone(), input.data, input.priority, input.delay,
                input.ttl, input.timeout, input.max_attempts, input.backoff,
                input.unique_key, input.depends_on,
            );
            ids.push(job.id);

            if !job.depends_on.is_empty() {
                if !job.depends_on.iter().all(|dep| completed.contains(dep)) {
                    waiting_jobs.push(job);
                    continue;
                }
            }
            created_jobs.push(job);
        }

        self.write_wal_batch(&created_jobs);

        {
            let mut shard = self.shards[idx].write();
            let heap = shard.queues.entry(queue).or_insert_with(BinaryHeap::new);
            for job in created_jobs {
                heap.push(job);
            }
            for job in waiting_jobs {
                shard.waiting_deps.insert(job.id, job);
            }
        }

        self.metrics.record_push(ids.len() as u64);
        self.notify.notify_waiters();
        ids
    }

    pub async fn pull(&self, queue_name: &str) -> Job {
        let idx = Self::shard_index(queue_name);

        loop {
            // Check if queue is paused
            let is_paused = self.paused_queues.read().contains(queue_name);
            if is_paused {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Check rate limit
            let rate_ok = {
                let mut limiters = self.rate_limiters.write();
                limiters.get_mut(queue_name).map_or(true, |l| l.try_acquire())
            };
            if !rate_ok {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            // Check concurrency limit
            let conc_ok = {
                let mut conc = self.concurrency_limiters.write();
                conc.get_mut(queue_name).map_or(true, |l| l.try_acquire())
            };
            if !conc_ok {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let now = Self::now_ms();
            let maybe_job = {
                let mut shard = self.shards[idx].write();
                let mut result = None;
                if let Some(heap) = shard.queues.get_mut(queue_name) {
                    while let Some(job) = heap.peek() {
                        if job.is_expired(now) {
                            heap.pop();
                            continue;
                        }
                        if job.is_ready(now) {
                            let mut job = heap.pop().unwrap();
                            job.started_at = now;
                            result = Some(job);
                            break;
                        }
                        break;
                    }
                }
                result
            };

            if let Some(job) = maybe_job {
                self.processing.write().insert(job.id, job.clone());
                return job;
            }

            // Release concurrency if we didn't get a job
            {
                let mut conc = self.concurrency_limiters.write();
                if let Some(limiter) = conc.get_mut(queue_name) {
                    limiter.release();
                }
            }

            self.notify.notified().await;
        }
    }

    pub async fn pull_batch(&self, queue_name: &str, count: usize) -> Vec<Job> {
        let idx = Self::shard_index(queue_name);
        let mut result = Vec::with_capacity(count);

        loop {
            // Check if queue is paused
            let is_paused = self.paused_queues.read().contains(queue_name);
            if is_paused {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let now = Self::now_ms();
            let jobs_to_process = {
                let mut shard = self.shards[idx].write();
                let mut jobs = Vec::new();

                if let Some(heap) = shard.queues.get_mut(queue_name) {
                    while jobs.len() < count {
                        // Check concurrency for each job
                        let conc_ok = {
                            let mut conc = self.concurrency_limiters.write();
                            conc.get_mut(queue_name).map_or(true, |l| l.try_acquire())
                        };
                        if !conc_ok { break; }

                        match heap.peek() {
                            Some(job) if job.is_expired(now) => { heap.pop(); }
                            Some(job) if job.is_ready(now) => {
                                let mut job = heap.pop().unwrap();
                                job.started_at = now;
                                jobs.push(job);
                            }
                            _ => {
                                // Release the concurrency we just acquired
                                let mut conc = self.concurrency_limiters.write();
                                if let Some(limiter) = conc.get_mut(queue_name) {
                                    limiter.release();
                                }
                                break;
                            }
                        }
                    }
                }
                jobs
            };

            if !jobs_to_process.is_empty() {
                let mut proc = self.processing.write();
                for job in jobs_to_process {
                    proc.insert(job.id, job.clone());
                    result.push(job);
                }
            }

            if !result.is_empty() { return result; }
            self.notify.notified().await;
        }
    }

    pub async fn ack(&self, job_id: u64, result: Option<Value>) -> Result<(), String> {
        let job = self.processing.write().remove(&job_id);
        if let Some(job) = job {
            // Release concurrency
            {
                let mut conc = self.concurrency_limiters.write();
                if let Some(limiter) = conc.get_mut(&job.queue) {
                    limiter.release();
                }
            }

            // Remove unique key
            if let Some(ref key) = job.unique_key {
                let idx = Self::shard_index(&job.queue);
                if let Some(keys) = self.shards[idx].write().unique_keys.get_mut(&job.queue) {
                    keys.remove(key);
                }
            }

            // Store result if provided
            if let Some(ref res) = result {
                self.job_results.write().insert(job_id, res.clone());
                self.write_wal(&WalEvent::AckWithResult { id: job_id, result: res.clone() });
            } else {
                self.write_wal(&WalEvent::Ack(job_id));
            }

            self.completed_jobs.write().insert(job_id);
            self.metrics.record_complete(Self::now_ms() - job.created_at);
            self.notify_subscribers("completed", &job.queue, &job);
            return Ok(());
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn ack_batch(&self, ids: &[u64]) -> usize {
        let mut acked = 0;
        let mut proc = self.processing.write();

        for &id in ids {
            if let Some(job) = proc.remove(&id) {
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
                self.completed_jobs.write().insert(id);
                acked += 1;
            }
        }
        drop(proc);

        if self.persistence && acked > 0 {
            self.write_wal_acks(ids);
        }

        self.metrics.total_completed.fetch_add(acked as u64, Ordering::Relaxed);
        acked
    }

    pub async fn fail(&self, job_id: u64, error: Option<String>) -> Result<(), String> {
        let job = self.processing.write().remove(&job_id);
        if let Some(mut job) = job {
            // Release concurrency
            {
                let mut conc = self.concurrency_limiters.write();
                if let Some(limiter) = conc.get_mut(&job.queue) {
                    limiter.release();
                }
            }

            job.attempts += 1;
            let idx = Self::shard_index(&job.queue);

            if job.should_go_to_dlq() {
                self.write_wal(&WalEvent::Dlq(job.clone()));
                self.notify_subscribers("failed", &job.queue, &job);
                self.shards[idx].write().dlq.entry(job.queue.clone()).or_default().push_back(job);
                self.metrics.record_fail();
                return Ok(());
            }

            let backoff = job.next_backoff();
            if backoff > 0 {
                job.run_at = Self::now_ms() + backoff;
            }
            job.started_at = 0;

            if let Some(err) = error {
                job.progress_msg = Some(err);
            }

            self.write_wal(&WalEvent::Fail(job_id));
            self.shards[idx].write().queues.entry(job.queue.clone()).or_insert_with(BinaryHeap::new).push(job);
            self.notify.notify_waiters();
            return Ok(());
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn get_result(&self, job_id: u64) -> Option<Value> {
        self.job_results.read().get(&job_id).cloned()
    }
}

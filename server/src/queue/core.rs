use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::Value;
use tokio::time::Duration;

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation};
use crate::protocol::{next_id, Job, JobEvent, JobInput};

/// Maximum job data size in bytes (1MB) to prevent DoS attacks
const MAX_JOB_DATA_SIZE: usize = 1_048_576;

/// Maximum queue name length
const MAX_QUEUE_NAME_LENGTH: usize = 256;

/// Validate queue name - must be alphanumeric, underscores, hyphens, or dots
#[inline]
fn validate_queue_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Queue name cannot be empty".to_string());
    }
    if name.len() > MAX_QUEUE_NAME_LENGTH {
        return Err(format!(
            "Queue name too long (max {} chars)",
            MAX_QUEUE_NAME_LENGTH
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(
            "Queue name must contain only alphanumeric characters, underscores, hyphens, or dots"
                .to_string(),
        );
    }
    Ok(())
}

/// Validate job data size
#[inline]
fn validate_job_data(data: &Value) -> Result<(), String> {
    let size = serde_json::to_string(data).map(|s| s.len()).unwrap_or(0);
    if size > MAX_JOB_DATA_SIZE {
        return Err(format!(
            "Job data too large ({} bytes, max {} bytes)",
            size, MAX_JOB_DATA_SIZE
        ));
    }
    Ok(())
}

impl QueueManager {
    #[allow(clippy::too_many_arguments)]
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
        tags: Option<Vec<String>>,
        lifo: bool,
    ) -> Job {
        let now = now_ms();
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
            tags: tags.unwrap_or_default(),
            lifo,
        }
    }

    #[allow(clippy::too_many_arguments)]
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
        tags: Option<Vec<String>>,
        lifo: bool,
    ) -> Result<Job, String> {
        // Validate inputs to prevent DoS attacks
        validate_queue_name(&queue)?;
        validate_job_data(&data)?;

        let job = self.create_job(
            queue.clone(),
            data,
            priority,
            delay,
            ttl,
            timeout,
            max_attempts,
            backoff,
            unique_key.clone(),
            depends_on.clone(),
            tags,
            lifo,
        );

        let idx = Self::shard_index(&queue);
        let queue_name = intern(&queue);

        {
            let mut shard = self.shards[idx].write();

            // Check unique key
            if let Some(ref key) = unique_key {
                let keys = shard
                    .unique_keys
                    .entry(Arc::clone(&queue_name))
                    .or_default();
                if keys.contains(key) {
                    return Err(format!("Duplicate job with key: {}", key));
                }
                keys.insert(key.clone());
            }

            // Check dependencies
            if !job.depends_on.is_empty() {
                let completed = self.completed_jobs.read();
                let deps_satisfied = job.depends_on.iter().all(|dep| completed.contains(dep));

                if !deps_satisfied {
                    shard.waiting_deps.insert(job.id, job.clone());
                    self.index_job(job.id, JobLocation::WaitingDeps { shard_idx: idx });
                    self.persist_push(&job, "waiting_children");
                    return Ok(job);
                }
            }

            shard
                .queues
                .entry(queue_name)
                .or_default()
                .push(job.clone());
            self.index_job(job.id, JobLocation::Queue { shard_idx: idx });
            self.persist_push(&job, "waiting");
        }

        self.metrics.record_push(1);
        self.notify_shard(idx);

        // Broadcast pushed event
        self.broadcast_event(JobEvent {
            event_type: "pushed".to_string(),
            queue: job.queue.clone(),
            job_id: job.id,
            timestamp: now_ms(),
            data: Some(job.data.clone()),
            error: None,
            progress: None,
        });

        Ok(job)
    }

    pub async fn push_batch(&self, queue: String, jobs: Vec<JobInput>) -> Vec<u64> {
        // Validate queue name
        if validate_queue_name(&queue).is_err() {
            return Vec::new();
        }

        let mut ids = Vec::with_capacity(jobs.len());
        let mut created_jobs = Vec::with_capacity(jobs.len());
        let mut waiting_jobs = Vec::new();

        let idx = Self::shard_index(&queue);
        let queue_name = intern(&queue);
        let completed = self.completed_jobs.read().clone();

        for input in jobs {
            // Skip jobs with data too large
            if validate_job_data(&input.data).is_err() {
                continue;
            }
            let job = self.create_job(
                queue.clone(),
                input.data,
                input.priority,
                input.delay,
                input.ttl,
                input.timeout,
                input.max_attempts,
                input.backoff,
                input.unique_key,
                input.depends_on,
                input.tags,
                input.lifo,
            );
            ids.push(job.id);

            if !job.depends_on.is_empty()
                && !job.depends_on.iter().all(|dep| completed.contains(dep))
            {
                waiting_jobs.push(job);
                continue;
            }
            created_jobs.push(job);
        }

        {
            let mut shard = self.shards[idx].write();
            let heap = shard.queues.entry(queue_name).or_default();
            for job in &created_jobs {
                self.index_job(job.id, JobLocation::Queue { shard_idx: idx });
                heap.push(job.clone());
            }
            for job in &waiting_jobs {
                self.index_job(job.id, JobLocation::WaitingDeps { shard_idx: idx });
                shard.waiting_deps.insert(job.id, job.clone());
            }
        }

        // Persist to PostgreSQL
        self.persist_push_batch(&created_jobs, "waiting");
        self.persist_push_batch(&waiting_jobs, "waiting_children");

        self.metrics.record_push(ids.len() as u64);
        self.notify_shard(idx);
        ids
    }

    /// Optimized pull with combined state check and lazy TTL deletion
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

        loop {
            let now = now_ms();

            // Single lock acquisition for all checks + job retrieval
            let pull_result = {
                let mut shard = self.shards[idx].write();
                let state = shard.get_state(&queue_arc);

                // Check paused
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
                else if state
                    .concurrency
                    .as_mut()
                    .is_some_and(|c| !c.try_acquire())
                {
                    PullResult::SleepConcurrency
                } else {
                    // Try to get a job (with lazy TTL deletion)
                    let mut result = None;
                    if let Some(heap) = shard.queues.get_mut(&queue_arc) {
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
                    self.processing.write().insert(job.id, (*job).clone());
                    return *job;
                }
                PullResult::SleepPaused => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                PullResult::SleepRateLimit | PullResult::SleepConcurrency => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                PullResult::Wait => {
                    // Wait for notification on this shard
                    let notify = {
                        let shard = self.shards[idx].read();
                        Arc::clone(&shard.notify)
                    };
                    notify.notified().await;
                }
            }
        }
    }

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

                // Check paused
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
                                        let mut job = heap.pop().unwrap();
                                        job.started_at = now;
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
                    let mut proc = self.processing.write();
                    for job in jobs {
                        self.index_job(job.id, JobLocation::Processing);
                        proc.insert(job.id, job.clone());
                        result.push(job);
                    }
                    return result;
                }
                BatchResult::Paused => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                BatchResult::Wait => {
                    let notify = {
                        let shard = self.shards[idx].read();
                        Arc::clone(&shard.notify)
                    };
                    notify.notified().await;
                }
            }
        }
    }

    pub async fn ack(&self, job_id: u64, result: Option<Value>) -> Result<(), String> {
        let job = self.processing.write().remove(&job_id);
        if let Some(job) = job {
            // Release concurrency
            let idx = Self::shard_index(&job.queue);
            let queue_arc = intern(&job.queue);
            {
                let mut shard = self.shards[idx].write();
                let state = shard.get_state(&queue_arc);
                if let Some(ref mut conc) = state.concurrency {
                    conc.release();
                }

                // Remove unique key
                if let Some(ref key) = job.unique_key {
                    if let Some(keys) = shard.unique_keys.get_mut(&queue_arc) {
                        keys.remove(key);
                    }
                }
            }

            // Store result if provided
            if let Some(ref res) = result {
                self.job_results.write().insert(job_id, res.clone());
            }

            self.completed_jobs.write().insert(job_id);
            self.index_job(job_id, JobLocation::Completed);

            // Persist to PostgreSQL
            self.persist_ack(job_id, result.clone());
            self.metrics.record_complete(now_ms() - job.created_at);
            self.notify_subscribers("completed", &job.queue, &job);

            // Broadcast completed event
            self.broadcast_event(JobEvent {
                event_type: "completed".to_string(),
                queue: job.queue.clone(),
                job_id: job.id,
                timestamp: now_ms(),
                data: result,
                error: None,
                progress: None,
            });

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
                self.completed_jobs.write().insert(id);
                self.index_job(id, JobLocation::Completed);
                acked += 1;
            }
        }
        drop(proc);

        // Persist to PostgreSQL
        if acked > 0 {
            self.persist_ack_batch(ids);
        }

        self.metrics
            .total_completed
            .fetch_add(acked as u64, Ordering::Relaxed);
        acked
    }

    pub async fn fail(&self, job_id: u64, error: Option<String>) -> Result<(), String> {
        let job = self.processing.write().remove(&job_id);
        if let Some(mut job) = job {
            let idx = Self::shard_index(&job.queue);
            let queue_arc = intern(&job.queue);

            // Release concurrency
            {
                let mut shard = self.shards[idx].write();
                let state = shard.get_state(&queue_arc);
                if let Some(ref mut conc) = state.concurrency {
                    conc.release();
                }
            }

            job.attempts += 1;

            if job.should_go_to_dlq() {
                self.notify_subscribers("failed", &job.queue, &job);
                self.index_job(job_id, JobLocation::Dlq { shard_idx: idx });
                self.shards[idx]
                    .write()
                    .dlq
                    .entry(queue_arc)
                    .or_default()
                    .push_back(job.clone());
                self.metrics.record_fail();

                // Persist to PostgreSQL
                self.persist_dlq(&job, error.as_deref());

                // Broadcast failed event
                self.broadcast_event(JobEvent {
                    event_type: "failed".to_string(),
                    queue: job.queue.clone(),
                    job_id: job.id,
                    timestamp: now_ms(),
                    data: None,
                    error: error.clone(),
                    progress: None,
                });

                return Ok(());
            }

            let backoff = job.next_backoff();
            let new_run_at = if backoff > 0 {
                now_ms() + backoff
            } else {
                job.run_at
            };
            job.run_at = new_run_at;
            job.started_at = 0;

            if let Some(err) = error {
                job.progress_msg = Some(err);
            }

            self.index_job(job_id, JobLocation::Queue { shard_idx: idx });
            self.shards[idx]
                .write()
                .queues
                .entry(queue_arc)
                .or_default()
                .push(job.clone());

            // Persist to PostgreSQL
            self.persist_fail(job_id, new_run_at, job.attempts);

            self.notify_shard(idx);
            return Ok(());
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn get_result(&self, job_id: u64) -> Option<Value> {
        self.job_results.read().get(&job_id).cloned()
    }
}

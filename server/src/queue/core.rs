use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::Value;
use tokio::time::Duration;

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation};
use crate::protocol::{Job, JobBuilder, JobEvent, JobInput};

/// Maximum job data size in bytes (1MB) to prevent DoS attacks
const MAX_JOB_DATA_SIZE: usize = 1_048_576;

/// Maximum queue name length
const MAX_QUEUE_NAME_LENGTH: usize = 256;

/// Maximum batch size to prevent DoS attacks
const MAX_BATCH_SIZE: usize = 1000;

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
    /// Create a job using the builder pattern.
    /// This is a convenience method that wraps JobBuilder for backwards compatibility.
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    pub fn create_job_with_id(
        &self,
        id: u64,
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
        remove_on_complete: bool,
        remove_on_fail: bool,
        stall_timeout: Option<u64>,
        custom_id: Option<String>,
        keep_completed_age: Option<u64>,
        keep_completed_count: Option<usize>,
    ) -> Job {
        JobBuilder::new(queue, data)
            .priority(priority)
            .delay_opt(delay)
            .ttl_opt(ttl)
            .timeout_opt(timeout)
            .max_attempts_opt(max_attempts)
            .backoff_opt(backoff)
            .unique_key_opt(unique_key)
            .depends_on_opt(depends_on)
            .tags_opt(tags)
            .lifo(lifo)
            .remove_on_complete(remove_on_complete)
            .remove_on_fail(remove_on_fail)
            .stall_timeout_opt(stall_timeout)
            .custom_id_opt(custom_id)
            .keep_completed_age_opt(keep_completed_age)
            .keep_completed_count_opt(keep_completed_count)
            .build(id, now_ms())
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
        remove_on_complete: bool,
        remove_on_fail: bool,
        stall_timeout: Option<u64>,
        debounce_id: Option<String>,
        debounce_ttl: Option<u64>,
        job_id: Option<String>,
        keep_completed_age: Option<u64>,
        keep_completed_count: Option<usize>,
    ) -> Result<Job, String> {
        // Validate inputs to prevent DoS attacks
        validate_queue_name(&queue)?;
        validate_job_data(&data)?;

        // Check debounce - prevent duplicate jobs within time window
        if let Some(ref id) = debounce_id {
            let now = now_ms();
            let debounce_key = format!("{}:{}", queue, id);
            let debounce_cache = self.debounce_cache.read();
            if let Some(&expiry) = debounce_cache.get(&debounce_key) {
                if now < expiry {
                    return Err(format!(
                        "Debounced: job with id '{}' was pushed recently",
                        id
                    ));
                }
            }
        }

        // Check custom job ID for idempotency
        if let Some(ref custom_id) = job_id {
            let custom_id_map = self.custom_id_map.read();
            if let Some(&existing_id) = custom_id_map.get(custom_id) {
                // Return the existing job instead of creating a duplicate
                if let Some(job) = self.get_job_by_internal_id(existing_id) {
                    return Ok(job);
                }
            }
        }

        // Get cluster-wide unique ID from PostgreSQL sequence
        let internal_id = self.next_job_id().await;

        let job = self.create_job_with_id(
            internal_id,
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
            remove_on_complete,
            remove_on_fail,
            stall_timeout,
            job_id.clone(),
            keep_completed_age,
            keep_completed_count,
        );

        let idx = Self::shard_index(&queue);
        let queue_name = intern(&queue);

        {
            let mut shard = self.shards[idx].write();

            // Check unique key
            if let Some(ref key) = unique_key {
                let keys = shard.unique_keys.entry(queue_name.clone()).or_default();
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

        // Update debounce cache
        if let Some(ref id) = debounce_id {
            let debounce_key = format!("{}:{}", queue, id);
            let ttl = debounce_ttl.unwrap_or(5000); // Default 5 seconds
            let expiry = now_ms() + ttl;
            self.debounce_cache.write().insert(debounce_key, expiry);
        }

        // Store custom ID mapping for idempotency
        if let Some(ref custom_id) = job_id {
            self.custom_id_map.write().insert(custom_id.clone(), job.id);
        }

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

        // Validate batch size to prevent DoS
        if jobs.len() > MAX_BATCH_SIZE {
            return Vec::new();
        }

        // Filter valid jobs and check debounce
        let now = now_ms();
        let valid_jobs: Vec<_> = {
            let debounce_cache = self.debounce_cache.read();
            jobs.into_iter()
                .filter(|input| {
                    // Check data validity
                    if validate_job_data(&input.data).is_err() {
                        return false;
                    }
                    // Check debounce
                    if let Some(ref id) = input.debounce_id {
                        let debounce_key = format!("{}:{}", queue, id);
                        if let Some(&expiry) = debounce_cache.get(&debounce_key) {
                            if now < expiry {
                                return false; // Debounced
                            }
                        }
                    }
                    true
                })
                .collect()
        };

        if valid_jobs.is_empty() {
            return Vec::new();
        }

        // Get cluster-wide unique IDs for all jobs at once
        let job_ids = self.next_job_ids(valid_jobs.len()).await;

        let mut ids = Vec::with_capacity(valid_jobs.len());
        let mut created_jobs = Vec::with_capacity(valid_jobs.len());
        let mut waiting_jobs = Vec::new();
        let mut debounce_updates: Vec<(String, u64)> = Vec::new();

        let idx = Self::shard_index(&queue);
        let queue_name = intern(&queue);

        // Check dependencies without cloning the entire completed set
        {
            let completed = self.completed_jobs.read();
            for (input, job_id) in valid_jobs.into_iter().zip(job_ids.into_iter()) {
                // Track debounce updates
                if let Some(ref id) = input.debounce_id {
                    let debounce_key = format!("{}:{}", queue, id);
                    let ttl = input.debounce_ttl.unwrap_or(5000);
                    debounce_updates.push((debounce_key, now + ttl));
                }
                let job = self.create_job_with_id(
                    job_id,
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
                    input.remove_on_complete,
                    input.remove_on_fail,
                    input.stall_timeout,
                    input.job_id,
                    input.keep_completed_age,
                    input.keep_completed_count,
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
        } // Release completed_jobs read lock here

        // Persist first (needs references), then insert (consumes jobs)
        self.persist_push_batch(&created_jobs, "waiting");
        self.persist_push_batch(&waiting_jobs, "waiting_children");

        {
            let mut shard = self.shards[idx].write();
            let heap = shard.queues.entry(queue_name).or_default();
            // Use into_iter to move jobs instead of cloning
            for job in created_jobs {
                self.index_job(job.id, JobLocation::Queue { shard_idx: idx });
                heap.push(job);
            }
            for job in waiting_jobs {
                let job_id = job.id;
                self.index_job(job_id, JobLocation::WaitingDeps { shard_idx: idx });
                shard.waiting_deps.insert(job_id, job);
            }
        }

        // Update debounce cache
        if !debounce_updates.is_empty() {
            let mut cache = self.debounce_cache.write();
            for (key, expiry) in debounce_updates {
                cache.insert(key, expiry);
            }
        }

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
                                let mut job = heap.pop().unwrap();
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
        let job = self.processing_remove(job_id);
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

            // Handle remove_on_complete option
            if job.remove_on_complete {
                // Don't store in completed_jobs or job_results
                self.unindex_job(job_id);
                // Clean up logs for this job
                self.job_logs.write().remove(&job_id);
                self.stalled_count.write().remove(&job_id);
            } else {
                // Store result if provided
                if let Some(ref res) = result {
                    self.job_results.write().insert(job_id, res.clone());
                }
                self.completed_jobs.write().insert(job_id);
                self.index_job(job_id, JobLocation::Completed);
            }

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
                data: result.clone(),
                error: None,
                progress: None,
            });

            // Notify any waiters (finished() promise)
            self.notify_job_waiters(job.id, result.clone());

            // Store retention info if needed
            if job.keep_completed_age > 0 || job.keep_completed_count > 0 {
                self.completed_retention
                    .write()
                    .insert(job.id, (now_ms(), job.keep_completed_age, result));
            }

            // If this job has a parent (is part of a flow), notify the parent
            if let Some(parent_id) = job.parent_id {
                self.on_child_completed(parent_id);
            }

            return Ok(());
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn ack_batch(&self, ids: &[u64]) -> usize {
        let mut acked = 0;

        for &id in ids {
            if let Some(job) = self.processing_remove(id) {
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
        let job = self.processing_remove(job_id);
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

                // Handle remove_on_fail option
                if job.remove_on_fail {
                    // Don't store in DLQ, just discard
                    self.unindex_job(job_id);
                    self.job_logs.write().remove(&job_id);
                    self.stalled_count.write().remove(&job_id);
                } else {
                    self.index_job(job_id, JobLocation::Dlq { shard_idx: idx });
                    // Persist first (needs reference)
                    self.persist_dlq(&job, error.as_deref());
                    // Extract data for broadcast before moving
                    let queue_name = job.queue.clone();
                    // Then move into DLQ (no clone)
                    self.shards[idx]
                        .write()
                        .dlq
                        .entry(queue_arc)
                        .or_default()
                        .push_back(job);

                    self.metrics.record_fail();

                    // Broadcast failed event
                    self.broadcast_event(JobEvent {
                        event_type: "failed".to_string(),
                        queue: queue_name,
                        job_id,
                        timestamp: now_ms(),
                        data: None,
                        error,
                        progress: None,
                    });
                    return Ok(());
                }

                self.metrics.record_fail();

                // Broadcast failed event for remove_on_fail case
                self.broadcast_event(JobEvent {
                    event_type: "failed".to_string(),
                    queue: job.queue.clone(),
                    job_id: job.id,
                    timestamp: now_ms(),
                    data: None,
                    error,
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
            // Persist first (uses primitive values, no job reference needed)
            self.persist_fail(job_id, new_run_at, job.attempts);
            // Then move job into queue (no clone)
            self.shards[idx]
                .write()
                .queues
                .entry(queue_arc)
                .or_default()
                .push(job);

            self.notify_shard(idx);
            return Ok(());
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn get_result(&self, job_id: u64) -> Option<Value> {
        self.job_results.read().get(&job_id).cloned()
    }

    // ============== Distributed Pull (Cluster Mode) ==============

    /// Pull a job using PostgreSQL's SELECT FOR UPDATE SKIP LOCKED.
    /// This prevents duplicate job processing across cluster nodes.
    /// Use this in cluster mode for consistency at the cost of performance.
    pub async fn pull_distributed(&self, queue_name: &str, timeout_ms: u64) -> Option<Job> {
        let storage = self.storage.as_ref()?;
        let deadline = now_ms() + timeout_ms;
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);

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
                    // No jobs available
                    if now >= deadline {
                        return None;
                    }
                    // Wait a bit before retrying
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    eprintln!("Distributed pull error: {}", e);
                    if now >= deadline {
                        return None;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Pull multiple jobs using PostgreSQL's SELECT FOR UPDATE SKIP LOCKED.
    /// Batch version of pull_distributed for better performance.
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
            match storage
                .pull_jobs_distributed_batch(queue_name, count as i32, now as i64)
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
                    // No jobs available
                    if now >= deadline {
                        return Vec::new();
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    eprintln!("Distributed batch pull error: {}", e);
                    if now >= deadline {
                        return Vec::new();
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

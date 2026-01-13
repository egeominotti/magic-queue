use std::sync::Arc;

use serde_json::Value;
use std::sync::atomic::Ordering;

use super::manager::QueueManager;
use super::types::now_ms;
use super::types::{intern, ConcurrencyLimiter, JobLocation, RateLimiter, Subscriber};
use crate::protocol::{CronJob, FlowChild, Job, JobLogEntry, MetricsData, QueueInfo, QueueMetrics};

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
            Some(JobLocation::WaitingChildren { shard_idx }) => {
                if self.shards[shard_idx]
                    .write()
                    .waiting_children
                    .remove(&job_id)
                    .is_some()
                {
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
            let heap = shard.queues.entry(Arc::clone(&queue_arc)).or_default();
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
                    paused: state.is_some_and(|s| s.paused),
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
            schedule: Some(schedule),
            repeat_every: None,
            priority,
            next_run,
            executions: 0,
            limit: None,
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

    // ============== Job Logs ==============

    /// Add a log entry to a job
    pub fn add_job_log(&self, job_id: u64, message: String, level: String) -> Result<(), String> {
        // Verify job exists (in processing or waiting)
        let location = self.job_index.read().get(&job_id).copied();
        if location.is_none() {
            return Err(format!("Job {} not found", job_id));
        }

        let entry = JobLogEntry {
            timestamp: now_ms(),
            message,
            level,
        };

        let mut logs = self.job_logs.write();
        let job_logs = logs.entry(job_id).or_default();
        job_logs.push(entry);

        // Limit logs per job (max 100)
        if job_logs.len() > 100 {
            job_logs.remove(0);
        }

        Ok(())
    }

    /// Get all logs for a job
    pub fn get_job_logs(&self, job_id: u64) -> Vec<JobLogEntry> {
        self.job_logs
            .read()
            .get(&job_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Cleanup logs for completed/old jobs
    pub(crate) fn cleanup_job_logs(&self) {
        let completed = self.completed_jobs.read();
        let index = self.job_index.read();
        let mut logs = self.job_logs.write();

        // Remove logs for jobs that are completed and no longer in index
        logs.retain(|job_id, _| {
            // Keep if job is still in index (active) or not completed
            index.contains_key(job_id) || !completed.contains(job_id)
        });

        // Global limit: max 10K job entries
        if logs.len() > 10_000 {
            // Remove oldest entries (by job_id, approximation)
            let mut job_ids: Vec<u64> = logs.keys().copied().collect();
            job_ids.sort();
            let to_remove = logs.len() - 7_500; // Keep 7.5K
            for job_id in job_ids.into_iter().take(to_remove) {
                logs.remove(&job_id);
            }
        }
    }

    /// Cleanup expired debounce entries
    pub(crate) fn cleanup_debounce_cache(&self) {
        let now = super::types::now_ms();
        let mut cache = self.debounce_cache.write();
        cache.retain(|_, &mut expiry| expiry > now);
    }

    // ============== Stalled Jobs Detection ==============

    /// Send heartbeat for a job to prevent stall detection
    pub fn heartbeat(&self, job_id: u64) -> Result<(), String> {
        let mut processing = self.processing.write();
        if let Some(job) = processing.get_mut(&job_id) {
            job.last_heartbeat = now_ms();
            // Reset stall count on successful heartbeat
            self.stalled_count.write().remove(&job_id);
            Ok(())
        } else {
            Err(format!("Job {} not in processing", job_id))
        }
    }

    /// Check for stalled jobs and handle them
    pub(crate) fn check_stalled_jobs(&self) {
        let now = now_ms();
        let mut stalled_jobs: Vec<(u64, Job)> = Vec::new();

        // Find stalled jobs
        {
            let processing = self.processing.read();
            for (id, job) in processing.iter() {
                let stall_timeout = if job.stall_timeout > 0 {
                    job.stall_timeout
                } else {
                    30_000 // Default 30 seconds
                };

                // Check if job is stalled (no heartbeat within timeout)
                let last_activity = if job.last_heartbeat > 0 {
                    job.last_heartbeat
                } else {
                    job.started_at
                };

                if now > last_activity + stall_timeout {
                    stalled_jobs.push((*id, job.clone()));
                }
            }
        }

        // Handle stalled jobs
        for (job_id, job) in stalled_jobs {
            let mut stall_counts = self.stalled_count.write();
            let count = stall_counts.entry(job_id).or_insert(0);
            *count += 1;

            if *count >= 3 {
                // Too many stalls, move to DLQ
                drop(stall_counts);
                if let Some(job) = self.processing.write().remove(&job_id) {
                    let idx = Self::shard_index(&job.queue);
                    let queue_arc = intern(&job.queue);
                    self.shards[idx]
                        .write()
                        .dlq
                        .entry(queue_arc)
                        .or_default()
                        .push_back(job.clone());
                    self.index_job(job_id, JobLocation::Dlq { shard_idx: idx });
                    self.persist_dlq(&job, Some("Job stalled"));
                    self.metrics.record_fail();

                    // Log the stall event
                    let _ = self.add_job_log(
                        job_id,
                        "Job moved to DLQ after 3 stall detections".to_string(),
                        "error".to_string(),
                    );
                }
            } else {
                // Update last_heartbeat to give more time, but increment stall count
                drop(stall_counts);
                if let Some(job_mut) = self.processing.write().get_mut(&job_id) {
                    job_mut.last_heartbeat = now;
                    job_mut.stall_count = job.stall_count + 1;
                }

                // Log the stall warning
                let _ = self.add_job_log(
                    job_id,
                    format!("Job stall detected (count: {})", job.stall_count + 1),
                    "warn".to_string(),
                );
            }
        }
    }

    // ============== Flows (Parent-Child Jobs) ==============

    /// Create a flow with parent and children jobs
    pub async fn push_flow(
        &self,
        queue: String,
        parent_data: serde_json::Value,
        children: Vec<FlowChild>,
        priority: i32,
    ) -> Result<(u64, Vec<u64>), String> {
        if children.is_empty() {
            return Err("Flow must have at least one child".to_string());
        }

        let now = now_ms();
        let parent_id = self.next_job_id().await;
        let children_ids = self.next_job_ids(children.len()).await;

        // Create parent job (will wait for children)
        let parent_job = Job {
            id: parent_id,
            queue: queue.clone(),
            data: parent_data,
            priority,
            created_at: now,
            run_at: now,
            started_at: 0,
            attempts: 0,
            max_attempts: 0,
            backoff: 0,
            ttl: 0,
            timeout: 0,
            unique_key: None,
            depends_on: Vec::new(),
            progress: 0,
            progress_msg: None,
            tags: Vec::new(),
            lifo: false,
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: children_ids.clone(),
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        };

        // Create child jobs
        let mut child_jobs = Vec::with_capacity(children.len());
        for (i, child) in children.into_iter().enumerate() {
            let child_job = Job {
                id: children_ids[i],
                queue: child.queue,
                data: child.data,
                priority: child.priority,
                created_at: now,
                run_at: now + child.delay.unwrap_or(0),
                started_at: 0,
                attempts: 0,
                max_attempts: 0,
                backoff: 0,
                ttl: 0,
                timeout: 0,
                unique_key: None,
                depends_on: Vec::new(),
                progress: 0,
                progress_msg: None,
                tags: Vec::new(),
                lifo: false,
                remove_on_complete: false,
                remove_on_fail: false,
                last_heartbeat: 0,
                stall_timeout: 0,
                stall_count: 0,
                parent_id: Some(parent_id),
                children_ids: Vec::new(),
                children_completed: 0,
                custom_id: None,
                keep_completed_age: 0,
                keep_completed_count: 0,
                completed_at: 0,
            };
            child_jobs.push(child_job);
        }

        // Store parent in waiting_children
        let parent_idx = Self::shard_index(&queue);
        {
            let mut shard = self.shards[parent_idx].write();
            shard.waiting_children.insert(parent_id, parent_job.clone());
        }
        self.index_job(
            parent_id,
            JobLocation::WaitingChildren {
                shard_idx: parent_idx,
            },
        );
        self.persist_push(&parent_job, "waiting_parent");

        // Push all children to their queues
        for child_job in &child_jobs {
            let child_idx = Self::shard_index(&child_job.queue);
            let queue_arc = intern(&child_job.queue);
            {
                let mut shard = self.shards[child_idx].write();
                shard
                    .queues
                    .entry(queue_arc)
                    .or_default()
                    .push(child_job.clone());
            }
            self.index_job(
                child_job.id,
                JobLocation::Queue {
                    shard_idx: child_idx,
                },
            );
            self.persist_push(child_job, "waiting");
            self.notify_shard(child_idx);
        }

        self.metrics.record_push((children_ids.len() + 1) as u64);

        Ok((parent_id, children_ids))
    }

    /// Called when a child job completes - check if parent is ready
    pub(crate) fn on_child_completed(&self, parent_id: u64) {
        // Find parent in waiting_children across all shards
        for (idx, shard) in self.shards.iter().enumerate() {
            let mut shard_w = shard.write();
            if let Some(parent) = shard_w.waiting_children.get_mut(&parent_id) {
                parent.children_completed += 1;

                // Check if all children completed
                if parent.children_completed >= parent.children_ids.len() as u32 {
                    // Move parent to queue (ready to process)
                    let parent_job = shard_w.waiting_children.remove(&parent_id).unwrap();
                    let queue_arc = intern(&parent_job.queue);
                    shard_w
                        .queues
                        .entry(queue_arc)
                        .or_default()
                        .push(parent_job);
                    drop(shard_w);

                    self.index_job(parent_id, JobLocation::Queue { shard_idx: idx });
                    self.notify_shard(idx);

                    // Log the event
                    let _ = self.add_job_log(
                        parent_id,
                        "All children completed, parent job ready".to_string(),
                        "info".to_string(),
                    );
                }
                return;
            }
        }
    }

    /// Get children status for a parent job
    pub fn get_children(&self, parent_id: u64) -> Option<(Vec<Job>, u32, u32)> {
        // Find parent
        for shard in &self.shards {
            let shard_r = shard.read();
            if let Some(parent) = shard_r.waiting_children.get(&parent_id) {
                let children_ids = &parent.children_ids;
                let mut children = Vec::with_capacity(children_ids.len());

                // Collect children jobs
                for child_id in children_ids {
                    let (job, _state) = self.get_job(*child_id);
                    if let Some(j) = job {
                        children.push(j);
                    }
                }

                return Some((
                    children,
                    parent.children_completed,
                    parent.children_ids.len() as u32,
                ));
            }
        }
        None
    }

    // ============== Repeatable Jobs (Cron with interval) ==============

    /// Add a cron job with optional repeat_every interval
    #[allow(clippy::too_many_arguments)]
    pub async fn add_cron_with_repeat(
        &self,
        name: String,
        queue: String,
        data: serde_json::Value,
        schedule: Option<String>,
        repeat_every: Option<u64>,
        priority: i32,
        limit: Option<u64>,
    ) -> Result<(), String> {
        // Must have either schedule or repeat_every
        if schedule.is_none() && repeat_every.is_none() {
            return Err("Must provide either 'schedule' or 'repeat_every'".to_string());
        }

        // Validate cron expression if provided
        if let Some(ref sched) = schedule {
            Self::validate_cron(sched)?;
        }

        let now = now_ms();
        let next_run = if let Some(interval) = repeat_every {
            now + interval
        } else {
            Self::parse_next_cron_run(schedule.as_ref().unwrap(), now)
        };

        let cron = CronJob {
            name: name.clone(),
            queue,
            data,
            schedule,
            repeat_every,
            priority,
            next_run,
            executions: 0,
            limit,
        };

        // Persist to PostgreSQL
        self.persist_cron(&cron);

        self.cron_jobs.write().insert(name, cron);
        Ok(())
    }

    // ============== BullMQ Advanced Features ==============

    /// Get jobs filtered by queue and/or state with pagination
    pub fn get_jobs(
        &self,
        queue_filter: Option<&str>,
        state_filter: Option<crate::protocol::JobState>,
        limit: usize,
        offset: usize,
    ) -> (Vec<crate::protocol::JobBrowserItem>, usize) {
        use crate::protocol::{JobBrowserItem, JobState};

        let now = now_ms();
        let mut jobs: Vec<JobBrowserItem> = Vec::new();

        // 1. Collect from all shards (waiting/delayed)
        for shard in &self.shards {
            let shard_r = shard.read();

            // Queues (waiting/delayed)
            for (queue_name, heap) in &shard_r.queues {
                if queue_filter.is_some_and(|f| f != queue_name.as_ref()) {
                    continue;
                }
                for job in heap.iter() {
                    let state = if job.run_at > now {
                        JobState::Delayed
                    } else {
                        JobState::Waiting
                    };
                    if state_filter.is_some_and(|s| s != state) {
                        continue;
                    }
                    jobs.push(JobBrowserItem {
                        job: job.clone(),
                        state,
                    });
                }
            }

            // DLQ (failed)
            if state_filter.is_none() || state_filter == Some(JobState::Failed) {
                for (queue_name, dlq) in &shard_r.dlq {
                    if queue_filter.is_some_and(|f| f != queue_name.as_ref()) {
                        continue;
                    }
                    for job in dlq.iter() {
                        jobs.push(JobBrowserItem {
                            job: job.clone(),
                            state: JobState::Failed,
                        });
                    }
                }
            }

            // Waiting for children (parent jobs)
            if state_filter.is_none() || state_filter == Some(JobState::WaitingParent) {
                for job in shard_r.waiting_children.values() {
                    if queue_filter.is_some_and(|f| f != job.queue.as_str()) {
                        continue;
                    }
                    jobs.push(JobBrowserItem {
                        job: job.clone(),
                        state: JobState::WaitingParent,
                    });
                }
            }

            // Waiting for dependencies
            if state_filter.is_none() || state_filter == Some(JobState::WaitingChildren) {
                for job in shard_r.waiting_deps.values() {
                    if queue_filter.is_some_and(|f| f != job.queue.as_str()) {
                        continue;
                    }
                    jobs.push(JobBrowserItem {
                        job: job.clone(),
                        state: JobState::WaitingChildren,
                    });
                }
            }
        }

        // 2. Processing (active)
        if state_filter.is_none() || state_filter == Some(JobState::Active) {
            let processing = self.processing.read();
            for job in processing.values() {
                if queue_filter.is_some_and(|f| f != job.queue.as_str()) {
                    continue;
                }
                jobs.push(JobBrowserItem {
                    job: job.clone(),
                    state: JobState::Active,
                });
            }
        }

        // 3. Sort by ID desc (newest first)
        jobs.sort_by(|a, b| b.job.id.cmp(&a.job.id));

        // 4. Paginate
        let total = jobs.len();
        let jobs = jobs.into_iter().skip(offset).take(limit).collect();

        (jobs, total)
    }

    /// Clean jobs older than grace period by state
    pub async fn clean(
        &self,
        queue_name: &str,
        grace_ms: u64,
        state: crate::protocol::JobState,
        limit: Option<usize>,
    ) -> usize {
        use crate::protocol::JobState;

        let now = now_ms();
        let cutoff = now.saturating_sub(grace_ms);
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut removed = 0;
        let max_remove = limit.unwrap_or(usize::MAX);

        match state {
            JobState::Failed => {
                // Clean from DLQ
                let mut shard = self.shards[idx].write();
                if let Some(dlq) = shard.dlq.get_mut(&queue_arc) {
                    let mut to_remove = Vec::new();

                    for (i, job) in dlq.iter().enumerate() {
                        // Use <= to include jobs created at exactly the cutoff time
                        // If grace_ms is 0, clean all jobs (cutoff = now)
                        if job.created_at <= cutoff && removed < max_remove {
                            to_remove.push(i);
                            self.unindex_job(job.id);
                            removed += 1;
                        }
                    }

                    // Remove in reverse order to preserve indices
                    for i in to_remove.into_iter().rev() {
                        dlq.remove(i);
                    }
                }
            }

            JobState::Waiting | JobState::Delayed => {
                // Clean from queue
                let mut shard = self.shards[idx].write();
                if let Some(heap) = shard.queues.get_mut(&queue_arc) {
                    let mut jobs: Vec<_> = std::mem::take(heap).into_vec();
                    let mut keys_to_remove: Vec<String> = Vec::new();

                    jobs.retain(|job| {
                        // Use <= to include jobs created at exactly the cutoff time
                        let should_remove = job.created_at <= cutoff && removed < max_remove;
                        if should_remove {
                            self.unindex_job(job.id);
                            if let Some(ref key) = job.unique_key {
                                keys_to_remove.push(key.clone());
                            }
                            removed += 1;
                        }
                        !should_remove
                    });

                    *heap = jobs.into_iter().collect();

                    // Remove unique keys after rebuilding heap
                    if !keys_to_remove.is_empty() {
                        if let Some(keys) = shard.unique_keys.get_mut(&queue_arc) {
                            for key in keys_to_remove {
                                keys.remove(&key);
                            }
                        }
                    }
                }
            }

            JobState::Completed => {
                // Clean from completed_jobs and job_results
                let mut completed = self.completed_jobs.write();
                let mut results = self.job_results.write();
                let mut logs = self.job_logs.write();

                // Note: completed_jobs only has IDs, no timestamps, so we remove oldest by ID
                let mut job_ids: Vec<u64> = completed.iter().copied().collect();
                job_ids.sort(); // Oldest first

                for id in job_ids.into_iter().take(max_remove) {
                    completed.remove(&id);
                    results.remove(&id);
                    logs.remove(&id);
                    self.unindex_job(id);
                    removed += 1;
                }
            }

            _ => {}
        }

        // Persist
        if let Some(ref storage) = self.storage {
            let state_str = match state {
                JobState::Waiting => "waiting",
                JobState::Delayed => "delayed",
                JobState::Active => "active",
                JobState::Completed => "completed",
                JobState::Failed => "failed",
                _ => "unknown",
            };
            let _ = storage.clean_jobs(queue_name, cutoff, state_str).await;
        }

        removed
    }

    /// Drain all waiting jobs from a queue (not DLQ or processing)
    pub async fn drain(&self, queue_name: &str) -> usize {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut removed = 0;

        {
            let mut shard = self.shards[idx].write();

            if let Some(heap) = shard.queues.remove(&queue_arc) {
                // Unindex all jobs
                for job in heap.iter() {
                    self.unindex_job(job.id);
                    // Clean up unique keys
                    if let Some(ref key) = job.unique_key {
                        if let Some(keys) = shard.unique_keys.get_mut(&queue_arc) {
                            keys.remove(key);
                        }
                    }
                }
                removed = heap.len();
            }
        }

        // Persist to PostgreSQL
        if let Some(ref storage) = self.storage {
            let _ = storage.drain_queue(queue_name).await;
        }

        removed
    }

    /// Remove ALL data for a queue (jobs, DLQ, cron, unique keys, queue state)
    pub async fn obliterate(&self, queue_name: &str) -> usize {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let mut total_removed = 0;

        // 1. Remove from shard
        {
            let mut shard = self.shards[idx].write();

            // Remove queue jobs
            if let Some(heap) = shard.queues.remove(&queue_arc) {
                for job in heap.iter() {
                    self.unindex_job(job.id);
                }
                total_removed += heap.len();
            }

            // Remove DLQ
            if let Some(dlq) = shard.dlq.remove(&queue_arc) {
                for job in dlq.iter() {
                    self.unindex_job(job.id);
                }
                total_removed += dlq.len();
            }

            // Remove unique keys
            shard.unique_keys.remove(&queue_arc);

            // Remove queue state
            shard.queue_state.remove(&queue_arc);

            // Remove waiting children for this queue
            let children_to_remove: Vec<u64> = shard
                .waiting_children
                .iter()
                .filter(|(_, job)| job.queue == queue_name)
                .map(|(id, _)| *id)
                .collect();

            for id in children_to_remove {
                if shard.waiting_children.remove(&id).is_some() {
                    self.unindex_job(id);
                    total_removed += 1;
                }
            }

            // Remove waiting deps for this queue
            let deps_to_remove: Vec<u64> = shard
                .waiting_deps
                .iter()
                .filter(|(_, job)| job.queue == queue_name)
                .map(|(id, _)| *id)
                .collect();

            for id in deps_to_remove {
                if shard.waiting_deps.remove(&id).is_some() {
                    self.unindex_job(id);
                    total_removed += 1;
                }
            }
        }

        // 2. Remove from processing (scan all processing jobs)
        {
            let mut processing = self.processing.write();
            let to_remove: Vec<u64> = processing
                .iter()
                .filter(|(_, job)| job.queue == queue_name)
                .map(|(id, _)| *id)
                .collect();

            for id in to_remove {
                processing.remove(&id);
                self.unindex_job(id);
                total_removed += 1;
            }
        }

        // 3. Remove cron jobs for this queue
        {
            let mut crons = self.cron_jobs.write();
            crons.retain(|_, cron| cron.queue != queue_name);
        }

        // 4. Persist
        if let Some(ref storage) = self.storage {
            let _ = storage.obliterate_queue(queue_name).await;
        }

        total_removed
    }

    /// Change priority of a job (waiting, delayed, or processing)
    pub async fn change_priority(&self, job_id: u64, new_priority: i32) -> Result<(), String> {
        let location = self.job_index.read().get(&job_id).copied();

        match location {
            Some(JobLocation::Processing) => {
                // Easy case: just update in HashMap
                {
                    let mut processing = self.processing.write();
                    if let Some(job) = processing.get_mut(&job_id) {
                        job.priority = new_priority;
                    } else {
                        return Err(format!("Job {} not found in processing", job_id));
                    }
                }

                // Persist (lock dropped before await)
                if let Some(ref storage) = self.storage {
                    let _ = storage.change_priority(job_id, new_priority).await;
                }
                Ok(())
            }

            Some(JobLocation::Queue { shard_idx }) => {
                // Hard case: need to rebuild heap
                let found = {
                    let mut shard = self.shards[shard_idx].write();

                    // Find which queue contains this job
                    let mut target_queue: Option<Arc<str>> = None;
                    for (queue_name, heap) in shard.queues.iter() {
                        if heap.iter().any(|j| j.id == job_id) {
                            target_queue = Some(Arc::clone(queue_name));
                            break;
                        }
                    }

                    if let Some(queue_name) = target_queue {
                        if let Some(heap) = shard.queues.get_mut(&queue_name) {
                            // Extract, modify, rebuild
                            let mut jobs: Vec<_> = std::mem::take(heap).into_vec();
                            if let Some(job) = jobs.iter_mut().find(|j| j.id == job_id) {
                                job.priority = new_priority;
                            }
                            *heap = jobs.into_iter().collect();
                        }
                        true
                    } else {
                        false
                    }
                };

                if found {
                    // Persist (lock dropped before await)
                    if let Some(ref storage) = self.storage {
                        let _ = storage.change_priority(job_id, new_priority).await;
                    }
                    return Ok(());
                }
                Err(format!("Job {} not found in queue", job_id))
            }

            Some(JobLocation::Dlq { shard_idx }) => {
                // Update in DLQ (VecDeque - easy)
                let found = {
                    let mut shard = self.shards[shard_idx].write();
                    let mut found = false;
                    for dlq in shard.dlq.values_mut() {
                        if let Some(job) = dlq.iter_mut().find(|j| j.id == job_id) {
                            job.priority = new_priority;
                            found = true;
                            break;
                        }
                    }
                    found
                };

                if found {
                    // Persist (lock dropped before await)
                    if let Some(ref storage) = self.storage {
                        let _ = storage.change_priority(job_id, new_priority).await;
                    }
                    return Ok(());
                }
                Err(format!("Job {} not found in DLQ", job_id))
            }

            Some(JobLocation::WaitingDeps { shard_idx }) => {
                let found = {
                    let mut shard = self.shards[shard_idx].write();
                    if let Some(job) = shard.waiting_deps.get_mut(&job_id) {
                        job.priority = new_priority;
                        true
                    } else {
                        false
                    }
                };

                if found {
                    // Persist (lock dropped before await)
                    if let Some(ref storage) = self.storage {
                        let _ = storage.change_priority(job_id, new_priority).await;
                    }
                    return Ok(());
                }
                Err(format!("Job {} not found in waiting deps", job_id))
            }

            Some(JobLocation::WaitingChildren { shard_idx }) => {
                let found = {
                    let mut shard = self.shards[shard_idx].write();
                    if let Some(job) = shard.waiting_children.get_mut(&job_id) {
                        job.priority = new_priority;
                        true
                    } else {
                        false
                    }
                };

                if found {
                    // Persist (lock dropped before await)
                    if let Some(ref storage) = self.storage {
                        let _ = storage.change_priority(job_id, new_priority).await;
                    }
                    return Ok(());
                }
                Err(format!("Job {} not found in waiting children", job_id))
            }

            _ => Err(format!(
                "Job {} not found or cannot change priority",
                job_id
            )),
        }
    }

    /// Move a processing job back to delayed state
    pub async fn move_to_delayed(&self, job_id: u64, delay_ms: u64) -> Result<(), String> {
        let now = now_ms();

        // 1. Check job is in processing and remove it
        let job = {
            let mut processing = self.processing.write();
            processing
                .remove(&job_id)
                .ok_or_else(|| format!("Job {} not in processing", job_id))?
        };

        // 2. Update job
        let mut job = job;
        job.run_at = now + delay_ms;
        job.started_at = 0;
        job.progress = 0;
        job.progress_msg = None;

        // 3. Get shard and push to queue
        let idx = Self::shard_index(&job.queue);
        let queue_arc = intern(&job.queue);

        {
            let mut shard = self.shards[idx].write();

            // Release concurrency slot if any
            let state = shard.get_state(&queue_arc);
            if let Some(ref mut conc) = state.concurrency {
                conc.release();
            }

            let heap = shard.queues.entry(Arc::clone(&queue_arc)).or_default();
            heap.push(job.clone());
        }

        // 4. Update index
        self.index_job(job_id, JobLocation::Queue { shard_idx: idx });

        // 5. Persist
        if let Some(ref storage) = self.storage {
            let _ = storage.move_to_delayed(job_id, job.run_at).await;
        }

        Ok(())
    }

    /// Promote a delayed job to waiting (make it ready immediately)
    pub async fn promote(&self, job_id: u64) -> Result<(), String> {
        let location = self.job_index.read().get(&job_id).copied();
        let now = now_ms();

        match location {
            Some(JobLocation::Queue { shard_idx }) => {
                let (found, was_delayed) = {
                    let mut shard = self.shards[shard_idx].write();

                    // Find which queue contains this job
                    let mut target_queue: Option<Arc<str>> = None;
                    for (queue_name, heap) in shard.queues.iter() {
                        if heap.iter().any(|j| j.id == job_id) {
                            target_queue = Some(Arc::clone(queue_name));
                            break;
                        }
                    }

                    if let Some(queue_name) = target_queue {
                        if let Some(heap) = shard.queues.get_mut(&queue_name) {
                            let mut jobs: Vec<_> = std::mem::take(heap).into_vec();
                            let mut was_delayed = false;

                            // Find and update the job's run_at to now
                            if let Some(job) = jobs.iter_mut().find(|j| j.id == job_id) {
                                if job.run_at > now {
                                    job.run_at = now;
                                    was_delayed = true;
                                }
                            }

                            *heap = jobs.into_iter().collect();
                            (true, was_delayed)
                        } else {
                            (false, false)
                        }
                    } else {
                        (false, false)
                    }
                };

                if !found {
                    return Err(format!("Job {} not found in queue", job_id));
                }
                if !was_delayed {
                    return Err(format!("Job {} is not delayed", job_id));
                }

                // Persist (lock dropped)
                if let Some(ref storage) = self.storage {
                    let _ = storage.promote_job(job_id, now).await;
                }

                Ok(())
            }
            _ => Err(format!("Job {} not found or not in delayed state", job_id)),
        }
    }

    /// Update job data
    pub async fn update_job_data(
        &self,
        job_id: u64,
        new_data: serde_json::Value,
    ) -> Result<(), String> {
        let location = self.job_index.read().get(&job_id).copied();

        match location {
            Some(JobLocation::Processing) => {
                let found = {
                    let mut processing = self.processing.write();
                    if let Some(job) = processing.get_mut(&job_id) {
                        job.data = new_data.clone();
                        true
                    } else {
                        false
                    }
                };

                if found {
                    // Persist (lock dropped)
                    if let Some(ref storage) = self.storage {
                        let _ = storage.update_job_data(job_id, &new_data).await;
                    }
                    Ok(())
                } else {
                    Err(format!("Job {} not found in processing", job_id))
                }
            }

            Some(JobLocation::Queue { shard_idx }) => {
                let found = {
                    let mut shard = self.shards[shard_idx].write();

                    let mut target_queue: Option<Arc<str>> = None;
                    for (queue_name, heap) in shard.queues.iter() {
                        if heap.iter().any(|j| j.id == job_id) {
                            target_queue = Some(Arc::clone(queue_name));
                            break;
                        }
                    }

                    if let Some(queue_name) = target_queue {
                        if let Some(heap) = shard.queues.get_mut(&queue_name) {
                            let mut jobs: Vec<_> = std::mem::take(heap).into_vec();
                            if let Some(job) = jobs.iter_mut().find(|j| j.id == job_id) {
                                job.data = new_data.clone();
                            }
                            *heap = jobs.into_iter().collect();
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if found {
                    // Persist (lock dropped)
                    if let Some(ref storage) = self.storage {
                        let _ = storage.update_job_data(job_id, &new_data).await;
                    }
                    Ok(())
                } else {
                    Err(format!("Job {} not found in queue", job_id))
                }
            }

            Some(JobLocation::Dlq { shard_idx }) => {
                let found = {
                    let mut shard = self.shards[shard_idx].write();
                    let mut updated = false;
                    for dlq in shard.dlq.values_mut() {
                        if let Some(job) = dlq.iter_mut().find(|j| j.id == job_id) {
                            job.data = new_data.clone();
                            updated = true;
                            break;
                        }
                    }
                    updated
                };

                if found {
                    // Persist (lock dropped)
                    if let Some(ref storage) = self.storage {
                        let _ = storage.update_job_data(job_id, &new_data).await;
                    }
                    Ok(())
                } else {
                    Err(format!("Job {} not found in DLQ", job_id))
                }
            }

            _ => Err(format!("Job {} not found", job_id)),
        }
    }

    /// Discard a job (prevent further retries, move to DLQ immediately)
    pub async fn discard(&self, job_id: u64) -> Result<(), String> {
        let location = self.job_index.read().get(&job_id).copied();

        match location {
            Some(JobLocation::Processing) => {
                // Remove from processing
                let job = {
                    let mut processing = self.processing.write();
                    processing
                        .remove(&job_id)
                        .ok_or_else(|| format!("Job {} not in processing", job_id))?
                };

                // Move directly to DLQ
                let idx = Self::shard_index(&job.queue);
                let queue_arc = intern(&job.queue);

                {
                    let mut shard = self.shards[idx].write();
                    let dlq = shard.dlq.entry(Arc::clone(&queue_arc)).or_default();
                    dlq.push_back(job);
                }

                // Update index
                self.index_job(job_id, JobLocation::Dlq { shard_idx: idx });

                // Persist
                if let Some(ref storage) = self.storage {
                    let _ = storage.discard_job(job_id).await;
                }

                Ok(())
            }

            Some(JobLocation::Queue { shard_idx }) => {
                // Remove from queue and move to DLQ
                let found = {
                    let mut shard = self.shards[shard_idx].write();

                    let mut target_queue: Option<Arc<str>> = None;
                    let mut found_job: Option<crate::protocol::Job> = None;

                    for (queue_name, heap) in shard.queues.iter() {
                        if heap.iter().any(|j| j.id == job_id) {
                            target_queue = Some(Arc::clone(queue_name));
                            break;
                        }
                    }

                    if let Some(queue_name) = target_queue.clone() {
                        if let Some(heap) = shard.queues.get_mut(&queue_name) {
                            let mut jobs: Vec<_> = std::mem::take(heap).into_vec();
                            if let Some(pos) = jobs.iter().position(|j| j.id == job_id) {
                                found_job = Some(jobs.remove(pos));
                            }
                            *heap = jobs.into_iter().collect();
                        }
                    }

                    if let Some(job) = found_job {
                        let dlq = shard
                            .dlq
                            .entry(target_queue.unwrap_or_else(|| intern(&job.queue)))
                            .or_default();
                        dlq.push_back(job);

                        // Update index
                        self.index_job(job_id, JobLocation::Dlq { shard_idx });
                        true
                    } else {
                        false
                    }
                };

                if found {
                    // Persist (lock dropped)
                    if let Some(ref storage) = self.storage {
                        let _ = storage.discard_job(job_id).await;
                    }
                    Ok(())
                } else {
                    Err(format!("Job {} not found in queue", job_id))
                }
            }

            _ => Err(format!(
                "Job {} not found or already in DLQ/completed",
                job_id
            )),
        }
    }

    /// Check if a queue is paused
    pub fn is_paused(&self, queue_name: &str) -> bool {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let shard = self.shards[idx].read();

        shard
            .queue_state
            .get(&queue_arc)
            .map(|s| s.paused)
            .unwrap_or(false)
    }

    /// Get total job count for a queue (waiting + delayed)
    pub fn count(&self, queue_name: &str) -> usize {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let shard = self.shards[idx].read();

        shard.queues.get(&queue_arc).map(|h| h.len()).unwrap_or(0)
    }

    /// Get job counts by state for a queue
    pub fn get_job_counts(&self, queue_name: &str) -> (usize, usize, usize, usize, usize) {
        let now = now_ms();
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);

        let mut waiting = 0;
        let mut delayed = 0;
        let mut failed = 0;

        // Count waiting and delayed
        {
            let shard = self.shards[idx].read();
            if let Some(heap) = shard.queues.get(&queue_arc) {
                for job in heap.iter() {
                    if job.run_at > now {
                        delayed += 1;
                    } else {
                        waiting += 1;
                    }
                }
            }

            // Count failed (DLQ)
            if let Some(dlq) = shard.dlq.get(&queue_arc) {
                failed = dlq.len();
            }
        }

        // Count active (processing)
        let active = {
            let processing = self.processing.read();
            processing
                .values()
                .filter(|j| j.queue == queue_name)
                .count()
        };

        // Count completed (we don't track per-queue completed, so return 0 or estimate)
        let completed = 0; // Note: completed_jobs doesn't track per-queue

        (waiting, active, delayed, completed, failed)
    }
}

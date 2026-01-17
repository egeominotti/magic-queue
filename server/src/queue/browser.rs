//! Job browser operations.
//!
//! List, filter, and cleanup jobs across states.

use super::manager::QueueManager;
use super::types::{intern, now_ms};
use crate::protocol::{JobBrowserItem, JobState};

impl QueueManager {
    /// Get jobs filtered by queue and/or state with pagination.
    /// This matches the original API signature used by main.rs and http.rs.
    pub fn get_jobs(
        &self,
        queue_filter: Option<&str>,
        state_filter: Option<JobState>,
        limit: usize,
        offset: usize,
    ) -> (Vec<JobBrowserItem>, usize) {
        let now = now_ms();
        let limit = limit.min(1000);
        let mut jobs: Vec<JobBrowserItem> = Vec::new();

        // 1. Collect from all shards (waiting/delayed)
        for shard in &self.shards {
            let shard_r = shard.read();

            // Queues (waiting/delayed)
            for (queue_name, heap) in &shard_r.queues {
                if queue_filter.is_some_and(|f| f != queue_name.as_str()) {
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
                    if queue_filter.is_some_and(|f| f != queue_name.as_str()) {
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

        // 2. Processing (active) - iterate all shards
        if state_filter.is_none() || state_filter == Some(JobState::Active) {
            self.processing_iter(|job| {
                if queue_filter.is_some_and(|f| f != job.queue.as_str()) {
                    return;
                }
                jobs.push(JobBrowserItem {
                    job: job.clone(),
                    state: JobState::Active,
                });
            });
        }

        // 3. Sort by ID desc (newest first)
        jobs.sort_by(|a, b| b.job.id.cmp(&a.job.id));

        // 4. Paginate
        let total = jobs.len();
        let jobs = jobs.into_iter().skip(offset).take(limit).collect();

        (jobs, total)
    }

    /// Clean jobs older than grace period by state.
    /// Matches the original API signature used by http.rs.
    pub async fn clean(
        &self,
        queue_name: &str,
        grace_ms: u64,
        state: JobState,
        limit: Option<usize>,
    ) -> usize {
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
                // Clean from queue using IPQ's retain method
                let mut shard = self.shards[idx].write();
                if let Some(heap) = shard.queues.get_mut(&queue_arc) {
                    let mut keys_to_remove: Vec<String> = Vec::new();
                    let mut jobs_to_remove: Vec<u64> = Vec::new();

                    // First pass: collect jobs to remove
                    for job in heap.iter() {
                        if job.created_at <= cutoff && removed < max_remove {
                            jobs_to_remove.push(job.id);
                            if let Some(ref key) = job.unique_key {
                                keys_to_remove.push(key.clone());
                            }
                            removed += 1;
                        }
                    }

                    // Second pass: O(1) removal for each job
                    for job_id in jobs_to_remove {
                        heap.remove(job_id);
                        self.unindex_job(job_id);
                    }

                    // Remove unique keys
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

    /// Drain all waiting jobs from a queue (not DLQ or processing).
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

    /// Remove ALL data for a queue (jobs, DLQ, cron, unique keys, queue state).
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

        // 2. Remove from processing (scan all processing shards)
        {
            let mut to_remove: Vec<u64> = Vec::new();
            self.processing_iter(|job| {
                if job.queue == queue_name {
                    to_remove.push(job.id);
                }
            });

            for id in to_remove {
                self.processing_remove(id);
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
}

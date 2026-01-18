//! Push operations for adding jobs to the queue.
//!
//! Contains `push` and `push_batch` implementations for the QueueManager.

use serde_json::Value;

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation};
use super::validation::{validate_job_data, validate_queue_name, MAX_BATCH_SIZE};
use crate::protocol::{Job, JobBuilder, JobEvent, JobInput};

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
        // OPTIMIZATION: Uses nested CompactString map to avoid String allocation
        if let Some(ref id) = debounce_id {
            let now = now_ms();
            let queue_key = intern(&queue);
            let id_key = intern(id);
            let debounce_cache = self.debounce_cache.read();
            if let Some(queue_map) = debounce_cache.get(&queue_key) {
                if let Some(&expiry) = queue_map.get(&id_key) {
                    if now < expiry {
                        return Err(format!(
                            "Debounced: job with id '{}' was pushed recently",
                            id
                        ));
                    }
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

        // Track if this job has unsatisfied dependencies
        let needs_waiting_deps: bool;

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
                needs_waiting_deps = !deps_satisfied;

                if needs_waiting_deps {
                    shard.waiting_deps.insert(job.id, job.clone());
                    self.index_job(job.id, JobLocation::WaitingDeps { shard_idx: idx });
                }
            } else {
                needs_waiting_deps = false;
            }

            // Add to queue if deps are satisfied or no deps
            if !needs_waiting_deps {
                shard
                    .queues
                    .entry(queue_name)
                    .or_default()
                    .push(job.clone());
                self.index_job(job.id, JobLocation::Queue { shard_idx: idx });
            }
        } // Lock released here before any await

        // Handle jobs waiting for dependencies
        if needs_waiting_deps {
            // Use sync persistence if enabled for durability guarantee
            if self.is_sync_persistence() {
                if let Err(e) = self.persist_push_sync(&job, "waiting_children").await {
                    // Rollback: remove from waiting_deps and index
                    let mut shard = self.shards[idx].write();
                    shard.waiting_deps.remove(&job.id);
                    self.unindex_job(job.id);
                    return Err(e);
                }
            } else {
                self.persist_push(&job, "waiting_children");
            }
            return Ok(job);
        }

        // Persist to PostgreSQL - use sync mode if enabled
        if self.is_sync_persistence() {
            if let Err(e) = self.persist_push_sync(&job, "waiting").await {
                // Rollback: remove from queue and index
                let rollback_queue = intern(&queue);
                let mut shard = self.shards[idx].write();
                if let Some(heap) = shard.queues.get_mut(&rollback_queue) {
                    heap.retain(|j| j.id != job.id);
                }
                self.unindex_job(job.id);
                return Err(e);
            }
        } else {
            self.persist_push(&job, "waiting");
        }

        self.metrics.record_push(1);
        self.notify_shard(idx);

        // Update debounce cache
        // OPTIMIZATION: Uses nested CompactString map to avoid String allocation
        if let Some(ref id) = debounce_id {
            let queue_key = intern(&queue);
            let id_key = intern(id);
            let ttl = debounce_ttl.unwrap_or(5000); // Default 5 seconds
            let expiry = now_ms() + ttl;
            self.debounce_cache
                .write()
                .entry(queue_key)
                .or_default()
                .insert(id_key, expiry);
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
        // OPTIMIZATION: Uses nested CompactString map to avoid String allocation
        let now = now_ms();
        let queue_key = intern(&queue);
        let valid_jobs: Vec<_> = {
            let debounce_cache = self.debounce_cache.read();
            let queue_debounce = debounce_cache.get(&queue_key);
            jobs.into_iter()
                .filter(|input| {
                    // Check data validity
                    if validate_job_data(&input.data).is_err() {
                        return false;
                    }
                    // Check debounce
                    if let Some(ref id) = input.debounce_id {
                        if let Some(queue_map) = queue_debounce {
                            let id_key = intern(id);
                            if let Some(&expiry) = queue_map.get(&id_key) {
                                if now < expiry {
                                    return false; // Debounced
                                }
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
        // OPTIMIZATION: Use CompactString for debounce updates to avoid String allocation
        let mut debounce_updates: Vec<(compact_str::CompactString, u64)> = Vec::new();

        let idx = Self::shard_index(&queue);
        let queue_name = intern(&queue);

        // Check dependencies without cloning the entire completed set
        {
            let completed = self.completed_jobs.read();
            for (input, job_id) in valid_jobs.into_iter().zip(job_ids.into_iter()) {
                // Track debounce updates
                if let Some(ref id) = input.debounce_id {
                    let id_key = intern(id);
                    let ttl = input.debounce_ttl.unwrap_or(5000);
                    debounce_updates.push((id_key, now + ttl));
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
        // OPTIMIZATION: Uses nested CompactString map
        if !debounce_updates.is_empty() {
            let mut cache = self.debounce_cache.write();
            let queue_map = cache.entry(queue_key).or_default();
            for (id_key, expiry) in debounce_updates {
                queue_map.insert(id_key, expiry);
            }
        }

        self.metrics.record_push(ids.len() as u64);
        self.notify_shard(idx);
        ids
    }
}

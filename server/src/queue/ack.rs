//! Acknowledgment and failure operations for completed/failed jobs.
//!
//! Contains `ack`, `ack_batch`, `fail`, and `get_result` implementations.

use std::sync::atomic::Ordering;

use serde_json::Value;

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation};
use crate::protocol::JobEvent;

impl QueueManager {
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

            // Persist to PostgreSQL - use sync mode if enabled for durability
            if self.is_sync_persistence() {
                // Note: If sync persist fails, the job is already removed from processing
                // but we still return success because the in-memory state is correct.
                // On restart, PostgreSQL will show the job as "active" and timeout logic
                // will handle re-queueing it.
                let _ = self.persist_ack_sync(job_id, result.clone()).await;
            } else {
                self.persist_ack(job_id, result.clone());
            }
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
                    // OPTIMIZATION: Update atomic counter (processing -> removed)
                    self.metrics.record_fail();
                    // Decrement processing counter manually since record_dlq() not called
                    self.metrics
                        .current_processing
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
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
                    // OPTIMIZATION: Update atomic counter (processing -> DLQ)
                    self.metrics.record_dlq();

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

            // Persist first - use sync mode if enabled for durability
            if self.is_sync_persistence() {
                let _ = self
                    .persist_fail_sync(job_id, new_run_at, job.attempts)
                    .await;
            } else {
                self.persist_fail(job_id, new_run_at, job.attempts);
            }

            // Then move job into queue (no clone)
            self.shards[idx]
                .write()
                .queues
                .entry(queue_arc)
                .or_default()
                .push(job);

            // OPTIMIZATION: Update atomic counter (processing -> queue)
            self.metrics.record_retry();

            self.notify_shard(idx);
            return Ok(());
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn get_result(&self, job_id: u64) -> Option<Value> {
        self.job_results.read().get(&job_id).cloned()
    }
}

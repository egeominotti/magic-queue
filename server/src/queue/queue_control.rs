//! Queue control operations.
//!
//! Pause, resume, list queues, and queue statistics.

use super::manager::QueueManager;
use super::types::{intern, now_ms};
use crate::protocol::QueueInfo;

impl QueueManager {
    /// Pause a queue (workers will block until resumed).
    pub async fn pause(&self, queue: &str) {
        let idx = Self::shard_index(queue);
        let queue_arc = intern(queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.paused = true;
    }

    /// Resume a paused queue.
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

    /// List all queues with their current stats.
    pub async fn list_queues(&self) -> Vec<QueueInfo> {
        let mut queues = Vec::new();

        for shard in &self.shards {
            // Use read lock instead of write lock - this is a read-only operation
            let s = shard.read();
            for (name, heap) in &s.queues {
                let state = s.queue_state.get(name);
                queues.push(QueueInfo {
                    name: name.to_string(),
                    pending: heap.len(),
                    processing: self.processing_count_by_queue(name.as_str()),
                    dlq: s.dlq.get(name).map_or(0, |d| d.len()),
                    paused: state.is_some_and(|s| s.paused),
                    rate_limit: state.and_then(|s| s.rate_limiter.as_ref().map(|r| r.limit)),
                    concurrency_limit: state.and_then(|s| s.concurrency.as_ref().map(|c| c.limit)),
                });
            }
        }
        queues
    }

    /// Check if a queue is paused.
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

    /// Get total job count for a queue (waiting + delayed).
    pub fn count(&self, queue_name: &str) -> usize {
        let idx = Self::shard_index(queue_name);
        let queue_arc = intern(queue_name);
        let shard = self.shards[idx].read();

        shard.queues.get(&queue_arc).map(|h| h.len()).unwrap_or(0)
    }

    /// Get job counts by state for a queue.
    /// Returns (waiting, active, delayed, completed, failed).
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

        // Count active (processing) - use sharded method
        let active = self.processing_count_by_queue(queue_name);

        // Count completed (we don't track per-queue completed, so return 0 or estimate)
        let completed = 0; // Note: completed_jobs doesn't track per-queue

        (waiting, active, delayed, completed, failed)
    }
}

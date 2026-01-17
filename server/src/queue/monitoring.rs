//! Monitoring operations.
//!
//! Metrics, statistics, and stalled job detection.

use std::sync::atomic::Ordering;

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation, Subscriber};
use crate::protocol::{MetricsData, QueueMetrics};

impl QueueManager {
    /// Get detailed metrics for all queues.
    pub async fn get_metrics(&self) -> MetricsData {
        let latency_count = self.metrics.latency_count.load(Ordering::Relaxed);
        let avg_latency = if latency_count > 0 {
            self.metrics.latency_sum.load(Ordering::Relaxed) as f64 / latency_count as f64
        } else {
            0.0
        };

        let mut queues = Vec::new();

        for shard in &self.shards {
            let s = shard.write();
            for (name, heap) in &s.queues {
                let state = s.queue_state.get(name);
                queues.push(QueueMetrics {
                    name: name.to_string(),
                    pending: heap.len(),
                    processing: self.processing_count_by_queue(name.as_str()),
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

    /// Get summary statistics.
    /// Returns (ready, processing, delayed, dlq).
    pub async fn stats(&self) -> (usize, usize, usize, usize) {
        let now = now_ms();
        let (mut ready, mut delayed, mut dlq) = (0, 0, 0);
        let processing = self.processing_len();

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

    // === Pub/Sub ===

    /// Subscribe to events for a queue.
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

    /// Unsubscribe from events for a queue.
    pub fn unsubscribe(&self, queue: &str) {
        let queue_arc = intern(queue);
        self.subscribers.write().retain(|s| s.queue != queue_arc);
    }

    // === Stalled Jobs Detection ===

    /// Send heartbeat for a job to prevent stall detection.
    pub fn heartbeat(&self, job_id: u64) -> Result<(), String> {
        let updated = self.processing_get_mut(job_id, |job| {
            job.last_heartbeat = now_ms();
        });
        if updated.is_some() {
            // Reset stall count on successful heartbeat
            self.stalled_count.write().remove(&job_id);
            Ok(())
        } else {
            Err(format!("Job {} not in processing", job_id))
        }
    }

    /// Check for stalled jobs and handle them.
    pub(crate) fn check_stalled_jobs(&self) {
        let now = now_ms();
        // Collect only job IDs and stall_count (no clone of entire Job)
        let mut stalled_job_ids: Vec<(u64, u32)> = Vec::new();

        // Find stalled jobs (iterate all processing shards)
        self.processing_iter(|job| {
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
                stalled_job_ids.push((job.id, job.stall_count));
            }
        });

        // Handle stalled jobs
        for (job_id, stall_count) in stalled_job_ids {
            let mut stall_counts = self.stalled_count.write();
            let count = stall_counts.entry(job_id).or_insert(0);
            *count += 1;

            if *count >= 3 {
                // Too many stalls, move to DLQ
                drop(stall_counts);
                if let Some(job) = self.processing_remove(job_id) {
                    let idx = Self::shard_index(&job.queue);
                    let queue_arc = intern(&job.queue);
                    // Persist first (needs reference), then move (consumes ownership)
                    self.persist_dlq(&job, Some("Job stalled"));
                    self.index_job(job_id, JobLocation::Dlq { shard_idx: idx });
                    self.shards[idx]
                        .write()
                        .dlq
                        .entry(queue_arc)
                        .or_default()
                        .push_back(job); // Move, no clone!
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
                self.processing_get_mut(job_id, |job_mut| {
                    job_mut.last_heartbeat = now;
                    job_mut.stall_count = stall_count + 1;
                });

                // Log the stall warning
                let _ = self.add_job_log(
                    job_id,
                    format!("Job stall detected (count: {})", stall_count + 1),
                    "warn".to_string(),
                );
            }
        }
    }
}

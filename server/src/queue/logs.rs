//! Job logs operations.
//!
//! Add and retrieve log entries for jobs during processing.

use super::manager::QueueManager;
use super::types::now_ms;
use crate::protocol::JobLogEntry;

impl QueueManager {
    /// Add a log entry to a job.
    pub fn add_job_log(&self, job_id: u64, message: String, level: String) -> Result<(), String> {
        // Verify job exists (in processing or waiting)
        let location = self.job_index.get(&job_id).map(|r| *r);
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
        job_logs.push_back(entry);

        // Limit logs per job (max 100) - O(1) with VecDeque
        if job_logs.len() > 100 {
            job_logs.pop_front();
        }

        Ok(())
    }

    /// Get all logs for a job.
    pub fn get_job_logs(&self, job_id: u64) -> Vec<JobLogEntry> {
        self.job_logs
            .read()
            .get(&job_id)
            .map(|logs| logs.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Cleanup logs for completed/old jobs.
    pub(crate) fn cleanup_job_logs(&self) {
        let completed = self.completed_jobs.read();
        let mut logs = self.job_logs.write();

        // Remove logs for jobs that are completed and no longer in index
        logs.retain(|job_id, _| {
            // Keep if job is still in index (active) or not completed
            self.job_index.contains_key(job_id) || !completed.contains(job_id)
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

    /// Cleanup expired debounce entries.
    /// OPTIMIZATION: Works with nested CompactString map structure
    pub(crate) fn cleanup_debounce_cache(&self) {
        let now = now_ms();
        let mut cache = self.debounce_cache.write();

        // Remove expired entries from each queue's debounce map
        for queue_map in cache.values_mut() {
            queue_map.retain(|_, &mut expiry| expiry > now);
        }

        // Remove empty queue maps
        cache.retain(|_, queue_map| !queue_map.is_empty());
    }
}

use std::collections::BinaryHeap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use croner::Cron;
use tokio::time::{interval, Duration};

use super::manager::QueueManager;
use super::types::{intern, cleanup_interned_strings};

impl QueueManager {
    pub async fn background_tasks(self: Arc<Self>) {
        let mut wakeup_ticker = interval(Duration::from_millis(100)); // Wake up waiting workers
        let mut cron_ticker = interval(Duration::from_secs(1));
        let mut cleanup_ticker = interval(Duration::from_secs(60));
        let mut timeout_ticker = interval(Duration::from_millis(500));

        loop {
            tokio::select! {
                _ = wakeup_ticker.tick() => {
                    // Wake up workers that may have missed push notifications
                    self.notify_all();
                    self.check_dependencies().await;
                }
                _ = timeout_ticker.tick() => {
                    self.check_timed_out_jobs().await;
                }
                _ = cron_ticker.tick() => {
                    self.run_cron_jobs().await;
                }
                _ = cleanup_ticker.tick() => {
                    self.cleanup_completed_jobs();
                    self.cleanup_job_results();
                    cleanup_interned_strings();
                }
            }
        }
    }

    pub(crate) async fn check_timed_out_jobs(&self) {
        let now = Self::now_ms();
        let mut timed_out = Vec::new();

        {
            let proc = self.processing.read();
            for (id, job) in proc.iter() {
                if job.is_timed_out(now) {
                    timed_out.push(*id);
                }
            }
        }

        for job_id in timed_out {
            if let Some(mut job) = self.processing.write().remove(&job_id) {
                // Release concurrency
                let idx = Self::shard_index(&job.queue);
                let queue_arc = intern(&job.queue);
                {
                    let mut shard = self.shards[idx].write();
                    let state = shard.get_state(&queue_arc);
                    if let Some(ref mut conc) = state.concurrency {
                        conc.release();
                    }
                }

                job.attempts += 1;

                if job.should_go_to_dlq() {
                    self.notify_subscribers("timeout", &job.queue, &job);
                    self.index_job(job_id, super::types::JobLocation::Dlq { shard_idx: idx });
                    self.shards[idx].write().dlq.entry(queue_arc).or_default().push_back(job.clone());
                    self.metrics.record_timeout();

                    // Persist to PostgreSQL
                    self.persist_dlq(&job, Some("Job timed out"));
                } else {
                    let backoff = job.next_backoff();
                    let new_run_at = if backoff > 0 { now + backoff } else { job.run_at };
                    job.run_at = new_run_at;
                    job.started_at = 0;
                    job.progress_msg = Some("Job timed out".to_string());

                    self.index_job(job_id, super::types::JobLocation::Queue { shard_idx: idx });
                    self.shards[idx].write().queues
                        .entry(queue_arc)
                        .or_insert_with(BinaryHeap::new)
                        .push(job.clone());

                    // Persist to PostgreSQL
                    self.persist_fail(job_id, new_run_at, job.attempts);

                    self.notify_shard(idx);
                }
            }
        }
    }

    pub(crate) async fn check_dependencies(&self) {
        let completed = self.completed_jobs.read().clone();
        if completed.is_empty() { return; }

        for (idx, shard) in self.shards.iter().enumerate() {
            let mut shard_w = shard.write();
            let mut ready_jobs = Vec::new();

            shard_w.waiting_deps.retain(|_, job| {
                if job.depends_on.iter().all(|dep| completed.contains(dep)) {
                    ready_jobs.push(job.clone());
                    false
                } else {
                    true
                }
            });

            if !ready_jobs.is_empty() {
                for job in ready_jobs {
                    let queue_arc = intern(&job.queue);
                    shard_w.queues
                        .entry(queue_arc)
                        .or_insert_with(BinaryHeap::new)
                        .push(job);
                }
                drop(shard_w);
                self.notify_shard(idx);
            }
        }
    }

    pub(crate) fn cleanup_completed_jobs(&self) {
        let mut completed = self.completed_jobs.write();
        if completed.len() > 100_000 {
            let to_remove: Vec<_> = completed.iter().take(50_000).copied().collect();
            for id in to_remove {
                completed.remove(&id);
            }
        }
    }

    pub(crate) fn cleanup_job_results(&self) {
        let mut results = self.job_results.write();
        if results.len() > 10_000 {
            let to_remove: Vec<_> = results.keys().take(5_000).copied().collect();
            for id in to_remove {
                results.remove(&id);
            }
        }
    }

    pub(crate) async fn run_cron_jobs(&self) {
        let now = Self::now_ms();
        let mut to_run = Vec::new();
        let mut next_run_updates = Vec::new();

        {
            let mut crons = self.cron_jobs.write();
            for cron in crons.values_mut() {
                if cron.next_run <= now {
                    to_run.push((cron.queue.clone(), cron.data.clone(), cron.priority));
                    let new_next_run = Self::parse_next_cron_run(&cron.schedule, now);
                    cron.next_run = new_next_run;
                    next_run_updates.push((cron.name.clone(), new_next_run));
                }
            }
        }

        // Persist next_run updates to PostgreSQL
        for (name, next_run) in next_run_updates {
            self.persist_cron_next_run(&name, next_run);
        }

        for (queue, data, priority) in to_run {
            let _ = self.push(queue, data, priority, None, None, None, None, None, None, None, None).await;
        }
    }

    /// Parse cron expression and calculate next run time.
    /// Supports:
    /// - Legacy format: `*/N` (every N seconds)
    /// - Full 6-field cron: `sec min hour day month weekday`
    /// - Standard 5-field cron: `min hour day month weekday` (assumes 0 seconds)
    pub(crate) fn parse_next_cron_run(schedule: &str, now: u64) -> u64 {
        // Backwards compatibility: */N format (simple interval in seconds)
        if let Some(interval_str) = schedule.strip_prefix("*/") {
            if let Ok(secs) = interval_str.parse::<u64>() {
                return now + secs * 1000;
            }
        }

        // Full cron expression parsing with croner
        match Cron::new(schedule).parse() {
            Ok(cron) => {
                let now_secs = (now / 1000) as i64;
                if let Some(now_dt) = DateTime::<Utc>::from_timestamp(now_secs, 0) {
                    if let Ok(next) = cron.find_next_occurrence(&now_dt, false) {
                        return (next.timestamp() as u64) * 1000;
                    }
                }
            }
            Err(_) => {}
        }

        // Default fallback: 1 minute
        now + 60_000
    }

    /// Validate a cron expression before saving.
    /// Returns Ok(()) if valid, Err(message) if invalid.
    pub(crate) fn validate_cron(schedule: &str) -> Result<(), String> {
        // Allow legacy */N format
        if let Some(interval_str) = schedule.strip_prefix("*/") {
            return interval_str.parse::<u64>()
                .map(|_| ())
                .map_err(|_| format!("Invalid interval format: {}", schedule));
        }

        // Validate full cron expression
        Cron::new(schedule).parse()
            .map(|_| ())
            .map_err(|e| format!("Invalid cron expression '{}': {}", schedule, e))
    }
}

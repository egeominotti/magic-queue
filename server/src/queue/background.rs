use std::collections::BinaryHeap;
use std::sync::Arc;

use tokio::time::{interval, Duration};

use super::manager::QueueManager;
use super::types::WalEvent;

impl QueueManager {
    pub async fn background_tasks(self: Arc<Self>) {
        let mut ticker = interval(Duration::from_millis(50));
        let mut cron_ticker = interval(Duration::from_secs(1));
        let mut cleanup_ticker = interval(Duration::from_secs(60));
        let mut timeout_ticker = interval(Duration::from_millis(500));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.notify.notify_waiters();
                    self.check_dependencies().await;
                    self.cleanup_expired().await;
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
                {
                    let mut conc = self.concurrency_limiters.write();
                    if let Some(limiter) = conc.get_mut(&job.queue) {
                        limiter.release();
                    }
                }

                job.attempts += 1;
                let idx = Self::shard_index(&job.queue);

                if job.should_go_to_dlq() {
                    self.write_wal(&WalEvent::Dlq(job.clone()));
                    self.notify_subscribers("timeout", &job.queue, &job);
                    self.shards[idx].write().dlq.entry(job.queue.clone()).or_default().push_back(job);
                    self.metrics.record_timeout();
                } else {
                    let backoff = job.next_backoff();
                    if backoff > 0 {
                        job.run_at = now + backoff;
                    }
                    job.started_at = 0;
                    job.progress_msg = Some("Job timed out".to_string());

                    self.write_wal(&WalEvent::Fail(job_id));
                    self.shards[idx].write().queues
                        .entry(job.queue.clone())
                        .or_insert_with(BinaryHeap::new)
                        .push(job);
                    self.notify.notify_waiters();
                }
            }
        }
    }

    pub(crate) async fn check_dependencies(&self) {
        let completed = self.completed_jobs.read().clone();
        if completed.is_empty() { return; }

        for shard in &self.shards {
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

            for job in ready_jobs {
                shard_w.queues
                    .entry(job.queue.clone())
                    .or_insert_with(BinaryHeap::new)
                    .push(job);
            }
        }
        self.notify.notify_waiters();
    }

    pub(crate) async fn cleanup_expired(&self) {
        let now = Self::now_ms();

        for shard in &self.shards {
            let mut shard_w = shard.write();
            for heap in shard_w.queues.values_mut() {
                let jobs: Vec<_> = std::mem::take(heap).into_vec();
                for job in jobs {
                    if !job.is_expired(now) {
                        heap.push(job);
                    }
                }
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

        {
            let mut crons = self.cron_jobs.write();
            for cron in crons.values_mut() {
                if cron.next_run <= now {
                    to_run.push((cron.queue.clone(), cron.data.clone(), cron.priority));
                    cron.next_run = Self::parse_next_cron_run(&cron.schedule, now);
                }
            }
        }

        for (queue, data, priority) in to_run {
            let _ = self.push(queue, data, priority, None, None, None, None, None, None, None).await;
        }
    }

    pub(crate) fn parse_next_cron_run(schedule: &str, now: u64) -> u64 {
        if let Some(interval_str) = schedule.strip_prefix("*/") {
            if let Ok(secs) = interval_str.parse::<u64>() {
                return now + secs * 1000;
            }
        }
        now + 60_000
    }
}

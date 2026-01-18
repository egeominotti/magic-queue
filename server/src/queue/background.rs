use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use croner::Cron;
use sqlx::postgres::PgListener;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

use super::manager::QueueManager;
use super::postgres::CLUSTER_SYNC_CHANNEL;
use super::types::{cleanup_interned_strings, intern, now_ms, JobLocation};

impl QueueManager {
    /// Run background tasks (cleanup, cron, metrics, etc.).
    /// Checks shutdown flag periodically for graceful termination.
    pub async fn background_tasks(self: Arc<Self>) {
        let mut wakeup_ticker = interval(Duration::from_millis(500)); // Wake up waiting workers (reduced from 100ms - notify_shard handles immediate wakeups)
        let mut cron_ticker = interval(Duration::from_secs(1));
        let mut cleanup_ticker = interval(Duration::from_secs(60));
        let mut timeout_ticker = interval(Duration::from_millis(500));
        let mut stalled_ticker = interval(Duration::from_secs(10)); // Check stalled jobs every 10s
        let mut metrics_ticker = interval(Duration::from_secs(5)); // Collect metrics every 5s
        let mut cluster_ticker = interval(Duration::from_secs(5)); // Cluster heartbeat every 5s
        let mut snapshot_ticker = interval(Duration::from_secs(1)); // Check snapshot every 1s

        info!("Background tasks started");

        loop {
            // Check for shutdown signal
            if self.is_shutdown() {
                info!("Background tasks received shutdown signal, stopping...");
                // Final snapshot before shutdown if enabled
                if self.snapshot_config.is_some() {
                    info!("Taking final snapshot before shutdown...");
                    self.maybe_snapshot().await;
                }
                // Unregister from cluster if enabled
                if let Some(ref cluster) = self.cluster {
                    info!("Unregistering from cluster...");
                    if let Err(e) = cluster.unregister().await {
                        warn!(error = %e, "Failed to unregister from cluster");
                    }
                }
                info!("Background tasks stopped");
                return;
            }

            tokio::select! {
                _ = wakeup_ticker.tick() => {
                    // Wake up workers that may have missed push notifications
                    // This runs on all nodes
                    self.notify_all();
                    self.check_dependencies().await;
                }
                _ = timeout_ticker.tick() => {
                    // Only leader checks timeouts
                    if self.is_leader() {
                        self.check_timed_out_jobs().await;
                    }
                }
                _ = stalled_ticker.tick() => {
                    // Only leader checks stalled jobs
                    if self.is_leader() {
                        self.check_stalled_jobs();
                    }
                }
                _ = cron_ticker.tick() => {
                    // Only leader runs cron jobs
                    if self.is_leader() {
                        self.run_cron_jobs().await;
                    }
                }
                _ = cleanup_ticker.tick() => {
                    // Only leader runs cleanup
                    if self.is_leader() {
                        self.cleanup_completed_jobs();
                        self.cleanup_job_results();
                        self.cleanup_job_logs();
                        self.cleanup_stale_index_entries();
                        self.cleanup_debounce_cache();
                        self.cleanup_expired_kv();
                        cleanup_interned_strings();
                    }
                }
                _ = metrics_ticker.tick() => {
                    // Metrics collection runs on all nodes
                    self.collect_metrics_history();
                }
                _ = cluster_ticker.tick() => {
                    // Cluster heartbeat and leader election
                    self.cluster_heartbeat().await;
                }
                _ = snapshot_ticker.tick() => {
                    // Snapshot persistence (Redis-style)
                    self.maybe_snapshot().await;
                }
            }
        }
    }

    /// Check if snapshot should be taken and execute it
    async fn maybe_snapshot(&self) {
        // Only run if snapshot mode is enabled
        let config = match &self.snapshot_config {
            Some(c) => c,
            None => return,
        };

        let now = now_ms();
        let last = self.last_snapshot.load(Ordering::Relaxed);
        let changes = self.snapshot_changes.load(Ordering::Relaxed);
        let interval_ms = config.interval_secs * 1000;

        // Check if we should snapshot:
        // - Enough time has passed AND minimum changes reached
        if now - last >= interval_ms && changes >= config.min_changes {
            self.execute_snapshot().await;
        }
    }

    /// Execute a full snapshot of all in-memory state to PostgreSQL
    async fn execute_snapshot(&self) {
        let storage = match &self.storage {
            Some(s) => Arc::clone(s),
            None => return,
        };

        let start = now_ms();
        let mut jobs_to_persist: Vec<(crate::protocol::Job, String)> = Vec::new();
        let mut dlq_to_persist: Vec<(crate::protocol::Job, Option<String>)> = Vec::new();

        // Collect all jobs from shards
        for shard in &self.shards {
            let shard = shard.read();

            // Jobs in queues (waiting/delayed)
            for heap in shard.queues.values() {
                for job in heap.iter() {
                    let state = if job.run_at > start {
                        "delayed"
                    } else {
                        "waiting"
                    };
                    jobs_to_persist.push((job.clone(), state.to_string()));
                }
            }

            // Jobs waiting for dependencies
            for job in shard.waiting_deps.values() {
                jobs_to_persist.push((job.clone(), "waiting_children".to_string()));
            }

            // Parent jobs waiting for children (Flows)
            for job in shard.waiting_children.values() {
                jobs_to_persist.push((job.clone(), "waiting_parent".to_string()));
            }

            // DLQ jobs
            for dlq in shard.dlq.values() {
                for job in dlq.iter() {
                    dlq_to_persist.push((job.clone(), None));
                }
            }
        }

        // Jobs in processing (active) - iterate all shards
        self.processing_iter(|job| {
            jobs_to_persist.push((job.clone(), "active".to_string()));
        });

        let job_count = jobs_to_persist.len();
        let dlq_count = dlq_to_persist.len();

        // Snapshot jobs
        if let Err(e) = storage.snapshot_jobs(&jobs_to_persist).await {
            error!(error = %e, "Snapshot failed");
            return;
        }

        // Snapshot DLQ if there are any
        if !dlq_to_persist.is_empty() {
            if let Err(e) = storage.snapshot_dlq(&dlq_to_persist).await {
                error!(error = %e, "DLQ snapshot failed");
            }
        }

        let elapsed = now_ms() - start;
        info!(
            jobs = job_count,
            dlq = dlq_count,
            elapsed_ms = elapsed,
            "Snapshot completed"
        );

        // Reset counters
        self.last_snapshot.store(now_ms(), Ordering::Relaxed);
        self.snapshot_changes.store(0, Ordering::Relaxed);
    }

    /// Start the cluster sync listener (runs in separate task)
    pub async fn start_cluster_sync_listener(self: Arc<Self>, database_url: &str) {
        let node_id = self.node_id().unwrap_or_default();

        loop {
            match PgListener::connect(database_url).await {
                Ok(mut listener) => {
                    if let Err(e) = listener.listen(CLUSTER_SYNC_CHANNEL).await {
                        error!(error = %e, "Failed to listen on cluster sync channel");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }

                    info!(node_id = %node_id, "Cluster sync listener started");

                    loop {
                        match listener.recv().await {
                            Ok(notification) => {
                                let payload = notification.payload();
                                self.handle_cluster_sync(payload, &node_id).await;
                            }
                            Err(e) => {
                                error!(error = %e, "Cluster sync listener error");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to connect cluster sync listener");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Handle a cluster sync notification
    async fn handle_cluster_sync(&self, payload: &str, my_node_id: &str) {
        let parts: Vec<&str> = payload.split(':').collect();
        if parts.is_empty() {
            return;
        }

        let source_node = parts[0];

        // Ignore notifications from ourselves
        if source_node == my_node_id {
            return;
        }

        // Handle batch sync: node_id:batch:queue:min_id:count
        if parts.len() >= 5 && parts[1] == "batch" {
            let queue = parts[2];
            let min_id: u64 = parts[3].parse().unwrap_or(0);
            let count: i64 = parts[4].parse().unwrap_or(0);

            if let Some(storage) = &self.storage {
                if let Ok(jobs) = storage.load_jobs_by_queue_since(queue, min_id, count).await {
                    for (job, state) in jobs {
                        self.sync_job_from_cluster(job, &state);
                    }
                }
            }
            return;
        }

        // Handle single job sync: node_id:job_id:queue
        if parts.len() >= 3 {
            let job_id: u64 = parts[1].parse().unwrap_or(0);
            if job_id == 0 {
                return;
            }

            if let Some(storage) = &self.storage {
                if let Ok(Some((job, state))) = storage.load_job_by_id(job_id).await {
                    self.sync_job_from_cluster(job, &state);
                }
            }
        }
    }

    /// Add a job received from cluster sync to local queues
    fn sync_job_from_cluster(&self, job: crate::protocol::Job, state: &str) {
        let job_id = job.id;
        let idx = Self::shard_index(&job.queue);
        let queue_name = intern(&job.queue);

        // Check if we already have this job (lock-free DashMap)
        if self.job_index.contains_key(&job_id) {
            return;
        }

        match state {
            "waiting" | "delayed" => {
                let mut shard = self.shards[idx].write();
                shard.queues.entry(queue_name).or_default().push(job);
                drop(shard);
                self.index_job(job_id, JobLocation::Queue { shard_idx: idx });
                self.notify_shard(idx);
            }
            "waiting_children" => {
                let mut shard = self.shards[idx].write();
                shard.waiting_deps.insert(job_id, job);
                drop(shard);
                self.index_job(job_id, JobLocation::WaitingDeps { shard_idx: idx });
            }
            _ => {}
        }
    }

    /// Perform cluster heartbeat and try to become leader
    async fn cluster_heartbeat(&self) {
        if let Some(cluster) = &self.cluster {
            // Send heartbeat
            if let Err(e) = cluster.heartbeat().await {
                warn!(error = %e, "Cluster heartbeat failed");
            }

            // Try to become leader if not already
            if let Err(e) = cluster.try_become_leader().await {
                warn!(error = %e, "Leader election check failed");
            }

            // Leader cleans up stale nodes
            if cluster.is_leader() {
                if let Err(e) = cluster.cleanup_stale_nodes().await {
                    warn!(error = %e, "Stale node cleanup failed");
                }
            }
        }
    }

    pub(crate) async fn check_timed_out_jobs(&self) {
        let now = Self::now_ms();
        let mut timed_out = Vec::new();

        // Iterate all processing shards to find timed out jobs
        self.processing_iter(|job| {
            if job.is_timed_out(now) {
                timed_out.push(job.id);
            }
        });

        for job_id in timed_out {
            if let Some(mut job) = self.processing_remove(job_id) {
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
                    // Persist first (needs reference)
                    self.persist_dlq(&job, Some("Job timed out"));
                    self.metrics.record_timeout();
                    // OPTIMIZATION: Update atomic counter (processing -> DLQ)
                    self.metrics.record_dlq();
                    // Then move (no clone)
                    self.shards[idx]
                        .write()
                        .dlq
                        .entry(queue_arc)
                        .or_default()
                        .push_back(job);
                } else {
                    let backoff = job.next_backoff();
                    let new_run_at = if backoff > 0 {
                        now + backoff
                    } else {
                        job.run_at
                    };
                    job.run_at = new_run_at;
                    job.started_at = 0;
                    job.progress_msg = Some("Job timed out".to_string());

                    self.index_job(job_id, super::types::JobLocation::Queue { shard_idx: idx });
                    // Persist first (uses primitives)
                    self.persist_fail(job_id, new_run_at, job.attempts);
                    // OPTIMIZATION: Update atomic counter (processing -> queue)
                    self.metrics.record_retry();
                    // Then move (no clone)
                    self.shards[idx]
                        .write()
                        .queues
                        .entry(queue_arc)
                        .or_default()
                        .push(job);

                    self.notify_shard(idx);
                }
            }
        }
    }

    pub(crate) async fn check_dependencies(&self) {
        // Check if there are any completed jobs without cloning
        if self.completed_jobs.read().is_empty() {
            return;
        }

        for (idx, shard) in self.shards.iter().enumerate() {
            let mut shard_w = shard.write();

            // First pass: identify ready job IDs (with completed lock held briefly)
            let ready_ids: Vec<u64> = {
                let completed = self.completed_jobs.read();
                shard_w
                    .waiting_deps
                    .iter()
                    .filter(|(_, job)| job.depends_on.iter().all(|dep| completed.contains(dep)))
                    .map(|(&id, _)| id)
                    .collect()
            };

            if ready_ids.is_empty() {
                continue;
            }

            // Second pass: remove and move jobs (no clone needed)
            for job_id in ready_ids {
                if let Some(job) = shard_w.waiting_deps.remove(&job_id) {
                    let queue_arc = intern(&job.queue);
                    shard_w.queues.entry(queue_arc).or_default().push(job);
                }
            }
            drop(shard_w);
            self.notify_shard(idx);
        }
    }

    /// Clean up completed jobs and their associated index entries.
    /// Also cleans up custom_id_map to prevent memory leaks and idempotency bugs.
    /// Uses a more aggressive cleanup strategy to prevent unbounded memory growth.
    pub(crate) fn cleanup_completed_jobs(&self) {
        const MAX_COMPLETED: usize = 50_000;
        const CLEANUP_BATCH: usize = 25_000;

        let mut completed = self.completed_jobs.write();
        if completed.len() > MAX_COMPLETED {
            let to_remove: Vec<_> = completed.iter().take(CLEANUP_BATCH).copied().collect();

            // Also clean up job_index for these completed jobs (lock-free DashMap)
            for &id in &to_remove {
                self.job_index.remove(&id);
            }

            // Clean up custom_id_map to prevent memory leak and idempotency bugs
            // This is critical: if we don't clean up, old custom IDs will point to
            // non-existent jobs, breaking idempotency guarantees
            {
                let mut custom_id_map = self.custom_id_map.write();
                custom_id_map.retain(|_, &mut internal_id| !to_remove.contains(&internal_id));
            }

            for id in to_remove {
                completed.remove(&id);
            }
        }
    }

    /// Clean up job results to prevent unbounded memory growth
    pub(crate) fn cleanup_job_results(&self) {
        const MAX_RESULTS: usize = 5_000;
        const CLEANUP_BATCH: usize = 2_500;

        let mut results = self.job_results.write();
        if results.len() > MAX_RESULTS {
            let to_remove: Vec<_> = results.keys().take(CLEANUP_BATCH).copied().collect();
            for id in to_remove {
                results.remove(&id);
            }
        }
    }

    /// Clean up stale entries in job_index that point to non-existent jobs.
    /// This handles edge cases where index entries weren't properly cleaned,
    /// including: completed jobs, processing jobs, queued jobs, and DLQ jobs.
    /// Called periodically by the cleanup background task.
    pub(crate) fn cleanup_stale_index_entries(&self) {
        use super::types::JobLocation;

        const MAX_INDEX_SIZE: usize = 100_000;
        const CLEANUP_BATCH: usize = 10_000;

        let index_len = self.job_index.len();
        if index_len <= MAX_INDEX_SIZE {
            return;
        }

        let mut to_remove = Vec::with_capacity(CLEANUP_BATCH);
        let completed_jobs = self.completed_jobs.read();

        // Iterate lock-free DashMap and check each location
        for entry in self.job_index.iter() {
            if to_remove.len() >= CLEANUP_BATCH {
                break;
            }

            let id = *entry.key();
            let location = *entry.value();

            let is_stale = match location {
                JobLocation::Completed => !completed_jobs.contains(&id),

                JobLocation::Processing => !self.processing_contains(id),

                JobLocation::Queue { shard_idx } => {
                    let shard = self.shards[shard_idx].read();
                    !shard.queues.values().any(|heap| heap.contains(id))
                }

                JobLocation::Dlq { shard_idx } => {
                    let shard = self.shards[shard_idx].read();
                    !shard.dlq.values().any(|dlq| dlq.iter().any(|j| j.id == id))
                }

                JobLocation::WaitingDeps { shard_idx } => {
                    let shard = self.shards[shard_idx].read();
                    !shard.waiting_deps.contains_key(&id)
                }

                JobLocation::WaitingChildren { shard_idx } => {
                    let shard = self.shards[shard_idx].read();
                    !shard.waiting_children.contains_key(&id)
                }
            };

            if is_stale {
                to_remove.push(id);
            }
        }

        drop(completed_jobs);

        // Remove stale entries (lock-free)
        for id in to_remove {
            self.job_index.remove(&id);
            // Also clean up associated data that might be orphaned
            self.job_logs.write().remove(&id);
            self.stalled_count.write().remove(&id);
        }
    }

    pub(crate) async fn run_cron_jobs(&self) {
        let now = Self::now_ms();
        let mut to_run = Vec::new();
        let mut next_run_updates = Vec::new();
        let mut to_remove = Vec::new();

        {
            let mut crons = self.cron_jobs.write();
            for cron in crons.values_mut() {
                if cron.next_run <= now {
                    // Check if limit reached
                    if let Some(limit) = cron.limit {
                        if cron.executions >= limit {
                            to_remove.push(cron.name.clone());
                            continue;
                        }
                    }

                    to_run.push((cron.queue.clone(), cron.data.clone(), cron.priority));
                    cron.executions += 1;

                    // Calculate next run time
                    let new_next_run = if let Some(interval) = cron.repeat_every {
                        now + interval
                    } else if let Some(ref schedule) = cron.schedule {
                        Self::parse_next_cron_run(schedule, now)
                    } else {
                        // Fallback: 1 minute
                        now + 60_000
                    };
                    cron.next_run = new_next_run;
                    next_run_updates.push((cron.name.clone(), new_next_run));
                }
            }

            // Remove crons that reached their limit
            for name in &to_remove {
                crons.remove(name);
            }
        }

        // Persist next_run updates to PostgreSQL
        for (name, next_run) in next_run_updates {
            self.persist_cron_next_run(&name, next_run);
        }

        // Remove completed crons from PostgreSQL
        for name in to_remove {
            self.persist_cron_delete(&name);
        }

        for (queue, data, priority) in to_run {
            let _ = self
                .push(
                    queue, data, priority, None, None, None, None, None, None, None, None, false,
                    false, false, None, None, None, None, None, None,
                )
                .await;
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

        // Full cron expression parsing with croner (supports both 5-field and 6-field with seconds)
        if let Ok(cron) = Cron::new(schedule).with_seconds_optional().parse() {
            let now_secs = (now / 1000) as i64;
            if let Some(now_dt) = DateTime::<Utc>::from_timestamp(now_secs, 0) {
                if let Ok(next) = cron.find_next_occurrence(&now_dt, false) {
                    return (next.timestamp() as u64) * 1000;
                }
            }
        }

        // Default fallback: 1 minute
        now + 60_000
    }

    /// Maximum cron schedule string length to prevent DoS
    const MAX_CRON_SCHEDULE_LENGTH: usize = 256;

    /// Validate a cron expression before saving.
    /// Returns Ok(()) if valid, Err(message) if invalid.
    pub(crate) fn validate_cron(schedule: &str) -> Result<(), String> {
        // Validate length to prevent DoS attacks with huge strings
        if schedule.len() > Self::MAX_CRON_SCHEDULE_LENGTH {
            return Err(format!(
                "Cron schedule too long ({} chars, max {} chars)",
                schedule.len(),
                Self::MAX_CRON_SCHEDULE_LENGTH
            ));
        }

        // Allow legacy */N format
        if let Some(interval_str) = schedule.strip_prefix("*/") {
            return interval_str
                .parse::<u64>()
                .map(|_| ())
                .map_err(|_| format!("Invalid interval format: {}", schedule));
        }

        // Validate full cron expression (supports both 5-field and 6-field with seconds)
        Cron::new(schedule)
            .with_seconds_optional()
            .parse()
            .map(|_| ())
            .map_err(|e| format!("Invalid cron expression '{}': {}", schedule, e))
    }
}

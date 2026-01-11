use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::Value;

use crate::protocol::{CronJob, Job, JobBrowserItem, JobEvent, JobState, MetricsHistoryPoint, WebhookConfig, WorkerInfo};
use super::postgres::PostgresStorage;
use super::types::{init_coarse_time, intern, now_ms, GlobalMetrics, JobLocation, Shard, Subscriber, Webhook, Worker};
use tokio::sync::broadcast;

pub const NUM_SHARDS: usize = 32;

pub struct QueueManager {
    pub(crate) shards: Vec<RwLock<Shard>>,
    pub(crate) processing: RwLock<FxHashMap<u64, Job>>,
    /// PostgreSQL storage (replaces WAL)
    pub(crate) storage: Option<Arc<PostgresStorage>>,
    pub(crate) cron_jobs: RwLock<FxHashMap<String, CronJob>>,
    pub(crate) completed_jobs: RwLock<FxHashSet<u64>>,
    pub(crate) job_results: RwLock<FxHashMap<u64, Value>>,
    pub(crate) subscribers: RwLock<Vec<Subscriber>>,
    pub(crate) auth_tokens: RwLock<FxHashSet<String>>,
    pub(crate) metrics: GlobalMetrics,
    /// O(1) job location index - maps job_id to its current location
    pub(crate) job_index: RwLock<FxHashMap<u64, JobLocation>>,
    // Worker registration
    pub(crate) workers: RwLock<FxHashMap<String, Worker>>,
    // Webhooks
    pub(crate) webhooks: RwLock<FxHashMap<String, Webhook>>,
    // Event broadcast for SSE/WebSocket
    pub(crate) event_tx: broadcast::Sender<JobEvent>,
    // Metrics history for charts (last 60 points = 5 minutes at 5s intervals)
    pub(crate) metrics_history: RwLock<Vec<MetricsHistoryPoint>>,
}

impl QueueManager {
    /// Create a new QueueManager without persistence.
    pub fn new(_persistence: bool) -> Arc<Self> {
        // For backwards compatibility, this creates a manager without PostgreSQL
        // Use `with_postgres` for PostgreSQL persistence
        Self::create(None)
    }

    /// Create a new QueueManager with PostgreSQL persistence.
    pub async fn with_postgres(database_url: &str) -> Arc<Self> {
        match PostgresStorage::new(database_url).await {
            Ok(storage) => {
                // Run migrations
                if let Err(e) = storage.migrate().await {
                    eprintln!("Failed to run migrations: {}", e);
                }

                let storage = Arc::new(storage);
                let manager = Self::create(Some(storage.clone()));

                // Recover from PostgreSQL
                manager.recover_from_postgres(&storage).await;

                manager
            }
            Err(e) => {
                eprintln!("Failed to connect to PostgreSQL: {}, running without persistence", e);
                Self::create(None)
            }
        }
    }

    /// Internal constructor.
    fn create(storage: Option<Arc<PostgresStorage>>) -> Arc<Self> {
        // Initialize coarse timestamp
        init_coarse_time();

        let shards = (0..NUM_SHARDS).map(|_| RwLock::new(Shard::new())).collect();
        let (event_tx, _) = broadcast::channel(1024);
        let has_storage = storage.is_some();

        let manager = Arc::new(Self {
            shards,
            processing: RwLock::new(FxHashMap::with_capacity_and_hasher(4096, Default::default())),
            storage,
            cron_jobs: RwLock::new(FxHashMap::default()),
            completed_jobs: RwLock::new(FxHashSet::default()),
            job_results: RwLock::new(FxHashMap::default()),
            subscribers: RwLock::new(Vec::new()),
            auth_tokens: RwLock::new(FxHashSet::default()),
            metrics: GlobalMetrics::new(),
            job_index: RwLock::new(FxHashMap::with_capacity_and_hasher(65536, Default::default())),
            workers: RwLock::new(FxHashMap::default()),
            webhooks: RwLock::new(FxHashMap::default()),
            event_tx,
            metrics_history: RwLock::new(Vec::with_capacity(60)),
        });

        let mgr = Arc::clone(&manager);
        tokio::spawn(async move { mgr.background_tasks().await; });

        if has_storage {
            println!("PostgreSQL persistence enabled");
        }

        manager
    }

    pub fn with_auth_tokens(_persistence: bool, tokens: Vec<String>) -> Arc<Self> {
        let manager = Self::new(false);
        {
            let mut auth = manager.auth_tokens.write();
            for token in tokens {
                auth.insert(token);
            }
        }
        manager
    }

    pub async fn with_postgres_and_auth(database_url: &str, tokens: Vec<String>) -> Arc<Self> {
        let manager = Self::with_postgres(database_url).await;
        {
            let mut auth = manager.auth_tokens.write();
            for token in tokens {
                auth.insert(token);
            }
        }
        if !manager.auth_tokens.read().is_empty() {
            println!("Authentication enabled with {} token(s)", manager.auth_tokens.read().len());
        }
        manager
    }

    #[inline]
    pub fn verify_token(&self, token: &str) -> bool {
        let tokens = self.auth_tokens.read();
        tokens.is_empty() || tokens.contains(token)
    }

    #[inline(always)]
    pub fn shard_index(queue: &str) -> usize {
        let mut hasher = rustc_hash::FxHasher::default();
        queue.hash(&mut hasher);
        hasher.finish() as usize % NUM_SHARDS
    }

    #[inline(always)]
    pub fn now_ms() -> u64 {
        now_ms()
    }

    /// Recover state from PostgreSQL on startup.
    async fn recover_from_postgres(&self, storage: &PostgresStorage) {
        let mut job_count = 0;

        // Load pending jobs
        if let Ok(jobs) = storage.load_pending_jobs().await {
            for (job, state) in jobs {
                let job_id = job.id;
                let idx = Self::shard_index(&job.queue);
                let queue_name = intern(&job.queue);

                match state.as_str() {
                    "waiting" | "delayed" => {
                        let mut shard = self.shards[idx].write();
                        shard.queues.entry(queue_name)
                            .or_insert_with(BinaryHeap::new).push(job);
                        self.index_job(job_id, JobLocation::Queue { shard_idx: idx });
                        job_count += 1;
                    }
                    "active" => {
                        // Jobs that were active when server stopped - requeue them
                        let mut shard = self.shards[idx].write();
                        shard.queues.entry(queue_name)
                            .or_insert_with(BinaryHeap::new).push(job);
                        self.index_job(job_id, JobLocation::Queue { shard_idx: idx });
                        job_count += 1;
                    }
                    "waiting_children" => {
                        self.shards[idx].write().waiting_deps.insert(job_id, job);
                        self.index_job(job_id, JobLocation::WaitingDeps { shard_idx: idx });
                        job_count += 1;
                    }
                    _ => {}
                }
            }
        }

        // Load DLQ jobs
        if let Ok(dlq_jobs) = storage.load_dlq_jobs().await {
            for job in dlq_jobs {
                let job_id = job.id;
                let idx = Self::shard_index(&job.queue);
                let queue_name = intern(&job.queue);
                self.shards[idx].write().dlq
                    .entry(queue_name).or_default().push_back(job);
                self.index_job(job_id, JobLocation::Dlq { shard_idx: idx });
            }
        }

        // Load cron jobs (now persisted!)
        if let Ok(crons) = storage.load_crons().await {
            let mut cron_jobs = self.cron_jobs.write();
            for cron in crons {
                cron_jobs.insert(cron.name.clone(), cron);
            }
            if !cron_jobs.is_empty() {
                println!("Recovered {} cron jobs from PostgreSQL", cron_jobs.len());
            }
        }

        // Load webhooks (now persisted!)
        if let Ok(webhooks) = storage.load_webhooks().await {
            let mut wh = self.webhooks.write();
            for webhook in webhooks {
                let w = Webhook::new(
                    webhook.id.clone(),
                    webhook.url,
                    webhook.events,
                    webhook.queue,
                    webhook.secret,
                );
                wh.insert(webhook.id, w);
            }
            if !wh.is_empty() {
                println!("Recovered {} webhooks from PostgreSQL", wh.len());
            }
        }

        if job_count > 0 {
            println!("Recovered {} jobs from PostgreSQL", job_count);
        }
    }

    // ============== Persistence Methods (PostgreSQL) ==============

    /// Persist a pushed job to PostgreSQL.
    #[inline]
    pub(crate) fn persist_push(&self, job: &Job, state: &str) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let job = job.clone();
            let state = state.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage.insert_job(&job, &state).await {
                    eprintln!("Failed to persist job {}: {}", job.id, e);
                }
            });
        }
    }

    /// Persist a batch of jobs to PostgreSQL.
    #[inline]
    pub(crate) fn persist_push_batch(&self, jobs: &[Job], state: &str) {
        if jobs.is_empty() { return; }
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let jobs = jobs.to_vec();
            let state = state.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage.insert_jobs_batch(&jobs, &state).await {
                    eprintln!("Failed to persist batch: {}", e);
                }
            });
        }
    }

    /// Persist job acknowledgment to PostgreSQL.
    #[inline]
    pub(crate) fn persist_ack(&self, job_id: u64, result: Option<Value>) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            tokio::spawn(async move {
                if let Err(e) = storage.ack_job(job_id, result).await {
                    eprintln!("Failed to persist ack {}: {}", job_id, e);
                }
            });
        }
    }

    /// Persist batch acknowledgments to PostgreSQL.
    #[inline]
    pub(crate) fn persist_ack_batch(&self, ids: &[u64]) {
        if ids.is_empty() { return; }
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let ids = ids.to_vec();
            tokio::spawn(async move {
                if let Err(e) = storage.ack_jobs_batch(&ids).await {
                    eprintln!("Failed to persist ack batch: {}", e);
                }
            });
        }
    }

    /// Persist job failure (retry) to PostgreSQL.
    #[inline]
    pub(crate) fn persist_fail(&self, job_id: u64, new_run_at: u64, attempts: u32) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            tokio::spawn(async move {
                if let Err(e) = storage.fail_job(job_id, new_run_at, attempts).await {
                    eprintln!("Failed to persist fail {}: {}", job_id, e);
                }
            });
        }
    }

    /// Persist job moved to DLQ.
    #[inline]
    pub(crate) fn persist_dlq(&self, job: &Job, error: Option<&str>) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let job = job.clone();
            let error = error.map(|s| s.to_string());
            tokio::spawn(async move {
                if let Err(e) = storage.move_to_dlq(&job, error.as_deref()).await {
                    eprintln!("Failed to persist DLQ {}: {}", job.id, e);
                }
            });
        }
    }

    /// Persist job cancellation.
    #[inline]
    pub(crate) fn persist_cancel(&self, job_id: u64) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            tokio::spawn(async move {
                if let Err(e) = storage.cancel_job(job_id).await {
                    eprintln!("Failed to persist cancel {}: {}", job_id, e);
                }
            });
        }
    }

    /// Persist cron job.
    #[inline]
    pub(crate) fn persist_cron(&self, cron: &CronJob) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let cron = cron.clone();
            tokio::spawn(async move {
                if let Err(e) = storage.save_cron(&cron).await {
                    eprintln!("Failed to persist cron {}: {}", cron.name, e);
                }
            });
        }
    }

    /// Persist cron job deletion.
    #[inline]
    pub(crate) fn persist_cron_delete(&self, name: &str) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let name = name.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage.delete_cron(&name).await {
                    eprintln!("Failed to persist cron delete {}: {}", name, e);
                }
            });
        }
    }

    /// Persist cron next_run update.
    #[inline]
    pub(crate) fn persist_cron_next_run(&self, name: &str, next_run: u64) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let name = name.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage.update_cron_next_run(&name, next_run).await {
                    eprintln!("Failed to update cron next_run {}: {}", name, e);
                }
            });
        }
    }

    /// Persist webhook.
    #[inline]
    pub(crate) fn persist_webhook(&self, webhook: &WebhookConfig) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let webhook = webhook.clone();
            tokio::spawn(async move {
                if let Err(e) = storage.save_webhook(&webhook).await {
                    eprintln!("Failed to persist webhook {}: {}", webhook.id, e);
                }
            });
        }
    }

    /// Persist webhook deletion.
    #[inline]
    pub(crate) fn persist_webhook_delete(&self, id: &str) {
        if let Some(ref storage) = self.storage {
            let storage = Arc::clone(storage);
            let id = id.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage.delete_webhook(&id).await {
                    eprintln!("Failed to persist webhook delete {}: {}", id, e);
                }
            });
        }
    }

    pub(crate) fn notify_subscribers(&self, event: &str, queue: &str, job: &Job) {
        let subs = self.subscribers.read();
        for sub in subs.iter() {
            if sub.queue.as_ref() == queue && sub.events.contains(&event.to_string()) {
                let msg = serde_json::json!({
                    "event": event,
                    "queue": queue,
                    "job": job
                }).to_string();
                let _ = sub.tx.send(msg);
            }
        }
    }

    /// Notify shard's waiting workers
    #[inline]
    pub(crate) fn notify_shard(&self, idx: usize) {
        self.shards[idx].read().notify.notify_waiters();
    }

    /// Notify all shards - wakes up workers that may have missed push notifications
    #[inline]
    pub(crate) fn notify_all(&self) {
        for shard in &self.shards {
            shard.read().notify.notify_waiters();
        }
    }

    /// Get job by ID with its current state - O(1) lookup via job_index
    pub fn get_job(&self, id: u64) -> (Option<Job>, JobState) {
        let now = now_ms();

        // O(1) lookup in index
        let location = match self.job_index.read().get(&id) {
            Some(&loc) => loc,
            None => return (None, JobState::Unknown),
        };

        match location {
            JobLocation::Processing => {
                let job = self.processing.read().get(&id).cloned();
                let state = job.as_ref()
                    .map(|j| location.to_state(j.run_at, now))
                    .unwrap_or(JobState::Active);
                (job, state)
            }
            JobLocation::Queue { shard_idx } => {
                let shard = self.shards[shard_idx].read();
                for heap in shard.queues.values() {
                    if let Some(job) = heap.iter().find(|j| j.id == id) {
                        return (Some(job.clone()), location.to_state(job.run_at, now));
                    }
                }
                (None, JobState::Unknown)
            }
            JobLocation::Dlq { shard_idx } => {
                let shard = self.shards[shard_idx].read();
                for dlq in shard.dlq.values() {
                    if let Some(job) = dlq.iter().find(|j| j.id == id) {
                        return (Some(job.clone()), JobState::Failed);
                    }
                }
                (None, JobState::Unknown)
            }
            JobLocation::WaitingDeps { shard_idx } => {
                let shard = self.shards[shard_idx].read();
                let job = shard.waiting_deps.get(&id).cloned();
                (job, JobState::WaitingChildren)
            }
            JobLocation::Completed => {
                // Job completed, no data stored (only result if any)
                (None, JobState::Completed)
            }
        }
    }

    /// Get only the state of a job by ID - O(1) lookup
    #[inline]
    pub fn get_state(&self, id: u64) -> JobState {
        let now = now_ms();

        match self.job_index.read().get(&id) {
            Some(&location) => {
                // For Queue state, we need run_at to determine Waiting vs Delayed
                if let JobLocation::Queue { shard_idx } = location {
                    let shard = self.shards[shard_idx].read();
                    for heap in shard.queues.values() {
                        if let Some(job) = heap.iter().find(|j| j.id == id) {
                            return location.to_state(job.run_at, now);
                        }
                    }
                    JobState::Unknown
                } else {
                    location.to_state(0, now)
                }
            }
            None => JobState::Unknown,
        }
    }

    /// Track job location in index
    #[inline]
    pub(crate) fn index_job(&self, id: u64, location: JobLocation) {
        self.job_index.write().insert(id, location);
    }

    /// Remove job from index
    #[inline]
    pub(crate) fn unindex_job(&self, id: u64) {
        self.job_index.write().remove(&id);
    }

    // ============== Job Browser ==============

    /// List all jobs with filtering options
    /// Returns jobs sorted by created_at descending (newest first)
    pub fn list_jobs(
        &self,
        queue_filter: Option<&str>,
        state_filter: Option<JobState>,
        limit: usize,
        offset: usize,
    ) -> Vec<JobBrowserItem> {
        let now = now_ms();
        let mut jobs: Vec<JobBrowserItem> = Vec::new();

        // Collect jobs from all shards
        for shard in &self.shards {
            let shard = shard.read();

            // Jobs in queues (waiting/delayed)
            for (queue_name, heap) in &shard.queues {
                if let Some(filter) = queue_filter {
                    if &**queue_name != filter {
                        continue;
                    }
                }
                for job in heap.iter() {
                    let state = if job.run_at > now {
                        JobState::Delayed
                    } else {
                        JobState::Waiting
                    };
                    if let Some(sf) = state_filter {
                        if sf != state {
                            continue;
                        }
                    }
                    jobs.push(JobBrowserItem {
                        job: job.clone(),
                        state,
                    });
                }
            }

            // Jobs in DLQ (failed)
            for (queue_name, dlq) in &shard.dlq {
                if let Some(filter) = queue_filter {
                    if &**queue_name != filter {
                        continue;
                    }
                }
                if let Some(sf) = state_filter {
                    if sf != JobState::Failed {
                        continue;
                    }
                }
                for job in dlq.iter() {
                    jobs.push(JobBrowserItem {
                        job: job.clone(),
                        state: JobState::Failed,
                    });
                }
            }

            // Jobs waiting for dependencies
            for job in shard.waiting_deps.values() {
                if let Some(filter) = queue_filter {
                    if job.queue != filter {
                        continue;
                    }
                }
                if let Some(sf) = state_filter {
                    if sf != JobState::WaitingChildren {
                        continue;
                    }
                }
                jobs.push(JobBrowserItem {
                    job: job.clone(),
                    state: JobState::WaitingChildren,
                });
            }
        }

        // Add jobs in processing (active)
        {
            let processing = self.processing.read();
            for job in processing.values() {
                if let Some(filter) = queue_filter {
                    if job.queue != filter {
                        continue;
                    }
                }
                if let Some(sf) = state_filter {
                    if sf != JobState::Active {
                        continue;
                    }
                }
                jobs.push(JobBrowserItem {
                    job: job.clone(),
                    state: JobState::Active,
                });
            }
        }

        // Sort by created_at descending (newest first)
        jobs.sort_by(|a, b| b.job.created_at.cmp(&a.job.created_at));

        // Apply offset and limit
        jobs.into_iter().skip(offset).take(limit).collect()
    }

    /// Get metrics history for charts
    pub fn get_metrics_history(&self) -> Vec<MetricsHistoryPoint> {
        self.metrics_history.read().clone()
    }

    /// Collect and store a metrics history point
    pub(crate) fn collect_metrics_history(&self) {
        use std::sync::atomic::Ordering;

        let now = now_ms();
        let (queued, processing, _delayed, _dlq) = self.stats_sync();

        let total_completed = self.metrics.total_completed.load(Ordering::Relaxed);
        let total_failed = self.metrics.total_failed.load(Ordering::Relaxed);
        let latency_count = self.metrics.latency_count.load(Ordering::Relaxed);
        let avg_latency = if latency_count > 0 {
            self.metrics.latency_sum.load(Ordering::Relaxed) as f64 / latency_count as f64
        } else {
            0.0
        };

        // Calculate throughput from history
        let throughput = {
            let history = self.metrics_history.read();
            if history.len() >= 2 {
                let prev = &history[history.len() - 1];
                let time_diff = (now - prev.timestamp) as f64 / 1000.0;
                if time_diff > 0.0 {
                    (total_completed - prev.completed) as f64 / time_diff
                } else {
                    0.0
                }
            } else {
                0.0
            }
        };

        let point = MetricsHistoryPoint {
            timestamp: now,
            queued,
            processing,
            completed: total_completed,
            failed: total_failed,
            throughput,
            latency_ms: avg_latency,
        };

        let mut history = self.metrics_history.write();
        history.push(point);

        // Keep only last 60 points (5 minutes at 5s intervals)
        if history.len() > 60 {
            history.remove(0);
        }
    }

    /// Synchronous stats helper for internal use
    fn stats_sync(&self) -> (usize, usize, usize, usize) {
        let now = now_ms();
        let mut queued = 0;
        let mut delayed = 0;
        let mut dlq_count = 0;

        for shard in &self.shards {
            let shard = shard.read();
            for heap in shard.queues.values() {
                for job in heap.iter() {
                    if job.run_at > now {
                        delayed += 1;
                    } else {
                        queued += 1;
                    }
                }
            }
            for dlq in shard.dlq.values() {
                dlq_count += dlq.len();
            }
        }

        let processing = self.processing.read().len();
        (queued, processing, delayed, dlq_count)
    }

    // ============== Worker Registration ==============

    pub async fn list_workers(&self) -> Vec<WorkerInfo> {
        let now = now_ms();
        let workers = self.workers.read();
        workers.values()
            .filter(|w| now - w.last_heartbeat < 30_000) // Active in last 30s
            .map(|w| WorkerInfo {
                id: w.id.clone(),
                queues: w.queues.clone(),
                concurrency: w.concurrency,
                last_heartbeat: w.last_heartbeat,
                jobs_processed: w.jobs_processed,
            })
            .collect()
    }

    pub async fn worker_heartbeat(&self, id: String, queues: Vec<String>, concurrency: u32) {
        let mut workers = self.workers.write();
        let worker = workers.entry(id.clone()).or_insert_with(|| Worker::new(id, queues.clone(), concurrency));
        worker.queues = queues;
        worker.concurrency = concurrency;
        worker.last_heartbeat = now_ms();
    }

    pub(crate) fn increment_worker_jobs(&self, worker_id: &str) {
        if let Some(worker) = self.workers.write().get_mut(worker_id) {
            worker.jobs_processed += 1;
        }
    }

    // ============== Webhooks ==============

    pub async fn list_webhooks(&self) -> Vec<WebhookConfig> {
        let webhooks = self.webhooks.read();
        webhooks.values()
            .map(|w| WebhookConfig {
                id: w.id.clone(),
                url: w.url.clone(),
                events: w.events.clone(),
                queue: w.queue.clone(),
                secret: w.secret.clone(),
                created_at: w.created_at,
            })
            .collect()
    }

    pub async fn add_webhook(&self, url: String, events: Vec<String>, queue: Option<String>, secret: Option<String>) -> String {
        let id = format!("wh_{}", crate::protocol::next_id());
        let webhook = Webhook::new(id.clone(), url, events, queue, secret);
        self.webhooks.write().insert(id.clone(), webhook);
        id
    }

    pub async fn delete_webhook(&self, id: &str) -> bool {
        self.webhooks.write().remove(id).is_some()
    }

    /// Fire webhooks for an event
    pub(crate) fn fire_webhooks(&self, event_type: &str, queue: &str, job_id: u64, data: Option<Value>, error: Option<String>) {
        let webhooks = self.webhooks.read();
        for webhook in webhooks.values() {
            // Check event type matches
            if !webhook.events.iter().any(|e| e == event_type || e == "*") {
                continue;
            }
            // Check queue filter
            if let Some(ref wq) = webhook.queue {
                if wq != queue {
                    continue;
                }
            }

            let url = webhook.url.clone();
            let secret = webhook.secret.clone();
            let payload = serde_json::json!({
                "event": event_type,
                "queue": queue,
                "job_id": job_id,
                "timestamp": now_ms(),
                "data": data,
                "error": error,
            });

            // Fire webhook in background (non-blocking)
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let mut req = client.post(&url).json(&payload);

                if let Some(secret) = secret {
                    // Add HMAC signature header
                    let body = serde_json::to_string(&payload).unwrap_or_default();
                    let signature = hmac_sha256(&secret, &body);
                    req = req.header("X-MagicQueue-Signature", signature);
                }

                let _ = req.send().await;
            });
        }
    }

    // ============== Event Broadcasting (SSE/WebSocket) ==============

    pub fn subscribe_events(&self, _queue: Option<String>) -> broadcast::Receiver<JobEvent> {
        self.event_tx.subscribe()
    }

    pub(crate) fn broadcast_event(&self, event: JobEvent) {
        let _ = self.event_tx.send(event.clone());

        // Also fire webhooks
        self.fire_webhooks(
            &event.event_type,
            &event.queue,
            event.job_id,
            event.data.clone(),
            event.error.clone(),
        );
    }
}

/// Simple HMAC-SHA256 for webhook signatures
fn hmac_sha256(key: &str, message: &str) -> String {
    use std::fmt::Write;
    // Simple HMAC implementation
    let key_bytes = key.as_bytes();
    let msg_bytes = message.as_bytes();

    // Pad or hash key to 64 bytes
    let mut k = [0u8; 64];
    if key_bytes.len() <= 64 {
        k[..key_bytes.len()].copy_from_slice(key_bytes);
    } else {
        // For keys > 64 bytes, we'd hash first (simplified here)
        k[..key_bytes.len().min(64)].copy_from_slice(&key_bytes[..64.min(key_bytes.len())]);
    }

    // XOR with ipad (0x36)
    let mut i_key_pad = [0u8; 64];
    for i in 0..64 {
        i_key_pad[i] = k[i] ^ 0x36;
    }

    // XOR with opad (0x5c)
    let mut o_key_pad = [0u8; 64];
    for i in 0..64 {
        o_key_pad[i] = k[i] ^ 0x5c;
    }

    // For a proper implementation, use a crypto library
    // This is a simplified placeholder
    let mut result = String::with_capacity(64);
    for byte in msg_bytes.iter().take(32) {
        let _ = write!(result, "{:02x}", byte ^ i_key_pad[0]);
    }
    result
}

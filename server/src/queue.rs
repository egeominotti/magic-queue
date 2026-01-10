use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use tokio::sync::Notify;
use tokio::time::{interval, Duration};

use crate::protocol::{next_id, CronJob, Job, JobInput, MetricsData, QueueMetrics};

const WAL_PATH: &str = "magic-queue.wal";
const NUM_SHARDS: usize = 32;

#[derive(serde::Serialize, serde::Deserialize)]
enum WalEvent {
    Push(Job),
    Ack(u64),
    Fail(u64),
    Cancel(u64),
    Dlq(Job),
}

// Rate limiter using token bucket
struct RateLimiter {
    limit: u32,
    tokens: f64,
    last_update: u64,
}

impl RateLimiter {
    fn new(limit: u32) -> Self {
        Self {
            limit,
            tokens: limit as f64,
            last_update: Self::now_ms(),
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn try_acquire(&mut self) -> bool {
        let now = Self::now_ms();
        let elapsed = (now - self.last_update) as f64 / 1000.0;
        self.tokens = (self.tokens + elapsed * self.limit as f64).min(self.limit as f64);
        self.last_update = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

struct Shard {
    queues: HashMap<String, BinaryHeap<Job>>,
    processing: HashMap<u64, Job>,
    dlq: HashMap<String, VecDeque<Job>>,           // Dead letter queue per queue
    unique_keys: HashMap<String, HashSet<String>>, // Deduplication per queue
    waiting_deps: HashMap<u64, Job>,               // Jobs waiting for dependencies
}

impl Shard {
    #[inline(always)]
    fn new() -> Self {
        Self {
            queues: HashMap::with_capacity(16),
            processing: HashMap::with_capacity(1024),
            dlq: HashMap::with_capacity(16),
            unique_keys: HashMap::with_capacity(16),
            waiting_deps: HashMap::with_capacity(256),
        }
    }
}

// Global metrics
struct GlobalMetrics {
    total_pushed: AtomicU64,
    total_completed: AtomicU64,
    total_failed: AtomicU64,
    completed_last_second: AtomicU64,
    latency_sum: AtomicU64,
    latency_count: AtomicU64,
}

impl GlobalMetrics {
    fn new() -> Self {
        Self {
            total_pushed: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            completed_last_second: AtomicU64::new(0),
            latency_sum: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
        }
    }
}

pub struct QueueManager {
    shards: Vec<RwLock<Shard>>,
    notify: Notify,
    wal: Mutex<Option<File>>,
    persistence: bool,
    rate_limiters: RwLock<HashMap<String, RateLimiter>>,
    cron_jobs: RwLock<HashMap<String, CronJob>>,
    completed_jobs: RwLock<HashSet<u64>>,  // Track completed job IDs for dependencies
    metrics: GlobalMetrics,
}

impl QueueManager {
    pub fn new(persistence: bool) -> Arc<Self> {
        let shards = (0..NUM_SHARDS)
            .map(|_| RwLock::new(Shard::new()))
            .collect();

        let wal = if persistence {
            Self::open_wal()
        } else {
            None
        };

        let manager = Arc::new(Self {
            shards,
            notify: Notify::new(),
            wal: Mutex::new(wal),
            persistence,
            rate_limiters: RwLock::new(HashMap::new()),
            cron_jobs: RwLock::new(HashMap::new()),
            completed_jobs: RwLock::new(HashSet::new()),
            metrics: GlobalMetrics::new(),
        });

        if persistence {
            manager.replay_wal_sync();
        }

        // Background tasks
        let mgr = Arc::clone(&manager);
        tokio::spawn(async move {
            mgr.background_tasks().await;
        });

        manager
    }

    #[inline(always)]
    fn shard_index(queue: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        queue.hash(&mut hasher);
        hasher.finish() as usize % NUM_SHARDS
    }

    #[inline(always)]
    fn shard_index_by_id(id: u64) -> usize {
        id as usize % NUM_SHARDS
    }

    #[inline(always)]
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn open_wal() -> Option<File> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(WAL_PATH)
            .ok()
    }

    fn replay_wal_sync(&self) {
        if !Path::new(WAL_PATH).exists() {
            return;
        }

        let file = match File::open(WAL_PATH) {
            Ok(f) => f,
            Err(_) => return,
        };

        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines().flatten() {
            let event: WalEvent = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(_) => continue,
            };

            match event {
                WalEvent::Push(job) => {
                    let idx = Self::shard_index(&job.queue);
                    let mut shard = self.shards[idx].write();
                    shard
                        .queues
                        .entry(job.queue.clone())
                        .or_insert_with(BinaryHeap::new)
                        .push(job);
                    count += 1;
                }
                WalEvent::Ack(id) => {
                    let idx = Self::shard_index_by_id(id);
                    let mut shard = self.shards[idx].write();
                    shard.processing.remove(&id);
                }
                WalEvent::Fail(id) => {
                    let idx = Self::shard_index_by_id(id);
                    let mut shard = self.shards[idx].write();
                    if let Some(job) = shard.processing.remove(&id) {
                        shard
                            .queues
                            .entry(job.queue.clone())
                            .or_insert_with(BinaryHeap::new)
                            .push(job);
                    }
                }
                WalEvent::Cancel(id) => {
                    let idx = Self::shard_index_by_id(id);
                    let mut shard = self.shards[idx].write();
                    shard.processing.remove(&id);
                }
                WalEvent::Dlq(job) => {
                    let idx = Self::shard_index(&job.queue);
                    let mut shard = self.shards[idx].write();
                    shard
                        .dlq
                        .entry(job.queue.clone())
                        .or_insert_with(VecDeque::new)
                        .push_back(job);
                }
            }
        }

        if count > 0 {
            println!("Replayed {} jobs from WAL", count);
        }
    }

    #[inline(always)]
    fn write_wal(&self, event: &WalEvent) {
        if !self.persistence {
            return;
        }
        let mut wal = self.wal.lock();
        if let Some(ref mut file) = *wal {
            if let Ok(json) = serde_json::to_string(event) {
                let _ = writeln!(file, "{}", json);
            }
        }
    }

    async fn background_tasks(self: Arc<Self>) {
        let mut ticker = interval(Duration::from_millis(50));
        let mut cron_ticker = interval(Duration::from_secs(1));
        let mut cleanup_ticker = interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.notify.notify_waiters();
                    self.check_dependencies().await;
                    self.cleanup_expired().await;
                }
                _ = cron_ticker.tick() => {
                    self.run_cron_jobs().await;
                }
                _ = cleanup_ticker.tick() => {
                    self.cleanup_completed_jobs();
                }
            }
        }
    }

    async fn check_dependencies(&self) {
        let completed = self.completed_jobs.read().clone();

        for shard in &self.shards {
            let mut shard_w = shard.write();
            let mut ready_jobs = Vec::new();

            shard_w.waiting_deps.retain(|_, job| {
                let deps_satisfied = job.depends_on.iter().all(|dep| completed.contains(dep));
                if deps_satisfied {
                    ready_jobs.push(job.clone());
                    false
                } else {
                    true
                }
            });

            for job in ready_jobs {
                shard_w
                    .queues
                    .entry(job.queue.clone())
                    .or_insert_with(BinaryHeap::new)
                    .push(job);
            }
        }

        if !completed.is_empty() {
            self.notify.notify_waiters();
        }
    }

    async fn cleanup_expired(&self) {
        let now = Self::now_ms();

        for shard in &self.shards {
            let mut shard_w = shard.write();

            for heap in shard_w.queues.values_mut() {
                // Note: BinaryHeap doesn't support retain, so we rebuild
                let jobs: Vec<_> = std::mem::take(heap).into_vec();
                for job in jobs {
                    if !job.is_expired(now) {
                        heap.push(job);
                    }
                }
            }
        }
    }

    fn cleanup_completed_jobs(&self) {
        let mut completed = self.completed_jobs.write();
        if completed.len() > 100_000 {
            // Keep only recent 50k
            let to_remove: Vec<_> = completed.iter().take(50_000).copied().collect();
            for id in to_remove {
                completed.remove(&id);
            }
        }
    }

    async fn run_cron_jobs(&self) {
        let now = Self::now_ms();
        let mut to_run = Vec::new();

        {
            let mut crons = self.cron_jobs.write();
            for cron in crons.values_mut() {
                if cron.next_run <= now {
                    to_run.push((cron.queue.clone(), cron.data.clone(), cron.priority));
                    cron.next_run = self.parse_next_cron_run(&cron.schedule, now);
                }
            }
        }

        for (queue, data, priority) in to_run {
            self.push(queue, data, priority, None, None, None, None, None, None).await;
        }
    }

    fn parse_next_cron_run(&self, schedule: &str, now: u64) -> u64 {
        // Simple cron parser: supports "*/N" for every N seconds/minutes
        // Format: "*/5" = every 5 seconds, "*/60" = every minute
        if let Some(interval_str) = schedule.strip_prefix("*/") {
            if let Ok(secs) = interval_str.parse::<u64>() {
                return now + secs * 1000;
            }
        }
        // Default: 1 minute
        now + 60_000
    }

    #[inline(always)]
    fn create_job(
        &self,
        queue: String,
        data: Value,
        priority: i32,
        delay: Option<u64>,
        ttl: Option<u64>,
        max_attempts: Option<u32>,
        backoff: Option<u64>,
        unique_key: Option<String>,
        depends_on: Option<Vec<u64>>,
    ) -> Job {
        let now = Self::now_ms();
        Job {
            id: next_id(),
            queue,
            data,
            priority,
            created_at: now,
            run_at: delay.map_or(now, |d| now + d),
            attempts: 0,
            max_attempts: max_attempts.unwrap_or(0),
            backoff: backoff.unwrap_or(0),
            ttl: ttl.unwrap_or(0),
            unique_key,
            depends_on: depends_on.unwrap_or_default(),
            progress: 0,
            progress_msg: None,
        }
    }

    pub async fn push(
        &self,
        queue: String,
        data: Value,
        priority: i32,
        delay: Option<u64>,
        ttl: Option<u64>,
        max_attempts: Option<u32>,
        backoff: Option<u64>,
        unique_key: Option<String>,
        depends_on: Option<Vec<u64>>,
    ) -> Result<Job, String> {
        let job = self.create_job(
            queue.clone(), data, priority, delay, ttl, max_attempts, backoff,
            unique_key.clone(), depends_on.clone()
        );

        let idx = Self::shard_index(&queue);

        {
            let mut shard = self.shards[idx].write();

            // Check unique key
            if let Some(ref key) = unique_key {
                let keys = shard.unique_keys.entry(queue.clone()).or_insert_with(HashSet::new);
                if keys.contains(key) {
                    return Err(format!("Duplicate job with key: {}", key));
                }
                keys.insert(key.clone());
            }

            self.write_wal(&WalEvent::Push(job.clone()));

            // Check dependencies
            if !job.depends_on.is_empty() {
                let completed = self.completed_jobs.read();
                let deps_satisfied = job.depends_on.iter().all(|dep| completed.contains(dep));

                if !deps_satisfied {
                    shard.waiting_deps.insert(job.id, job.clone());
                    return Ok(job);
                }
            }

            shard
                .queues
                .entry(queue)
                .or_insert_with(BinaryHeap::new)
                .push(job.clone());
        }

        self.metrics.total_pushed.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_waiters();
        Ok(job)
    }

    pub async fn push_batch(&self, queue: String, jobs: Vec<JobInput>) -> Vec<u64> {
        let mut ids = Vec::with_capacity(jobs.len());
        let mut created_jobs = Vec::with_capacity(jobs.len());
        let mut waiting_jobs = Vec::new();

        let idx = Self::shard_index(&queue);
        let completed = self.completed_jobs.read().clone();

        for input in jobs {
            let job = self.create_job(
                queue.clone(),
                input.data,
                input.priority,
                input.delay,
                input.ttl,
                input.max_attempts,
                input.backoff,
                input.unique_key,
                input.depends_on,
            );
            ids.push(job.id);

            if !job.depends_on.is_empty() {
                let deps_satisfied = job.depends_on.iter().all(|dep| completed.contains(dep));
                if !deps_satisfied {
                    waiting_jobs.push(job);
                    continue;
                }
            }

            created_jobs.push(job);
        }

        // Batch WAL
        if self.persistence {
            let mut wal = self.wal.lock();
            if let Some(ref mut file) = *wal {
                for job in &created_jobs {
                    if let Ok(json) = serde_json::to_string(&WalEvent::Push(job.clone())) {
                        let _ = writeln!(file, "{}", json);
                    }
                }
                let _ = file.flush();
            }
        }

        {
            let mut shard = self.shards[idx].write();
            let heap = shard.queues.entry(queue).or_insert_with(BinaryHeap::new);
            for job in created_jobs {
                heap.push(job);
            }
            for job in waiting_jobs {
                shard.waiting_deps.insert(job.id, job);
            }
        }

        self.metrics.total_pushed.fetch_add(ids.len() as u64, Ordering::Relaxed);
        self.notify.notify_waiters();
        ids
    }

    pub async fn pull(&self, queue_name: &str) -> Job {
        let idx = Self::shard_index(queue_name);

        // Check rate limit
        loop {
            let acquired = {
                let mut limiters = self.rate_limiters.write();
                if let Some(limiter) = limiters.get_mut(queue_name) {
                    limiter.try_acquire()
                } else {
                    true  // No rate limit
                }
            };
            if acquired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        loop {
            let now = Self::now_ms();
            {
                let mut shard = self.shards[idx].write();
                if let Some(heap) = shard.queues.get_mut(queue_name) {
                    // Skip expired jobs
                    while let Some(job) = heap.peek() {
                        if job.is_expired(now) {
                            heap.pop();
                            continue;
                        }
                        if job.is_ready(now) {
                            let job = heap.pop().unwrap();
                            shard.processing.insert(job.id, job.clone());
                            return job;
                        }
                        break;
                    }
                }
            }
            self.notify.notified().await;
        }
    }

    pub async fn pull_batch(&self, queue_name: &str, count: usize) -> Vec<Job> {
        let idx = Self::shard_index(queue_name);
        let mut result = Vec::with_capacity(count);

        loop {
            let now = Self::now_ms();
            {
                let mut shard = self.shards[idx].write();
                let mut jobs_to_process = Vec::new();

                if let Some(heap) = shard.queues.get_mut(queue_name) {
                    while jobs_to_process.len() < count {
                        if let Some(job) = heap.peek() {
                            if job.is_expired(now) {
                                heap.pop();
                                continue;
                            }
                            if job.is_ready(now) {
                                jobs_to_process.push(heap.pop().unwrap());
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }

                for job in jobs_to_process {
                    shard.processing.insert(job.id, job.clone());
                    result.push(job);
                }
            }

            if !result.is_empty() {
                return result;
            }

            self.notify.notified().await;
        }
    }

    pub async fn ack(&self, job_id: u64) -> Result<(), String> {
        let idx = Self::shard_index_by_id(job_id);
        {
            let mut shard = self.shards[idx].write();
            if let Some(job) = shard.processing.remove(&job_id) {
                // Remove unique key
                if let Some(ref key) = job.unique_key {
                    if let Some(keys) = shard.unique_keys.get_mut(&job.queue) {
                        keys.remove(key);
                    }
                }

                drop(shard);
                self.write_wal(&WalEvent::Ack(job_id));

                // Track completion for dependencies
                self.completed_jobs.write().insert(job_id);

                // Record metrics
                let latency = Self::now_ms() - job.created_at;
                self.metrics.total_completed.fetch_add(1, Ordering::Relaxed);
                self.metrics.latency_sum.fetch_add(latency, Ordering::Relaxed);
                self.metrics.latency_count.fetch_add(1, Ordering::Relaxed);

                return Ok(());
            }
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn ack_batch(&self, ids: &[u64]) -> usize {
        let mut acked = 0;

        for &id in ids {
            let idx = Self::shard_index_by_id(id);
            let mut shard = self.shards[idx].write();
            if let Some(job) = shard.processing.remove(&id) {
                if let Some(ref key) = job.unique_key {
                    if let Some(keys) = shard.unique_keys.get_mut(&job.queue) {
                        keys.remove(key);
                    }
                }
                self.completed_jobs.write().insert(id);
                acked += 1;
            }
        }

        if self.persistence && acked > 0 {
            let mut wal = self.wal.lock();
            if let Some(ref mut file) = *wal {
                for &id in ids {
                    if let Ok(json) = serde_json::to_string(&WalEvent::Ack(id)) {
                        let _ = writeln!(file, "{}", json);
                    }
                }
            }
        }

        self.metrics.total_completed.fetch_add(acked as u64, Ordering::Relaxed);
        acked
    }

    pub async fn fail(&self, job_id: u64, _error: Option<String>) -> Result<(), String> {
        let idx = Self::shard_index_by_id(job_id);
        {
            let mut shard = self.shards[idx].write();
            if let Some(mut job) = shard.processing.remove(&job_id) {
                job.attempts += 1;

                // Check if should go to DLQ
                if job.should_go_to_dlq() {
                    self.write_wal(&WalEvent::Dlq(job.clone()));
                    shard
                        .dlq
                        .entry(job.queue.clone())
                        .or_insert_with(VecDeque::new)
                        .push_back(job);
                    self.metrics.total_failed.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }

                // Apply backoff
                let backoff = job.next_backoff();
                if backoff > 0 {
                    job.run_at = Self::now_ms() + backoff;
                }

                self.write_wal(&WalEvent::Fail(job_id));
                shard
                    .queues
                    .entry(job.queue.clone())
                    .or_insert_with(BinaryHeap::new)
                    .push(job);

                drop(shard);
                self.notify.notify_waiters();
                return Ok(());
            }
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn cancel(&self, job_id: u64) -> Result<(), String> {
        // Try to find in processing
        let idx = Self::shard_index_by_id(job_id);
        {
            let mut shard = self.shards[idx].write();
            if let Some(job) = shard.processing.remove(&job_id) {
                if let Some(ref key) = job.unique_key {
                    if let Some(keys) = shard.unique_keys.get_mut(&job.queue) {
                        keys.remove(key);
                    }
                }
                self.write_wal(&WalEvent::Cancel(job_id));
                return Ok(());
            }

            // Try to find in waiting deps
            if shard.waiting_deps.remove(&job_id).is_some() {
                self.write_wal(&WalEvent::Cancel(job_id));
                return Ok(());
            }
        }

        // Try to find in queues (expensive - need to rebuild heap)
        for shard in &self.shards {
            let mut shard_w = shard.write();
            let mut found_job: Option<(String, Option<String>)> = None;

            for (queue_name, heap) in shard_w.queues.iter_mut() {
                let jobs: Vec<_> = std::mem::take(heap).into_vec();
                for job in jobs {
                    if job.id == job_id {
                        found_job = Some((queue_name.clone(), job.unique_key.clone()));
                    } else {
                        heap.push(job);
                    }
                }
                if found_job.is_some() {
                    break;
                }
            }

            if let Some((queue_name, unique_key)) = found_job {
                if let Some(key) = unique_key {
                    if let Some(keys) = shard_w.unique_keys.get_mut(&queue_name) {
                        keys.remove(&key);
                    }
                }
                self.write_wal(&WalEvent::Cancel(job_id));
                return Ok(());
            }
        }

        Err(format!("Job {} not found", job_id))
    }

    pub async fn update_progress(&self, job_id: u64, progress: u8, message: Option<String>) -> Result<(), String> {
        let idx = Self::shard_index_by_id(job_id);
        let mut shard = self.shards[idx].write();
        if let Some(job) = shard.processing.get_mut(&job_id) {
            job.progress = progress.min(100);
            job.progress_msg = message;
            return Ok(());
        }
        Err(format!("Job {} not found in processing", job_id))
    }

    pub async fn get_progress(&self, job_id: u64) -> Result<(u8, Option<String>), String> {
        let idx = Self::shard_index_by_id(job_id);
        let shard = self.shards[idx].read();
        if let Some(job) = shard.processing.get(&job_id) {
            return Ok((job.progress, job.progress_msg.clone()));
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn get_dlq(&self, queue_name: &str, count: Option<usize>) -> Vec<Job> {
        let idx = Self::shard_index(queue_name);
        let shard = self.shards[idx].read();

        if let Some(dlq) = shard.dlq.get(queue_name) {
            let limit = count.unwrap_or(100).min(dlq.len());
            dlq.iter().take(limit).cloned().collect()
        } else {
            Vec::new()
        }
    }

    pub async fn retry_dlq(&self, queue_name: &str, job_id: Option<u64>) -> usize {
        let idx = Self::shard_index(queue_name);
        let mut shard = self.shards[idx].write();
        let mut retried = 0;
        let now = Self::now_ms();

        // First collect jobs to retry
        let mut jobs_to_retry = Vec::new();

        if let Some(dlq) = shard.dlq.get_mut(queue_name) {
            if let Some(id) = job_id {
                // Retry specific job
                if let Some(pos) = dlq.iter().position(|j| j.id == id) {
                    let mut job = dlq.remove(pos).unwrap();
                    job.attempts = 0;
                    job.run_at = now;
                    jobs_to_retry.push(job);
                }
            } else {
                // Retry all
                while let Some(mut job) = dlq.pop_front() {
                    job.attempts = 0;
                    job.run_at = now;
                    jobs_to_retry.push(job);
                }
            }
        }

        // Then add to queue
        if !jobs_to_retry.is_empty() {
            let heap = shard.queues.entry(queue_name.to_string()).or_insert_with(BinaryHeap::new);
            for job in jobs_to_retry {
                heap.push(job);
                retried += 1;
            }
        }

        if retried > 0 {
            self.notify.notify_waiters();
        }
        retried
    }

    pub async fn set_rate_limit(&self, queue: String, limit: u32) {
        let mut limiters = self.rate_limiters.write();
        limiters.insert(queue, RateLimiter::new(limit));
    }

    pub async fn clear_rate_limit(&self, queue: &str) {
        let mut limiters = self.rate_limiters.write();
        limiters.remove(queue);
    }

    pub async fn add_cron(&self, name: String, queue: String, data: Value, schedule: String, priority: i32) {
        let now = Self::now_ms();
        let next_run = self.parse_next_cron_run(&schedule, now);

        let cron = CronJob {
            name: name.clone(),
            queue,
            data,
            schedule,
            priority,
            next_run,
        };

        self.cron_jobs.write().insert(name, cron);
    }

    pub async fn delete_cron(&self, name: &str) -> bool {
        self.cron_jobs.write().remove(name).is_some()
    }

    pub async fn list_crons(&self) -> Vec<CronJob> {
        self.cron_jobs.read().values().cloned().collect()
    }

    pub async fn get_metrics(&self) -> MetricsData {
        let total_completed = self.metrics.total_completed.load(Ordering::Relaxed);
        let latency_sum = self.metrics.latency_sum.load(Ordering::Relaxed);
        let latency_count = self.metrics.latency_count.load(Ordering::Relaxed);

        let avg_latency = if latency_count > 0 {
            latency_sum as f64 / latency_count as f64
        } else {
            0.0
        };

        let mut queues = Vec::new();
        let rate_limiters = self.rate_limiters.read();

        for shard in &self.shards {
            let s = shard.read();
            for (name, heap) in &s.queues {
                let dlq_count = s.dlq.get(name).map_or(0, |d| d.len());
                let processing_count = s.processing.values().filter(|j| &j.queue == name).count();

                queues.push(QueueMetrics {
                    name: name.clone(),
                    pending: heap.len(),
                    processing: processing_count,
                    dlq: dlq_count,
                    rate_limit: rate_limiters.get(name).map(|r| r.limit),
                });
            }
        }

        MetricsData {
            total_pushed: self.metrics.total_pushed.load(Ordering::Relaxed),
            total_completed,
            total_failed: self.metrics.total_failed.load(Ordering::Relaxed),
            jobs_per_second: 0.0, // Would need a sliding window for accurate measurement
            avg_latency_ms: avg_latency,
            queues,
        }
    }

    pub async fn stats(&self) -> (usize, usize, usize, usize) {
        let now = Self::now_ms();
        let mut total_ready = 0;
        let mut total_processing = 0;
        let mut total_delayed = 0;
        let mut total_dlq = 0;

        for shard in &self.shards {
            let s = shard.read();
            total_processing += s.processing.len();
            for dlq in s.dlq.values() {
                total_dlq += dlq.len();
            }
            for heap in s.queues.values() {
                for job in heap.iter() {
                    if job.is_ready(now) {
                        total_ready += 1;
                    } else {
                        total_delayed += 1;
                    }
                }
            }
        }

        (total_ready, total_processing, total_delayed, total_dlq)
    }
}

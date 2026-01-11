use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::Value;

use crate::protocol::{CronJob, Job, JobState};
use super::types::{init_coarse_time, intern, now_ms, GlobalMetrics, Shard, Subscriber, WalEvent};

const WAL_PATH: &str = "magic-queue.wal";
const WAL_MAX_SIZE: u64 = 100 * 1024 * 1024; // 100MB max before compaction
pub const NUM_SHARDS: usize = 32;

pub struct QueueManager {
    pub(crate) shards: Vec<RwLock<Shard>>,
    pub(crate) processing: RwLock<FxHashMap<u64, Job>>,
    pub(crate) wal: Mutex<Option<BufWriter<File>>>,
    pub(crate) wal_size: AtomicU64,
    pub(crate) persistence: bool,
    pub(crate) cron_jobs: RwLock<FxHashMap<String, CronJob>>,
    pub(crate) completed_jobs: RwLock<FxHashSet<u64>>,
    pub(crate) job_results: RwLock<FxHashMap<u64, Value>>,
    pub(crate) subscribers: RwLock<Vec<Subscriber>>,
    pub(crate) auth_tokens: RwLock<FxHashSet<String>>,
    pub(crate) metrics: GlobalMetrics,
}

impl QueueManager {
    pub fn new(persistence: bool) -> Arc<Self> {
        // Initialize coarse timestamp
        init_coarse_time();

        let shards = (0..NUM_SHARDS).map(|_| RwLock::new(Shard::new())).collect();
        let (wal, wal_size) = if persistence {
            let (file, size) = Self::open_wal();
            (file.map(|f| BufWriter::new(f)), size)
        } else {
            (None, 0)
        };

        let manager = Arc::new(Self {
            shards,
            processing: RwLock::new(FxHashMap::with_capacity_and_hasher(4096, Default::default())),
            wal: Mutex::new(wal),
            wal_size: AtomicU64::new(wal_size),
            persistence,
            cron_jobs: RwLock::new(FxHashMap::default()),
            completed_jobs: RwLock::new(FxHashSet::default()),
            job_results: RwLock::new(FxHashMap::default()),
            subscribers: RwLock::new(Vec::new()),
            auth_tokens: RwLock::new(FxHashSet::default()),
            metrics: GlobalMetrics::new(),
        });

        if persistence {
            manager.replay_wal_sync();
        }

        let mgr = Arc::clone(&manager);
        tokio::spawn(async move { mgr.background_tasks().await; });

        manager
    }

    pub fn with_auth_tokens(persistence: bool, tokens: Vec<String>) -> Arc<Self> {
        let manager = Self::new(persistence);
        {
            let mut auth = manager.auth_tokens.write();
            for token in tokens {
                auth.insert(token);
            }
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

    fn open_wal() -> (Option<File>, u64) {
        match OpenOptions::new().create(true).append(true).open(WAL_PATH) {
            Ok(file) => {
                let size = file.metadata().map(|m| m.len()).unwrap_or(0);
                (Some(file), size)
            }
            Err(_) => (None, 0)
        }
    }

    pub(crate) fn replay_wal_sync(&self) {
        if !Path::new(WAL_PATH).exists() { return; }
        let file = match File::open(WAL_PATH) { Ok(f) => f, Err(_) => return };
        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines().flatten() {
            let event: WalEvent = match serde_json::from_str(&line) {
                Ok(e) => e, Err(_) => continue
            };

            match event {
                WalEvent::Push(job) => {
                    let idx = Self::shard_index(&job.queue);
                    let queue_name = intern(&job.queue);
                    let mut shard = self.shards[idx].write();
                    shard.queues.entry(queue_name)
                        .or_insert_with(BinaryHeap::new).push(job);
                    count += 1;
                }
                WalEvent::Ack(id) => {
                    self.processing.write().remove(&id);
                }
                WalEvent::AckWithResult { id, result } => {
                    self.processing.write().remove(&id);
                    self.job_results.write().insert(id, result);
                }
                WalEvent::Fail(id) => {
                    if let Some(job) = self.processing.write().remove(&id) {
                        let idx = Self::shard_index(&job.queue);
                        let queue_name = intern(&job.queue);
                        self.shards[idx].write().queues
                            .entry(queue_name)
                            .or_insert_with(BinaryHeap::new).push(job);
                    }
                }
                WalEvent::Cancel(id) => {
                    self.processing.write().remove(&id);
                }
                WalEvent::Dlq(job) => {
                    let idx = Self::shard_index(&job.queue);
                    let queue_name = intern(&job.queue);
                    self.shards[idx].write().dlq
                        .entry(queue_name).or_default().push_back(job);
                }
            }
        }

        if count > 0 { println!("Replayed {} jobs from WAL", count); }
    }

    #[inline(always)]
    pub(crate) fn write_wal(&self, event: &WalEvent) {
        if !self.persistence { return; }
        let mut wal = self.wal.lock();
        if let Some(ref mut writer) = *wal {
            if let Ok(json) = serde_json::to_string(event) {
                let bytes = json.len() as u64 + 1; // +1 for newline
                if writeln!(writer, "{}", json).is_ok() {
                    let _ = writer.flush();
                    self.wal_size.fetch_add(bytes, Ordering::Relaxed);
                }
            }
        }
    }

    pub(crate) fn write_wal_batch(&self, jobs: &[Job]) {
        if !self.persistence || jobs.is_empty() { return; }
        let mut wal = self.wal.lock();
        if let Some(ref mut writer) = *wal {
            let mut bytes_written = 0u64;
            for job in jobs {
                if let Ok(json) = serde_json::to_string(&WalEvent::Push(job.clone())) {
                    bytes_written += json.len() as u64 + 1;
                    let _ = writeln!(writer, "{}", json);
                }
            }
            let _ = writer.flush();
            self.wal_size.fetch_add(bytes_written, Ordering::Relaxed);
        }
    }

    pub(crate) fn write_wal_acks(&self, ids: &[u64]) {
        if !self.persistence { return; }
        let mut wal = self.wal.lock();
        if let Some(ref mut writer) = *wal {
            let mut bytes_written = 0u64;
            for &id in ids {
                if let Ok(json) = serde_json::to_string(&WalEvent::Ack(id)) {
                    bytes_written += json.len() as u64 + 1;
                    let _ = writeln!(writer, "{}", json);
                }
            }
            let _ = writer.flush();
            self.wal_size.fetch_add(bytes_written, Ordering::Relaxed);
        }
    }

    /// Compact WAL by writing only current state
    pub(crate) fn compact_wal(&self) {
        if !self.persistence { return; }

        let current_size = self.wal_size.load(Ordering::Relaxed);
        if current_size < WAL_MAX_SIZE { return; }

        let temp_path = format!("{}.tmp", WAL_PATH);

        // Collect all current jobs from queues
        let mut all_jobs = Vec::new();
        for shard in &self.shards {
            let s = shard.read();
            for heap in s.queues.values() {
                all_jobs.extend(heap.iter().cloned());
            }
            for dlq in s.dlq.values() {
                all_jobs.extend(dlq.iter().cloned());
            }
        }

        // Write compacted WAL
        if let Ok(file) = File::create(&temp_path) {
            let mut writer = BufWriter::new(file);
            let mut new_size = 0u64;

            for job in &all_jobs {
                if let Ok(json) = serde_json::to_string(&WalEvent::Push(job.clone())) {
                    new_size += json.len() as u64 + 1;
                    let _ = writeln!(writer, "{}", json);
                }
            }
            let _ = writer.flush();
            drop(writer);

            // Swap files
            let mut wal = self.wal.lock();
            *wal = None;

            if std::fs::rename(&temp_path, WAL_PATH).is_ok() {
                if let Ok(file) = OpenOptions::new().append(true).open(WAL_PATH) {
                    *wal = Some(BufWriter::new(file));
                    self.wal_size.store(new_size, Ordering::Relaxed);
                    println!("WAL compacted: {} -> {} bytes", current_size, new_size);
                }
            }
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

    /// Get job by ID with its current state
    /// Searches all locations: processing, queues, dlq, waiting_deps, completed
    pub fn get_job(&self, id: u64) -> (Option<Job>, JobState) {
        let now = now_ms();

        // Check processing (Active)
        {
            let processing = self.processing.read();
            if let Some(job) = processing.get(&id) {
                return (Some(job.clone()), JobState::Active);
            }
        }

        // Check completed jobs
        {
            let completed = self.completed_jobs.read();
            if completed.contains(&id) {
                return (None, JobState::Completed);
            }
        }

        // Search through all shards
        for shard in &self.shards {
            let s = shard.read();

            // Check queues (Waiting/Delayed)
            for heap in s.queues.values() {
                for job in heap.iter() {
                    if job.id == id {
                        let state = if job.run_at > now {
                            JobState::Delayed
                        } else {
                            JobState::Waiting
                        };
                        return (Some(job.clone()), state);
                    }
                }
            }

            // Check DLQ (Failed)
            for dlq in s.dlq.values() {
                for job in dlq.iter() {
                    if job.id == id {
                        return (Some(job.clone()), JobState::Failed);
                    }
                }
            }

            // Check waiting_deps (WaitingChildren)
            if let Some(job) = s.waiting_deps.get(&id) {
                return (Some(job.clone()), JobState::WaitingChildren);
            }
        }

        // Not found
        (None, JobState::Unknown)
    }

    /// Get only the state of a job by ID (lighter than get_job)
    pub fn get_state(&self, id: u64) -> JobState {
        let now = now_ms();

        // Check processing (Active)
        if self.processing.read().contains_key(&id) {
            return JobState::Active;
        }

        // Check completed jobs
        if self.completed_jobs.read().contains(&id) {
            return JobState::Completed;
        }

        // Search through all shards
        for shard in &self.shards {
            let s = shard.read();

            // Check queues (Waiting/Delayed)
            for heap in s.queues.values() {
                for job in heap.iter() {
                    if job.id == id {
                        return if job.run_at > now {
                            JobState::Delayed
                        } else {
                            JobState::Waiting
                        };
                    }
                }
            }

            // Check DLQ (Failed)
            for dlq in s.dlq.values() {
                for job in dlq.iter() {
                    if job.id == id {
                        return JobState::Failed;
                    }
                }
            }

            // Check waiting_deps (WaitingChildren)
            if s.waiting_deps.contains_key(&id) {
                return JobState::WaitingChildren;
            }
        }

        JobState::Unknown
    }
}

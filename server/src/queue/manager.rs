use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use tokio::sync::Notify;

use crate::protocol::{CronJob, Job};
use super::types::{ConcurrencyLimiter, GlobalMetrics, RateLimiter, Shard, Subscriber, WalEvent};

const WAL_PATH: &str = "magic-queue.wal";
pub const NUM_SHARDS: usize = 32;

pub struct QueueManager {
    pub(crate) shards: Vec<RwLock<Shard>>,
    pub(crate) processing: RwLock<HashMap<u64, Job>>,
    pub(crate) notify: Notify,
    pub(crate) wal: Mutex<Option<File>>,
    pub(crate) persistence: bool,
    pub(crate) rate_limiters: RwLock<HashMap<String, RateLimiter>>,
    pub(crate) concurrency_limiters: RwLock<HashMap<String, ConcurrencyLimiter>>,
    pub(crate) cron_jobs: RwLock<HashMap<String, CronJob>>,
    pub(crate) completed_jobs: RwLock<HashSet<u64>>,
    pub(crate) job_results: RwLock<HashMap<u64, Value>>,
    pub(crate) paused_queues: RwLock<HashSet<String>>,
    pub(crate) subscribers: RwLock<Vec<Subscriber>>,
    pub(crate) auth_tokens: RwLock<HashSet<String>>,
    pub(crate) metrics: GlobalMetrics,
}

impl QueueManager {
    pub fn new(persistence: bool) -> Arc<Self> {
        let shards = (0..NUM_SHARDS).map(|_| RwLock::new(Shard::new())).collect();
        let wal = if persistence { Self::open_wal() } else { None };

        let manager = Arc::new(Self {
            shards,
            processing: RwLock::new(HashMap::with_capacity(4096)),
            notify: Notify::new(),
            wal: Mutex::new(wal),
            persistence,
            rate_limiters: RwLock::new(HashMap::new()),
            concurrency_limiters: RwLock::new(HashMap::new()),
            cron_jobs: RwLock::new(HashMap::new()),
            completed_jobs: RwLock::new(HashSet::new()),
            job_results: RwLock::new(HashMap::new()),
            paused_queues: RwLock::new(HashSet::new()),
            subscribers: RwLock::new(Vec::new()),
            auth_tokens: RwLock::new(HashSet::new()),
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

    pub fn verify_token(&self, token: &str) -> bool {
        let tokens = self.auth_tokens.read();
        tokens.is_empty() || tokens.contains(token)
    }

    #[inline(always)]
    pub fn shard_index(queue: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        queue.hash(&mut hasher);
        hasher.finish() as usize % NUM_SHARDS
    }

    #[inline(always)]
    pub fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    fn open_wal() -> Option<File> {
        OpenOptions::new().create(true).append(true).open(WAL_PATH).ok()
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
                    let mut shard = self.shards[idx].write();
                    shard.queues.entry(job.queue.clone())
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
                        self.shards[idx].write().queues
                            .entry(job.queue.clone())
                            .or_insert_with(BinaryHeap::new).push(job);
                    }
                }
                WalEvent::Cancel(id) => {
                    self.processing.write().remove(&id);
                }
                WalEvent::Dlq(job) => {
                    let idx = Self::shard_index(&job.queue);
                    self.shards[idx].write().dlq
                        .entry(job.queue.clone()).or_default().push_back(job);
                }
            }
        }

        if count > 0 { println!("Replayed {} jobs from WAL", count); }
    }

    #[inline(always)]
    pub(crate) fn write_wal(&self, event: &WalEvent) {
        if !self.persistence { return; }
        let mut wal = self.wal.lock();
        if let Some(ref mut file) = *wal {
            if let Ok(json) = serde_json::to_string(event) {
                let _ = writeln!(file, "{}", json);
            }
        }
    }

    pub(crate) fn write_wal_batch(&self, jobs: &[Job]) {
        if !self.persistence || jobs.is_empty() { return; }
        let mut wal = self.wal.lock();
        if let Some(ref mut file) = *wal {
            for job in jobs {
                if let Ok(json) = serde_json::to_string(&WalEvent::Push(job.clone())) {
                    let _ = writeln!(file, "{}", json);
                }
            }
            let _ = file.flush();
        }
    }

    pub(crate) fn write_wal_acks(&self, ids: &[u64]) {
        if !self.persistence { return; }
        let mut wal = self.wal.lock();
        if let Some(ref mut file) = *wal {
            for &id in ids {
                if let Ok(json) = serde_json::to_string(&WalEvent::Ack(id)) {
                    let _ = writeln!(file, "{}", json);
                }
            }
        }
    }

    pub(crate) fn notify_subscribers(&self, event: &str, queue: &str, job: &Job) {
        let subs = self.subscribers.read();
        for sub in subs.iter() {
            if sub.queue == queue && sub.events.contains(&event.to_string()) {
                let msg = serde_json::json!({
                    "event": event,
                    "queue": queue,
                    "job": job
                }).to_string();
                let _ = sub.tx.send(msg);
            }
        }
    }
}

use std::collections::{BinaryHeap, HashMap};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use tokio::sync::Notify;
use tokio::time::{interval, Duration};

use crate::protocol::{next_id, Job, JobInput};

const WAL_PATH: &str = "magic-queue.wal";
const NUM_SHARDS: usize = 32; // More shards = less contention

#[derive(serde::Serialize, serde::Deserialize)]
enum WalEvent {
    Push(Job),
    Ack(u64),
    Fail(u64),
}

struct Shard {
    queues: HashMap<String, BinaryHeap<Job>>,
    processing: HashMap<u64, Job>,
}

impl Shard {
    #[inline(always)]
    fn new() -> Self {
        Self {
            queues: HashMap::with_capacity(16),
            processing: HashMap::with_capacity(1024),
        }
    }
}

pub struct QueueManager {
    shards: Vec<RwLock<Shard>>,
    notify: Notify,
    wal: Mutex<Option<File>>,
    persistence: bool,
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
        });

        if persistence {
            manager.replay_wal_sync();
        }

        let mgr = Arc::clone(&manager);
        tokio::spawn(async move {
            mgr.delayed_promoter().await;
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

    async fn delayed_promoter(self: Arc<Self>) {
        let mut ticker = interval(Duration::from_millis(50));
        loop {
            ticker.tick().await;
            self.notify.notify_waiters();
        }
    }

    #[inline(always)]
    fn create_job(&self, queue: String, data: Value, priority: i32, delay: Option<u64>) -> Job {
        let now = Self::now_ms();
        Job {
            id: next_id(),
            queue,
            data,
            priority,
            created_at: now,
            run_at: delay.map_or(now, |d| now + d),
        }
    }

    pub async fn push(&self, queue: String, data: Value, priority: i32, delay: Option<u64>) -> Job {
        let job = self.create_job(queue.clone(), data, priority, delay);
        self.write_wal(&WalEvent::Push(job.clone()));

        let idx = Self::shard_index(&queue);
        {
            let mut shard = self.shards[idx].write();
            shard
                .queues
                .entry(queue)
                .or_insert_with(BinaryHeap::new)
                .push(job.clone());
        }

        self.notify.notify_waiters();
        job
    }

    pub async fn push_batch(&self, queue: String, jobs: Vec<JobInput>) -> Vec<u64> {
        let mut ids = Vec::with_capacity(jobs.len());
        let mut created_jobs = Vec::with_capacity(jobs.len());

        for input in jobs {
            let job = self.create_job(queue.clone(), input.data, input.priority, input.delay);
            ids.push(job.id);
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

        let idx = Self::shard_index(&queue);
        {
            let mut shard = self.shards[idx].write();
            let heap = shard.queues.entry(queue).or_insert_with(BinaryHeap::new);
            for job in created_jobs {
                heap.push(job);
            }
        }

        self.notify.notify_waiters();
        ids
    }

    pub async fn pull(&self, queue_name: &str) -> Job {
        let idx = Self::shard_index(queue_name);
        loop {
            let now = Self::now_ms();
            {
                let mut shard = self.shards[idx].write();
                if let Some(heap) = shard.queues.get_mut(queue_name) {
                    if let Some(job) = heap.peek() {
                        if job.is_ready(now) {
                            let job = heap.pop().unwrap();
                            shard.processing.insert(job.id, job.clone());
                            return job;
                        }
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
            if shard.processing.remove(&job_id).is_some() {
                drop(shard);
                self.write_wal(&WalEvent::Ack(job_id));
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
            if shard.processing.remove(&id).is_some() {
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

        acked
    }

    pub async fn fail(&self, job_id: u64, _error: Option<String>) -> Result<(), String> {
        let idx = Self::shard_index_by_id(job_id);
        {
            let mut shard = self.shards[idx].write();
            if let Some(job) = shard.processing.remove(&job_id) {
                shard
                    .queues
                    .entry(job.queue.clone())
                    .or_insert_with(BinaryHeap::new)
                    .push(job);
                drop(shard);
                self.write_wal(&WalEvent::Fail(job_id));
                self.notify.notify_waiters();
                return Ok(());
            }
        }
        Err(format!("Job {} not found", job_id))
    }

    pub async fn stats(&self) -> (usize, usize, usize) {
        let now = Self::now_ms();
        let mut total_ready = 0;
        let mut total_processing = 0;
        let mut total_delayed = 0;

        for shard in &self.shards {
            let s = shard.read();
            total_processing += s.processing.len();
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

        (total_ready, total_processing, total_delayed)
    }
}

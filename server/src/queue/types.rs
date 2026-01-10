use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use crate::protocol::Job;

/// WAL event types for persistence
#[derive(serde::Serialize, serde::Deserialize)]
pub enum WalEvent {
    Push(Job),
    Ack(u64),
    AckWithResult { id: u64, result: Value },
    Fail(u64),
    Cancel(u64),
    Dlq(Job),
}

/// Token bucket rate limiter
pub struct RateLimiter {
    pub limit: u32,
    tokens: f64,
    last_update: u64,
}

impl RateLimiter {
    pub fn new(limit: u32) -> Self {
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

    pub fn try_acquire(&mut self) -> bool {
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

/// Concurrency limiter - tracks active jobs per queue
pub struct ConcurrencyLimiter {
    pub limit: u32,
    pub current: u32,
}

impl ConcurrencyLimiter {
    pub fn new(limit: u32) -> Self {
        Self { limit, current: 0 }
    }

    pub fn try_acquire(&mut self) -> bool {
        if self.current < self.limit {
            self.current += 1;
            true
        } else {
            false
        }
    }

    pub fn release(&mut self) {
        if self.current > 0 {
            self.current -= 1;
        }
    }
}

/// Shard containing queues (no processing - that's global)
pub struct Shard {
    pub queues: HashMap<String, BinaryHeap<Job>>,
    pub dlq: HashMap<String, VecDeque<Job>>,
    pub unique_keys: HashMap<String, HashSet<String>>,
    pub waiting_deps: HashMap<u64, Job>,
}

impl Shard {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            queues: HashMap::with_capacity(16),
            dlq: HashMap::with_capacity(16),
            unique_keys: HashMap::with_capacity(16),
            waiting_deps: HashMap::with_capacity(256),
        }
    }
}

/// Global metrics counters
pub struct GlobalMetrics {
    pub total_pushed: AtomicU64,
    pub total_completed: AtomicU64,
    pub total_failed: AtomicU64,
    pub total_timed_out: AtomicU64,
    pub latency_sum: AtomicU64,
    pub latency_count: AtomicU64,
}

impl GlobalMetrics {
    pub fn new() -> Self {
        Self {
            total_pushed: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            total_timed_out: AtomicU64::new(0),
            latency_sum: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn record_push(&self, count: u64) {
        self.total_pushed.fetch_add(count, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_complete(&self, latency: u64) {
        self.total_completed.fetch_add(1, Ordering::Relaxed);
        self.latency_sum.fetch_add(latency, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_fail(&self) {
        self.total_failed.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_timeout(&self) {
        self.total_timed_out.fetch_add(1, Ordering::Relaxed);
    }
}

/// Subscriber for pub/sub events
#[derive(Clone)]
pub struct Subscriber {
    pub queue: String,
    pub events: Vec<String>,
    pub tx: tokio::sync::mpsc::UnboundedSender<String>,
}

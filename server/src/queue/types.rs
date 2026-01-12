use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use gxhash::GxHasher;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::protocol::{Job, JobState};

// ============== GxHash Type Aliases ==============
// GxHash is 20-30% faster than FxHash (uses AES-NI instructions)

pub type GxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<GxHasher>>;
pub type GxHashSet<T> = HashSet<T, BuildHasherDefault<GxHasher>>;

// ============== Coarse Timestamp ==============
// Cached timestamp updated every 1ms - avoids syscall per operation

static COARSE_TIME_MS: AtomicU64 = AtomicU64::new(0);

/// Initialize coarse timestamp background updater
pub fn init_coarse_time() {
    // Set initial value
    COARSE_TIME_MS.store(actual_now_ms(), Ordering::Relaxed);

    // Spawn background updater
    tokio::spawn(async {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1));
        loop {
            interval.tick().await;
            COARSE_TIME_MS.store(actual_now_ms(), Ordering::Relaxed);
        }
    });
}

#[inline(always)]
fn actual_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Get current timestamp (coarse, ±1ms precision, zero syscall)
#[inline(always)]
pub fn now_ms() -> u64 {
    let cached = COARSE_TIME_MS.load(Ordering::Relaxed);
    if cached == 0 {
        // Fallback if not initialized
        actual_now_ms()
    } else {
        cached
    }
}

// ============== String Interning ==============
// Avoid repeated allocations for queue names

static INTERNED_STRINGS: Lazy<RwLock<GxHashSet<Arc<str>>>> =
    Lazy::new(|| RwLock::new(GxHashSet::default()));

/// Maximum number of interned strings to prevent DOS attacks
const MAX_INTERNED_STRINGS: usize = 10_000;

/// Intern a string - returns Arc<str> that can be cheaply cloned
#[inline]
pub fn intern(s: &str) -> Arc<str> {
    // Fast path: check if already interned
    {
        let set = INTERNED_STRINGS.read();
        if let Some(arc) = set.get(s) {
            return Arc::clone(arc);
        }
    }

    // Slow path: insert new string
    let mut set = INTERNED_STRINGS.write();
    // Double-check after acquiring write lock
    if let Some(arc) = set.get(s) {
        return Arc::clone(arc);
    }

    // Prevent unbounded growth - if at limit, don't intern (return non-interned Arc)
    if set.len() >= MAX_INTERNED_STRINGS {
        return s.into();
    }

    let arc: Arc<str> = s.into();
    set.insert(Arc::clone(&arc));
    arc
}

/// Cleanup unused interned strings (strong_count == 1 means only in the set)
pub fn cleanup_interned_strings() {
    let mut set = INTERNED_STRINGS.write();
    let before = set.len();
    set.retain(|arc| Arc::strong_count(arc) > 1);
    let removed = before - set.len();
    if removed > 0 {
        println!("Cleaned up {} unused interned strings", removed);
    }
}

// ============== Job Location Index ==============
// O(1) lookup for job state - avoids scanning all shards

#[derive(Debug, Clone, Copy)]
pub enum JobLocation {
    /// Job is in a queue (waiting or delayed)
    Queue { shard_idx: usize },
    /// Job is being processed
    Processing,
    /// Job is in DLQ
    Dlq { shard_idx: usize },
    /// Job is waiting for dependencies
    WaitingDeps { shard_idx: usize },
    /// Job completed (may have result stored)
    Completed,
}

impl JobLocation {
    /// Convert location to JobState, checking delayed status if needed
    #[inline]
    pub fn to_state(self, run_at: u64, now: u64) -> JobState {
        match self {
            JobLocation::Queue { .. } => {
                if run_at > now {
                    JobState::Delayed
                } else {
                    JobState::Waiting
                }
            }
            JobLocation::Processing => JobState::Active,
            JobLocation::Dlq { .. } => JobState::Failed,
            JobLocation::WaitingDeps { .. } => JobState::WaitingChildren,
            JobLocation::Completed => JobState::Completed,
        }
    }
}

// ============== Rate Limiter ==============
// Token bucket algorithm

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
            last_update: now_ms(),
        }
    }

    #[inline]
    pub fn try_acquire(&mut self) -> bool {
        let now = now_ms();
        let elapsed = (now.saturating_sub(self.last_update)) as f64 / 1000.0;
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

// ============== Concurrency Limiter ==============

pub struct ConcurrencyLimiter {
    pub limit: u32,
    pub current: u32,
}

impl ConcurrencyLimiter {
    pub fn new(limit: u32) -> Self {
        Self { limit, current: 0 }
    }

    #[inline]
    pub fn try_acquire(&mut self) -> bool {
        if self.current < self.limit {
            self.current += 1;
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn release(&mut self) {
        self.current = self.current.saturating_sub(1);
    }
}

// ============== Queue State ==============
// Combined state to reduce lock acquisitions

pub struct QueueState {
    pub paused: bool,
    pub rate_limiter: Option<RateLimiter>,
    pub concurrency: Option<ConcurrencyLimiter>,
}

impl QueueState {
    pub fn new() -> Self {
        Self {
            paused: false,
            rate_limiter: None,
            concurrency: None,
        }
    }
}

impl Default for QueueState {
    fn default() -> Self {
        Self::new()
    }
}

// ============== Shard ==============
// Each shard contains queues and their state

pub struct Shard {
    pub queues: GxHashMap<Arc<str>, BinaryHeap<Job>>,
    pub dlq: GxHashMap<Arc<str>, VecDeque<Job>>,
    pub unique_keys: GxHashMap<Arc<str>, GxHashSet<String>>,
    pub waiting_deps: GxHashMap<u64, Job>,
    pub queue_state: GxHashMap<Arc<str>, QueueState>,
    pub notify: Arc<Notify>, // Per-shard notify for targeted wakeups
}

impl Shard {
    #[inline]
    pub fn new() -> Self {
        Self {
            queues: GxHashMap::with_capacity_and_hasher(16, Default::default()),
            dlq: GxHashMap::with_capacity_and_hasher(16, Default::default()),
            unique_keys: GxHashMap::with_capacity_and_hasher(16, Default::default()),
            waiting_deps: GxHashMap::with_capacity_and_hasher(256, Default::default()),
            queue_state: GxHashMap::with_capacity_and_hasher(16, Default::default()),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Get or create queue state
    #[inline]
    pub fn get_state(&mut self, queue: &Arc<str>) -> &mut QueueState {
        self.queue_state.entry(Arc::clone(queue)).or_default()
    }
}

impl Default for Shard {
    fn default() -> Self {
        Self::new()
    }
}

// ============== Global Metrics ==============

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

impl Default for GlobalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ============== Subscriber ==============

#[derive(Clone)]
pub struct Subscriber {
    pub queue: Arc<str>,
    pub events: Vec<String>,
    pub tx: tokio::sync::mpsc::UnboundedSender<String>,
}

// ============== Worker ==============

pub struct Worker {
    pub id: String,
    pub queues: Vec<String>,
    pub concurrency: u32,
    pub last_heartbeat: u64,
    pub jobs_processed: u64,
}

impl Worker {
    pub fn new(id: String, queues: Vec<String>, concurrency: u32) -> Self {
        Self {
            id,
            queues,
            concurrency,
            last_heartbeat: now_ms(),
            jobs_processed: 0,
        }
    }
}

// ============== Webhook ==============

pub struct Webhook {
    pub id: String,
    pub url: String,
    pub events: Vec<String>,
    pub queue: Option<String>,
    pub secret: Option<String>,
    pub created_at: u64,
}

impl Webhook {
    pub fn new(
        id: String,
        url: String,
        events: Vec<String>,
        queue: Option<String>,
        secret: Option<String>,
    ) -> Self {
        Self {
            id,
            url,
            events,
            queue,
            secret,
            created_at: now_ms(),
        }
    }
}

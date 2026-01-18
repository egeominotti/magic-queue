use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use compact_str::CompactString;
use gxhash::GxHasher;
use tokio::sync::Notify;

use crate::protocol::{Job, JobState};

// ============== Indexed Priority Queue ==============
// O(log n) for push, pop, find, update, remove by job_id
// Combines HashMap (O(1) lookup) with BinaryHeap (O(log n) priority ops)

/// Wrapper that tracks position for heap operations
#[derive(Debug, Clone)]
struct HeapEntry {
    job: Job,
    /// Generation counter to handle stale entries after updates
    generation: u64,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.job.id == other.job.id
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Delegate to Job's Ord implementation
        self.job.cmp(&other.job)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Indexed Priority Queue with O(log n) operations for find/update/remove
pub struct IndexedPriorityQueue {
    /// The actual heap for priority ordering
    heap: BinaryHeap<HeapEntry>,
    /// Map from job_id to (job, generation) for O(1) lookup
    index: GxHashMap<u64, (Job, u64)>,
    /// Current generation counter (incremented on updates)
    generation: u64,
}

impl IndexedPriorityQueue {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            index: GxHashMap::with_capacity_and_hasher(256, Default::default()),
            generation: 0,
        }
    }

    #[allow(dead_code)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity),
            index: GxHashMap::with_capacity_and_hasher(capacity, Default::default()),
            generation: 0,
        }
    }

    /// Push a job - O(log n)
    #[inline]
    pub fn push(&mut self, job: Job) {
        let id = job.id;
        let gen = self.generation;
        self.generation += 1;

        self.index.insert(id, (job.clone(), gen));
        self.heap.push(HeapEntry {
            job,
            generation: gen,
        });
    }

    /// Pop highest priority job - O(log n) amortized
    /// Skips stale entries (jobs that were updated/removed)
    #[inline]
    pub fn pop(&mut self) -> Option<Job> {
        while let Some(entry) = self.heap.pop() {
            // Check if this entry is still valid (not stale)
            if let Some((_, stored_gen)) = self.index.get(&entry.job.id) {
                if *stored_gen == entry.generation {
                    // Valid entry - remove from index and return
                    self.index.remove(&entry.job.id);
                    return Some(entry.job);
                }
            }
            // Stale entry - skip it
        }
        None
    }

    /// Peek at highest priority job - O(log n) amortized (skips stale entries)
    #[inline]
    #[allow(dead_code)]
    pub fn peek(&mut self) -> Option<&Job> {
        // Clean up stale entries from the top
        while let Some(entry) = self.heap.peek() {
            if let Some((_, stored_gen)) = self.index.get(&entry.job.id) {
                if *stored_gen == entry.generation {
                    // Valid entry
                    return self.index.get(&entry.job.id).map(|(job, _)| job);
                }
            }
            // Stale entry - remove it
            self.heap.pop();
        }
        None
    }

    /// Get a job by ID - O(1)
    #[inline]
    #[allow(dead_code)]
    pub fn get(&self, job_id: u64) -> Option<&Job> {
        self.index.get(&job_id).map(|(job, _)| job)
    }

    /// Check if job exists - O(1)
    #[inline]
    pub fn contains(&self, job_id: u64) -> bool {
        self.index.contains_key(&job_id)
    }

    /// Remove a job by ID - O(1) (lazy removal, cleaned on pop/peek)
    #[inline]
    pub fn remove(&mut self, job_id: u64) -> Option<Job> {
        self.index.remove(&job_id).map(|(job, _)| job)
    }

    /// Update a job (e.g., change priority) - O(log n)
    /// Returns old job if found
    #[inline]
    #[allow(dead_code)]
    pub fn update(&mut self, job: Job) -> Option<Job> {
        let id = job.id;
        let old = self.remove(id);
        self.push(job);
        old
    }

    /// Number of jobs (actual, not including stale heap entries)
    #[inline]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if empty
    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Iterate over all jobs (not in priority order)
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Job> {
        self.index.values().map(|(job, _)| job)
    }

    /// Drain all jobs (not in priority order)
    #[inline]
    #[allow(dead_code)]
    pub fn drain(&mut self) -> impl Iterator<Item = Job> + '_ {
        self.heap.clear();
        self.index.drain().map(|(_, (job, _))| job)
    }

    /// Clear all jobs
    #[inline]
    pub fn clear(&mut self) {
        self.heap.clear();
        self.index.clear();
    }

    /// Retain jobs matching predicate
    #[inline]
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&Job) -> bool,
    {
        let removed: Vec<u64> = self
            .index
            .iter()
            .filter(|(_, (job, _))| !f(job))
            .map(|(id, _)| *id)
            .collect();

        for id in removed {
            self.index.remove(&id);
        }
    }
}

impl Default for IndexedPriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ============== GxHash Type Aliases ==============
// GxHash is 20-30% faster than FxHash (uses AES-NI instructions)

pub type GxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<GxHasher>>;
pub type GxHashSet<T> = HashSet<T, BuildHasherDefault<GxHasher>>;

// Re-export CompactString for use in other modules (use the crate import)
#[allow(dead_code)]
pub type QueueName = CompactString;

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

// ============== Queue Name Helper ==============
// CompactString: inline up to 24 chars (zero heap allocation for typical queue names)
// No interning needed - CompactString is stack-allocated and cheap to clone

/// Create a CompactString from a queue name (zero allocation for names <= 24 chars)
#[inline(always)]
#[allow(dead_code)]
pub fn queue_name(s: &str) -> CompactString {
    CompactString::from(s)
}

/// Legacy alias for compatibility - now just creates a CompactString
#[inline(always)]
pub fn intern(s: &str) -> CompactString {
    CompactString::from(s)
}

/// No-op for compatibility - CompactString doesn't need cleanup
#[inline(always)]
pub fn cleanup_interned_strings() {
    // CompactString is stack-allocated, no cleanup needed
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
    /// Parent job waiting for children to complete (Flows)
    WaitingChildren { shard_idx: usize },
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
            JobLocation::WaitingChildren { .. } => JobState::WaitingParent,
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
// Uses CompactString for queue names (inline up to 24 chars, zero heap alloc)
// Uses IndexedPriorityQueue for O(log n) find/update/remove by job_id

pub struct Shard {
    pub queues: GxHashMap<CompactString, IndexedPriorityQueue>,
    pub dlq: GxHashMap<CompactString, VecDeque<Job>>,
    pub unique_keys: GxHashMap<CompactString, GxHashSet<String>>,
    pub waiting_deps: GxHashMap<u64, Job>,
    pub waiting_children: GxHashMap<u64, Job>, // Parent jobs waiting for children (Flows)
    pub queue_state: GxHashMap<CompactString, QueueState>,
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
            waiting_children: GxHashMap::with_capacity_and_hasher(256, Default::default()),
            queue_state: GxHashMap::with_capacity_and_hasher(16, Default::default()),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Get or create queue state
    #[inline]
    pub fn get_state(&mut self, queue: &CompactString) -> &mut QueueState {
        self.queue_state.entry(queue.clone()).or_default()
    }
}

impl Default for Shard {
    fn default() -> Self {
        Self::new()
    }
}

// ============== Global Metrics ==============

/// Global metrics with atomic counters for O(1) stats queries.
///
/// ## Performance Optimization
///
/// Instead of iterating all 32 shards to count jobs (O(n) where n = shards),
/// we maintain atomic counters that are updated on push/pull/ack/fail.
/// This makes stats() calls O(1) instead of O(32).
pub struct GlobalMetrics {
    pub total_pushed: AtomicU64,
    pub total_completed: AtomicU64,
    pub total_failed: AtomicU64,
    pub total_timed_out: AtomicU64,
    pub latency_sum: AtomicU64,
    pub latency_count: AtomicU64,
    // === O(1) Stats Counters ===
    /// Current number of jobs in queues (waiting + delayed)
    pub current_queued: AtomicU64,
    /// Current number of jobs being processed
    pub current_processing: AtomicU64,
    /// Current number of jobs in DLQ
    pub current_dlq: AtomicU64,
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
            current_queued: AtomicU64::new(0),
            current_processing: AtomicU64::new(0),
            current_dlq: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn record_push(&self, count: u64) {
        self.total_pushed.fetch_add(count, Ordering::Relaxed);
        self.current_queued.fetch_add(count, Ordering::Relaxed);
    }

    /// Record job pulled from queue to processing
    #[inline(always)]
    pub fn record_pull(&self) {
        self.current_queued.fetch_sub(1, Ordering::Relaxed);
        self.current_processing.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_complete(&self, latency: u64) {
        self.total_completed.fetch_add(1, Ordering::Relaxed);
        self.latency_sum.fetch_add(latency, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
        self.current_processing.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_fail(&self) {
        self.total_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record job moved to DLQ
    #[inline(always)]
    pub fn record_dlq(&self) {
        self.current_processing.fetch_sub(1, Ordering::Relaxed);
        self.current_dlq.fetch_add(1, Ordering::Relaxed);
    }

    /// Record job retried (back to queue from processing)
    #[inline(always)]
    pub fn record_retry(&self) {
        self.current_processing.fetch_sub(1, Ordering::Relaxed);
        self.current_queued.fetch_add(1, Ordering::Relaxed);
    }

    /// Record job removed from DLQ (retry or purge)
    #[inline(always)]
    #[allow(dead_code)]
    pub fn record_dlq_remove(&self) {
        self.current_dlq.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record job moved from DLQ back to queue
    #[inline(always)]
    #[allow(dead_code)]
    pub fn record_dlq_retry(&self) {
        self.current_dlq.fetch_sub(1, Ordering::Relaxed);
        self.current_queued.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_timeout(&self) {
        self.total_timed_out.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current stats (O(1) instead of O(shards))
    #[inline(always)]
    #[allow(dead_code)]
    pub fn get_current_stats(&self) -> (u64, u64, u64) {
        (
            self.current_queued.load(Ordering::Relaxed),
            self.current_processing.load(Ordering::Relaxed),
            self.current_dlq.load(Ordering::Relaxed),
        )
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
    pub queue: CompactString,
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

// ============== Snapshot Config ==============
// Redis-style periodic snapshot persistence

pub struct SnapshotConfig {
    /// Interval between snapshots in seconds (default: 60)
    pub interval_secs: u64,
    /// Minimum changes before triggering snapshot (default: 100)
    pub min_changes: u64,
}

impl SnapshotConfig {
    pub fn from_env() -> Option<Self> {
        let enabled = std::env::var("SNAPSHOT_MODE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        if !enabled {
            return None;
        }

        let interval_secs = std::env::var("SNAPSHOT_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);

        let min_changes = std::env::var("SNAPSHOT_MIN_CHANGES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        Some(Self {
            interval_secs,
            min_changes,
        })
    }
}

// ============== Circuit Breaker ==============

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests allowed
    Closed,
    /// Too many failures - requests blocked
    Open,
    /// Testing if service recovered - limited requests allowed
    HalfOpen,
}

/// Circuit breaker entry for a single endpoint
#[derive(Debug)]
pub struct CircuitBreakerEntry {
    pub state: CircuitState,
    pub failures: u32,
    pub successes: u32,
    pub last_failure_at: u64,
    pub opened_at: u64,
}

impl Default for CircuitBreakerEntry {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            failures: 0,
            successes: 0,
            last_failure_at: 0,
            opened_at: 0,
        }
    }
}

impl CircuitBreakerEntry {
    /// Check if requests should be allowed through
    pub fn should_allow(&self, now: u64, recovery_timeout_ms: u64) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => true, // Allow one test request
            CircuitState::Open => {
                // Check if recovery timeout has passed
                now >= self.opened_at + recovery_timeout_ms
            }
        }
    }

    /// Record a successful request
    pub fn record_success(&mut self) {
        match self.state {
            CircuitState::Closed => {
                // Reset failures on success
                self.failures = 0;
            }
            CircuitState::HalfOpen => {
                self.successes += 1;
                // After N successes in half-open, close the circuit
                if self.successes >= 2 {
                    self.state = CircuitState::Closed;
                    self.failures = 0;
                    self.successes = 0;
                }
            }
            CircuitState::Open => {
                // Shouldn't happen - we block requests when open
            }
        }
    }

    /// Record a failed request
    pub fn record_failure(&mut self, now: u64, failure_threshold: u32) {
        self.failures += 1;
        self.last_failure_at = now;

        match self.state {
            CircuitState::Closed => {
                if self.failures >= failure_threshold {
                    self.state = CircuitState::Open;
                    self.opened_at = now;
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open goes back to open
                self.state = CircuitState::Open;
                self.opened_at = now;
                self.successes = 0;
            }
            CircuitState::Open => {
                // Already open, update opened_at
                self.opened_at = now;
            }
        }
    }

    /// Transition from open to half-open for testing
    pub fn try_half_open(&mut self, now: u64, recovery_timeout_ms: u64) -> bool {
        if self.state == CircuitState::Open && now >= self.opened_at + recovery_timeout_ms {
            self.state = CircuitState::HalfOpen;
            self.successes = 0;
            true
        } else {
            false
        }
    }
}

/// Circuit breaker configuration
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u32,
    /// Time in ms before trying to close (half-open state)
    pub recovery_timeout_ms: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout_ms: 30_000, // 30 seconds
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

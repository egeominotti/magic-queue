//! Core QueueManager struct and constructors.
//!
//! The actual operations are implemented in separate modules:
//! - persistence.rs - PostgreSQL persistence (persist_*)
//! - query.rs - Job query operations (get_job, get_state, wait_for_job)
//! - admin.rs - Admin operations (workers, webhooks, settings, reset)

use std::collections::VecDeque;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::Arc;

use dashmap::DashMap;
use gxhash::GxHasher;
use parking_lot::RwLock;
use serde_json::Value;

use super::cluster::ClusterManager;
use super::postgres::PostgresStorage;
use super::types::{
    init_coarse_time, intern, now_ms, CircuitBreakerConfig, CircuitBreakerEntry, GlobalMetrics,
    GxHashMap, GxHashSet, JobLocation, Shard, SnapshotConfig, Subscriber, Webhook, Worker,
};

/// Type alias for DashMap with GxHash for lock-free job index
pub type JobIndexMap = DashMap<u64, JobLocation, BuildHasherDefault<GxHasher>>;

/// Type alias for sharded processing maps
pub type ProcessingShard = RwLock<GxHashMap<u64, Job>>;
use crate::protocol::{set_id_counter, CronJob, Job, JobEvent, JobLogEntry, MetricsHistoryPoint};
use tokio::sync::broadcast;
use tracing::{error, info};

pub const NUM_SHARDS: usize = 32;

/// Constant-time byte slice comparison to prevent timing attacks.
/// Returns true if slices are equal, false otherwise.
#[inline]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

pub struct QueueManager {
    pub(crate) shards: Vec<RwLock<Shard>>,
    /// Sharded processing map - 32 shards for parallel ack/fail (6x less contention)
    pub(crate) processing_shards: Vec<ProcessingShard>,
    /// PostgreSQL storage (replaces WAL)
    pub(crate) storage: Option<Arc<PostgresStorage>>,
    pub(crate) cron_jobs: RwLock<GxHashMap<String, CronJob>>,
    pub(crate) completed_jobs: RwLock<GxHashSet<u64>>,
    pub(crate) job_results: RwLock<GxHashMap<u64, Value>>,
    pub(crate) subscribers: RwLock<Vec<Subscriber>>,
    pub(crate) auth_tokens: RwLock<GxHashSet<String>>,
    pub(crate) metrics: GlobalMetrics,
    /// O(1) job location index - lock-free DashMap (40% faster lookups)
    pub(crate) job_index: JobIndexMap,
    // Worker registration
    pub(crate) workers: RwLock<GxHashMap<String, Worker>>,
    // Webhooks
    pub(crate) webhooks: RwLock<GxHashMap<String, Webhook>>,
    // Circuit breaker for webhooks (URL -> circuit state) - Arc for async tasks
    pub(crate) webhook_circuits: Arc<RwLock<GxHashMap<String, CircuitBreakerEntry>>>,
    pub(crate) circuit_breaker_config: CircuitBreakerConfig,
    // Event broadcast for SSE/WebSocket
    pub(crate) event_tx: broadcast::Sender<JobEvent>,
    // Metrics history for charts (last 60 points = 5 minutes at 5s intervals)
    pub(crate) metrics_history: RwLock<VecDeque<MetricsHistoryPoint>>,
    // Cluster manager for HA
    pub(crate) cluster: Option<Arc<ClusterManager>>,
    // Snapshot mode (Redis-style persistence)
    pub(crate) snapshot_config: Option<SnapshotConfig>,
    pub(crate) snapshot_changes: std::sync::atomic::AtomicU64,
    pub(crate) last_snapshot: std::sync::atomic::AtomicU64,
    // === New BullMQ-like features ===
    // Job logs storage (job_id -> logs) - VecDeque for O(1) pop_front
    pub(crate) job_logs: RwLock<GxHashMap<u64, VecDeque<JobLogEntry>>>,
    // Stalled job tracking (job_id -> stall_count)
    pub(crate) stalled_count: RwLock<GxHashMap<u64, u32>>,
    // Distributed pull mode: use SELECT FOR UPDATE SKIP LOCKED
    // Prevents duplicate job processing in cluster mode (slower but consistent)
    pub(crate) distributed_pull: bool,
    // Sync persistence mode: wait for PostgreSQL before returning to client
    // Prevents job loss on crash but reduces throughput (~10x slower)
    // Enable with SYNC_PERSISTENCE=1 for maximum durability
    pub(crate) sync_persistence: bool,
    // Debounce cache: nested map queue -> debounce_id -> expiry_timestamp
    // OPTIMIZATION: Uses CompactString keys (inline up to 24 chars) to avoid String allocation
    // Old: format!("{}:{}", queue, id) allocated ~50 bytes per push
    // New: CompactString keys inline up to 24 chars each with zero heap allocation
    pub(crate) debounce_cache:
        RwLock<GxHashMap<compact_str::CompactString, GxHashMap<compact_str::CompactString, u64>>>,
    // Custom job ID mapping: custom_id -> internal job ID
    pub(crate) custom_id_map: RwLock<GxHashMap<String, u64>>,
    // Job waiters for finished() promise: job_id -> list of waiting channels
    #[allow(clippy::type_complexity)]
    pub(crate) job_waiters:
        RwLock<GxHashMap<u64, Vec<tokio::sync::oneshot::Sender<Option<Value>>>>>,
    // Completed jobs with retention: job_id -> (completed_at, keep_age, result)
    #[allow(clippy::type_complexity)]
    pub(crate) completed_retention: RwLock<GxHashMap<u64, (u64, u64, Option<Value>)>>,
    // Queue defaults for new jobs
    pub(crate) queue_defaults: RwLock<QueueDefaults>,
    // Cleanup settings
    pub(crate) cleanup_settings: RwLock<CleanupSettings>,
    // TCP connection counter
    pub(crate) tcp_connection_count: std::sync::atomic::AtomicUsize,
    // Shared HTTP client for webhooks (reuse connections)
    pub(crate) http_client: reqwest::Client,
    // Shutdown flag for graceful shutdown of background tasks
    pub(crate) shutdown_flag: std::sync::atomic::AtomicBool,
}

#[derive(Debug, Clone, Default)]
pub struct QueueDefaults {
    pub timeout: Option<u64>,
    pub max_attempts: Option<u32>,
    pub backoff: Option<u64>,
    pub ttl: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CleanupSettings {
    pub max_completed_jobs: usize,
    pub max_job_results: usize,
    pub cleanup_interval_secs: u64,
    pub metrics_history_size: usize,
}

impl Default for CleanupSettings {
    fn default() -> Self {
        Self {
            max_completed_jobs: 50000,
            max_job_results: 5000,
            cleanup_interval_secs: 60,
            metrics_history_size: 1000,
        }
    }
}

impl QueueManager {
    /// Create a new QueueManager without persistence.
    pub fn new(_persistence: bool) -> Arc<Self> {
        // For backwards compatibility, this creates a manager without PostgreSQL
        // Use `with_postgres` for PostgreSQL persistence
        Self::create(None, None)
    }

    /// Create a new QueueManager with PostgreSQL persistence.
    pub async fn with_postgres(database_url: &str) -> Arc<Self> {
        match PostgresStorage::new(database_url).await {
            Ok(storage) => {
                // Run migrations
                if let Err(e) = storage.migrate().await {
                    error!(error = %e, "Failed to run migrations");
                }

                let storage = Arc::new(storage);
                let manager = Self::create(Some(storage.clone()), None);

                // Recover from PostgreSQL
                manager.recover_from_postgres(&storage).await;

                manager
            }
            Err(e) => {
                error!(
                    error = %e,
                    "Failed to connect to PostgreSQL, running without persistence"
                );
                Self::create(None, None)
            }
        }
    }

    /// Create a new QueueManager with PostgreSQL and clustering support.
    pub async fn with_cluster(
        database_url: &str,
        node_id: String,
        host: String,
        port: i32,
    ) -> Arc<Self> {
        match PostgresStorage::new(database_url).await {
            Ok(storage) => {
                // Run migrations
                if let Err(e) = storage.migrate().await {
                    error!(error = %e, "Failed to run migrations");
                }

                let storage = Arc::new(storage);

                // Create cluster manager
                let cluster = Arc::new(ClusterManager::new(
                    node_id,
                    host,
                    port,
                    Some(storage.pool().clone()),
                ));

                // Initialize cluster tables
                if let Err(e) = cluster.init_tables().await {
                    error!(error = %e, "Failed to init cluster tables");
                }

                // Register this node
                if let Err(e) = cluster.register_node().await {
                    error!(error = %e, "Failed to register node");
                }

                // Try to become leader
                if let Err(e) = cluster.try_become_leader().await {
                    error!(error = %e, "Failed to try leadership");
                }

                let manager = Self::create(Some(storage.clone()), Some(cluster));

                // Recover from PostgreSQL
                manager.recover_from_postgres(&storage).await;

                // Start cluster sync listener for real-time job synchronization
                let db_url = database_url.to_string();
                let mgr = Arc::clone(&manager);
                tokio::spawn(async move {
                    mgr.start_cluster_sync_listener(&db_url).await;
                });

                manager
            }
            Err(e) => {
                error!(
                    error = %e,
                    "Failed to connect to PostgreSQL, running without persistence or cluster"
                );
                Self::create(None, None)
            }
        }
    }

    /// Internal constructor.
    fn create(
        storage: Option<Arc<PostgresStorage>>,
        cluster: Option<Arc<ClusterManager>>,
    ) -> Arc<Self> {
        use std::sync::atomic::AtomicU64;

        // Initialize coarse timestamp
        init_coarse_time();

        let shards = (0..NUM_SHARDS).map(|_| RwLock::new(Shard::new())).collect();
        let (event_tx, _) = broadcast::channel(1024);
        let has_storage = storage.is_some();
        let cluster_enabled = cluster.as_ref().map(|c| c.is_enabled()).unwrap_or(false);

        // Check for snapshot mode
        let snapshot_config = if has_storage {
            SnapshotConfig::from_env()
        } else {
            None
        };
        let snapshot_enabled = snapshot_config.is_some();

        // Check for distributed pull mode (SELECT FOR UPDATE SKIP LOCKED)
        // Enabled by default in cluster mode, can be disabled with DISTRIBUTED_PULL=0
        let distributed_pull = if cluster_enabled {
            // In cluster mode, default to distributed pull unless explicitly disabled
            std::env::var("DISTRIBUTED_PULL")
                .map(|v| v != "0" && v.to_lowercase() != "false")
                .unwrap_or(true)
        } else {
            // In non-cluster mode, only enable if explicitly requested
            std::env::var("DISTRIBUTED_PULL")
                .map(|v| v == "1" || v.to_lowercase() == "true")
                .unwrap_or(false)
        };

        // Check for sync persistence mode (wait for PostgreSQL before returning)
        // This prevents job loss on crash but reduces throughput significantly
        // Enable with SYNC_PERSISTENCE=1 for maximum durability
        let sync_persistence = std::env::var("SYNC_PERSISTENCE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        // Create sharded processing map (32 shards for parallel ack/fail)
        let processing_shards: Vec<ProcessingShard> = (0..NUM_SHARDS)
            .map(|_| RwLock::new(GxHashMap::with_capacity_and_hasher(128, Default::default())))
            .collect();

        let manager = Arc::new(Self {
            shards,
            processing_shards,
            storage,
            cron_jobs: RwLock::new(GxHashMap::default()),
            completed_jobs: RwLock::new(GxHashSet::default()),
            job_results: RwLock::new(GxHashMap::default()),
            subscribers: RwLock::new(Vec::new()),
            auth_tokens: RwLock::new(GxHashSet::default()),
            metrics: GlobalMetrics::new(),
            // Lock-free DashMap for job index (40% faster lookups)
            job_index: DashMap::with_capacity_and_hasher(65536, Default::default()),
            workers: RwLock::new(GxHashMap::default()),
            webhooks: RwLock::new(GxHashMap::default()),
            webhook_circuits: Arc::new(RwLock::new(GxHashMap::default())),
            circuit_breaker_config: CircuitBreakerConfig::default(),
            event_tx,
            metrics_history: RwLock::new(VecDeque::with_capacity(60)),
            cluster,
            snapshot_config,
            snapshot_changes: AtomicU64::new(0),
            last_snapshot: AtomicU64::new(now_ms()),
            // New BullMQ-like features
            job_logs: RwLock::new(GxHashMap::default()),
            stalled_count: RwLock::new(GxHashMap::default()),
            distributed_pull,
            sync_persistence,
            debounce_cache: RwLock::new(GxHashMap::with_capacity_and_hasher(
                64,
                Default::default(),
            )),
            // Custom job ID and finished() promise support
            custom_id_map: RwLock::new(GxHashMap::default()),
            job_waiters: RwLock::new(GxHashMap::default()),
            completed_retention: RwLock::new(GxHashMap::default()),
            queue_defaults: RwLock::new(QueueDefaults::default()),
            cleanup_settings: RwLock::new(CleanupSettings::default()),
            tcp_connection_count: std::sync::atomic::AtomicUsize::new(0),
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .pool_max_idle_per_host(10)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            shutdown_flag: std::sync::atomic::AtomicBool::new(false),
        });

        let mgr = Arc::clone(&manager);
        tokio::spawn(async move {
            mgr.background_tasks().await;
        });

        if has_storage {
            if snapshot_enabled {
                info!("PostgreSQL persistence enabled (SNAPSHOT MODE)");
            } else {
                info!("PostgreSQL persistence enabled (SYNC MODE)");
            }
        }
        if cluster_enabled {
            info!("Cluster mode enabled");
        }
        if distributed_pull {
            info!("Distributed pull enabled (SELECT FOR UPDATE SKIP LOCKED)");
        }
        if sync_persistence {
            info!("Sync persistence enabled (DURABLE MODE - waits for PostgreSQL)");
        }

        manager
    }

    /// Check if sync persistence mode is enabled.
    /// When enabled, push operations wait for PostgreSQL confirmation before returning.
    #[inline]
    pub fn is_sync_persistence(&self) -> bool {
        self.sync_persistence
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
            info!(
                token_count = manager.auth_tokens.read().len(),
                "Authentication enabled"
            );
        }
        manager
    }

    /// Verify authentication token using constant-time comparison to prevent timing attacks.
    #[inline]
    pub fn verify_token(&self, token: &str) -> bool {
        let tokens = self.auth_tokens.read();
        if tokens.is_empty() {
            return true;
        }

        // Use constant-time comparison to prevent timing attacks
        let mut found = false;
        for valid_token in tokens.iter() {
            found |= constant_time_eq(token.as_bytes(), valid_token.as_bytes());
        }
        found
    }

    #[inline]
    pub fn is_postgres_connected(&self) -> bool {
        self.storage.is_some()
    }

    /// Check if snapshot mode is enabled.
    #[inline]
    pub fn is_snapshot_mode(&self) -> bool {
        self.snapshot_config.is_some()
    }

    /// Increment snapshot change counter.
    #[inline]
    pub(crate) fn record_change(&self) {
        self.snapshot_changes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get current change count.
    #[allow(dead_code)]
    #[inline]
    pub fn snapshot_change_count(&self) -> u64 {
        self.snapshot_changes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get next job ID from PostgreSQL sequence (cluster-wide unique).
    /// In cluster mode, falls back to node-specific local IDs to avoid collisions.
    /// In standalone mode, uses simple atomic counter.
    pub async fn next_job_id(&self) -> u64 {
        if let Some(ref storage) = self.storage {
            if let Ok(id) = storage.next_sequence_id().await {
                return id;
            }
            // In cluster mode, use node-specific fallback to avoid collisions
            // Format: high 16 bits = node hash, low 48 bits = local counter
            // This gives ~281 trillion unique IDs per node before collision risk
            tracing::warn!("PostgreSQL sequence unavailable, using node-specific fallback ID");
            return self.node_specific_fallback_id();
        }
        // Standalone mode: simple atomic counter
        crate::protocol::next_id()
    }

    /// Get next N job IDs from PostgreSQL sequence (cluster-wide unique).
    /// In cluster mode, falls back to node-specific local IDs to avoid collisions.
    pub async fn next_job_ids(&self, count: usize) -> Vec<u64> {
        if let Some(ref storage) = self.storage {
            if let Ok(ids) = storage.next_sequence_ids(count as i64).await {
                return ids;
            }
            // In cluster mode, use node-specific fallback to avoid collisions
            tracing::warn!(
                count = count,
                "PostgreSQL sequence unavailable, using node-specific fallback IDs"
            );
            return (0..count)
                .map(|_| self.node_specific_fallback_id())
                .collect();
        }
        // Standalone mode: simple atomic counter
        (0..count).map(|_| crate::protocol::next_id()).collect()
    }

    /// Generate a node-specific fallback ID to avoid collisions in cluster mode.
    /// Uses high 16 bits for node hash, low 48 bits for local counter.
    /// This prevents ID collisions between nodes when PostgreSQL is unavailable.
    fn node_specific_fallback_id(&self) -> u64 {
        let local_id = crate::protocol::next_id();
        let node_hash = self.node_id_hash();
        // Combine: top 16 bits = node hash, bottom 48 bits = local counter
        // This allows ~281 trillion IDs per node before wraparound
        ((node_hash as u64) << 48) | (local_id & 0x0000_FFFF_FFFF_FFFF)
    }

    /// Get a 16-bit hash of the node ID for use in fallback ID generation.
    fn node_id_hash(&self) -> u16 {
        if let Some(ref cluster) = self.cluster {
            // Use FNV-1a hash for good distribution
            let mut hash: u32 = 2166136261;
            for byte in cluster.node_id.as_bytes() {
                hash ^= *byte as u32;
                hash = hash.wrapping_mul(16777619);
            }
            (hash & 0xFFFF) as u16
        } else {
            // Standalone mode: use 0
            0
        }
    }

    #[inline]
    pub fn auth_token_count(&self) -> usize {
        self.auth_tokens.read().len()
    }

    /// Check if this node is the cluster leader (or if clustering is disabled).
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.cluster.as_ref().map(|c| c.is_leader()).unwrap_or(true)
    }

    /// Check if cluster mode is enabled.
    #[inline]
    pub fn is_cluster_enabled(&self) -> bool {
        self.cluster
            .as_ref()
            .map(|c| c.is_enabled())
            .unwrap_or(false)
    }

    /// Check if distributed pull mode is enabled.
    /// When enabled, uses SELECT FOR UPDATE SKIP LOCKED for consistent job claiming.
    #[inline]
    pub fn is_distributed_pull(&self) -> bool {
        self.distributed_pull
    }

    /// Get the node ID.
    #[inline]
    pub fn node_id(&self) -> Option<String> {
        self.cluster.as_ref().map(|c| c.node_id.clone())
    }

    /// Get cluster manager reference.
    #[inline]
    pub fn cluster(&self) -> Option<&Arc<ClusterManager>> {
        self.cluster.as_ref()
    }

    /// Signal shutdown to stop background tasks.
    pub fn shutdown(&self) {
        self.shutdown_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if shutdown has been requested.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_flag
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get shard index for a queue name.
    #[inline(always)]
    pub fn shard_index(queue: &str) -> usize {
        let mut hasher = GxHasher::default();
        queue.hash(&mut hasher);
        hasher.finish() as usize % NUM_SHARDS
    }

    /// Get processing shard index for a job ID.
    /// OPTIMIZATION: Uses bit masking instead of modulo (faster on most CPUs)
    /// Only works because NUM_SHARDS is a power of 2 (32 = 2^5)
    #[inline(always)]
    pub fn processing_shard_index(job_id: u64) -> usize {
        // 0x1F = 31 = NUM_SHARDS - 1 (bit mask for modulo 32)
        (job_id as usize) & 0x1F
    }

    /// Insert job into processing (sharded).
    #[inline]
    pub(crate) fn processing_insert(&self, job: Job) {
        let idx = Self::processing_shard_index(job.id);
        self.processing_shards[idx].write().insert(job.id, job);
    }

    /// Remove job from processing (sharded), returns the job if found.
    #[inline]
    pub(crate) fn processing_remove(&self, job_id: u64) -> Option<Job> {
        let idx = Self::processing_shard_index(job_id);
        self.processing_shards[idx].write().remove(&job_id)
    }

    /// Get job from processing (sharded).
    /// NOTE: This clones the job. For read-only access, prefer `processing_get_ref`.
    #[inline]
    pub(crate) fn processing_get(&self, job_id: u64) -> Option<Job> {
        let idx = Self::processing_shard_index(job_id);
        self.processing_shards[idx].read().get(&job_id).cloned()
    }

    /// Check if a job is in processing (sharded, O(1) lookup).
    #[inline]
    pub(crate) fn processing_contains(&self, job_id: u64) -> bool {
        let idx = Self::processing_shard_index(job_id);
        self.processing_shards[idx].read().contains_key(&job_id)
    }

    /// OPTIMIZATION: Get read-only reference to job via closure (avoids cloning).
    /// Use this when you only need to read job data without modifying it.
    ///
    /// Example:
    /// ```ignore
    /// let queue_name = manager.processing_get_ref(job_id, |job| job.queue.clone());
    /// ```
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn processing_get_ref<F, R>(&self, job_id: u64, f: F) -> Option<R>
    where
        F: FnOnce(&Job) -> R,
    {
        let idx = Self::processing_shard_index(job_id);
        self.processing_shards[idx].read().get(&job_id).map(f)
    }

    /// Get mutable reference to job in processing via closure.
    #[inline]
    pub(crate) fn processing_get_mut<F, R>(&self, job_id: u64, f: F) -> Option<R>
    where
        F: FnOnce(&mut Job) -> R,
    {
        let idx = Self::processing_shard_index(job_id);
        self.processing_shards[idx].write().get_mut(&job_id).map(f)
    }

    /// Count all jobs in processing (across all shards).
    #[inline]
    pub(crate) fn processing_len(&self) -> usize {
        self.processing_shards.iter().map(|s| s.read().len()).sum()
    }

    /// Get the count of jobs currently being processed (public API for graceful shutdown).
    #[inline]
    pub fn processing_count(&self) -> usize {
        self.processing_len()
    }

    /// Iterate over all processing jobs (for stats, etc.).
    pub(crate) fn processing_iter<F>(&self, mut f: F)
    where
        F: FnMut(&Job),
    {
        for shard in &self.processing_shards {
            let shard = shard.read();
            for job in shard.values() {
                f(job);
            }
        }
    }

    /// Iterate over all processing jobs for a specific queue.
    pub(crate) fn processing_count_by_queue(&self, queue: &str) -> usize {
        let mut count = 0;
        for shard in &self.processing_shards {
            let shard = shard.read();
            count += shard.values().filter(|j| j.queue == queue).count();
        }
        count
    }

    /// Get current timestamp in milliseconds.
    #[inline(always)]
    pub fn now_ms() -> u64 {
        now_ms()
    }

    /// Recover state from PostgreSQL on startup.
    async fn recover_from_postgres(&self, storage: &PostgresStorage) {
        // CRITICAL: Sync PostgreSQL sequence with max job ID to avoid ID conflicts
        match storage.get_max_job_id().await {
            Ok(max_id) => {
                if max_id > 0 {
                    // Set PostgreSQL sequence to max_id so next call returns max_id + 1
                    if let Err(e) = storage.set_sequence_value(max_id).await {
                        error!(error = %e, "Failed to set sequence value");
                    } else {
                        info!(next_id = max_id + 1, "Synced job ID sequence");
                    }
                    // Also set local counter as fallback
                    set_id_counter(max_id + 1);
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to recover max job ID");
            }
        }

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
                        shard.queues.entry(queue_name).or_default().push(job);
                        self.index_job(job_id, JobLocation::Queue { shard_idx: idx });
                        job_count += 1;
                    }
                    "active" => {
                        // Jobs that were active when server stopped - requeue them
                        let mut shard = self.shards[idx].write();
                        shard.queues.entry(queue_name).or_default().push(job);
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
                self.shards[idx]
                    .write()
                    .dlq
                    .entry(queue_name)
                    .or_default()
                    .push_back(job);
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
                info!(
                    count = cron_jobs.len(),
                    "Recovered cron jobs from PostgreSQL"
                );
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
                info!(count = wh.len(), "Recovered webhooks from PostgreSQL");
            }
        }

        if job_count > 0 {
            info!(count = job_count, "Recovered jobs from PostgreSQL");
        }
    }
}

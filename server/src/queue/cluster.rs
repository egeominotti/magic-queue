//! Cluster management for High Availability
//!
//! Uses PostgreSQL advisory locks for leader election.
//! Only the leader runs background tasks (cron, cleanup, timeouts).
//!
//! Features:
//! - Leader election via PostgreSQL advisory locks
//! - Intelligent load balancing based on node metrics
//! - Sticky sessions for WebSocket connections
//! - Cluster-wide metrics aggregation

use sqlx::{PgPool, Row};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Advisory lock key for leader election
const LEADER_LOCK_KEY: i64 = 12345;

/// Node information for cluster registry
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub host: String,
    pub port: i32,
    pub is_leader: bool,
    pub last_heartbeat: i64,
    pub started_at: i64,
}

/// Extended node info with metrics for load balancing
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeMetrics {
    pub node_id: String,
    pub host: String,
    pub port: i32,
    pub is_leader: bool,
    pub jobs_processing: u64,
    pub jobs_completed: u64,
    pub jobs_failed: u64,
    pub throughput_per_sec: f64,
    pub avg_latency_ms: f64,
    pub cpu_load: f64, // 0.0 - 1.0
    pub last_heartbeat: i64,
    pub started_at: i64,
}

/// Cluster-wide aggregated metrics
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ClusterMetrics {
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub leader_node: Option<String>,
    pub total_jobs_processing: u64,
    pub total_jobs_completed: u64,
    pub total_jobs_failed: u64,
    pub cluster_throughput_per_sec: f64,
    pub avg_cluster_latency_ms: f64,
}

/// Load balancing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LoadBalanceStrategy {
    /// Round-robin distribution
    RoundRobin,
    /// Least connections (lowest jobs_processing)
    LeastConnections,
    /// Weighted by inverse latency
    WeightedLatency,
    /// Random selection
    Random,
}

/// Cluster manager handles leader election and node registry
pub struct ClusterManager {
    pub node_id: String,
    pool: Option<PgPool>,
    is_leader: AtomicBool,
    host: String,
    port: i32,
    enabled: bool,
    // Local node metrics
    jobs_processing: AtomicU64,
    jobs_completed: AtomicU64,
    jobs_failed: AtomicU64,
    latency_sum: AtomicU64,
    latency_count: AtomicU64,
    // Load balancing
    round_robin_counter: AtomicU64,
    load_balance_strategy: parking_lot::RwLock<LoadBalanceStrategy>,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub fn new(node_id: String, host: String, port: i32, pool: Option<PgPool>) -> Self {
        let enabled = pool.is_some() && std::env::var("CLUSTER_MODE").is_ok();

        Self {
            node_id,
            pool,
            is_leader: AtomicBool::new(!enabled), // If not enabled, act as leader
            host,
            port,
            enabled,
            // Initialize local metrics
            jobs_processing: AtomicU64::new(0),
            jobs_completed: AtomicU64::new(0),
            jobs_failed: AtomicU64::new(0),
            latency_sum: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            // Load balancing
            round_robin_counter: AtomicU64::new(0),
            load_balance_strategy: parking_lot::RwLock::new(LoadBalanceStrategy::LeastConnections),
        }
    }

    /// Check if clustering is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Initialize cluster tables
    pub async fn init_tables(&self) -> Result<(), sqlx::Error> {
        if let Some(pool) = &self.pool {
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS cluster_nodes (
                    node_id VARCHAR(64) PRIMARY KEY,
                    host VARCHAR(255) NOT NULL,
                    port INTEGER NOT NULL,
                    is_leader BOOLEAN DEFAULT FALSE,
                    jobs_processing BIGINT DEFAULT 0,
                    jobs_completed BIGINT DEFAULT 0,
                    jobs_failed BIGINT DEFAULT 0,
                    throughput_per_sec DOUBLE PRECISION DEFAULT 0,
                    avg_latency_ms DOUBLE PRECISION DEFAULT 0,
                    last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
                    started_at TIMESTAMPTZ DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;

            // Add columns if they don't exist (for existing installations)
            let _ = sqlx::query(
                "ALTER TABLE cluster_nodes ADD COLUMN IF NOT EXISTS jobs_processing BIGINT DEFAULT 0",
            )
            .execute(pool)
            .await;
            let _ = sqlx::query(
                "ALTER TABLE cluster_nodes ADD COLUMN IF NOT EXISTS jobs_completed BIGINT DEFAULT 0",
            )
            .execute(pool)
            .await;
            let _ = sqlx::query(
                "ALTER TABLE cluster_nodes ADD COLUMN IF NOT EXISTS jobs_failed BIGINT DEFAULT 0",
            )
            .execute(pool)
            .await;
            let _ = sqlx::query(
                "ALTER TABLE cluster_nodes ADD COLUMN IF NOT EXISTS throughput_per_sec DOUBLE PRECISION DEFAULT 0",
            )
            .execute(pool)
            .await;
            let _ = sqlx::query(
                "ALTER TABLE cluster_nodes ADD COLUMN IF NOT EXISTS avg_latency_ms DOUBLE PRECISION DEFAULT 0",
            )
            .execute(pool)
            .await;
        }
        Ok(())
    }

    /// Register this node in the cluster
    pub async fn register_node(&self) -> Result<(), sqlx::Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(pool) = &self.pool {
            sqlx::query(
                r#"
                INSERT INTO cluster_nodes (node_id, host, port, is_leader, last_heartbeat, started_at)
                VALUES ($1, $2, $3, FALSE, NOW(), NOW())
                ON CONFLICT (node_id) DO UPDATE SET
                    host = EXCLUDED.host,
                    port = EXCLUDED.port,
                    last_heartbeat = NOW()
                "#,
            )
            .bind(&self.node_id)
            .bind(&self.host)
            .bind(self.port)
            .execute(pool)
            .await?;

            println!("Node {} registered in cluster", self.node_id);
        }
        Ok(())
    }

    /// Try to acquire leadership using PostgreSQL advisory lock
    pub async fn try_become_leader(&self) -> Result<bool, sqlx::Error> {
        if !self.enabled {
            return Ok(true);
        }

        if let Some(pool) = &self.pool {
            // Try to acquire advisory lock (non-blocking)
            let result: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
                .bind(LEADER_LOCK_KEY)
                .fetch_one(pool)
                .await?;

            let acquired = result.0;

            if acquired {
                // Update our status in the registry
                sqlx::query("UPDATE cluster_nodes SET is_leader = TRUE WHERE node_id = $1")
                    .bind(&self.node_id)
                    .execute(pool)
                    .await?;

                // Clear leader flag from other nodes
                sqlx::query("UPDATE cluster_nodes SET is_leader = FALSE WHERE node_id != $1")
                    .bind(&self.node_id)
                    .execute(pool)
                    .await?;

                let was_leader = self.is_leader.swap(true, Ordering::SeqCst);
                if !was_leader {
                    println!("Node {} became LEADER", self.node_id);
                }
            } else {
                let was_leader = self.is_leader.swap(false, Ordering::SeqCst);
                if was_leader {
                    println!("Node {} is now FOLLOWER", self.node_id);
                }
            }

            return Ok(acquired);
        }
        Ok(true)
    }

    /// Release leadership
    #[allow(dead_code)]
    pub async fn release_leadership(&self) -> Result<(), sqlx::Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(pool) = &self.pool {
            // Release advisory lock
            sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(LEADER_LOCK_KEY)
                .execute(pool)
                .await?;

            // Update registry
            sqlx::query("UPDATE cluster_nodes SET is_leader = FALSE WHERE node_id = $1")
                .bind(&self.node_id)
                .execute(pool)
                .await?;

            self.is_leader.store(false, Ordering::SeqCst);
            println!("Node {} released leadership", self.node_id);
        }
        Ok(())
    }

    /// Send heartbeat and update last_heartbeat with metrics
    pub async fn heartbeat(&self) -> Result<(), sqlx::Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(pool) = &self.pool {
            let jobs_processing = self.jobs_processing.load(Ordering::Relaxed);
            let jobs_completed = self.jobs_completed.load(Ordering::Relaxed);
            let jobs_failed = self.jobs_failed.load(Ordering::Relaxed);
            let latency_count = self.latency_count.load(Ordering::Relaxed);
            let avg_latency = if latency_count > 0 {
                self.latency_sum.load(Ordering::Relaxed) as f64 / latency_count as f64
            } else {
                0.0
            };

            sqlx::query(
                r#"
                UPDATE cluster_nodes SET
                    last_heartbeat = NOW(),
                    jobs_processing = $2,
                    jobs_completed = $3,
                    jobs_failed = $4,
                    avg_latency_ms = $5
                WHERE node_id = $1
                "#,
            )
            .bind(&self.node_id)
            .bind(jobs_processing as i64)
            .bind(jobs_completed as i64)
            .bind(jobs_failed as i64)
            .bind(avg_latency)
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    /// List all nodes in the cluster
    pub async fn list_nodes(&self) -> Result<Vec<NodeInfo>, sqlx::Error> {
        if let Some(pool) = &self.pool {
            let rows = sqlx::query(
                r#"
                SELECT node_id, host, port, is_leader,
                       EXTRACT(EPOCH FROM last_heartbeat)::bigint * 1000 as last_heartbeat_ms,
                       EXTRACT(EPOCH FROM started_at)::bigint * 1000 as started_at_ms
                FROM cluster_nodes
                ORDER BY started_at ASC
                "#,
            )
            .fetch_all(pool)
            .await?;

            Ok(rows
                .into_iter()
                .map(|row| NodeInfo {
                    node_id: row.get("node_id"),
                    host: row.get("host"),
                    port: row.get("port"),
                    is_leader: row.get("is_leader"),
                    last_heartbeat: row.get("last_heartbeat_ms"),
                    started_at: row.get("started_at_ms"),
                })
                .collect())
        } else {
            // Return self as only node if no PostgreSQL
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            Ok(vec![NodeInfo {
                node_id: self.node_id.clone(),
                host: self.host.clone(),
                port: self.port,
                is_leader: true,
                last_heartbeat: now,
                started_at: now,
            }])
        }
    }

    /// Cleanup stale nodes (no heartbeat for 30s)
    pub async fn cleanup_stale_nodes(&self) -> Result<u64, sqlx::Error> {
        if !self.enabled {
            return Ok(0);
        }

        if let Some(pool) = &self.pool {
            let result = sqlx::query(
                "DELETE FROM cluster_nodes WHERE last_heartbeat < NOW() - INTERVAL '30 seconds'",
            )
            .execute(pool)
            .await?;

            let removed = result.rows_affected();
            if removed > 0 {
                println!("Cleaned up {} stale nodes", removed);
            }
            return Ok(removed);
        }
        Ok(0)
    }

    /// Unregister this node from the cluster
    #[allow(dead_code)]
    pub async fn unregister(&self) -> Result<(), sqlx::Error> {
        if !self.enabled {
            return Ok(());
        }

        // Release leadership first
        self.release_leadership().await?;

        if let Some(pool) = &self.pool {
            sqlx::query("DELETE FROM cluster_nodes WHERE node_id = $1")
                .bind(&self.node_id)
                .execute(pool)
                .await?;
            println!("Node {} unregistered from cluster", self.node_id);
        }
        Ok(())
    }

    // ============== Local Metrics ==============

    /// Record a job started processing
    #[inline]
    #[allow(dead_code)]
    pub fn record_job_start(&self) {
        self.jobs_processing.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a job completed successfully
    #[inline]
    #[allow(dead_code)]
    pub fn record_job_complete(&self, latency_ms: u64) {
        self.jobs_processing.fetch_sub(1, Ordering::Relaxed);
        self.jobs_completed.fetch_add(1, Ordering::Relaxed);
        self.latency_sum.fetch_add(latency_ms, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a job failed
    #[inline]
    #[allow(dead_code)]
    pub fn record_job_fail(&self) {
        self.jobs_processing.fetch_sub(1, Ordering::Relaxed);
        self.jobs_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get local node metrics
    pub fn local_metrics(&self) -> NodeMetrics {
        let latency_count = self.latency_count.load(Ordering::Relaxed);
        let avg_latency = if latency_count > 0 {
            self.latency_sum.load(Ordering::Relaxed) as f64 / latency_count as f64
        } else {
            0.0
        };

        NodeMetrics {
            node_id: self.node_id.clone(),
            host: self.host.clone(),
            port: self.port,
            is_leader: self.is_leader(),
            jobs_processing: self.jobs_processing.load(Ordering::Relaxed),
            jobs_completed: self.jobs_completed.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            throughput_per_sec: 0.0, // Calculated at query time
            avg_latency_ms: avg_latency,
            cpu_load: 0.0, // Could be extended with sys-info crate
            last_heartbeat: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            started_at: 0,
        }
    }

    // ============== Load Balancing ==============

    /// Set load balancing strategy
    pub fn set_load_balance_strategy(&self, strategy: LoadBalanceStrategy) {
        *self.load_balance_strategy.write() = strategy;
    }

    /// Get current load balancing strategy
    pub fn load_balance_strategy(&self) -> LoadBalanceStrategy {
        *self.load_balance_strategy.read()
    }

    /// List nodes with metrics for load balancing
    pub async fn list_nodes_with_metrics(&self) -> Result<Vec<NodeMetrics>, sqlx::Error> {
        if let Some(pool) = &self.pool {
            let rows = sqlx::query(
                r#"
                SELECT node_id, host, port, is_leader,
                       COALESCE(jobs_processing, 0) as jobs_processing,
                       COALESCE(jobs_completed, 0) as jobs_completed,
                       COALESCE(jobs_failed, 0) as jobs_failed,
                       COALESCE(throughput_per_sec, 0) as throughput_per_sec,
                       COALESCE(avg_latency_ms, 0) as avg_latency_ms,
                       EXTRACT(EPOCH FROM last_heartbeat)::bigint * 1000 as last_heartbeat_ms,
                       EXTRACT(EPOCH FROM started_at)::bigint * 1000 as started_at_ms
                FROM cluster_nodes
                WHERE last_heartbeat > NOW() - INTERVAL '30 seconds'
                ORDER BY jobs_processing ASC
                "#,
            )
            .fetch_all(pool)
            .await?;

            Ok(rows
                .into_iter()
                .map(|row| NodeMetrics {
                    node_id: row.get("node_id"),
                    host: row.get("host"),
                    port: row.get("port"),
                    is_leader: row.get("is_leader"),
                    jobs_processing: row.get::<i64, _>("jobs_processing") as u64,
                    jobs_completed: row.get::<i64, _>("jobs_completed") as u64,
                    jobs_failed: row.get::<i64, _>("jobs_failed") as u64,
                    throughput_per_sec: row.get("throughput_per_sec"),
                    avg_latency_ms: row.get("avg_latency_ms"),
                    cpu_load: 0.0,
                    last_heartbeat: row.get("last_heartbeat_ms"),
                    started_at: row.get("started_at_ms"),
                })
                .collect())
        } else {
            Ok(vec![self.local_metrics()])
        }
    }

    /// Select best node based on current strategy
    pub async fn select_best_node(&self) -> Result<Option<NodeMetrics>, sqlx::Error> {
        let nodes = self.list_nodes_with_metrics().await?;
        if nodes.is_empty() {
            return Ok(None);
        }

        let strategy = *self.load_balance_strategy.read();
        let selected = match strategy {
            LoadBalanceStrategy::RoundRobin => {
                let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize;
                nodes.get(idx % nodes.len()).cloned()
            }
            LoadBalanceStrategy::LeastConnections => {
                // Already sorted by jobs_processing ASC
                nodes.first().cloned()
            }
            LoadBalanceStrategy::WeightedLatency => {
                // Select node with lowest latency (if available)
                nodes
                    .iter()
                    .filter(|n| n.avg_latency_ms > 0.0)
                    .min_by(|a, b| {
                        a.avg_latency_ms
                            .partial_cmp(&b.avg_latency_ms)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .or(nodes.first())
                    .cloned()
            }
            LoadBalanceStrategy::Random => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let seed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as usize;
                nodes.get(seed % nodes.len()).cloned()
            }
        };

        Ok(selected)
    }

    // ============== Sticky Sessions ==============

    /// Get sticky node for a given session key (e.g., WebSocket connection ID, user ID)
    /// Uses consistent hashing to always route same key to same node
    pub async fn get_sticky_node(
        &self,
        session_key: &str,
    ) -> Result<Option<NodeMetrics>, sqlx::Error> {
        let nodes = self.list_nodes_with_metrics().await?;
        if nodes.is_empty() {
            return Ok(None);
        }

        // Consistent hash based on session key
        let hash = hash_string(session_key);
        let idx = hash as usize % nodes.len();

        Ok(nodes.into_iter().nth(idx))
    }

    /// Get sticky node for a queue (useful for queue affinity)
    #[allow(dead_code)]
    pub async fn get_node_for_queue(
        &self,
        queue_name: &str,
    ) -> Result<Option<NodeMetrics>, sqlx::Error> {
        self.get_sticky_node(queue_name).await
    }

    // ============== Cluster Metrics ==============

    /// Get aggregated cluster-wide metrics
    pub async fn cluster_metrics(&self) -> Result<ClusterMetrics, sqlx::Error> {
        if let Some(pool) = &self.pool {
            let row = sqlx::query(
                r#"
                SELECT
                    COUNT(*) as total_nodes,
                    COUNT(*) FILTER (WHERE last_heartbeat > NOW() - INTERVAL '30 seconds') as active_nodes,
                    (SELECT node_id FROM cluster_nodes WHERE is_leader = true LIMIT 1) as leader_node,
                    COALESCE(SUM(jobs_processing), 0) as total_jobs_processing,
                    COALESCE(SUM(jobs_completed), 0) as total_jobs_completed,
                    COALESCE(SUM(jobs_failed), 0) as total_jobs_failed,
                    COALESCE(SUM(throughput_per_sec), 0) as cluster_throughput,
                    COALESCE(AVG(avg_latency_ms) FILTER (WHERE avg_latency_ms > 0), 0) as avg_latency
                FROM cluster_nodes
                WHERE last_heartbeat > NOW() - INTERVAL '30 seconds'
                "#,
            )
            .fetch_one(pool)
            .await?;

            Ok(ClusterMetrics {
                total_nodes: row.get::<i64, _>("total_nodes") as usize,
                active_nodes: row.get::<i64, _>("active_nodes") as usize,
                leader_node: row.get("leader_node"),
                total_jobs_processing: row.get::<i64, _>("total_jobs_processing") as u64,
                total_jobs_completed: row.get::<i64, _>("total_jobs_completed") as u64,
                total_jobs_failed: row.get::<i64, _>("total_jobs_failed") as u64,
                cluster_throughput_per_sec: row.get("cluster_throughput"),
                avg_cluster_latency_ms: row.get("avg_latency"),
            })
        } else {
            // Single node mode
            let local = self.local_metrics();
            Ok(ClusterMetrics {
                total_nodes: 1,
                active_nodes: 1,
                leader_node: Some(self.node_id.clone()),
                total_jobs_processing: local.jobs_processing,
                total_jobs_completed: local.jobs_completed,
                total_jobs_failed: local.jobs_failed,
                cluster_throughput_per_sec: local.throughput_per_sec,
                avg_cluster_latency_ms: local.avg_latency_ms,
            })
        }
    }
}

/// Hash a string to u64 for consistent hashing
#[inline]
fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

/// Generate a unique node ID using ULID (sortable, faster than UUID)
pub fn generate_node_id() -> String {
    std::env::var("NODE_ID")
        .unwrap_or_else(|_| format!("node-{}", &ulid::Ulid::new().to_string()[..8]))
}

// ============== Tests ==============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_string_consistency() {
        // Same input should always produce same hash
        let hash1 = hash_string("test-session-123");
        let hash2 = hash_string("test-session-123");
        assert_eq!(hash1, hash2);

        // Different inputs should produce different hashes
        let hash3 = hash_string("test-session-456");
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_generate_node_id() {
        let id1 = generate_node_id();
        let id2 = generate_node_id();
        // Without NODE_ID env, should generate unique IDs
        if std::env::var("NODE_ID").is_err() {
            assert!(id1.starts_with("node-"));
            assert!(id2.starts_with("node-"));
        }
    }

    #[test]
    fn test_cluster_manager_new_disabled() {
        let cm = ClusterManager::new("test-node".to_string(), "localhost".to_string(), 6789, None);
        // Without pool and CLUSTER_MODE, should not be enabled
        assert!(!cm.is_enabled());
        // Should act as leader when not enabled
        assert!(cm.is_leader());
    }

    #[test]
    fn test_local_metrics_recording() {
        let cm = ClusterManager::new("test-node".to_string(), "localhost".to_string(), 6789, None);

        // Initial state
        let metrics = cm.local_metrics();
        assert_eq!(metrics.jobs_processing, 0);
        assert_eq!(metrics.jobs_completed, 0);
        assert_eq!(metrics.jobs_failed, 0);

        // Start a job
        cm.record_job_start();
        let metrics = cm.local_metrics();
        assert_eq!(metrics.jobs_processing, 1);

        // Complete with latency
        cm.record_job_complete(100);
        let metrics = cm.local_metrics();
        assert_eq!(metrics.jobs_processing, 0);
        assert_eq!(metrics.jobs_completed, 1);
        assert_eq!(metrics.avg_latency_ms, 100.0);

        // Start and fail
        cm.record_job_start();
        cm.record_job_fail();
        let metrics = cm.local_metrics();
        assert_eq!(metrics.jobs_processing, 0);
        assert_eq!(metrics.jobs_failed, 1);
    }

    #[test]
    fn test_load_balance_strategy_change() {
        let cm = ClusterManager::new("test-node".to_string(), "localhost".to_string(), 6789, None);

        // Default is LeastConnections
        assert_eq!(
            cm.load_balance_strategy(),
            LoadBalanceStrategy::LeastConnections
        );

        // Change to RoundRobin
        cm.set_load_balance_strategy(LoadBalanceStrategy::RoundRobin);
        assert_eq!(cm.load_balance_strategy(), LoadBalanceStrategy::RoundRobin);

        // Change to WeightedLatency
        cm.set_load_balance_strategy(LoadBalanceStrategy::WeightedLatency);
        assert_eq!(
            cm.load_balance_strategy(),
            LoadBalanceStrategy::WeightedLatency
        );
    }

    #[test]
    fn test_node_metrics_struct() {
        let metrics = NodeMetrics {
            node_id: "node-1".to_string(),
            host: "localhost".to_string(),
            port: 6789,
            is_leader: true,
            jobs_processing: 10,
            jobs_completed: 100,
            jobs_failed: 5,
            throughput_per_sec: 50.0,
            avg_latency_ms: 25.5,
            cpu_load: 0.75,
            last_heartbeat: 1234567890,
            started_at: 1234567800,
        };

        assert_eq!(metrics.node_id, "node-1");
        assert!(metrics.is_leader);
        assert_eq!(metrics.jobs_processing, 10);
    }

    #[test]
    fn test_cluster_metrics_default() {
        let metrics = ClusterMetrics::default();
        assert_eq!(metrics.total_nodes, 0);
        assert_eq!(metrics.active_nodes, 0);
        assert!(metrics.leader_node.is_none());
        assert_eq!(metrics.total_jobs_processing, 0);
    }

    #[tokio::test]
    async fn test_select_best_node_single_node() {
        let cm = ClusterManager::new("test-node".to_string(), "localhost".to_string(), 6789, None);

        // Without pool, should return local metrics
        let result = cm.select_best_node().await.unwrap();
        assert!(result.is_some());
        let node = result.unwrap();
        assert_eq!(node.node_id, "test-node");
    }

    #[tokio::test]
    async fn test_get_sticky_node_consistency() {
        let cm = ClusterManager::new("test-node".to_string(), "localhost".to_string(), 6789, None);

        // Same session key should always return same node
        let node1 = cm.get_sticky_node("user-123").await.unwrap();
        let node2 = cm.get_sticky_node("user-123").await.unwrap();

        assert!(node1.is_some());
        assert!(node2.is_some());
        assert_eq!(node1.unwrap().node_id, node2.unwrap().node_id);
    }

    #[tokio::test]
    async fn test_cluster_metrics_single_node() {
        let cm = ClusterManager::new("test-node".to_string(), "localhost".to_string(), 6789, None);

        // Record some activity
        cm.record_job_start();
        cm.record_job_complete(50);
        cm.record_job_start();
        cm.record_job_fail();

        let metrics = cm.cluster_metrics().await.unwrap();
        assert_eq!(metrics.total_nodes, 1);
        assert_eq!(metrics.active_nodes, 1);
        assert_eq!(metrics.leader_node, Some("test-node".to_string()));
        assert_eq!(metrics.total_jobs_completed, 1);
        assert_eq!(metrics.total_jobs_failed, 1);
    }

    #[tokio::test]
    async fn test_get_node_for_queue() {
        let cm = ClusterManager::new("test-node".to_string(), "localhost".to_string(), 6789, None);

        // Same queue should always route to same node
        let node1 = cm.get_node_for_queue("emails").await.unwrap();
        let node2 = cm.get_node_for_queue("emails").await.unwrap();

        assert!(node1.is_some());
        assert_eq!(node1.unwrap().node_id, node2.unwrap().node_id);
    }
}

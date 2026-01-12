//! Cluster management for High Availability
//!
//! Uses PostgreSQL advisory locks for leader election.
//! Only the leader runs background tasks (cron, cleanup, timeouts).

use sqlx::{PgPool, Row};
use std::sync::atomic::{AtomicBool, Ordering};

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

/// Cluster manager handles leader election and node registry
pub struct ClusterManager {
    pub node_id: String,
    pool: Option<PgPool>,
    is_leader: AtomicBool,
    host: String,
    port: i32,
    enabled: bool,
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
                    last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
                    started_at TIMESTAMPTZ DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
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

    /// Send heartbeat and update last_heartbeat
    pub async fn heartbeat(&self) -> Result<(), sqlx::Error> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(pool) = &self.pool {
            sqlx::query("UPDATE cluster_nodes SET last_heartbeat = NOW() WHERE node_id = $1")
                .bind(&self.node_id)
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
}

/// Generate a unique node ID using ULID (sortable, faster than UUID)
pub fn generate_node_id() -> String {
    std::env::var("NODE_ID")
        .unwrap_or_else(|_| format!("node-{}", &ulid::Ulid::new().to_string()[..8]))
}

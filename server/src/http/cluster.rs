//! Health check and cluster HTTP handlers.

use axum::{
    extract::{Path, State},
    response::Json,
};
use serde::{Deserialize, Serialize};

use crate::queue::{ClusterMetrics, LoadBalanceStrategy, NodeInfo, NodeMetrics};

use super::settings::get_start_time;
use super::types::{ApiResponse, AppState};

/// Health check response.
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub node_id: Option<String>,
    pub is_leader: bool,
    pub cluster_enabled: bool,
    pub postgres_connected: bool,
}

/// Health check endpoint.
pub async fn health_check(State(qm): State<AppState>) -> Json<ApiResponse<HealthResponse>> {
    let health = HealthResponse {
        status: "healthy",
        node_id: qm.node_id(),
        is_leader: qm.is_leader(),
        cluster_enabled: qm.is_cluster_enabled(),
        postgres_connected: qm.is_postgres_connected(),
    };
    ApiResponse::success(health)
}

/// List cluster nodes.
pub async fn cluster_nodes(State(qm): State<AppState>) -> Json<ApiResponse<Vec<NodeInfo>>> {
    if let Some(cluster) = qm.cluster() {
        match cluster.list_nodes().await {
            Ok(nodes) => ApiResponse::success(nodes),
            Err(e) => ApiResponse::error(format!("Failed to list nodes: {}", e)),
        }
    } else {
        // Not in cluster mode, return self as only node
        ApiResponse::success(vec![NodeInfo {
            node_id: "standalone".to_string(),
            host: "localhost".to_string(),
            port: std::env::var("HTTP_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(6790),
            is_leader: true,
            last_heartbeat: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            started_at: get_start_time()
                .map(|t| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64 - t.elapsed().as_millis() as i64)
                        .unwrap_or(0)
                })
                .unwrap_or(0),
        }])
    }
}

/// List cluster nodes with metrics.
pub async fn cluster_nodes_metrics(
    State(qm): State<AppState>,
) -> Json<ApiResponse<Vec<NodeMetrics>>> {
    if let Some(cluster) = qm.cluster() {
        match cluster.list_nodes_with_metrics().await {
            Ok(nodes) => ApiResponse::success(nodes),
            Err(e) => ApiResponse::error(format!("Failed to list nodes: {}", e)),
        }
    } else {
        // Return local metrics in single-node mode
        ApiResponse::success(vec![])
    }
}

/// Get cluster metrics.
pub async fn cluster_metrics(State(qm): State<AppState>) -> Json<ApiResponse<ClusterMetrics>> {
    if let Some(cluster) = qm.cluster() {
        match cluster.cluster_metrics().await {
            Ok(metrics) => ApiResponse::success(metrics),
            Err(e) => ApiResponse::error(format!("Failed to get cluster metrics: {}", e)),
        }
    } else {
        // Single node mode - return basic metrics
        ApiResponse::success(ClusterMetrics::default())
    }
}

/// Select best node for load balancing.
pub async fn cluster_best_node(
    State(qm): State<AppState>,
) -> Json<ApiResponse<Option<NodeMetrics>>> {
    if let Some(cluster) = qm.cluster() {
        match cluster.select_best_node().await {
            Ok(node) => ApiResponse::success(node),
            Err(e) => ApiResponse::error(format!("Failed to select best node: {}", e)),
        }
    } else {
        ApiResponse::success(None)
    }
}

/// Get sticky node for a key.
pub async fn cluster_sticky_node(
    State(qm): State<AppState>,
    Path(key): Path<String>,
) -> Json<ApiResponse<Option<NodeMetrics>>> {
    if let Some(cluster) = qm.cluster() {
        match cluster.get_sticky_node(&key).await {
            Ok(node) => ApiResponse::success(node),
            Err(e) => ApiResponse::error(format!("Failed to get sticky node: {}", e)),
        }
    } else {
        ApiResponse::success(None)
    }
}

/// Load balance strategy response.
#[derive(Serialize)]
pub(crate) struct LoadBalanceStrategyResponse {
    pub strategy: LoadBalanceStrategy,
}

/// Get load balance strategy.
pub async fn get_load_balance_strategy(
    State(qm): State<AppState>,
) -> Json<ApiResponse<LoadBalanceStrategyResponse>> {
    if let Some(cluster) = qm.cluster() {
        ApiResponse::success(LoadBalanceStrategyResponse {
            strategy: cluster.load_balance_strategy(),
        })
    } else {
        ApiResponse::success(LoadBalanceStrategyResponse {
            strategy: LoadBalanceStrategy::LeastConnections,
        })
    }
}

/// Set load balance strategy request.
#[derive(Deserialize)]
pub(crate) struct SetLoadBalanceStrategyRequest {
    pub strategy: LoadBalanceStrategy,
}

/// Set load balance strategy.
pub async fn set_load_balance_strategy(
    State(qm): State<AppState>,
    Json(req): Json<SetLoadBalanceStrategyRequest>,
) -> Json<ApiResponse<()>> {
    if let Some(cluster) = qm.cluster() {
        cluster.set_load_balance_strategy(req.strategy);
        ApiResponse::success(())
    } else {
        ApiResponse::error("Cluster mode not enabled")
    }
}

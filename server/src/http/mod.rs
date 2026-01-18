//! HTTP API module.
//!
//! Provides REST API endpoints for queue management, job operations,
//! metrics, WebSocket events, and cluster management.

mod cluster;
mod cron;
mod events;
mod jobs;
mod metrics;
mod queues;
mod settings;
mod types;
mod webhooks;
mod websocket;
mod workers;

use axum::{
    http::{header, Method},
    routing::{delete, get, post},
    Router,
};
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::dashboard;

pub use types::AppState;

/// Create CORS layer based on environment configuration.
/// Set CORS_ALLOW_ORIGIN env var for production (comma-separated list of origins).
/// If not set, allows all origins (development mode).
fn create_cors_layer() -> CorsLayer {
    let allowed_origins = std::env::var("CORS_ALLOW_ORIGIN").ok();

    let cors = match allowed_origins {
        Some(origins) if !origins.is_empty() && origins != "*" => {
            // Production: specific origins
            let origins: Vec<_> = origins
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(origins))
                .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
                .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION, header::ACCEPT])
        }
        _ => {
            // Development: allow all (with warning)
            if std::env::var("CORS_ALLOW_ORIGIN").is_err() {
                tracing::warn!(
                    "CORS_ALLOW_ORIGIN not set - allowing all origins. Set this in production!"
                );
            }
            CorsLayer::permissive()
        }
    };

    cors
}

/// Create the HTTP router with all API routes.
pub fn create_router(state: AppState) -> Router {
    let cors = create_cors_layer();

    // Initialize start time for uptime tracking
    settings::init_start_time();

    let api_routes = Router::new()
        // Queue operations
        .route("/queues", get(queues::list_queues))
        .route("/queues/{queue}/jobs", post(queues::push_job))
        .route("/queues/{queue}/jobs", get(queues::pull_jobs))
        .route("/queues/{queue}/pause", post(queues::pause_queue))
        .route("/queues/{queue}/resume", post(queues::resume_queue))
        .route("/queues/{queue}/dlq", get(queues::get_dlq))
        .route("/queues/{queue}/dlq/retry", post(queues::retry_dlq))
        .route("/queues/{queue}/rate-limit", post(queues::set_rate_limit))
        .route(
            "/queues/{queue}/rate-limit",
            delete(queues::clear_rate_limit),
        )
        .route("/queues/{queue}/concurrency", post(queues::set_concurrency))
        .route(
            "/queues/{queue}/concurrency",
            delete(queues::clear_concurrency),
        )
        // BullMQ Advanced queue operations
        .route("/queues/{queue}/drain", post(queues::drain_queue))
        .route(
            "/queues/{queue}/obliterate",
            delete(queues::obliterate_queue),
        )
        .route("/queues/{queue}/clean", post(queues::clean_queue))
        // Job operations
        .route("/jobs", get(jobs::list_jobs))
        .route("/jobs/{id}", get(jobs::get_job))
        .route("/jobs/{id}/ack", post(jobs::ack_job))
        .route("/jobs/{id}/fail", post(jobs::fail_job))
        .route("/jobs/{id}/cancel", post(jobs::cancel_job))
        .route("/jobs/{id}/progress", post(jobs::update_progress))
        .route("/jobs/{id}/progress", get(jobs::get_progress))
        .route("/jobs/{id}/result", get(jobs::get_result))
        // BullMQ Advanced job operations
        .route("/jobs/{id}/priority", post(jobs::change_priority))
        .route("/jobs/{id}/move-to-delayed", post(jobs::move_to_delayed))
        .route("/jobs/{id}/promote", post(jobs::promote_job))
        .route("/jobs/{id}/discard", post(jobs::discard_job))
        // Cron jobs
        .route("/crons", get(cron::list_crons))
        .route("/crons/{name}", post(cron::create_cron))
        .route("/crons/{name}", delete(cron::delete_cron))
        // Stats & Metrics
        .route("/stats", get(metrics::get_stats))
        .route("/metrics", get(metrics::get_metrics))
        .route("/metrics/history", get(metrics::get_metrics_history))
        .route("/metrics/prometheus", get(metrics::get_prometheus_metrics))
        // SSE Events
        .route("/events", get(events::sse_events))
        .route("/events/{queue}", get(events::sse_queue_events))
        // WebSocket Events
        .route("/ws", get(websocket::ws_handler))
        .route("/ws/{queue}", get(websocket::ws_queue_handler))
        // Dashboard WebSocket (real-time stats/metrics)
        .route("/ws/dashboard", get(websocket::ws_dashboard_handler))
        // Workers
        .route("/workers", get(workers::list_workers))
        .route("/workers/{id}/heartbeat", post(workers::worker_heartbeat))
        // Webhooks
        .route("/webhooks", get(webhooks::list_webhooks))
        .route("/webhooks", post(webhooks::create_webhook))
        .route("/webhooks/{id}", delete(webhooks::delete_webhook))
        .route(
            "/webhooks/incoming/{queue}",
            post(webhooks::incoming_webhook),
        )
        // Server management
        .route("/settings", get(settings::get_settings))
        .route("/settings/test-db", post(settings::test_db_connection))
        .route("/settings/database", post(settings::save_db_settings))
        .route("/settings/auth", post(settings::save_auth_settings))
        .route(
            "/settings/queue-defaults",
            post(settings::save_queue_defaults),
        )
        .route("/settings/cleanup", post(settings::save_cleanup_settings))
        .route("/settings/cleanup/run", post(settings::run_cleanup_now))
        .route("/server/shutdown", post(settings::shutdown_server))
        .route("/server/restart", post(settings::restart_server))
        .route("/server/reset", post(settings::reset_server))
        .route("/server/clear-queues", post(settings::clear_all_queues))
        .route("/server/clear-dlq", post(settings::clear_all_dlq))
        .route(
            "/server/clear-completed",
            post(settings::clear_completed_jobs),
        )
        .route("/server/reset-metrics", post(settings::reset_metrics))
        // System metrics
        .route("/system/metrics", get(settings::get_system_metrics))
        // Health & Cluster
        .route("/health", get(cluster::health_check))
        .route("/cluster/nodes", get(cluster::cluster_nodes))
        .route(
            "/cluster/nodes/metrics",
            get(cluster::cluster_nodes_metrics),
        )
        .route("/cluster/metrics", get(cluster::cluster_metrics))
        .route("/cluster/best-node", get(cluster::cluster_best_node))
        .route(
            "/cluster/sticky-node/{key}",
            get(cluster::cluster_sticky_node),
        )
        .route(
            "/cluster/load-balance-strategy",
            get(cluster::get_load_balance_strategy),
        )
        .route(
            "/cluster/load-balance-strategy",
            post(cluster::set_load_balance_strategy),
        )
        .with_state(state);

    Router::new()
        .merge(dashboard::dashboard_routes())
        .merge(api_routes)
        .layer(cors)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::queue::QueueManager;

    /// Test create_cors_layer function with default settings
    #[test]
    fn test_create_cors_layer_default() {
        // Without CORS_ALLOW_ORIGIN env var, should create permissive layer
        std::env::remove_var("CORS_ALLOW_ORIGIN");
        let _ = create_cors_layer();
        // If it doesn't panic, the test passes
    }

    /// Test create_cors_layer function with specific origins
    #[test]
    fn test_create_cors_layer_with_origins() {
        std::env::set_var(
            "CORS_ALLOW_ORIGIN",
            "http://localhost:3000,http://example.com",
        );
        let _ = create_cors_layer();
        std::env::remove_var("CORS_ALLOW_ORIGIN");
    }

    /// Test create_cors_layer function with wildcard
    #[test]
    fn test_create_cors_layer_wildcard() {
        std::env::set_var("CORS_ALLOW_ORIGIN", "*");
        let _ = create_cors_layer();
        std::env::remove_var("CORS_ALLOW_ORIGIN");
    }

    /// Test AppState creation (type alias for Arc<QueueManager>)
    #[tokio::test]
    async fn test_app_state_creation() {
        let state: AppState = QueueManager::new(false);
        // AppState is Arc<QueueManager>, verify it can be cloned (Arc)
        let _cloned = state.clone();
    }

    /// Test router creation doesn't panic
    #[tokio::test]
    async fn test_create_router_no_panic() {
        let state: AppState = QueueManager::new(false);
        let _router = create_router(state);
        // If it doesn't panic, the test passes
    }
}

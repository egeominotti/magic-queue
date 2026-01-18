//! Server settings HTTP handlers.

use axum::{extract::State, response::Json};
use serde::{Deserialize, Serialize};

use super::types::{ApiResponse, AppState};

static START_TIME: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// Initialize start time (call once at server startup).
pub fn init_start_time() {
    START_TIME.get_or_init(std::time::Instant::now);
}

/// Get server uptime in seconds.
pub fn get_uptime_seconds() -> u64 {
    START_TIME.get().map(|t| t.elapsed().as_secs()).unwrap_or(0)
}

/// Get start time instant.
pub fn get_start_time() -> Option<&'static std::time::Instant> {
    START_TIME.get()
}

/// Server settings response.
#[derive(Serialize)]
pub struct ServerSettings {
    pub version: &'static str,
    pub tcp_port: u16,
    pub http_port: u16,
    pub database_connected: bool,
    pub database_url: Option<String>,
    pub auth_enabled: bool,
    pub auth_token_count: usize,
    pub uptime_seconds: u64,
}

/// Get server settings.
pub async fn get_settings(State(qm): State<AppState>) -> Json<ApiResponse<ServerSettings>> {
    let uptime = get_uptime_seconds();

    let db_url = std::env::var("DATABASE_URL").ok();
    let db_connected = qm.is_postgres_connected();

    let settings = ServerSettings {
        version: env!("CARGO_PKG_VERSION"),
        tcp_port: std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(6789),
        http_port: std::env::var("HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(6790),
        database_connected: db_connected,
        database_url: db_url.map(|u| {
            // Mask password in URL
            if let Some(at_pos) = u.find('@') {
                if let Some(colon_pos) = u[..at_pos].rfind(':') {
                    return format!("{}:****{}", &u[..colon_pos], &u[at_pos..]);
                }
            }
            u
        }),
        auth_enabled: !qm.verify_token(""),
        auth_token_count: qm.auth_token_count(),
        uptime_seconds: uptime,
    };
    ApiResponse::success(settings)
}

/// Shutdown server.
pub async fn shutdown_server() -> Json<ApiResponse<&'static str>> {
    // Spawn task to exit after response is sent
    tokio::spawn(async {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        std::process::exit(0);
    });
    ApiResponse::success("Server shutting down...")
}

/// Restart server.
pub async fn restart_server() -> Json<ApiResponse<&'static str>> {
    // Spawn task to exit with special code after response is sent
    // Exit code 100 signals restart request (can be handled by process manager/wrapper)
    tokio::spawn(async {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        std::process::exit(100);
    });
    ApiResponse::success("Server restarting...")
}

/// Reset server memory.
pub async fn reset_server(State(qm): State<AppState>) -> Json<ApiResponse<&'static str>> {
    qm.reset().await;
    ApiResponse::success("Server memory cleared")
}

/// Clear all queues.
pub async fn clear_all_queues(State(qm): State<AppState>) -> Json<ApiResponse<u64>> {
    let count = qm.clear_all_queues().await;
    ApiResponse::success(count)
}

/// Clear all DLQ.
pub async fn clear_all_dlq(State(qm): State<AppState>) -> Json<ApiResponse<u64>> {
    let count = qm.clear_all_dlq().await;
    ApiResponse::success(count)
}

/// Clear completed jobs.
pub async fn clear_completed_jobs(State(qm): State<AppState>) -> Json<ApiResponse<u64>> {
    let count = qm.clear_completed_jobs().await;
    ApiResponse::success(count)
}

/// Reset metrics.
pub async fn reset_metrics(State(qm): State<AppState>) -> Json<ApiResponse<&'static str>> {
    qm.reset_metrics().await;
    ApiResponse::success("Metrics reset")
}

/// Test database connection request.
#[derive(Deserialize)]
pub struct TestDbRequest {
    pub url: String,
}

/// Test database connection.
pub async fn test_db_connection(Json(req): Json<TestDbRequest>) -> Json<ApiResponse<&'static str>> {
    // Try to connect to the database
    match sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&req.url)
        .await
    {
        Ok(_) => ApiResponse::success("Connection successful"),
        Err(e) => ApiResponse::error(format!("Connection failed: {}", e)),
    }
}

/// Save database settings request.
#[derive(Deserialize)]
#[allow(dead_code)]
pub struct SaveDbRequest {
    pub url: String,
}

/// Save database settings.
pub async fn save_db_settings(Json(_req): Json<SaveDbRequest>) -> Json<ApiResponse<&'static str>> {
    // Note: Cannot change DATABASE_URL at runtime safely
    // This would require reconnecting all pool connections
    ApiResponse::error("Cannot change database URL at runtime. Set DATABASE_URL environment variable and restart the server.")
}

/// Save auth settings request.
#[derive(Deserialize)]
pub struct SaveAuthRequest {
    pub tokens: String,
}

/// Save auth settings.
pub async fn save_auth_settings(
    State(qm): State<AppState>,
    Json(req): Json<SaveAuthRequest>,
) -> Json<ApiResponse<&'static str>> {
    let tokens: Vec<String> = req
        .tokens
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    qm.set_auth_tokens(tokens);
    ApiResponse::success("Auth tokens updated")
}

/// Queue defaults request.
#[derive(Deserialize)]
pub struct QueueDefaultsRequest {
    pub default_timeout: Option<u64>,
    pub default_max_attempts: Option<u32>,
    pub default_backoff: Option<u64>,
    pub default_ttl: Option<u64>,
}

/// Save queue defaults.
pub async fn save_queue_defaults(
    State(qm): State<AppState>,
    Json(req): Json<QueueDefaultsRequest>,
) -> Json<ApiResponse<&'static str>> {
    qm.set_queue_defaults(
        req.default_timeout,
        req.default_max_attempts,
        req.default_backoff,
        req.default_ttl,
    );
    ApiResponse::success("Queue defaults updated")
}

/// Cleanup settings request.
#[derive(Deserialize)]
pub struct CleanupSettingsRequest {
    pub max_completed_jobs: Option<usize>,
    pub max_job_results: Option<usize>,
    pub cleanup_interval_secs: Option<u64>,
    pub metrics_history_size: Option<usize>,
}

/// Save cleanup settings.
pub async fn save_cleanup_settings(
    State(qm): State<AppState>,
    Json(req): Json<CleanupSettingsRequest>,
) -> Json<ApiResponse<&'static str>> {
    qm.set_cleanup_settings(
        req.max_completed_jobs,
        req.max_job_results,
        req.cleanup_interval_secs,
        req.metrics_history_size,
    );
    ApiResponse::success("Cleanup settings updated")
}

/// Run cleanup now.
pub async fn run_cleanup_now(State(qm): State<AppState>) -> Json<ApiResponse<&'static str>> {
    qm.run_cleanup();
    ApiResponse::success("Cleanup triggered")
}

/// System metrics response.
#[derive(Serialize)]
pub struct SystemMetrics {
    pub memory_used_mb: f64,
    pub memory_total_mb: f64,
    pub memory_percent: f64,
    pub cpu_percent: f64,
    pub tcp_connections: usize,
    pub uptime_seconds: u64,
    pub process_id: u32,
}

/// Get system metrics.
pub async fn get_system_metrics(State(qm): State<AppState>) -> Json<ApiResponse<SystemMetrics>> {
    let uptime = get_uptime_seconds();

    // Get memory info using sysinfo-like approach
    let (memory_used, memory_total) = get_memory_info();
    let tcp_connections = qm.connection_count();

    let metrics = SystemMetrics {
        memory_used_mb: memory_used,
        memory_total_mb: memory_total,
        memory_percent: if memory_total > 0.0 {
            (memory_used / memory_total) * 100.0
        } else {
            0.0
        },
        cpu_percent: 0.0, // CPU tracking requires background monitoring
        tcp_connections,
        uptime_seconds: uptime,
        process_id: std::process::id(),
    };
    ApiResponse::success(metrics)
}

fn get_memory_info() -> (f64, f64) {
    // Try to read from /proc/self/statm on Linux
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            let parts: Vec<&str> = statm.split_whitespace().collect();
            if parts.len() >= 2 {
                let page_size = 4096.0; // Typically 4KB pages
                let resident: f64 = parts[1].parse().unwrap_or(0.0) * page_size / 1024.0 / 1024.0;
                // Get total memory from /proc/meminfo
                if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
                    for line in meminfo.lines() {
                        if line.starts_with("MemTotal:") {
                            let total_kb: f64 = line
                                .split_whitespace()
                                .nth(1)
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            return (resident, total_kb / 1024.0);
                        }
                    }
                }
                return (resident, 0.0);
            }
        }
    }

    // macOS: use mach APIs or fallback
    #[cfg(target_os = "macos")]
    {
        // Simple fallback - return approximate values
        // In production, you'd use mach_task_basic_info
        (0.0, 0.0)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        (0.0, 0.0)
    }
}

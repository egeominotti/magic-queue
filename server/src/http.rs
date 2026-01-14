use std::convert::Infallible;
use std::sync::Arc;

use axum::response::sse::{Event, KeepAlive};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Json, Sse},
    routing::{delete, get, post},
    Router,
};
use futures::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

use crate::dashboard;
use crate::protocol::{
    CronJob, Job, JobBrowserItem, JobState, MetricsData, MetricsHistoryPoint, QueueInfo,
};
use crate::queue::{ClusterMetrics, LoadBalanceStrategy, NodeInfo, NodeMetrics, QueueManager};

type AppState = Arc<QueueManager>;

#[derive(Deserialize)]
pub struct PushRequest {
    pub data: Value,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub delay: Option<u64>,
    #[serde(default)]
    pub ttl: Option<u64>,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub backoff: Option<u64>,
    #[serde(default)]
    pub unique_key: Option<String>,
    #[serde(default)]
    pub depends_on: Option<Vec<u64>>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub lifo: bool,
    #[serde(default)]
    pub remove_on_complete: bool,
    #[serde(default)]
    pub remove_on_fail: bool,
    #[serde(default)]
    pub stall_timeout: Option<u64>,
    #[serde(default)]
    pub debounce_id: Option<String>,
    #[serde(default)]
    pub debounce_ttl: Option<u64>,
    #[serde(default)]
    pub job_id: Option<String>,
    #[serde(default)]
    pub keep_completed_age: Option<u64>,
    #[serde(default)]
    pub keep_completed_count: Option<usize>,
}

#[derive(Deserialize)]
pub struct AckRequest {
    #[serde(default)]
    pub result: Option<Value>,
}

#[derive(Deserialize)]
pub struct FailRequest {
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Deserialize)]
pub struct ProgressRequest {
    pub progress: u8,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Deserialize)]
pub struct CronRequest {
    pub queue: String,
    pub data: Value,
    pub schedule: String,
    #[serde(default)]
    pub priority: i32,
}

#[derive(Deserialize)]
pub struct RateLimitRequest {
    pub limit: u32,
}

#[derive(Deserialize)]
pub struct ConcurrencyRequest {
    pub limit: u32,
}

#[derive(Deserialize)]
pub struct PullQuery {
    #[serde(default = "default_count")]
    pub count: usize,
}

fn default_count() -> usize {
    1
}

#[derive(Deserialize)]
pub struct WsQuery {
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Serialize)]
pub struct ApiResponse<T> {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Json<Self> {
        Json(Self {
            ok: true,
            data: Some(data),
            error: None,
        })
    }

    fn error(msg: impl Into<String>) -> Json<Self> {
        Json(Self {
            ok: false,
            data: None,
            error: Some(msg.into()),
        })
    }
}

pub fn create_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api_routes = Router::new()
        // Queue operations
        .route("/queues", get(list_queues))
        .route("/queues/{queue}/jobs", post(push_job))
        .route("/queues/{queue}/jobs", get(pull_jobs))
        .route("/queues/{queue}/pause", post(pause_queue))
        .route("/queues/{queue}/resume", post(resume_queue))
        .route("/queues/{queue}/dlq", get(get_dlq))
        .route("/queues/{queue}/dlq/retry", post(retry_dlq))
        .route("/queues/{queue}/rate-limit", post(set_rate_limit))
        .route("/queues/{queue}/rate-limit", delete(clear_rate_limit))
        .route("/queues/{queue}/concurrency", post(set_concurrency))
        .route("/queues/{queue}/concurrency", delete(clear_concurrency))
        // BullMQ Advanced queue operations
        .route("/queues/{queue}/drain", post(drain_queue))
        .route("/queues/{queue}/obliterate", delete(obliterate_queue))
        .route("/queues/{queue}/clean", post(clean_queue))
        // Job operations
        .route("/jobs", get(list_jobs))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/ack", post(ack_job))
        .route("/jobs/{id}/fail", post(fail_job))
        .route("/jobs/{id}/cancel", post(cancel_job))
        .route("/jobs/{id}/progress", post(update_progress))
        .route("/jobs/{id}/progress", get(get_progress))
        .route("/jobs/{id}/result", get(get_result))
        // BullMQ Advanced job operations
        .route("/jobs/{id}/priority", post(change_priority))
        .route("/jobs/{id}/move-to-delayed", post(move_to_delayed))
        // Cron jobs
        .route("/crons", get(list_crons))
        .route("/crons/{name}", post(create_cron))
        .route("/crons/{name}", delete(delete_cron))
        // Stats & Metrics
        .route("/stats", get(get_stats))
        .route("/metrics", get(get_metrics))
        .route("/metrics/history", get(get_metrics_history))
        .route("/metrics/prometheus", get(get_prometheus_metrics))
        // SSE Events
        .route("/events", get(sse_events))
        .route("/events/{queue}", get(sse_queue_events))
        // WebSocket Events
        .route("/ws", get(ws_handler))
        .route("/ws/{queue}", get(ws_queue_handler))
        // Dashboard WebSocket (real-time stats/metrics)
        .route("/ws/dashboard", get(ws_dashboard_handler))
        // Workers
        .route("/workers", get(list_workers))
        .route("/workers/{id}/heartbeat", post(worker_heartbeat))
        // Webhooks
        .route("/webhooks", get(list_webhooks))
        .route("/webhooks", post(create_webhook))
        .route("/webhooks/{id}", delete(delete_webhook))
        .route("/webhooks/incoming/{queue}", post(incoming_webhook))
        // Server management
        .route("/settings", get(get_settings))
        .route("/settings/test-db", post(test_db_connection))
        .route("/settings/database", post(save_db_settings))
        .route("/settings/auth", post(save_auth_settings))
        .route("/settings/queue-defaults", post(save_queue_defaults))
        .route("/settings/cleanup", post(save_cleanup_settings))
        .route("/settings/cleanup/run", post(run_cleanup_now))
        .route("/server/shutdown", post(shutdown_server))
        .route("/server/restart", post(restart_server))
        .route("/server/reset", post(reset_server))
        .route("/server/clear-queues", post(clear_all_queues))
        .route("/server/clear-dlq", post(clear_all_dlq))
        .route("/server/clear-completed", post(clear_completed_jobs))
        .route("/server/reset-metrics", post(reset_metrics))
        // System metrics
        .route("/system/metrics", get(get_system_metrics))
        // Health & Cluster
        .route("/health", get(health_check))
        .route("/cluster/nodes", get(cluster_nodes))
        .route("/cluster/nodes/metrics", get(cluster_nodes_metrics))
        .route("/cluster/metrics", get(cluster_metrics))
        .route("/cluster/best-node", get(cluster_best_node))
        .route("/cluster/sticky-node/{key}", get(cluster_sticky_node))
        .route(
            "/cluster/load-balance-strategy",
            get(get_load_balance_strategy),
        )
        .route(
            "/cluster/load-balance-strategy",
            post(set_load_balance_strategy),
        )
        .with_state(state);

    Router::new()
        .merge(dashboard::dashboard_routes())
        .merge(api_routes)
        .layer(cors)
}

// === Queue Operations ===

async fn list_queues(State(qm): State<AppState>) -> Json<ApiResponse<Vec<QueueInfo>>> {
    let queues = qm.list_queues().await;
    ApiResponse::success(queues)
}

async fn push_job(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Json(req): Json<PushRequest>,
) -> Json<ApiResponse<Job>> {
    match qm
        .push(
            queue,
            req.data,
            req.priority,
            req.delay,
            req.ttl,
            req.timeout,
            req.max_attempts,
            req.backoff,
            req.unique_key,
            req.depends_on,
            req.tags,
            req.lifo,
            req.remove_on_complete,
            req.remove_on_fail,
            req.stall_timeout,
            req.debounce_id,
            req.debounce_ttl,
            req.job_id,
            req.keep_completed_age,
            req.keep_completed_count,
        )
        .await
    {
        Ok(job) => ApiResponse::success(job),
        Err(e) => ApiResponse::error(e),
    }
}

async fn pull_jobs(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Query(params): Query<PullQuery>,
) -> Json<ApiResponse<Vec<Job>>> {
    if params.count == 1 {
        let job = qm.pull(&queue).await;
        ApiResponse::success(vec![job])
    } else {
        let jobs = qm.pull_batch(&queue, params.count).await;
        ApiResponse::success(jobs)
    }
}

async fn pause_queue(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
) -> Json<ApiResponse<()>> {
    qm.pause(&queue).await;
    ApiResponse::success(())
}

async fn resume_queue(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
) -> Json<ApiResponse<()>> {
    qm.resume(&queue).await;
    ApiResponse::success(())
}

async fn get_dlq(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Query(params): Query<PullQuery>,
) -> Json<ApiResponse<Vec<Job>>> {
    let jobs = qm.get_dlq(&queue, Some(params.count)).await;
    ApiResponse::success(jobs)
}

async fn retry_dlq(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
) -> Json<ApiResponse<usize>> {
    let count = qm.retry_dlq(&queue, None).await;
    ApiResponse::success(count)
}

async fn set_rate_limit(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Json(req): Json<RateLimitRequest>,
) -> Json<ApiResponse<()>> {
    qm.set_rate_limit(queue, req.limit).await;
    ApiResponse::success(())
}

async fn clear_rate_limit(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
) -> Json<ApiResponse<()>> {
    qm.clear_rate_limit(&queue).await;
    ApiResponse::success(())
}

async fn set_concurrency(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Json(req): Json<ConcurrencyRequest>,
) -> Json<ApiResponse<()>> {
    qm.set_concurrency(queue, req.limit).await;
    ApiResponse::success(())
}

async fn clear_concurrency(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
) -> Json<ApiResponse<()>> {
    qm.clear_concurrency(&queue).await;
    ApiResponse::success(())
}

// === BullMQ Advanced Queue Operations ===

#[derive(Deserialize)]
pub struct CleanRequest {
    pub grace: u64,
    pub state: String,
    #[serde(default)]
    pub limit: Option<usize>,
}

async fn drain_queue(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
) -> Json<ApiResponse<usize>> {
    let count = qm.drain(&queue).await;
    ApiResponse::success(count)
}

async fn obliterate_queue(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
) -> Json<ApiResponse<usize>> {
    let count = qm.obliterate(&queue).await;
    ApiResponse::success(count)
}

async fn clean_queue(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Json(req): Json<CleanRequest>,
) -> Json<ApiResponse<usize>> {
    let state_enum = match req.state.to_lowercase().as_str() {
        "waiting" => JobState::Waiting,
        "delayed" => JobState::Delayed,
        "completed" => JobState::Completed,
        "failed" => JobState::Failed,
        _ => return ApiResponse::error("Invalid state. Use: waiting, delayed, completed, failed"),
    };
    let count = qm.clean(&queue, req.grace, state_enum, req.limit).await;
    ApiResponse::success(count)
}

// === Job Browser ===

#[derive(Deserialize)]
pub struct JobsQuery {
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default = "default_job_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

fn default_job_limit() -> usize {
    100
}

async fn list_jobs(
    State(qm): State<AppState>,
    Query(params): Query<JobsQuery>,
) -> Json<ApiResponse<Vec<JobBrowserItem>>> {
    let state_filter = params.state.as_deref().and_then(|s| match s {
        "waiting" => Some(JobState::Waiting),
        "delayed" => Some(JobState::Delayed),
        "active" => Some(JobState::Active),
        "completed" => Some(JobState::Completed),
        "failed" => Some(JobState::Failed),
        "waiting-children" | "waitingchildren" => Some(JobState::WaitingChildren),
        _ => None,
    });

    let jobs = qm.list_jobs(
        params.queue.as_deref(),
        state_filter,
        params.limit,
        params.offset,
    );
    ApiResponse::success(jobs)
}

#[derive(Serialize)]
pub struct JobDetailResponse {
    #[serde(flatten)]
    pub job: Option<Job>,
    pub state: JobState,
    pub result: Option<Value>,
}

async fn get_job(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
) -> Json<ApiResponse<JobDetailResponse>> {
    let (job, state) = qm.get_job(id);
    let result = qm.get_result(id).await;
    ApiResponse::success(JobDetailResponse { job, state, result })
}

// === Job Operations ===

async fn ack_job(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
    Json(req): Json<AckRequest>,
) -> Json<ApiResponse<()>> {
    match qm.ack(id, req.result).await {
        Ok(()) => ApiResponse::success(()),
        Err(e) => ApiResponse::error(e),
    }
}

async fn fail_job(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
    Json(req): Json<FailRequest>,
) -> Json<ApiResponse<()>> {
    match qm.fail(id, req.error).await {
        Ok(()) => ApiResponse::success(()),
        Err(e) => ApiResponse::error(e),
    }
}

async fn cancel_job(State(qm): State<AppState>, Path(id): Path<u64>) -> Json<ApiResponse<()>> {
    match qm.cancel(id).await {
        Ok(()) => ApiResponse::success(()),
        Err(e) => ApiResponse::error(e),
    }
}

async fn update_progress(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
    Json(req): Json<ProgressRequest>,
) -> Json<ApiResponse<()>> {
    match qm.update_progress(id, req.progress, req.message).await {
        Ok(()) => ApiResponse::success(()),
        Err(e) => ApiResponse::error(e),
    }
}

async fn get_progress(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
) -> Json<ApiResponse<(u8, Option<String>)>> {
    match qm.get_progress(id).await {
        Ok(progress) => ApiResponse::success(progress),
        Err(e) => ApiResponse::error(e),
    }
}

async fn get_result(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
) -> Json<ApiResponse<Option<Value>>> {
    let result = qm.get_result(id).await;
    ApiResponse::success(result)
}

// === BullMQ Advanced Job Operations ===

#[derive(Deserialize)]
pub struct ChangePriorityRequest {
    pub priority: i32,
}

#[derive(Deserialize)]
pub struct MoveToDelayedRequest {
    pub delay: u64,
}

async fn change_priority(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
    Json(req): Json<ChangePriorityRequest>,
) -> Json<ApiResponse<()>> {
    match qm.change_priority(id, req.priority).await {
        Ok(()) => ApiResponse::success(()),
        Err(e) => ApiResponse::error(e),
    }
}

async fn move_to_delayed(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
    Json(req): Json<MoveToDelayedRequest>,
) -> Json<ApiResponse<()>> {
    match qm.move_to_delayed(id, req.delay).await {
        Ok(()) => ApiResponse::success(()),
        Err(e) => ApiResponse::error(e),
    }
}

// === Cron Jobs ===

async fn list_crons(State(qm): State<AppState>) -> Json<ApiResponse<Vec<CronJob>>> {
    let crons = qm.list_crons().await;
    ApiResponse::success(crons)
}

async fn create_cron(
    State(qm): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<CronRequest>,
) -> Json<ApiResponse<()>> {
    match qm
        .add_cron(name, req.queue, req.data, req.schedule, req.priority)
        .await
    {
        Ok(()) => ApiResponse::success(()),
        Err(e) => ApiResponse::error(e),
    }
}

async fn delete_cron(
    State(qm): State<AppState>,
    Path(name): Path<String>,
) -> Json<ApiResponse<bool>> {
    let deleted = qm.delete_cron(&name).await;
    ApiResponse::success(deleted)
}

// === Stats & Metrics ===

#[derive(Serialize)]
pub struct StatsResponse {
    pub queued: usize,
    pub processing: usize,
    pub delayed: usize,
    pub dlq: usize,
}

async fn get_stats(State(qm): State<AppState>) -> Json<ApiResponse<StatsResponse>> {
    let (queued, processing, delayed, dlq) = qm.stats().await;
    ApiResponse::success(StatsResponse {
        queued,
        processing,
        delayed,
        dlq,
    })
}

async fn get_metrics(State(qm): State<AppState>) -> Json<ApiResponse<MetricsData>> {
    let metrics = qm.get_metrics().await;
    ApiResponse::success(metrics)
}

async fn get_metrics_history(
    State(qm): State<AppState>,
) -> Json<ApiResponse<Vec<MetricsHistoryPoint>>> {
    let history = qm.get_metrics_history();
    ApiResponse::success(history)
}

// === Prometheus Metrics ===

async fn get_prometheus_metrics(State(qm): State<AppState>) -> impl IntoResponse {
    let metrics = qm.get_metrics().await;
    let (queued, processing, delayed, dlq) = qm.stats().await;

    let mut output = String::with_capacity(2048);

    // Global metrics
    output.push_str("# HELP flashq_jobs_total Total number of jobs\n");
    output.push_str("# TYPE flashq_jobs_total counter\n");
    output.push_str(&format!(
        "flashq_jobs_pushed_total {}\n",
        metrics.total_pushed
    ));
    output.push_str(&format!(
        "flashq_jobs_completed_total {}\n",
        metrics.total_completed
    ));
    output.push_str(&format!(
        "flashq_jobs_failed_total {}\n",
        metrics.total_failed
    ));

    output.push_str("# HELP flashq_jobs_current Current number of jobs by state\n");
    output.push_str("# TYPE flashq_jobs_current gauge\n");
    output.push_str(&format!(
        "flashq_jobs_current{{state=\"queued\"}} {}\n",
        queued
    ));
    output.push_str(&format!(
        "flashq_jobs_current{{state=\"processing\"}} {}\n",
        processing
    ));
    output.push_str(&format!(
        "flashq_jobs_current{{state=\"delayed\"}} {}\n",
        delayed
    ));
    output.push_str(&format!("flashq_jobs_current{{state=\"dlq\"}} {}\n", dlq));

    output.push_str("# HELP flashq_throughput_per_second Jobs processed per second\n");
    output.push_str("# TYPE flashq_throughput_per_second gauge\n");
    output.push_str(&format!(
        "flashq_throughput_per_second {:.2}\n",
        metrics.jobs_per_second
    ));

    output.push_str("# HELP flashq_latency_ms Average job latency in milliseconds\n");
    output.push_str("# TYPE flashq_latency_ms gauge\n");
    output.push_str(&format!(
        "flashq_latency_ms {:.2}\n",
        metrics.avg_latency_ms
    ));

    // Per-queue metrics
    output.push_str("# HELP flashq_queue_jobs Queue job counts\n");
    output.push_str("# TYPE flashq_queue_jobs gauge\n");
    for q in &metrics.queues {
        // Sanitize queue name for Prometheus labels (escape backslashes and quotes)
        let safe_name = q.name.replace('\\', "\\\\").replace('"', "\\\"");
        output.push_str(&format!(
            "flashq_queue_jobs{{queue=\"{}\",state=\"pending\"}} {}\n",
            safe_name, q.pending
        ));
        output.push_str(&format!(
            "flashq_queue_jobs{{queue=\"{}\",state=\"processing\"}} {}\n",
            safe_name, q.processing
        ));
        output.push_str(&format!(
            "flashq_queue_jobs{{queue=\"{}\",state=\"dlq\"}} {}\n",
            safe_name, q.dlq
        ));
    }

    // Workers
    let workers = qm.list_workers().await;
    output.push_str("# HELP flashq_workers_active Number of active workers\n");
    output.push_str("# TYPE flashq_workers_active gauge\n");
    output.push_str(&format!("flashq_workers_active {}\n", workers.len()));

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

// === SSE Events ===

async fn sse_events(
    State(qm): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = qm.subscribe_events(None);
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(
        |result: Result<crate::protocol::JobEvent, _>| async move {
            result.ok().map(|event| {
                Ok(Event::default()
                    .event(&event.event_type)
                    .json_data(&event)
                    .unwrap_or_default())
            })
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn sse_queue_events(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = qm.subscribe_events(Some(queue.clone()));
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(
        move |result: Result<crate::protocol::JobEvent, _>| {
            let queue = queue.clone();
            async move {
                result.ok().and_then(|event| {
                    if event.queue == queue {
                        Some(Ok(Event::default()
                            .event(&event.event_type)
                            .json_data(&event)
                            .unwrap_or_default()))
                    } else {
                        None
                    }
                })
            }
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// === WebSocket Events ===

async fn ws_handler(
    State(qm): State<AppState>,
    Query(params): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // Validate token if authentication is enabled
    let token = params.token.as_deref().unwrap_or("");
    if !qm.verify_token(token) {
        return (StatusCode::UNAUTHORIZED, "Invalid or missing token").into_response();
    }

    ws.on_upgrade(move |socket| handle_websocket(socket, qm, None))
}

async fn ws_queue_handler(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Query(params): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // Validate token if authentication is enabled
    let token = params.token.as_deref().unwrap_or("");
    if !qm.verify_token(token) {
        return (StatusCode::UNAUTHORIZED, "Invalid or missing token").into_response();
    }

    ws.on_upgrade(move |socket| handle_websocket(socket, qm, Some(queue)))
}

async fn handle_websocket(
    mut socket: WebSocket,
    qm: Arc<QueueManager>,
    queue_filter: Option<String>,
) {
    let mut rx = qm.subscribe_events(queue_filter.clone());

    loop {
        tokio::select! {
            // Receive events from broadcast channel and send to WebSocket
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        // Filter by queue if specified
                        if let Some(ref filter) = queue_filter {
                            if event.queue != *filter {
                                continue;
                            }
                        }

                        // Serialize and send event
                        if let Ok(json) = serde_json::to_string(&event) {
                            if socket.send(Message::Text(json.into())).await.is_err() {
                                break; // Client disconnected
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Missed some messages, log and continue
                        eprintln!("WebSocket client lagged behind by {} messages", n);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break; // Channel closed
                    }
                }
            }

            // Handle incoming WebSocket messages (ping/pong, close)
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        // Pong received, connection is alive
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break; // Client closed connection
                    }
                    Some(Ok(Message::Text(_))) | Some(Ok(Message::Binary(_))) => {
                        // Ignore client messages (this is a push-only WebSocket)
                    }
                    Some(Err(_)) => {
                        break; // Error reading from socket
                    }
                }
            }
        }
    }
}

// === Dashboard WebSocket (Real-time) ===

#[derive(Serialize)]
struct DashboardUpdate {
    stats: StatsResponse,
    metrics: MetricsData,
    queues: Vec<QueueInfo>,
    workers: Vec<crate::protocol::WorkerInfo>,
    metrics_history: Vec<MetricsHistoryPoint>,
    timestamp: u64,
}

async fn ws_dashboard_handler(
    State(qm): State<AppState>,
    Query(params): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let token = params.token.as_deref().unwrap_or("");
    if !qm.verify_token(token) {
        return (StatusCode::UNAUTHORIZED, "Invalid or missing token").into_response();
    }

    ws.on_upgrade(move |socket| handle_dashboard_websocket(socket, qm))
}

async fn handle_dashboard_websocket(mut socket: WebSocket, qm: Arc<QueueManager>) {
    // Send updates every second
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Collect all dashboard data
                let (queued, processing, delayed, dlq) = qm.stats().await;
                let metrics = qm.get_metrics().await;
                let queues = qm.list_queues().await;
                let workers = qm.list_workers().await;
                let metrics_history = qm.get_metrics_history();

                let update = DashboardUpdate {
                    stats: StatsResponse { queued, processing, delayed, dlq },
                    metrics,
                    queues,
                    workers,
                    metrics_history,
                    timestamp: crate::queue::QueueManager::now_ms(),
                };

                if let Ok(json) = serde_json::to_string(&update) {
                    if socket.send(Message::Text(json.into())).await.is_err() {
                        break; // Client disconnected
                    }
                }
            }

            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Text(_))) | Some(Ok(Message::Binary(_))) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }
}

// === Workers ===

#[derive(Deserialize)]
pub struct WorkerHeartbeatRequest {
    pub queues: Vec<String>,
    #[serde(default)]
    pub concurrency: u32,
    #[serde(default)]
    pub jobs_processed: u64,
}

async fn list_workers(
    State(qm): State<AppState>,
) -> Json<ApiResponse<Vec<crate::protocol::WorkerInfo>>> {
    let workers = qm.list_workers().await;
    ApiResponse::success(workers)
}

async fn worker_heartbeat(
    State(qm): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<WorkerHeartbeatRequest>,
) -> Json<ApiResponse<()>> {
    qm.worker_heartbeat(id, req.queues, req.concurrency, req.jobs_processed)
        .await;
    ApiResponse::success(())
}

// === Webhooks ===

#[derive(Deserialize)]
pub struct CreateWebhookRequest {
    pub url: String,
    pub events: Vec<String>,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
}

async fn list_webhooks(
    State(qm): State<AppState>,
) -> Json<ApiResponse<Vec<crate::protocol::WebhookConfig>>> {
    let webhooks = qm.list_webhooks().await;
    ApiResponse::success(webhooks)
}

async fn create_webhook(
    State(qm): State<AppState>,
    Json(req): Json<CreateWebhookRequest>,
) -> Json<ApiResponse<String>> {
    let id = qm
        .add_webhook(req.url, req.events, req.queue, req.secret)
        .await;
    ApiResponse::success(id)
}

async fn delete_webhook(
    State(qm): State<AppState>,
    Path(id): Path<String>,
) -> Json<ApiResponse<bool>> {
    let deleted = qm.delete_webhook(&id).await;
    ApiResponse::success(deleted)
}

// === Incoming Webhooks ===

async fn incoming_webhook(
    State(qm): State<AppState>,
    Path(queue): Path<String>,
    Json(data): Json<Value>,
) -> Json<ApiResponse<Job>> {
    match qm
        .push(
            queue, data, 0, None, None, None, None, None, None, None, None, false, false, false,
            None, None, None, None, None, None,
        )
        .await
    {
        Ok(job) => ApiResponse::success(job),
        Err(e) => ApiResponse::error(e),
    }
}

// === Server Settings ===

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

static START_TIME: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

async fn get_settings(State(qm): State<AppState>) -> Json<ApiResponse<ServerSettings>> {
    let start = START_TIME.get_or_init(std::time::Instant::now);
    let uptime = start.elapsed().as_secs();

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

async fn shutdown_server() -> Json<ApiResponse<&'static str>> {
    // Spawn task to exit after response is sent
    tokio::spawn(async {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        std::process::exit(0);
    });
    ApiResponse::success("Server shutting down...")
}

async fn restart_server() -> Json<ApiResponse<&'static str>> {
    // Spawn task to exit with special code after response is sent
    // Exit code 100 signals restart request (can be handled by process manager/wrapper)
    tokio::spawn(async {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        std::process::exit(100);
    });
    ApiResponse::success("Server restarting...")
}

async fn reset_server(State(qm): State<AppState>) -> Json<ApiResponse<&'static str>> {
    qm.reset().await;
    ApiResponse::success("Server memory cleared")
}

async fn clear_all_queues(State(qm): State<AppState>) -> Json<ApiResponse<u64>> {
    let count = qm.clear_all_queues().await;
    ApiResponse::success(count)
}

async fn clear_all_dlq(State(qm): State<AppState>) -> Json<ApiResponse<u64>> {
    let count = qm.clear_all_dlq().await;
    ApiResponse::success(count)
}

async fn clear_completed_jobs(State(qm): State<AppState>) -> Json<ApiResponse<u64>> {
    let count = qm.clear_completed_jobs().await;
    ApiResponse::success(count)
}

async fn reset_metrics(State(qm): State<AppState>) -> Json<ApiResponse<&'static str>> {
    qm.reset_metrics().await;
    ApiResponse::success("Metrics reset")
}

// === Settings Endpoints ===

#[derive(Deserialize)]
pub struct TestDbRequest {
    pub url: String,
}

async fn test_db_connection(Json(req): Json<TestDbRequest>) -> Json<ApiResponse<&'static str>> {
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

#[derive(Deserialize)]
pub struct SaveDbRequest {
    pub url: String,
}

async fn save_db_settings(Json(_req): Json<SaveDbRequest>) -> Json<ApiResponse<&'static str>> {
    // Note: Cannot change DATABASE_URL at runtime safely
    // This would require reconnecting all pool connections
    ApiResponse::error("Cannot change database URL at runtime. Set DATABASE_URL environment variable and restart the server.")
}

#[derive(Deserialize)]
pub struct SaveAuthRequest {
    pub tokens: String,
}

async fn save_auth_settings(
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

#[derive(Deserialize)]
pub struct QueueDefaultsRequest {
    pub default_timeout: Option<u64>,
    pub default_max_attempts: Option<u32>,
    pub default_backoff: Option<u64>,
    pub default_ttl: Option<u64>,
}

async fn save_queue_defaults(
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

#[derive(Deserialize)]
pub struct CleanupSettingsRequest {
    pub max_completed_jobs: Option<usize>,
    pub max_job_results: Option<usize>,
    pub cleanup_interval_secs: Option<u64>,
    pub metrics_history_size: Option<usize>,
}

async fn save_cleanup_settings(
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

async fn run_cleanup_now(State(qm): State<AppState>) -> Json<ApiResponse<&'static str>> {
    qm.run_cleanup();
    ApiResponse::success("Cleanup triggered")
}

// === System Metrics ===

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

async fn get_system_metrics(State(qm): State<AppState>) -> Json<ApiResponse<SystemMetrics>> {
    let start = START_TIME.get_or_init(std::time::Instant::now);
    let uptime = start.elapsed().as_secs();

    // Get memory info using sysinfo-like approach
    // For now, use process memory from /proc or fallback values
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

// === Health & Cluster ===

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub node_id: Option<String>,
    pub is_leader: bool,
    pub cluster_enabled: bool,
    pub postgres_connected: bool,
}

async fn health_check(State(qm): State<AppState>) -> Json<ApiResponse<HealthResponse>> {
    let health = HealthResponse {
        status: "healthy",
        node_id: qm.node_id(),
        is_leader: qm.is_leader(),
        cluster_enabled: qm.is_cluster_enabled(),
        postgres_connected: qm.is_postgres_connected(),
    };
    ApiResponse::success(health)
}

async fn cluster_nodes(State(qm): State<AppState>) -> Json<ApiResponse<Vec<NodeInfo>>> {
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
            started_at: START_TIME
                .get()
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

async fn cluster_nodes_metrics(State(qm): State<AppState>) -> Json<ApiResponse<Vec<NodeMetrics>>> {
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

async fn cluster_metrics(State(qm): State<AppState>) -> Json<ApiResponse<ClusterMetrics>> {
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

async fn cluster_best_node(State(qm): State<AppState>) -> Json<ApiResponse<Option<NodeMetrics>>> {
    if let Some(cluster) = qm.cluster() {
        match cluster.select_best_node().await {
            Ok(node) => ApiResponse::success(node),
            Err(e) => ApiResponse::error(format!("Failed to select best node: {}", e)),
        }
    } else {
        ApiResponse::success(None)
    }
}

async fn cluster_sticky_node(
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

#[derive(Serialize)]
struct LoadBalanceStrategyResponse {
    strategy: LoadBalanceStrategy,
}

async fn get_load_balance_strategy(
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

#[derive(Deserialize)]
struct SetLoadBalanceStrategyRequest {
    strategy: LoadBalanceStrategy,
}

async fn set_load_balance_strategy(
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

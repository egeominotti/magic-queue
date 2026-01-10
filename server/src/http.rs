use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::{delete, get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower_http::cors::{Any, CorsLayer};

use crate::dashboard;
use crate::protocol::{CronJob, Job, MetricsData, QueueInfo};
use crate::queue::QueueManager;

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

fn default_count() -> usize { 1 }

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
        Json(Self { ok: true, data: Some(data), error: None })
    }

    fn error(msg: impl Into<String>) -> Json<Self> {
        Json(Self { ok: false, data: None, error: Some(msg.into()) })
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
        // Job operations
        .route("/jobs/{id}/ack", post(ack_job))
        .route("/jobs/{id}/fail", post(fail_job))
        .route("/jobs/{id}/cancel", post(cancel_job))
        .route("/jobs/{id}/progress", post(update_progress))
        .route("/jobs/{id}/progress", get(get_progress))
        .route("/jobs/{id}/result", get(get_result))
        // Cron jobs
        .route("/crons", get(list_crons))
        .route("/crons/{name}", post(create_cron))
        .route("/crons/{name}", delete(delete_cron))
        // Stats & Metrics
        .route("/stats", get(get_stats))
        .route("/metrics", get(get_metrics))
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
    match qm.push(
        queue, req.data, req.priority, req.delay, req.ttl, req.timeout,
        req.max_attempts, req.backoff, req.unique_key, req.depends_on,
    ).await {
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

async fn cancel_job(
    State(qm): State<AppState>,
    Path(id): Path<u64>,
) -> Json<ApiResponse<()>> {
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
    qm.add_cron(name, req.queue, req.data, req.schedule, req.priority).await;
    ApiResponse::success(())
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
    ApiResponse::success(StatsResponse { queued, processing, delayed, dlq })
}

async fn get_metrics(State(qm): State<AppState>) -> Json<ApiResponse<MetricsData>> {
    let metrics = qm.get_metrics().await;
    ApiResponse::success(metrics)
}

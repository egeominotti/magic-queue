use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

/// Request wrapper with optional request ID for multiplexing
#[derive(Debug, Deserialize)]
pub struct Request {
    /// Optional request ID for response matching (multiplexing)
    #[serde(default, rename = "reqId")]
    pub req_id: Option<String>,
    /// The actual command
    #[serde(flatten)]
    pub command: Command,
}

/// Response wrapper that includes the request ID if provided
#[derive(Debug, Serialize)]
pub struct ResponseWithId {
    /// Echo back the request ID for client matching
    #[serde(rename = "reqId", skip_serializing_if = "Option::is_none")]
    pub req_id: Option<String>,
    /// The actual response
    #[serde(flatten)]
    pub response: Response,
}

impl ResponseWithId {
    #[inline(always)]
    pub fn new(response: Response, req_id: Option<String>) -> Self {
        Self { req_id, response }
    }
}

/// Fast atomic ID generator
static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Set the ID counter to a specific value (for recovery from database)
pub fn set_id_counter(value: u64) {
    ID_COUNTER.store(value, Ordering::Relaxed);
}

/// Get the current ID counter value (for debugging)
#[allow(dead_code)]
pub fn get_id_counter() -> u64 {
    ID_COUNTER.load(Ordering::Relaxed)
}

/// Job state enum - similar to BullMQ states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Waiting,         // In queue, ready to be processed
    Delayed,         // In queue, but run_at is in the future
    Active,          // Currently being processed
    Completed,       // Successfully completed
    Failed,          // In DLQ after max_attempts
    WaitingChildren, // Waiting for dependencies to complete
    WaitingParent,   // Parent waiting for children to complete (Flows)
    Stalled,         // Job is stalled (no heartbeat)
    Unknown,         // Job not found or state cannot be determined
}

/// Job log entry for debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobLogEntry {
    pub timestamp: u64,
    pub message: String,
    pub level: String, // "info", "warn", "error", "debug"
}

/// Flow child definition
#[derive(Debug, Clone, Deserialize)]
pub struct FlowChild {
    pub queue: String,
    pub data: Value,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub delay: Option<u64>,
}

/// Flow result
#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub struct FlowResult {
    pub parent_id: u64,
    pub children_ids: Vec<u64>,
}

#[inline(always)]
pub fn next_id() -> u64 {
    ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "UPPERCASE")]
pub enum Command {
    // === Core Commands ===
    Push {
        queue: String,
        data: Value,
        #[serde(default)]
        priority: i32,
        #[serde(default)]
        delay: Option<u64>,
        #[serde(default)]
        ttl: Option<u64>, // Job expires after N ms
        #[serde(default)]
        timeout: Option<u64>, // Auto-fail after N ms in processing
        #[serde(default)]
        max_attempts: Option<u32>, // Max retries before DLQ
        #[serde(default)]
        backoff: Option<u64>, // Base backoff in ms (exponential)
        #[serde(default)]
        unique_key: Option<String>, // Deduplication key
        #[serde(default)]
        depends_on: Option<Vec<u64>>, // Job dependencies
        #[serde(default)]
        tags: Option<Vec<String>>, // Job tags for filtering
        #[serde(default)]
        lifo: bool, // LIFO mode: last in, first out
        #[serde(default)]
        remove_on_complete: bool, // Don't store in completed_jobs after ACK
        #[serde(default)]
        remove_on_fail: bool, // Don't store in DLQ after failure
        #[serde(default)]
        stall_timeout: Option<u64>, // Stall detection timeout in ms (default 30s)
        #[serde(default)]
        debounce_id: Option<String>, // Debounce identifier (prevents duplicates within ttl window)
        #[serde(default)]
        debounce_ttl: Option<u64>, // Debounce window in ms (default 5000)
        #[serde(default)]
        job_id: Option<String>, // Custom job ID for idempotency
        #[serde(default)]
        keep_completed_age: Option<u64>, // Keep completed job for N ms (retention policy)
        #[serde(default)]
        keep_completed_count: Option<usize>, // Keep in last N completed jobs (retention policy)
    },
    Pushb {
        queue: String,
        jobs: Vec<JobInput>,
    },
    Pull {
        queue: String,
        #[serde(default)]
        timeout: Option<u64>, // Optional timeout in ms (server-side)
    },
    Pullb {
        queue: String,
        count: usize,
        #[serde(default)]
        timeout: Option<u64>, // Optional timeout in ms (server-side)
    },
    Ack {
        id: u64,
        #[serde(default)]
        result: Option<Value>, // Store job result
    },
    Ackb {
        ids: Vec<u64>,
    },
    Fail {
        id: u64,
        error: Option<String>,
    },
    GetResult {
        id: u64,
    },
    GetJob {
        id: u64,
    },
    GetState {
        id: u64,
    },
    /// Wait for job to complete and return result (finished() promise)
    WaitJob {
        id: u64,
        #[serde(default)]
        timeout: Option<u64>, // Timeout in ms (default 30000)
    },
    /// Get job by custom ID
    GetJobByCustomId {
        job_id: String,
    },

    // === New Commands ===
    Cancel {
        id: u64,
    },
    Progress {
        id: u64,
        progress: u8, // 0-100
        message: Option<String>,
    },
    GetProgress {
        id: u64,
    },
    Dlq {
        queue: String,
        #[serde(default)]
        count: Option<usize>,
    },
    RetryDlq {
        queue: String,
        #[serde(default)]
        id: Option<u64>, // Retry specific job or all
    },
    Subscribe {
        queue: String,
        events: Vec<String>, // "completed", "failed", "progress"
    },
    Unsubscribe {
        queue: String,
    },
    Metrics,
    Stats,

    // === Cron Jobs / Repeatable Jobs ===
    Cron {
        name: String,
        queue: String,
        data: Value,
        #[serde(default)]
        schedule: Option<String>, // Cron expression (optional if repeat_every is set)
        #[serde(default)]
        repeat_every: Option<u64>, // Repeat every N ms (alternative to schedule)
        #[serde(default)]
        priority: i32,
        #[serde(default)]
        limit: Option<u64>, // Max executions (None = infinite)
    },
    CronDelete {
        name: String,
    },
    CronList,

    // === Rate Limiting ===
    RateLimit {
        queue: String,
        limit: u32, // Jobs per second
    },
    RateLimitClear {
        queue: String,
    },

    // === Queue Control ===
    Pause {
        queue: String,
    },
    Resume {
        queue: String,
    },
    SetConcurrency {
        queue: String,
        limit: u32, // Max concurrent jobs in processing
    },
    ClearConcurrency {
        queue: String,
    },
    ListQueues,

    // === Authentication ===
    Auth {
        token: String,
    },

    // === Job Logs ===
    Log {
        id: u64,
        message: String,
        #[serde(default = "default_log_level")]
        level: String,
    },
    GetLogs {
        id: u64,
    },

    // === Stalled Jobs ===
    Heartbeat {
        id: u64,
    },

    // === Flows (Parent-Child) ===
    Flow {
        queue: String,
        data: Value,
        children: Vec<FlowChild>,
        #[serde(default)]
        priority: i32,
    },
    GetChildren {
        parent_id: u64,
    },

    // === BullMQ Advanced Commands ===
    /// Get jobs filtered by queue and/or state with pagination
    GetJobs {
        #[serde(default)]
        queue: Option<String>,
        #[serde(default)]
        state: Option<String>, // "waiting", "delayed", "active", "completed", "failed"
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        offset: Option<usize>,
    },
    /// Clean jobs older than grace period by state
    Clean {
        queue: String,
        grace: u64,    // Keep jobs newer than this (ms)
        state: String, // "completed", "failed", "waiting", "delayed"
        #[serde(default)]
        limit: Option<usize>,
    },
    /// Drain all waiting jobs from a queue
    Drain {
        queue: String,
    },
    /// Remove ALL data for a queue (jobs, DLQ, cron, etc.)
    Obliterate {
        queue: String,
    },
    /// Change priority of a job
    ChangePriority {
        id: u64,
        priority: i32,
    },
    /// Move a processing job back to delayed state
    MoveToDelayed {
        id: u64,
        delay: u64,
    },
    /// Promote a delayed job to waiting (make it ready immediately)
    Promote {
        id: u64,
    },
    /// Update job data
    UpdateJob {
        id: u64,
        data: Value,
    },
    /// Discard a job (prevent further retries, move to DLQ)
    Discard {
        id: u64,
    },
    /// Check if a queue is paused
    IsPaused {
        queue: String,
    },
    /// Get total job count for a queue
    Count {
        queue: String,
    },
    /// Get job counts by state for a queue
    GetJobCounts {
        queue: String,
    },
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Deserialize)]
pub struct JobInput {
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
    pub job_id: Option<String>, // Custom job ID for idempotency
    #[serde(default)]
    pub keep_completed_age: Option<u64>, // Retention: keep for N ms after completion
    #[serde(default)]
    pub keep_completed_count: Option<usize>, // Retention: keep in last N completed
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: u64,
    pub queue: String,
    pub data: Value,
    pub priority: i32,
    pub created_at: u64,
    pub run_at: u64,
    #[serde(default)]
    pub started_at: u64, // When job started processing
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub max_attempts: u32, // 0 = infinite
    #[serde(default)]
    pub backoff: u64, // Base backoff ms
    #[serde(default)]
    pub ttl: u64, // 0 = no expiration
    #[serde(default)]
    pub timeout: u64, // 0 = no timeout
    #[serde(default)]
    pub unique_key: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<u64>,
    #[serde(default)]
    pub progress: u8,
    #[serde(default)]
    pub progress_msg: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>, // Job tags for filtering
    #[serde(default)]
    pub lifo: bool, // LIFO mode: last in, first out
    // === New fields for BullMQ-like features ===
    #[serde(default)]
    pub remove_on_complete: bool, // Don't store in completed_jobs after ACK
    #[serde(default)]
    pub remove_on_fail: bool, // Don't store in DLQ after failure
    #[serde(default)]
    pub last_heartbeat: u64, // Last heartbeat from worker (for stall detection)
    #[serde(default)]
    pub stall_timeout: u64, // Stall detection timeout in ms (0 = disabled, default 30s)
    #[serde(default)]
    pub stall_count: u32, // Number of times job was marked as stalled
    // === Flow (Parent-Child) fields ===
    #[serde(default)]
    pub parent_id: Option<u64>, // Parent job ID (for child jobs in flows)
    #[serde(default)]
    pub children_ids: Vec<u64>, // Child job IDs (for parent jobs in flows)
    #[serde(default)]
    pub children_completed: u32, // Number of children that completed
    // === Custom ID and Retention ===
    #[serde(default)]
    pub custom_id: Option<String>, // User-provided custom job ID
    #[serde(default)]
    pub keep_completed_age: u64, // Keep completed job for N ms (0 = use default)
    #[serde(default)]
    pub keep_completed_count: usize, // Keep in last N completed (0 = use default)
    #[serde(default)]
    pub completed_at: u64, // When job was completed (for retention)
}

/// Job with its current state (for browser/API)
#[derive(Debug, Clone, Serialize)]
pub struct JobBrowserItem {
    #[serde(flatten)]
    pub job: Job,
    pub state: JobState,
}

/// Historical metrics point for charts
#[derive(Debug, Clone, Serialize)]
pub struct MetricsHistoryPoint {
    pub timestamp: u64,
    pub queued: usize,
    pub processing: usize,
    pub completed: u64,
    pub failed: u64,
    pub throughput: f64,
    pub latency_ms: f64,
}

impl Job {
    #[inline(always)]
    pub fn is_ready(&self, now: u64) -> bool {
        self.run_at <= now
    }

    #[inline(always)]
    pub fn is_expired(&self, now: u64) -> bool {
        self.ttl > 0 && now > self.created_at + self.ttl
    }

    #[inline(always)]
    pub fn is_timed_out(&self, now: u64) -> bool {
        self.timeout > 0 && self.started_at > 0 && now > self.started_at + self.timeout
    }

    #[inline(always)]
    pub fn should_go_to_dlq(&self) -> bool {
        self.max_attempts > 0 && self.attempts >= self.max_attempts
    }

    #[inline(always)]
    pub fn next_backoff(&self) -> u64 {
        if self.backoff == 0 {
            return 0;
        }
        // Exponential backoff: base * 2^attempts
        self.backoff * (1 << self.attempts.min(10))
    }
}

impl Eq for Job {}

impl PartialEq for Job {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Ord for Job {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority = greater (popped first from max-heap)
        // Earlier run_at = greater (popped first for delayed jobs)
        // LIFO: higher ID = greater (newer jobs first)
        // FIFO: lower ID = greater (older jobs first)
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.run_at.cmp(&self.run_at))
            .then_with(|| {
                if self.lifo || other.lifo {
                    // LIFO: prefer higher ID (newer)
                    self.id.cmp(&other.id)
                } else {
                    // FIFO: prefer lower ID (older)
                    other.id.cmp(&self.id)
                }
            })
    }
}

impl PartialOrd for Job {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronJob {
    pub name: String,
    pub queue: String,
    pub data: Value,
    #[serde(default)]
    pub schedule: Option<String>, // Cron expression (optional if repeat_every is set)
    #[serde(default)]
    pub repeat_every: Option<u64>, // Repeat every N ms (alternative to schedule)
    pub priority: i32,
    pub next_run: u64,
    #[serde(default)]
    pub executions: u64, // Number of times this job has been executed
    #[serde(default)]
    pub limit: Option<u64>, // Max executions (None = infinite)
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsData {
    pub total_pushed: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub jobs_per_second: f64,
    pub avg_latency_ms: f64,
    pub queues: Vec<QueueMetrics>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueMetrics {
    pub name: String,
    pub pending: usize,
    pub processing: usize,
    pub dlq: usize,
    pub rate_limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProgressInfo {
    pub id: u64,
    pub progress: u8,
    pub message: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Response {
    Ok {
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<u64>,
    },
    Batch {
        ok: bool,
        ids: Vec<u64>,
    },
    Job {
        ok: bool,
        job: Job,
    },
    Jobs {
        ok: bool,
        jobs: Vec<Job>,
    },
    /// Response for pull when no job is available (timeout)
    NullJob {
        ok: bool,
        job: Option<Job>, // Always None, but serializes as { ok: true, job: null }
    },
    Stats {
        ok: bool,
        queued: usize,
        processing: usize,
        delayed: usize,
        dlq: usize,
    },
    Metrics {
        ok: bool,
        metrics: MetricsData,
    },
    Progress {
        ok: bool,
        progress: ProgressInfo,
    },
    CronList {
        ok: bool,
        crons: Vec<CronJob>,
    },
    Result {
        ok: bool,
        id: u64,
        result: Option<Value>,
    },
    Queues {
        ok: bool,
        queues: Vec<QueueInfo>,
    },
    JobWithState {
        ok: bool,
        job: Option<Job>,
        state: JobState,
    },
    State {
        ok: bool,
        id: u64,
        state: JobState,
    },
    Logs {
        ok: bool,
        id: u64,
        logs: Vec<JobLogEntry>,
    },
    Flow {
        ok: bool,
        parent_id: u64,
        children_ids: Vec<u64>,
    },
    Children {
        ok: bool,
        parent_id: u64,
        children: Vec<Job>,
        completed: u32,
        total: u32,
    },
    /// Response for GetJobs with pagination
    JobsWithTotal {
        ok: bool,
        jobs: Vec<JobBrowserItem>,
        total: usize,
    },
    /// Response for operations that return a count (Clean, Drain, Obliterate)
    Count {
        ok: bool,
        count: usize,
    },
    /// Response for isPaused
    Paused {
        ok: bool,
        paused: bool,
    },
    /// Response for getJobCounts
    JobCounts {
        ok: bool,
        waiting: usize,
        active: usize,
        delayed: usize,
        completed: usize,
        failed: usize,
    },
    /// Response for WaitJob (finished() promise)
    JobResult {
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Error {
        ok: bool,
        error: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueInfo {
    pub name: String,
    pub pending: usize,
    pub processing: usize,
    pub dlq: usize,
    pub paused: bool,
    pub rate_limit: Option<u32>,
    pub concurrency_limit: Option<u32>,
}

// === Worker Registration ===

#[derive(Debug, Clone, Serialize)]
pub struct WorkerInfo {
    pub id: String,
    pub queues: Vec<String>,
    pub concurrency: u32,
    pub last_heartbeat: u64,
    pub jobs_processed: u64,
}

// === Webhooks ===

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub id: String,
    pub url: String,
    pub events: Vec<String>, // "job.completed", "job.failed", "job.progress"
    pub queue: Option<String>, // Filter by queue, None = all queues
    pub secret: Option<String>, // HMAC signing secret
    pub created_at: u64,
}

// === Events (for SSE/WebSocket) ===

#[derive(Debug, Clone, Serialize)]
pub struct JobEvent {
    pub event_type: String, // "completed", "failed", "progress", "pushed"
    pub queue: String,
    pub job_id: u64,
    pub timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<u8>,
}

impl Response {
    #[inline(always)]
    pub fn ok() -> Self {
        Response::Ok { ok: true, id: None }
    }

    #[inline(always)]
    pub fn ok_with_id(id: u64) -> Self {
        Response::Ok {
            ok: true,
            id: Some(id),
        }
    }

    #[inline(always)]
    pub fn batch(ids: Vec<u64>) -> Self {
        Response::Batch { ok: true, ids }
    }

    #[inline(always)]
    pub fn job(job: Job) -> Self {
        Response::Job { ok: true, job }
    }

    #[inline(always)]
    pub fn jobs(jobs: Vec<Job>) -> Self {
        Response::Jobs { ok: true, jobs }
    }

    #[inline(always)]
    pub fn null_job() -> Self {
        Response::NullJob { ok: true, job: None }
    }

    #[inline(always)]
    pub fn stats(queued: usize, processing: usize, delayed: usize, dlq: usize) -> Self {
        Response::Stats {
            ok: true,
            queued,
            processing,
            delayed,
            dlq,
        }
    }

    #[inline(always)]
    pub fn metrics(metrics: MetricsData) -> Self {
        Response::Metrics { ok: true, metrics }
    }

    #[inline(always)]
    pub fn progress(id: u64, progress: u8, message: Option<String>) -> Self {
        Response::Progress {
            ok: true,
            progress: ProgressInfo {
                id,
                progress,
                message,
            },
        }
    }

    #[inline(always)]
    pub fn cron_list(crons: Vec<CronJob>) -> Self {
        Response::CronList { ok: true, crons }
    }

    #[inline(always)]
    pub fn result(id: u64, result: Option<Value>) -> Self {
        Response::Result {
            ok: true,
            id,
            result,
        }
    }

    #[inline(always)]
    pub fn queues(queues: Vec<QueueInfo>) -> Self {
        Response::Queues { ok: true, queues }
    }

    #[inline(always)]
    pub fn job_with_state(job: Option<Job>, state: JobState) -> Self {
        Response::JobWithState {
            ok: true,
            job,
            state,
        }
    }

    #[inline(always)]
    pub fn state(id: u64, state: JobState) -> Self {
        Response::State {
            ok: true,
            id,
            state,
        }
    }

    #[inline(always)]
    pub fn error(msg: impl Into<String>) -> Self {
        Response::Error {
            ok: false,
            error: msg.into(),
        }
    }

    #[inline(always)]
    pub fn logs(id: u64, logs: Vec<JobLogEntry>) -> Self {
        Response::Logs { ok: true, id, logs }
    }

    #[inline(always)]
    pub fn flow(parent_id: u64, children_ids: Vec<u64>) -> Self {
        Response::Flow {
            ok: true,
            parent_id,
            children_ids,
        }
    }

    #[inline(always)]
    pub fn children(parent_id: u64, children: Vec<Job>, completed: u32, total: u32) -> Self {
        Response::Children {
            ok: true,
            parent_id,
            children,
            completed,
            total,
        }
    }

    #[inline(always)]
    pub fn jobs_with_total(jobs: Vec<JobBrowserItem>, total: usize) -> Self {
        Response::JobsWithTotal {
            ok: true,
            jobs,
            total,
        }
    }

    #[inline(always)]
    pub fn count(count: usize) -> Self {
        Response::Count { ok: true, count }
    }

    #[inline(always)]
    pub fn paused(paused: bool) -> Self {
        Response::Paused { ok: true, paused }
    }

    #[inline(always)]
    pub fn job_counts(
        waiting: usize,
        active: usize,
        delayed: usize,
        completed: usize,
        failed: usize,
    ) -> Self {
        Response::JobCounts {
            ok: true,
            waiting,
            active,
            delayed,
            completed,
            failed,
        }
    }
}

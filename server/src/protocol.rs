use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

/// Fast atomic ID generator
static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

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
    Unknown,         // Job not found or state cannot be determined
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
    },
    Pushb {
        queue: String,
        jobs: Vec<JobInput>,
    },
    Pull {
        queue: String,
    },
    Pullb {
        queue: String,
        count: usize,
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

    // === Cron Jobs ===
    Cron {
        name: String,
        queue: String,
        data: Value,
        schedule: String, // Cron expression
        #[serde(default)]
        priority: i32,
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

#[derive(Debug, Clone, Serialize)]
pub struct CronJob {
    pub name: String,
    pub queue: String,
    pub data: Value,
    pub schedule: String,
    pub priority: i32,
    pub next_run: u64,
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
}

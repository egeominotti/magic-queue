use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

/// Fast atomic ID generator
static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

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
        ttl: Option<u64>,           // Job expires after N ms
        #[serde(default)]
        max_attempts: Option<u32>,  // Max retries before DLQ
        #[serde(default)]
        backoff: Option<u64>,       // Base backoff in ms (exponential)
        #[serde(default)]
        unique_key: Option<String>, // Deduplication key
        #[serde(default)]
        depends_on: Option<Vec<u64>>, // Job dependencies
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
    },
    Ackb {
        ids: Vec<u64>,
    },
    Fail {
        id: u64,
        error: Option<String>,
    },

    // === New Commands ===
    Cancel {
        id: u64,
    },
    Progress {
        id: u64,
        progress: u8,  // 0-100
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
        id: Option<u64>,  // Retry specific job or all
    },
    Subscribe {
        queue: String,
        events: Vec<String>,  // "completed", "failed", "progress"
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
        schedule: String,  // Cron expression
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
        limit: u32,       // Jobs per second
    },
    RateLimitClear {
        queue: String,
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
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub backoff: Option<u64>,
    #[serde(default)]
    pub unique_key: Option<String>,
    #[serde(default)]
    pub depends_on: Option<Vec<u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: u64,
    pub queue: String,
    pub data: Value,
    pub priority: i32,
    pub created_at: u64,
    pub run_at: u64,
    // New fields
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub max_attempts: u32,      // 0 = infinite
    #[serde(default)]
    pub backoff: u64,           // Base backoff ms
    #[serde(default)]
    pub ttl: u64,               // 0 = no expiration
    #[serde(default)]
    pub unique_key: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<u64>,
    #[serde(default)]
    pub progress: u8,
    #[serde(default)]
    pub progress_msg: Option<String>,
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
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| self.run_at.cmp(&other.run_at))
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
    Event {
        event: String,
        queue: String,
        job: Job,
    },
    Error {
        ok: bool,
        error: String,
    },
}

impl Response {
    #[inline(always)]
    pub fn ok() -> Self {
        Response::Ok { ok: true, id: None }
    }

    #[inline(always)]
    pub fn ok_with_id(id: u64) -> Self {
        Response::Ok { ok: true, id: Some(id) }
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
            progress: ProgressInfo { id, progress, message },
        }
    }

    #[inline(always)]
    pub fn cron_list(crons: Vec<CronJob>) -> Self {
        Response::CronList { ok: true, crons }
    }

    #[inline(always)]
    pub fn event(event: &str, queue: &str, job: Job) -> Self {
        Response::Event {
            event: event.to_string(),
            queue: queue.to_string(),
            job,
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

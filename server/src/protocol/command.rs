//! Protocol commands for flashQ.
//!
//! Contains the Command enum and related input types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::types::FlowChild;

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Serialize, Deserialize)]
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
    /// Purge all jobs from dead letter queue
    PurgeDlq {
        queue: String,
    },
    /// Get multiple jobs by IDs in a single call (batch status)
    GetJobsBatch {
        ids: Vec<u64>,
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

    // === Key-Value Storage (Redis-like) ===
    /// Set a key-value pair with optional TTL
    KvSet {
        key: String,
        value: Value,
        #[serde(default)]
        ttl: Option<u64>, // TTL in milliseconds
    },
    /// Get a value by key
    KvGet {
        key: String,
    },
    /// Delete a key
    KvDel {
        key: String,
    },
    /// Get multiple values by keys
    KvMget {
        keys: Vec<String>,
    },
    /// Set multiple key-value pairs
    KvMset {
        entries: Vec<KvEntry>,
    },
    /// Check if a key exists
    KvExists {
        key: String,
    },
    /// Set TTL on existing key
    KvExpire {
        key: String,
        ttl: u64, // TTL in milliseconds
    },
    /// Get remaining TTL for a key
    KvTtl {
        key: String,
    },
    /// List keys matching pattern (simple glob: * and ?)
    KvKeys {
        #[serde(default)]
        pattern: Option<String>,
    },
    /// Increment a numeric value
    KvIncr {
        key: String,
        #[serde(default = "default_incr_by")]
        by: i64,
    },
}

fn default_incr_by() -> i64 {
    1
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KvEntry {
    pub key: String,
    pub value: Value,
    #[serde(default)]
    pub ttl: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
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

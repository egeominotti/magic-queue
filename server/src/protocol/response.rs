//! Protocol responses for flashQ.
//!
//! Contains the Response enum and helper constructors.

use serde::Serialize;
use serde_json::Value;

use super::types::{
    CronJob, Job, JobBrowserItem, JobLogEntry, JobState, MetricsData, ProgressInfo, QueueInfo,
};

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
    /// Response for GetJobsBatch (batch status)
    JobsBatch {
        ok: bool,
        jobs: Vec<JobBrowserItem>,
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
    // === Key-Value Storage Responses ===
    /// Response for KvGet
    KvValue {
        ok: bool,
        value: Option<Value>,
    },
    /// Response for KvMget
    KvValues {
        ok: bool,
        values: Vec<Option<Value>>,
    },
    /// Response for KvKeys
    KvKeys {
        ok: bool,
        keys: Vec<String>,
    },
    /// Response for KvExists
    KvExists {
        ok: bool,
        exists: bool,
    },
    /// Response for KvTtl (-1 = no TTL, -2 = key doesn't exist)
    KvTtl {
        ok: bool,
        ttl: i64,
    },
    /// Response for KvIncr
    KvIncr {
        ok: bool,
        value: i64,
    },
    // === Pub/Sub Responses ===
    /// Response for Pub (number of receivers)
    PubCount {
        ok: bool,
        receivers: usize,
    },
    /// Incoming message on subscribed channel
    PubMessage {
        channel: String,
        message: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        pattern: Option<String>, // Set if received via pattern subscription
    },
    /// Response for Sub/Psub (list of subscribed channels/patterns)
    PubSubscribed {
        ok: bool,
        channels: Vec<String>,
    },
    /// Response for PubsubChannels
    PubChannels {
        ok: bool,
        channels: Vec<String>,
    },
    /// Response for PubsubNumsub
    PubNumsub {
        ok: bool,
        counts: Vec<(String, usize)>,
    },
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
        Response::NullJob {
            ok: true,
            job: None,
        }
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

    #[inline(always)]
    pub fn jobs_batch(jobs: Vec<JobBrowserItem>) -> Self {
        Response::JobsBatch { ok: true, jobs }
    }

    // === Key-Value Storage Response Helpers ===
    #[inline(always)]
    pub fn kv_value(value: Option<Value>) -> Self {
        Response::KvValue { ok: true, value }
    }

    #[inline(always)]
    pub fn kv_values(values: Vec<Option<Value>>) -> Self {
        Response::KvValues { ok: true, values }
    }

    #[inline(always)]
    pub fn kv_keys(keys: Vec<String>) -> Self {
        Response::KvKeys { ok: true, keys }
    }

    #[inline(always)]
    pub fn kv_exists(exists: bool) -> Self {
        Response::KvExists { ok: true, exists }
    }

    #[inline(always)]
    pub fn kv_ttl(ttl: i64) -> Self {
        Response::KvTtl { ok: true, ttl }
    }

    #[inline(always)]
    pub fn kv_incr(value: i64) -> Self {
        Response::KvIncr { ok: true, value }
    }

    // === Pub/Sub Response Helpers ===
    #[inline(always)]
    pub fn pub_count(receivers: usize) -> Self {
        Response::PubCount {
            ok: true,
            receivers,
        }
    }

    #[inline(always)]
    pub fn pub_message(channel: String, message: Value, pattern: Option<String>) -> Self {
        Response::PubMessage {
            channel,
            message,
            pattern,
        }
    }

    #[inline(always)]
    pub fn pub_subscribed(channels: Vec<String>) -> Self {
        Response::PubSubscribed { ok: true, channels }
    }

    #[inline(always)]
    pub fn pub_channels(channels: Vec<String>) -> Self {
        Response::PubChannels { ok: true, channels }
    }

    #[inline(always)]
    pub fn pub_numsub(counts: Vec<(String, usize)>) -> Self {
        Response::PubNumsub { ok: true, counts }
    }
}

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

/// Fast atomic ID generator - much faster than UUID
static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[inline(always)]
pub fn next_id() -> u64 {
    ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "UPPERCASE")]
pub enum Command {
    Push {
        queue: String,
        data: Value,
        #[serde(default)]
        priority: i32,
        #[serde(default)]
        delay: Option<u64>,
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
    Stats,
}

#[derive(Debug, Deserialize)]
pub struct JobInput {
    pub data: Value,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub delay: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: u64,
    pub queue: String,
    pub data: Value,
    pub priority: i32,
    pub created_at: u64,
    pub run_at: u64,
}

impl Job {
    #[inline(always)]
    pub fn is_ready(&self, now: u64) -> bool {
        self.run_at <= now
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
    pub fn stats(queued: usize, processing: usize, delayed: usize) -> Self {
        Response::Stats {
            ok: true,
            queued,
            processing,
            delayed,
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

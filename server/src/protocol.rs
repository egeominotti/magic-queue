use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    /// Pull batch - prende N job in una volta
    Pullb {
        queue: String,
        count: usize,
    },
    Ack {
        id: String,
    },
    /// Batch ACK - conferma multipli job
    Ackb {
        ids: Vec<String>,
    },
    Fail {
        id: String,
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
    pub id: String,
    pub queue: String,
    pub data: Value,
    pub priority: i32,
    pub created_at: u64,
    pub run_at: u64,
}

impl Job {
    #[inline]
    pub fn is_ready(&self, now: u64) -> bool {
        self.run_at <= now
    }
}

impl Eq for Job {}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| self.run_at.cmp(&other.run_at))
    }
}

impl PartialOrd for Job {
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
        id: Option<String>,
    },
    Batch {
        ok: bool,
        ids: Vec<String>,
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
    #[inline]
    pub fn ok() -> Self {
        Response::Ok { ok: true, id: None }
    }

    #[inline]
    pub fn ok_with_id(id: String) -> Self {
        Response::Ok { ok: true, id: Some(id) }
    }

    #[inline]
    pub fn batch(ids: Vec<String>) -> Self {
        Response::Batch { ok: true, ids }
    }

    #[inline]
    pub fn job(job: Job) -> Self {
        Response::Job { ok: true, job }
    }

    #[inline]
    pub fn jobs(jobs: Vec<Job>) -> Self {
        Response::Jobs { ok: true, jobs }
    }

    #[inline]
    pub fn stats(queued: usize, processing: usize, delayed: usize) -> Self {
        Response::Stats {
            ok: true,
            queued,
            processing,
            delayed,
        }
    }

    #[inline]
    pub fn error(msg: impl Into<String>) -> Self {
        Response::Error {
            ok: false,
            error: msg.into(),
        }
    }
}

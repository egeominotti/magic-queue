//! Protocol module for flashQ.
//!
//! Contains all protocol-related types, commands, responses, and serialization.
//!
//! # Binary Protocol (MessagePack)
//! Protocol detection: First 4 bytes are length prefix (big-endian u32)
//! If first byte is '{' (0x7B), it's JSON (text mode)
//! Otherwise, it's binary mode with length-prefixed MessagePack

mod builder;
mod command;
mod response;
mod types;

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

// Re-export all public types
pub use builder::JobBuilder;
pub use command::{Command, JobInput};
pub use response::Response;
pub use types::{
    CronJob, FlowChild, Job, JobBrowserItem, JobEvent, JobLogEntry, JobState, MetricsData,
    MetricsHistoryPoint, QueueInfo, QueueMetrics, WebhookConfig, WorkerInfo,
};
// FlowResult and ProgressInfo are only used internally in response.rs
#[allow(unused_imports)]
pub use types::{FlowResult, ProgressInfo};

// ============== Binary Protocol Functions ==============

/// Serialize Request to MessagePack bytes (with named fields for interoperability)
#[inline]
pub fn serialize_msgpack<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value).map_err(|e| format!("MessagePack serialize error: {}", e))
}

/// Deserialize Request from MessagePack bytes
#[inline]
pub fn deserialize_msgpack<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, String> {
    rmp_serde::from_slice(bytes).map_err(|e| format!("MessagePack deserialize error: {}", e))
}

/// Create a length-prefixed binary frame
#[inline]
pub fn create_binary_frame(data: &[u8]) -> Vec<u8> {
    let len = data.len() as u32;
    let mut frame = Vec::with_capacity(4 + data.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(data);
    frame
}

/// Check if the first byte indicates binary protocol (not '{')
#[inline]
#[allow(dead_code)]
pub fn is_binary_protocol(first_byte: u8) -> bool {
    first_byte != b'{'
}

// ============== Request/Response Wrappers ==============

/// Request wrapper with optional request ID for multiplexing
#[derive(Debug, Serialize, Deserialize)]
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

// ============== ID Generator ==============

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

#[inline(always)]
pub fn next_id() -> u64 {
    ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ============== Binary Protocol Tests ==============

    #[test]
    fn test_serialize_deserialize_msgpack() {
        let request = Request {
            req_id: Some("test-123".to_string()),
            command: Command::Stats,
        };

        let bytes = serialize_msgpack(&request).unwrap();
        assert!(!bytes.is_empty());

        let deserialized: Request = deserialize_msgpack(&bytes).unwrap();
        assert_eq!(deserialized.req_id, Some("test-123".to_string()));
    }

    #[test]
    fn test_create_binary_frame() {
        let data = b"hello";
        let frame = create_binary_frame(data);

        // Frame should be 4 bytes length + 5 bytes data
        assert_eq!(frame.len(), 9);

        // First 4 bytes should be length (big-endian)
        let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
        assert_eq!(len, 5);

        // Rest should be data
        assert_eq!(&frame[4..], data);
    }

    #[test]
    fn test_is_binary_protocol() {
        assert!(!is_binary_protocol(b'{')); // JSON starts with {
        assert!(is_binary_protocol(0x00)); // Binary
        assert!(is_binary_protocol(0x93)); // MessagePack array
    }

    #[test]
    fn test_deserialize_invalid_msgpack() {
        let invalid = vec![0xFF, 0xFF, 0xFF];
        let result: Result<Request, _> = deserialize_msgpack(&invalid);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("MessagePack deserialize error"));
    }

    // ============== ID Generator Tests ==============

    #[test]
    fn test_next_id_increments() {
        let id1 = next_id();
        let id2 = next_id();
        assert!(id2 > id1);
    }

    #[test]
    fn test_set_id_counter() {
        set_id_counter(1000);
        let id = next_id();
        assert_eq!(id, 1000);
    }

    // ============== Request/Response Wrapper Tests ==============

    #[test]
    fn test_response_with_id_new() {
        let response = Response::ok();
        let wrapped = ResponseWithId::new(response, Some("req-456".to_string()));
        assert_eq!(wrapped.req_id, Some("req-456".to_string()));
    }

    #[test]
    fn test_response_with_id_none() {
        let response = Response::ok();
        let wrapped = ResponseWithId::new(response, None);
        assert_eq!(wrapped.req_id, None);
    }

    // ============== Response Constructor Tests ==============

    #[test]
    fn test_response_ok() {
        let response = Response::ok();
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":true"));
    }

    #[test]
    fn test_response_ok_with_id() {
        let response = Response::ok_with_id(42);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("\"id\":42"));
    }

    #[test]
    fn test_response_error() {
        let response = Response::error("Something went wrong");
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":false"));
        assert!(json.contains("Something went wrong"));
    }

    #[test]
    fn test_response_batch() {
        let response = Response::batch(vec![1, 2, 3]);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("[1,2,3]"));
    }

    #[test]
    fn test_response_stats() {
        let response = Response::stats(10, 5, 3, 2);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"queued\":10"));
        assert!(json.contains("\"processing\":5"));
        assert!(json.contains("\"delayed\":3"));
        assert!(json.contains("\"dlq\":2"));
    }

    #[test]
    fn test_response_count() {
        let response = Response::count(42);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"count\":42"));
    }

    #[test]
    fn test_response_paused() {
        let response = Response::paused(true);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"paused\":true"));
    }

    #[test]
    fn test_response_job_counts() {
        let response = Response::job_counts(10, 5, 3, 100, 2);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"waiting\":10"));
        assert!(json.contains("\"active\":5"));
        assert!(json.contains("\"delayed\":3"));
        assert!(json.contains("\"completed\":100"));
        assert!(json.contains("\"failed\":2"));
    }

    #[test]
    fn test_response_state() {
        let response = Response::state(123, JobState::Active);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"id\":123"));
        assert!(json.contains("\"state\":\"active\""));
    }

    // ============== JobBuilder Tests ==============

    #[test]
    fn test_job_builder_basic() {
        let job = JobBuilder::new("test-queue", json!({"task": "process"})).build(1, 1000);

        assert_eq!(job.id, 1);
        assert_eq!(job.queue, "test-queue");
        assert_eq!(job.created_at, 1000);
        assert_eq!(job.run_at, 1000);
        assert_eq!(job.priority, 0);
    }

    #[test]
    fn test_job_builder_with_priority() {
        let job = JobBuilder::new("queue", json!({}))
            .priority(10)
            .build(1, 1000);

        assert_eq!(job.priority, 10);
    }

    #[test]
    fn test_job_builder_with_delay() {
        let job = JobBuilder::new("queue", json!({}))
            .delay(5000)
            .build(1, 1000);

        assert_eq!(job.run_at, 6000); // 1000 + 5000
    }

    #[test]
    fn test_job_builder_with_ttl() {
        let job = JobBuilder::new("queue", json!({}))
            .ttl(60000)
            .build(1, 1000);

        assert_eq!(job.ttl, 60000);
    }

    #[test]
    fn test_job_builder_with_max_attempts() {
        let job = JobBuilder::new("queue", json!({}))
            .max_attempts(3)
            .backoff(1000)
            .build(1, 1000);

        assert_eq!(job.max_attempts, 3);
        assert_eq!(job.backoff, 1000);
    }

    #[test]
    fn test_job_builder_with_unique_key() {
        let job = JobBuilder::new("queue", json!({}))
            .unique_key("order-123")
            .build(1, 1000);

        assert_eq!(job.unique_key, Some("order-123".to_string()));
    }

    #[test]
    fn test_job_builder_with_tags() {
        let job = JobBuilder::new("queue", json!({}))
            .tags(vec!["urgent".to_string(), "priority".to_string()])
            .build(1, 1000);

        assert_eq!(job.tags, vec!["urgent", "priority"]);
    }

    #[test]
    fn test_job_builder_lifo_mode() {
        let job = JobBuilder::new("queue", json!({}))
            .lifo(true)
            .build(1, 1000);

        assert!(job.lifo);
    }

    #[test]
    fn test_job_builder_remove_on_complete() {
        let job = JobBuilder::new("queue", json!({}))
            .remove_on_complete(true)
            .build(1, 1000);

        assert!(job.remove_on_complete);
    }

    #[test]
    fn test_job_builder_with_auto_id() {
        let job = JobBuilder::new("queue", json!({})).build_with_auto_id(1000);

        assert!(job.id > 0);
    }

    #[test]
    fn test_job_builder_chained() {
        let job = JobBuilder::new("my-queue", json!({"action": "send-email"}))
            .priority(5)
            .delay(10000)
            .ttl(300000)
            .timeout(60000)
            .max_attempts(3)
            .backoff(2000)
            .unique_key("email-user-123")
            .tags(vec!["email".to_string()])
            .lifo(false)
            .build(42, 5000);

        assert_eq!(job.id, 42);
        assert_eq!(job.queue, "my-queue");
        assert_eq!(job.priority, 5);
        assert_eq!(job.run_at, 15000);
        assert_eq!(job.ttl, 300000);
        assert_eq!(job.timeout, 60000);
        assert_eq!(job.max_attempts, 3);
        assert_eq!(job.backoff, 2000);
        assert_eq!(job.unique_key, Some("email-user-123".to_string()));
        assert_eq!(job.tags, vec!["email"]);
        assert!(!job.lifo);
    }
}

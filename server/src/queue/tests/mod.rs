//! Test suite for QueueManager - organized by functionality.
//!
//! Run all tests: `cargo test`
//! Run specific module: `cargo test queue::tests::core`

use super::*;
use serde_json::json;
use std::sync::Arc;

/// Create a new QueueManager for testing (in-memory, no persistence).
pub fn setup() -> Arc<QueueManager> {
    QueueManager::new(false)
}

// Core operations: push, pull, ack, fail, batch
mod core;

// Dead letter queue
mod dlq;

// Job management: unique keys, cancel, progress
mod job_management;

// Priority, delayed jobs, dependencies
mod priority;

// Queue control: pause/resume, rate limiting, concurrency
mod queue_control;

// Cron/repeatable jobs
mod cron;

// Stats and metrics
mod monitoring;

// Authentication
mod auth;

// Sharding
mod sharding;

// Edge cases and validation
mod edge_cases;

// Concurrent operations
mod concurrent;

// Protocol and Job struct methods
mod protocol;

// Job tags
mod tags;

// Worker registration
mod workers;

// Webhooks
mod webhooks;

// Event subscription
mod events;

// Cleanup and index consistency
mod cleanup;

// Background tasks
mod background;

// PostgreSQL integration (requires DATABASE_URL)
mod postgres;

// Cluster mode (requires CLUSTER_MODE + DATABASE_URL)
mod cluster;

// Advanced BullMQ-like operations: drain, obliterate, clean, etc.
mod advanced;

// Async operations: wait_for_job
mod async_ops;

// Job browser: list_jobs, get_jobs, get_jobs_batch
mod browser;

// Admin operations: reset, clear_all, settings
mod admin_ops;

// Job options: remove_on_complete/fail, debounce, retention, logs, heartbeat
mod job_options;

// Advanced cron: add_cron_with_repeat
mod cron_advanced;

// Remaining coverage: paused pull, get_result, subscribe, shutdown, manager info
mod remaining;

// Key-Value storage
mod kv;

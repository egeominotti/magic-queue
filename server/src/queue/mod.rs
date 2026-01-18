//! Queue module - job queue management with sharding and persistence.
//!
//! ## Module Organization (after refactoring)
//!
//! - `manager.rs` - Core QueueManager struct, constructors, shard helpers
//! - `types.rs` - IndexedPriorityQueue, RateLimiter, Shard, GlobalMetrics
//! - `postgres.rs` - PostgreSQL persistence layer
//! - `cluster.rs` - Clustering and leader election
//! - `background.rs` - Background tasks (cleanup, cron, timeout)
//!
//! ### Core operations (split from core.rs for maintainability)
//!
//! - `validation.rs` - Input validation and size limits
//! - `push.rs` - Push and push_batch operations
//! - `pull.rs` - Pull, pull_batch, and distributed pull operations
//! - `ack.rs` - Ack, ack_batch, fail, and get_result operations
//!
//! ### Manager modules (split from manager.rs for maintainability)
//!
//! - `persistence.rs` - PostgreSQL persistence methods (persist_*)
//! - `query.rs` - Job query operations (get_job, get_state, wait_for_job)
//! - `admin.rs` - Admin operations (workers, webhooks, settings, reset)
//!
//! ### Feature modules (split from features.rs for maintainability)
//!
//! - `dlq.rs` - Dead letter queue operations
//! - `rate_limit.rs` - Rate limiting and concurrency control
//! - `queue_control.rs` - Pause, resume, list queues
//! - `cron.rs` - Cron/repeatable job scheduling
//! - `job_ops.rs` - Job operations (cancel, progress, priority, etc.)
//! - `flows.rs` - Parent-child job workflows
//! - `monitoring.rs` - Metrics, stats, stalled job detection
//! - `logs.rs` - Job logging
//! - `browser.rs` - Job browser (list, filter, clean, drain, obliterate)

mod background;
pub mod cluster;
mod manager;
mod postgres;
mod types;

// Core operations (split from core.rs)
mod ack;
mod pull;
mod push;
mod validation;

// Manager modules (split from manager.rs)
mod admin;
mod persistence;
mod query;

// Feature modules (split from features.rs)
mod browser;
mod cron;
mod dlq;
mod flows;
mod job_ops;
mod kv;
mod logs;
mod monitoring;
mod pubsub;
mod queue_control;
mod rate_limit;

#[cfg(test)]
mod tests;

pub use cluster::{generate_node_id, ClusterMetrics, LoadBalanceStrategy, NodeInfo, NodeMetrics};
pub use manager::QueueManager;

//! Rate limiting and concurrency control operations.
//!
//! Token bucket rate limiter and concurrency limits per queue.

use super::manager::QueueManager;
use super::types::{intern, ConcurrencyLimiter, RateLimiter};

impl QueueManager {
    // === Rate Limiting ===

    /// Set a rate limit for a queue (jobs per second).
    pub async fn set_rate_limit(&self, queue: String, limit: u32) {
        let idx = Self::shard_index(&queue);
        let queue_arc = intern(&queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.rate_limiter = Some(RateLimiter::new(limit));
    }

    /// Clear the rate limit for a queue.
    pub async fn clear_rate_limit(&self, queue: &str) {
        let idx = Self::shard_index(queue);
        let queue_arc = intern(queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.rate_limiter = None;
    }

    // === Concurrency Limiting ===

    /// Set a concurrency limit for a queue (max parallel jobs in processing).
    pub async fn set_concurrency(&self, queue: String, limit: u32) {
        let idx = Self::shard_index(&queue);
        let queue_arc = intern(&queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.concurrency = Some(ConcurrencyLimiter::new(limit));
    }

    /// Clear the concurrency limit for a queue.
    pub async fn clear_concurrency(&self, queue: &str) {
        let idx = Self::shard_index(queue);
        let queue_arc = intern(queue);
        let mut shard = self.shards[idx].write();
        let state = shard.get_state(&queue_arc);
        state.concurrency = None;
    }
}

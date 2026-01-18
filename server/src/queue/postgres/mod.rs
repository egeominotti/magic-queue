//! PostgreSQL storage layer for flashQ persistence.
//!
//! This module replaces WAL with durable PostgreSQL storage.

mod advanced;
mod batch;
mod cluster_sync;
mod cron;
mod distributed;
mod jobs;
mod migration;
mod sequence;
mod snapshot;
mod types;
mod webhooks;

use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool};

use crate::protocol::{CronJob, Job, WebhookConfig};

pub use types::{ClusterSyncSender, CLUSTER_SYNC_CHANNEL};

/// PostgreSQL storage layer for flashQ persistence.
pub struct PostgresStorage {
    pool: PgPool,
    /// Sender for cluster sync events (for future use).
    #[allow(dead_code)]
    sync_tx: Option<ClusterSyncSender>,
}

impl PostgresStorage {
    /// Create a new PostgreSQL storage connection pool.
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .min_connections(5)
            .connect(database_url)
            .await?;

        Ok(Self {
            pool,
            sync_tx: None,
        })
    }

    /// Create storage with cluster sync channel.
    #[allow(dead_code)]
    pub async fn with_sync(
        database_url: &str,
        sync_tx: ClusterSyncSender,
    ) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .min_connections(5)
            .connect(database_url)
            .await?;

        Ok(Self {
            pool,
            sync_tx: Some(sync_tx),
        })
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the database URL for creating a listener connection.
    #[allow(dead_code)]
    pub fn database_url(&self) -> Option<String> {
        // The pool doesn't expose the URL, so we'll need to store it separately
        // For now, this is handled at the manager level
        None
    }

    /// Run database migrations.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        migration::migrate(&self.pool).await
    }

    // ============== Job Operations ==============

    /// Insert or update a job.
    pub async fn insert_job(&self, job: &Job, state: &str) -> Result<(), sqlx::Error> {
        jobs::insert_job(&self.pool, job, state).await
    }

    /// Update job state.
    #[allow(dead_code)]
    pub async fn update_job_state(&self, job_id: u64, state: &str) -> Result<(), sqlx::Error> {
        jobs::update_job_state(&self.pool, job_id, state).await
    }

    /// Acknowledge a job as completed.
    pub async fn ack_job(&self, job_id: u64, result: Option<Value>) -> Result<(), sqlx::Error> {
        jobs::ack_job(&self.pool, job_id, result).await
    }

    /// Move job to DLQ.
    pub async fn move_to_dlq(&self, job: &Job, error: Option<&str>) -> Result<(), sqlx::Error> {
        jobs::move_to_dlq(&self.pool, job, error).await
    }

    /// Mark job as failed and update for retry.
    pub async fn fail_job(
        &self,
        job_id: u64,
        new_run_at: u64,
        attempts: u32,
    ) -> Result<(), sqlx::Error> {
        jobs::fail_job(&self.pool, job_id, new_run_at, attempts).await
    }

    /// Cancel a job.
    pub async fn cancel_job(&self, job_id: u64) -> Result<(), sqlx::Error> {
        jobs::cancel_job(&self.pool, job_id).await
    }

    /// Load all pending jobs from PostgreSQL for recovery.
    pub async fn load_pending_jobs(&self) -> Result<Vec<(Job, String)>, sqlx::Error> {
        jobs::load_pending_jobs(&self.pool).await
    }

    /// Load DLQ jobs.
    pub async fn load_dlq_jobs(&self) -> Result<Vec<Job>, sqlx::Error> {
        jobs::load_dlq_jobs(&self.pool).await
    }

    /// Load a specific job by ID (for sync).
    pub async fn load_job_by_id(&self, job_id: u64) -> Result<Option<(Job, String)>, sqlx::Error> {
        jobs::load_job_by_id(&self.pool, job_id).await
    }

    /// Get the maximum job ID from the database (for ID recovery on startup).
    pub async fn get_max_job_id(&self) -> Result<u64, sqlx::Error> {
        jobs::get_max_job_id(&self.pool).await
    }

    // ============== Cron Jobs ==============

    /// Save a cron job.
    pub async fn save_cron(&self, cron_job: &CronJob) -> Result<(), sqlx::Error> {
        cron::save_cron(&self.pool, cron_job).await
    }

    /// Delete a cron job.
    pub async fn delete_cron(&self, name: &str) -> Result<bool, sqlx::Error> {
        cron::delete_cron(&self.pool, name).await
    }

    /// Load all cron jobs.
    pub async fn load_crons(&self) -> Result<Vec<CronJob>, sqlx::Error> {
        cron::load_crons(&self.pool).await
    }

    /// Update cron job next_run time.
    pub async fn update_cron_next_run(&self, name: &str, next_run: u64) -> Result<(), sqlx::Error> {
        cron::update_cron_next_run(&self.pool, name, next_run).await
    }

    // ============== Webhooks ==============

    /// Save a webhook.
    #[allow(dead_code)]
    pub async fn save_webhook(&self, webhook: &WebhookConfig) -> Result<(), sqlx::Error> {
        webhooks::save_webhook(&self.pool, webhook).await
    }

    /// Delete a webhook.
    #[allow(dead_code)]
    pub async fn delete_webhook(&self, id: &str) -> Result<bool, sqlx::Error> {
        webhooks::delete_webhook(&self.pool, id).await
    }

    /// Load all webhooks.
    pub async fn load_webhooks(&self) -> Result<Vec<WebhookConfig>, sqlx::Error> {
        webhooks::load_webhooks(&self.pool).await
    }

    // ============== Batch Operations ==============

    /// Insert multiple jobs in a batch.
    pub async fn insert_jobs_batch(&self, jobs: &[Job], state: &str) -> Result<(), sqlx::Error> {
        batch::insert_jobs_batch(&self.pool, jobs, state).await
    }

    /// Acknowledge multiple jobs in a batch.
    pub async fn ack_jobs_batch(&self, ids: &[u64]) -> Result<(), sqlx::Error> {
        batch::ack_jobs_batch(&self.pool, ids).await
    }

    // ============== Cluster Sync (LISTEN/NOTIFY) ==============

    /// Send a notification to other nodes about a new job.
    pub async fn notify_job_pushed(&self, job_id: u64, queue: &str, node_id: &str) {
        cluster_sync::notify_job_pushed(&self.pool, job_id, queue, node_id).await
    }

    /// Send a notification about multiple jobs pushed.
    pub async fn notify_jobs_pushed(&self, job_ids: &[u64], queue: &str, node_id: &str) {
        cluster_sync::notify_jobs_pushed(&self.pool, job_ids, queue, node_id).await
    }

    // ============== Sequence Operations ==============

    /// Get the next ID from the PostgreSQL sequence (for cluster-unique IDs).
    pub async fn next_sequence_id(&self) -> Result<u64, sqlx::Error> {
        sequence::next_sequence_id(&self.pool).await
    }

    /// Get the next N IDs from the PostgreSQL sequence (for batch operations).
    pub async fn next_sequence_ids(&self, count: i64) -> Result<Vec<u64>, sqlx::Error> {
        sequence::next_sequence_ids(&self.pool, count).await
    }

    /// Set the sequence to a specific value (for recovery).
    pub async fn set_sequence_value(&self, value: u64) -> Result<(), sqlx::Error> {
        sequence::set_sequence_value(&self.pool, value).await
    }

    // ============== Snapshot Operations ==============

    /// Snapshot all jobs to PostgreSQL (Redis-style persistence).
    pub async fn snapshot_jobs(&self, jobs: &[(Job, String)]) -> Result<(), sqlx::Error> {
        snapshot::snapshot_jobs(&self.pool, jobs).await
    }

    /// Snapshot DLQ jobs to PostgreSQL.
    pub async fn snapshot_dlq(
        &self,
        dlq_jobs: &[(Job, Option<String>)],
    ) -> Result<(), sqlx::Error> {
        snapshot::snapshot_dlq(&self.pool, dlq_jobs).await
    }

    // ============== Distributed Pull (SELECT FOR UPDATE SKIP LOCKED) ==============

    /// Pull a single job atomically using SELECT FOR UPDATE SKIP LOCKED.
    pub async fn pull_job_distributed(
        &self,
        queue: &str,
        now_ms: i64,
    ) -> Result<Option<Job>, sqlx::Error> {
        distributed::pull_job_distributed(&self.pool, queue, now_ms).await
    }

    /// Pull multiple jobs atomically using SELECT FOR UPDATE SKIP LOCKED.
    pub async fn pull_jobs_distributed_batch(
        &self,
        queue: &str,
        count: i32,
        now_ms: i64,
    ) -> Result<Vec<Job>, sqlx::Error> {
        distributed::pull_jobs_distributed_batch(&self.pool, queue, count, now_ms).await
    }

    /// Load jobs in a range by IDs (for batch sync).
    pub async fn load_jobs_by_queue_since(
        &self,
        queue: &str,
        min_id: u64,
        limit: i64,
    ) -> Result<Vec<(Job, String)>, sqlx::Error> {
        distributed::load_jobs_by_queue_since(&self.pool, queue, min_id, limit).await
    }

    // ============== BullMQ Advanced Operations ==============

    /// Drain all waiting jobs from a queue (delete from DB).
    pub async fn drain_queue(&self, queue: &str) -> Result<u64, sqlx::Error> {
        advanced::drain_queue(&self.pool, queue).await
    }

    /// Obliterate all data for a queue (delete everything).
    pub async fn obliterate_queue(&self, queue: &str) -> Result<u64, sqlx::Error> {
        advanced::obliterate_queue(&self.pool, queue).await
    }

    /// Change priority of a job.
    pub async fn change_priority(&self, job_id: u64, priority: i32) -> Result<(), sqlx::Error> {
        advanced::change_priority(&self.pool, job_id, priority).await
    }

    /// Move a job to delayed state.
    pub async fn move_to_delayed(&self, job_id: u64, run_at: u64) -> Result<(), sqlx::Error> {
        advanced::move_to_delayed(&self.pool, job_id, run_at).await
    }

    /// Clean jobs by age and state.
    pub async fn clean_jobs(
        &self,
        queue: &str,
        cutoff: u64,
        state: &str,
    ) -> Result<u64, sqlx::Error> {
        advanced::clean_jobs(&self.pool, queue, cutoff, state).await
    }

    /// Promote a delayed job to waiting.
    pub async fn promote_job(&self, job_id: u64, run_at: u64) -> Result<(), sqlx::Error> {
        advanced::promote_job(&self.pool, job_id, run_at).await
    }

    /// Update job data.
    pub async fn update_job_data(
        &self,
        job_id: u64,
        data: &serde_json::Value,
    ) -> Result<(), sqlx::Error> {
        advanced::update_job_data(&self.pool, job_id, data).await
    }

    /// Discard a job (move to DLQ).
    pub async fn discard_job(&self, job_id: u64) -> Result<(), sqlx::Error> {
        advanced::discard_job(&self.pool, job_id).await
    }

    /// Purge all jobs from DLQ for a queue.
    pub async fn purge_dlq(&self, queue: &str) -> Result<u64, sqlx::Error> {
        advanced::purge_dlq(&self.pool, queue).await
    }
}

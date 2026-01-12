use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

use crate::protocol::{CronJob, Job, WebhookConfig};

/// PostgreSQL storage layer for flashQ persistence.
/// Replaces WAL with durable PostgreSQL storage.
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    /// Create a new PostgreSQL storage connection pool.
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .min_connections(5)
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run database migrations.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        // Create tables if they don't exist
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id BIGINT PRIMARY KEY,
                queue VARCHAR(255) NOT NULL,
                data JSONB NOT NULL,
                priority INT NOT NULL DEFAULT 0,
                created_at BIGINT NOT NULL,
                run_at BIGINT NOT NULL,
                started_at BIGINT NOT NULL DEFAULT 0,
                attempts INT NOT NULL DEFAULT 0,
                max_attempts INT NOT NULL DEFAULT 0,
                backoff BIGINT NOT NULL DEFAULT 0,
                ttl BIGINT NOT NULL DEFAULT 0,
                timeout BIGINT NOT NULL DEFAULT 0,
                unique_key VARCHAR(255),
                depends_on BIGINT[] NOT NULL DEFAULT '{}',
                progress SMALLINT NOT NULL DEFAULT 0,
                progress_msg TEXT,
                tags TEXT[] NOT NULL DEFAULT '{}',
                state VARCHAR(32) NOT NULL DEFAULT 'waiting',
                lifo BOOLEAN NOT NULL DEFAULT FALSE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Add lifo column for existing databases
        sqlx::query(
            r#"
            ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lifo BOOLEAN NOT NULL DEFAULT FALSE
        "#,
        )
        .execute(&self.pool)
        .await
        .ok();

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_jobs_queue_state ON jobs(queue, state)
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(r#"
            CREATE INDEX IF NOT EXISTS idx_jobs_run_at ON jobs(run_at) WHERE state IN ('waiting', 'delayed')
        "#).execute(&self.pool).await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS job_results (
                job_id BIGINT PRIMARY KEY,
                result JSONB NOT NULL,
                completed_at BIGINT NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS dlq_jobs (
                id BIGSERIAL PRIMARY KEY,
                job_id BIGINT NOT NULL,
                queue VARCHAR(255) NOT NULL,
                data JSONB NOT NULL,
                error TEXT,
                failed_at BIGINT NOT NULL,
                attempts INT NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_dlq_queue ON dlq_jobs(queue)
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cron_jobs (
                name VARCHAR(255) PRIMARY KEY,
                queue VARCHAR(255) NOT NULL,
                data JSONB NOT NULL,
                schedule VARCHAR(255) NOT NULL,
                priority INT NOT NULL DEFAULT 0,
                next_run BIGINT NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS webhooks (
                id VARCHAR(255) PRIMARY KEY,
                url TEXT NOT NULL,
                events TEXT[] NOT NULL,
                queue VARCHAR(255),
                secret VARCHAR(255),
                created_at BIGINT NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS queue_config (
                queue VARCHAR(255) PRIMARY KEY,
                paused BOOLEAN NOT NULL DEFAULT FALSE,
                rate_limit INT,
                concurrency_limit INT
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ============== Job Operations ==============

    /// Insert or update a job.
    pub async fn insert_job(&self, job: &Job, state: &str) -> Result<(), sqlx::Error> {
        let depends_on: Vec<i64> = job.depends_on.iter().map(|&x| x as i64).collect();

        sqlx::query(r#"
            INSERT INTO jobs (id, queue, data, priority, created_at, run_at, started_at,
                attempts, max_attempts, backoff, ttl, timeout, unique_key, depends_on,
                progress, progress_msg, tags, state, lifo)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            ON CONFLICT (id) DO UPDATE SET
                state = EXCLUDED.state,
                started_at = EXCLUDED.started_at,
                attempts = EXCLUDED.attempts,
                progress = EXCLUDED.progress,
                progress_msg = EXCLUDED.progress_msg,
                run_at = EXCLUDED.run_at
        "#)
        .bind(job.id as i64)
        .bind(&job.queue)
        .bind(&job.data)
        .bind(job.priority)
        .bind(job.created_at as i64)
        .bind(job.run_at as i64)
        .bind(job.started_at as i64)
        .bind(job.attempts as i32)
        .bind(job.max_attempts as i32)
        .bind(job.backoff as i64)
        .bind(job.ttl as i64)
        .bind(job.timeout as i64)
        .bind(&job.unique_key)
        .bind(&depends_on)
        .bind(job.progress as i16)
        .bind(&job.progress_msg)
        .bind(&job.tags)
        .bind(state)
        .bind(job.lifo)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Update job state.
    #[allow(dead_code)]
    pub async fn update_job_state(&self, job_id: u64, state: &str) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE jobs SET state = $1 WHERE id = $2")
            .bind(state)
            .bind(job_id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Acknowledge a job as completed.
    pub async fn ack_job(&self, job_id: u64, result: Option<Value>) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("UPDATE jobs SET state = 'completed' WHERE id = $1")
            .bind(job_id as i64)
            .execute(&mut *tx)
            .await?;

        if let Some(res) = result {
            let now = super::types::now_ms();
            sqlx::query(r#"
                INSERT INTO job_results (job_id, result, completed_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (job_id) DO UPDATE SET result = EXCLUDED.result, completed_at = EXCLUDED.completed_at
            "#)
            .bind(job_id as i64)
            .bind(&res)
            .bind(now as i64)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Move job to DLQ.
    pub async fn move_to_dlq(&self, job: &Job, error: Option<&str>) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let now = super::types::now_ms();

        sqlx::query("UPDATE jobs SET state = 'failed' WHERE id = $1")
            .bind(job.id as i64)
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            r#"
            INSERT INTO dlq_jobs (job_id, queue, data, error, failed_at, attempts)
            VALUES ($1, $2, $3, $4, $5, $6)
        "#,
        )
        .bind(job.id as i64)
        .bind(&job.queue)
        .bind(&job.data)
        .bind(error)
        .bind(now as i64)
        .bind(job.attempts as i32)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Mark job as failed and update for retry.
    pub async fn fail_job(
        &self,
        job_id: u64,
        new_run_at: u64,
        attempts: u32,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE jobs SET state = 'waiting', run_at = $1, attempts = $2, started_at = 0 WHERE id = $3")
            .bind(new_run_at as i64)
            .bind(attempts as i32)
            .bind(job_id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Cancel a job.
    pub async fn cancel_job(&self, job_id: u64) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Load all pending jobs from PostgreSQL for recovery.
    pub async fn load_pending_jobs(&self) -> Result<Vec<(Job, String)>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT id, queue, data, priority, created_at, run_at, started_at, attempts,
                   max_attempts, backoff, ttl, timeout, unique_key, depends_on, progress,
                   progress_msg, tags, state, lifo
            FROM jobs
            WHERE state IN ('waiting', 'delayed', 'active', 'waiting_children')
        "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            let depends_on: Vec<i64> = row.get("depends_on");
            let job = Job {
                id: row.get::<i64, _>("id") as u64,
                queue: row.get("queue"),
                data: row.get("data"),
                priority: row.get("priority"),
                created_at: row.get::<i64, _>("created_at") as u64,
                run_at: row.get::<i64, _>("run_at") as u64,
                started_at: row.get::<i64, _>("started_at") as u64,
                attempts: row.get::<i32, _>("attempts") as u32,
                max_attempts: row.get::<i32, _>("max_attempts") as u32,
                backoff: row.get::<i64, _>("backoff") as u64,
                ttl: row.get::<i64, _>("ttl") as u64,
                timeout: row.get::<i64, _>("timeout") as u64,
                unique_key: row.get("unique_key"),
                depends_on: depends_on.into_iter().map(|x| x as u64).collect(),
                progress: row.get::<i16, _>("progress") as u8,
                progress_msg: row.get("progress_msg"),
                tags: row.get("tags"),
                lifo: row.get("lifo"),
            };
            let state: String = row.get("state");
            jobs.push((job, state));
        }

        Ok(jobs)
    }

    /// Load DLQ jobs.
    pub async fn load_dlq_jobs(&self) -> Result<Vec<Job>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT j.id, j.queue, j.data, j.priority, j.created_at, j.run_at, j.started_at,
                   j.attempts, j.max_attempts, j.backoff, j.ttl, j.timeout, j.unique_key,
                   j.depends_on, j.progress, j.progress_msg, j.tags, j.lifo
            FROM jobs j
            INNER JOIN dlq_jobs d ON j.id = d.job_id
        "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            let depends_on: Vec<i64> = row.get("depends_on");
            jobs.push(Job {
                id: row.get::<i64, _>("id") as u64,
                queue: row.get("queue"),
                data: row.get("data"),
                priority: row.get("priority"),
                created_at: row.get::<i64, _>("created_at") as u64,
                run_at: row.get::<i64, _>("run_at") as u64,
                started_at: row.get::<i64, _>("started_at") as u64,
                attempts: row.get::<i32, _>("attempts") as u32,
                max_attempts: row.get::<i32, _>("max_attempts") as u32,
                backoff: row.get::<i64, _>("backoff") as u64,
                ttl: row.get::<i64, _>("ttl") as u64,
                timeout: row.get::<i64, _>("timeout") as u64,
                unique_key: row.get("unique_key"),
                depends_on: depends_on.into_iter().map(|x| x as u64).collect(),
                progress: row.get::<i16, _>("progress") as u8,
                progress_msg: row.get("progress_msg"),
                tags: row.get("tags"),
                lifo: row.get("lifo"),
            });
        }

        Ok(jobs)
    }

    // ============== Cron Jobs ==============

    /// Save a cron job.
    pub async fn save_cron(&self, cron: &CronJob) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO cron_jobs (name, queue, data, schedule, priority, next_run)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (name) DO UPDATE SET
                queue = EXCLUDED.queue,
                data = EXCLUDED.data,
                schedule = EXCLUDED.schedule,
                priority = EXCLUDED.priority,
                next_run = EXCLUDED.next_run
        "#,
        )
        .bind(&cron.name)
        .bind(&cron.queue)
        .bind(&cron.data)
        .bind(&cron.schedule)
        .bind(cron.priority)
        .bind(cron.next_run as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Delete a cron job.
    pub async fn delete_cron(&self, name: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM cron_jobs WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Load all cron jobs.
    pub async fn load_crons(&self) -> Result<Vec<CronJob>, sqlx::Error> {
        let rows =
            sqlx::query("SELECT name, queue, data, schedule, priority, next_run FROM cron_jobs")
                .fetch_all(&self.pool)
                .await?;

        let mut crons = Vec::with_capacity(rows.len());
        for row in rows {
            crons.push(CronJob {
                name: row.get("name"),
                queue: row.get("queue"),
                data: row.get("data"),
                schedule: row.get("schedule"),
                priority: row.get("priority"),
                next_run: row.get::<i64, _>("next_run") as u64,
            });
        }

        Ok(crons)
    }

    /// Update cron job next_run time.
    pub async fn update_cron_next_run(&self, name: &str, next_run: u64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE cron_jobs SET next_run = $1 WHERE name = $2")
            .bind(next_run as i64)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ============== Webhooks ==============

    /// Save a webhook.
    #[allow(dead_code)]
    pub async fn save_webhook(&self, webhook: &WebhookConfig) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO webhooks (id, url, events, queue, secret, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                url = EXCLUDED.url,
                events = EXCLUDED.events,
                queue = EXCLUDED.queue,
                secret = EXCLUDED.secret
        "#,
        )
        .bind(&webhook.id)
        .bind(&webhook.url)
        .bind(&webhook.events)
        .bind(&webhook.queue)
        .bind(&webhook.secret)
        .bind(webhook.created_at as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Delete a webhook.
    #[allow(dead_code)]
    pub async fn delete_webhook(&self, id: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM webhooks WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Load all webhooks.
    pub async fn load_webhooks(&self) -> Result<Vec<WebhookConfig>, sqlx::Error> {
        let rows = sqlx::query("SELECT id, url, events, queue, secret, created_at FROM webhooks")
            .fetch_all(&self.pool)
            .await?;

        let mut webhooks = Vec::with_capacity(rows.len());
        for row in rows {
            webhooks.push(WebhookConfig {
                id: row.get("id"),
                url: row.get("url"),
                events: row.get("events"),
                queue: row.get("queue"),
                secret: row.get("secret"),
                created_at: row.get::<i64, _>("created_at") as u64,
            });
        }

        Ok(webhooks)
    }

    // ============== Batch Operations ==============

    /// Insert multiple jobs in a batch.
    pub async fn insert_jobs_batch(&self, jobs: &[Job], state: &str) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        for job in jobs {
            let depends_on: Vec<i64> = job.depends_on.iter().map(|&x| x as i64).collect();

            sqlx::query(r#"
                INSERT INTO jobs (id, queue, data, priority, created_at, run_at, started_at,
                    attempts, max_attempts, backoff, ttl, timeout, unique_key, depends_on,
                    progress, progress_msg, tags, state, lifo)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                ON CONFLICT (id) DO NOTHING
            "#)
            .bind(job.id as i64)
            .bind(&job.queue)
            .bind(&job.data)
            .bind(job.priority)
            .bind(job.created_at as i64)
            .bind(job.run_at as i64)
            .bind(job.started_at as i64)
            .bind(job.attempts as i32)
            .bind(job.max_attempts as i32)
            .bind(job.backoff as i64)
            .bind(job.ttl as i64)
            .bind(job.timeout as i64)
            .bind(&job.unique_key)
            .bind(&depends_on)
            .bind(job.progress as i16)
            .bind(&job.progress_msg)
            .bind(&job.tags)
            .bind(state)
            .bind(job.lifo)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Acknowledge multiple jobs in a batch.
    pub async fn ack_jobs_batch(&self, ids: &[u64]) -> Result<(), sqlx::Error> {
        if ids.is_empty() {
            return Ok(());
        }

        let ids_i64: Vec<i64> = ids.iter().map(|&x| x as i64).collect();
        sqlx::query("UPDATE jobs SET state = 'completed' WHERE id = ANY($1)")
            .bind(&ids_i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

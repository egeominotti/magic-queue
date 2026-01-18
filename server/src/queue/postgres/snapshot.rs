//! Snapshot operations for persisting in-memory state to PostgreSQL.

use sqlx::PgPool;

use crate::protocol::Job;
use crate::queue::types::now_ms;

/// Snapshot all jobs to PostgreSQL (Redis-style persistence).
/// This truncates the jobs table and inserts all current jobs in a single transaction.
pub async fn snapshot_jobs(pool: &PgPool, jobs: &[(Job, String)]) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Truncate jobs table (fast, minimal logging)
    sqlx::query("TRUNCATE TABLE jobs").execute(&mut *tx).await?;

    // Batch insert all jobs
    for job_chunk in jobs.chunks(1000) {
        for (job, state) in job_chunk {
            let depends_on: Vec<i64> = job.depends_on.iter().map(|&x| x as i64).collect();

            sqlx::query(
                r#"
                INSERT INTO jobs (id, queue, data, priority, created_at, run_at, started_at,
                    attempts, max_attempts, backoff, ttl, timeout, unique_key, depends_on,
                    progress, progress_msg, tags, state, lifo)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            "#,
            )
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
    }

    tx.commit().await?;
    Ok(())
}

/// Snapshot DLQ jobs to PostgreSQL.
pub async fn snapshot_dlq(
    pool: &PgPool,
    dlq_jobs: &[(Job, Option<String>)],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Truncate DLQ table
    sqlx::query("TRUNCATE TABLE dlq_jobs")
        .execute(&mut *tx)
        .await?;

    let now = now_ms();

    // Insert all DLQ jobs
    for (job, error) in dlq_jobs {
        sqlx::query(
            r#"
            INSERT INTO dlq_jobs (job_id, queue, data, error, failed_at, attempts)
            VALUES ($1, $2, $3, $4, $5, $6)
        "#,
        )
        .bind(job.id as i64)
        .bind(&job.queue)
        .bind(&job.data)
        .bind(error.as_deref())
        .bind(now as i64)
        .bind(job.attempts as i32)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

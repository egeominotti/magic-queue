//! Batch database operations.

use sqlx::PgPool;

use crate::protocol::Job;

/// Insert multiple jobs in a batch.
pub async fn insert_jobs_batch(
    pool: &PgPool,
    jobs: &[Job],
    state: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    for job in jobs {
        let depends_on: Vec<i64> = job.depends_on.iter().map(|&x| x as i64).collect();

        sqlx::query(
            r#"
            INSERT INTO jobs (id, queue, data, priority, created_at, run_at, started_at,
                attempts, max_attempts, backoff, ttl, timeout, unique_key, depends_on,
                progress, progress_msg, tags, state, lifo)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            ON CONFLICT (id) DO NOTHING
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

    tx.commit().await?;
    Ok(())
}

/// Acknowledge multiple jobs in a batch.
pub async fn ack_jobs_batch(pool: &PgPool, ids: &[u64]) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Ok(());
    }

    let ids_i64: Vec<i64> = ids.iter().map(|&x| x as i64).collect();
    sqlx::query("UPDATE jobs SET state = 'completed' WHERE id = ANY($1)")
        .bind(&ids_i64)
        .execute(pool)
        .await?;
    Ok(())
}

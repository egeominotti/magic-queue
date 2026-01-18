//! Job database operations.

use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::protocol::Job;
use crate::queue::types::now_ms;

/// Insert or update a job.
pub async fn insert_job(pool: &PgPool, job: &Job, state: &str) -> Result<(), sqlx::Error> {
    let depends_on: Vec<i64> = job.depends_on.iter().map(|&x| x as i64).collect();

    sqlx::query(
        r#"
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
    .execute(pool)
    .await?;

    Ok(())
}

/// Update job state.
#[allow(dead_code)]
pub async fn update_job_state(pool: &PgPool, job_id: u64, state: &str) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE jobs SET state = $1 WHERE id = $2")
        .bind(state)
        .bind(job_id as i64)
        .execute(pool)
        .await?;
    Ok(())
}

/// Acknowledge a job as completed.
pub async fn ack_job(pool: &PgPool, job_id: u64, result: Option<Value>) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    sqlx::query("UPDATE jobs SET state = 'completed' WHERE id = $1")
        .bind(job_id as i64)
        .execute(&mut *tx)
        .await?;

    if let Some(res) = result {
        let now = now_ms();
        sqlx::query(
            r#"
            INSERT INTO job_results (job_id, result, completed_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (job_id) DO UPDATE SET result = EXCLUDED.result, completed_at = EXCLUDED.completed_at
        "#,
        )
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
pub async fn move_to_dlq(pool: &PgPool, job: &Job, error: Option<&str>) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let now = now_ms();

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
    pool: &PgPool,
    job_id: u64,
    new_run_at: u64,
    attempts: u32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE jobs SET state = 'waiting', run_at = $1, attempts = $2, started_at = 0 WHERE id = $3",
    )
    .bind(new_run_at as i64)
    .bind(attempts as i32)
    .bind(job_id as i64)
    .execute(pool)
    .await?;
    Ok(())
}

/// Cancel a job.
pub async fn cancel_job(pool: &PgPool, job_id: u64) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM jobs WHERE id = $1")
        .bind(job_id as i64)
        .execute(pool)
        .await?;
    Ok(())
}

/// Load all pending jobs from PostgreSQL for recovery.
pub async fn load_pending_jobs(pool: &PgPool) -> Result<Vec<(Job, String)>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, queue, data, priority, created_at, run_at, started_at, attempts,
               max_attempts, backoff, ttl, timeout, unique_key, depends_on, progress,
               progress_msg, tags, state, lifo
        FROM jobs
        WHERE state IN ('waiting', 'delayed', 'active', 'waiting_children')
    "#,
    )
    .fetch_all(pool)
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
            // New fields with defaults
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: Vec::new(),
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        };
        let state: String = row.get("state");
        jobs.push((job, state));
    }

    Ok(jobs)
}

/// Load DLQ jobs.
pub async fn load_dlq_jobs(pool: &PgPool) -> Result<Vec<Job>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT j.id, j.queue, j.data, j.priority, j.created_at, j.run_at, j.started_at,
               j.attempts, j.max_attempts, j.backoff, j.ttl, j.timeout, j.unique_key,
               j.depends_on, j.progress, j.progress_msg, j.tags, j.lifo
        FROM jobs j
        INNER JOIN dlq_jobs d ON j.id = d.job_id
    "#,
    )
    .fetch_all(pool)
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
            // New fields with defaults
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: Vec::new(),
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        });
    }

    Ok(jobs)
}

/// Load a specific job by ID (for sync).
pub async fn load_job_by_id(
    pool: &PgPool,
    job_id: u64,
) -> Result<Option<(Job, String)>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT id, queue, data, priority, created_at, run_at, started_at, attempts,
               max_attempts, backoff, ttl, timeout, unique_key, depends_on, progress,
               progress_msg, tags, state, lifo
        FROM jobs
        WHERE id = $1
    "#,
    )
    .bind(job_id as i64)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => {
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
                remove_on_complete: false,
                remove_on_fail: false,
                last_heartbeat: 0,
                stall_timeout: 0,
                stall_count: 0,
                parent_id: None,
                children_ids: Vec::new(),
                children_completed: 0,
                custom_id: None,
                keep_completed_age: 0,
                keep_completed_count: 0,
                completed_at: 0,
            };
            let state: String = row.get("state");
            Ok(Some((job, state)))
        }
        None => Ok(None),
    }
}

/// Get the maximum job ID from the database (for ID recovery on startup).
pub async fn get_max_job_id(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let row = sqlx::query("SELECT COALESCE(MAX(id), 0) as max_id FROM jobs")
        .fetch_one(pool)
        .await?;
    let max_id: i64 = row.get("max_id");
    Ok(max_id as u64)
}

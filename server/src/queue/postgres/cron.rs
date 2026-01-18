//! Cron job database operations.

use sqlx::{PgPool, Row};

use crate::protocol::CronJob;

/// Save a cron job.
pub async fn save_cron(pool: &PgPool, cron: &CronJob) -> Result<(), sqlx::Error> {
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
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete a cron job.
pub async fn delete_cron(pool: &PgPool, name: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM cron_jobs WHERE name = $1")
        .bind(name)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Load all cron jobs.
pub async fn load_crons(pool: &PgPool) -> Result<Vec<CronJob>, sqlx::Error> {
    let rows = sqlx::query("SELECT name, queue, data, schedule, priority, next_run FROM cron_jobs")
        .fetch_all(pool)
        .await?;

    let mut crons = Vec::with_capacity(rows.len());
    for row in rows {
        let schedule: Option<String> = row.get("schedule");
        crons.push(CronJob {
            name: row.get("name"),
            queue: row.get("queue"),
            data: row.get("data"),
            schedule,
            repeat_every: None, // Not stored in DB yet
            priority: row.get("priority"),
            next_run: row.get::<i64, _>("next_run") as u64,
            executions: 0,
            limit: None,
        });
    }

    Ok(crons)
}

/// Update cron job next_run time.
pub async fn update_cron_next_run(
    pool: &PgPool,
    name: &str,
    next_run: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE cron_jobs SET next_run = $1 WHERE name = $2")
        .bind(next_run as i64)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
}

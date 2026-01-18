//! Cron job operations.
//!
//! Schedule recurring jobs using cron expressions or repeat intervals.

use super::manager::QueueManager;
use super::types::now_ms;
use crate::protocol::CronJob;
use serde_json::Value;

impl QueueManager {
    /// Add a cron job with a cron schedule expression.
    pub async fn add_cron(
        &self,
        name: String,
        queue: String,
        data: Value,
        schedule: String,
        priority: i32,
    ) -> Result<(), String> {
        // Validate cron expression first
        Self::validate_cron(&schedule)?;

        let now = now_ms();
        let next_run = Self::parse_next_cron_run(&schedule, now);
        let cron = CronJob {
            name: name.clone(),
            queue,
            data,
            schedule: Some(schedule),
            repeat_every: None,
            priority,
            next_run,
            executions: 0,
            limit: None,
        };

        // Persist to PostgreSQL
        self.persist_cron(&cron);

        self.cron_jobs.write().insert(name, cron);
        Ok(())
    }

    /// Add a cron job with optional repeat_every interval.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_cron_with_repeat(
        &self,
        name: String,
        queue: String,
        data: Value,
        schedule: Option<String>,
        repeat_every: Option<u64>,
        priority: i32,
        limit: Option<u64>,
    ) -> Result<(), String> {
        // Must have either schedule or repeat_every
        if schedule.is_none() && repeat_every.is_none() {
            return Err("Must provide either 'schedule' or 'repeat_every'".to_string());
        }

        // Validate cron expression if provided
        if let Some(ref sched) = schedule {
            Self::validate_cron(sched)?;
        }

        let now = now_ms();
        let next_run = if let Some(interval) = repeat_every {
            now + interval
        } else {
            // Safe: we checked above that at least one of schedule/repeat_every is Some
            Self::parse_next_cron_run(
                schedule
                    .as_ref()
                    .expect("schedule must exist if repeat_every is None"),
                now,
            )
        };

        let cron = CronJob {
            name: name.clone(),
            queue,
            data,
            schedule,
            repeat_every,
            priority,
            next_run,
            executions: 0,
            limit,
        };

        // Persist to PostgreSQL
        self.persist_cron(&cron);

        self.cron_jobs.write().insert(name, cron);
        Ok(())
    }

    /// Delete a cron job by name.
    pub async fn delete_cron(&self, name: &str) -> bool {
        let removed = self.cron_jobs.write().remove(name).is_some();
        if removed {
            // Persist deletion to PostgreSQL
            self.persist_cron_delete(name);
        }
        removed
    }

    /// List all cron jobs.
    pub async fn list_crons(&self) -> Vec<CronJob> {
        self.cron_jobs.read().values().cloned().collect()
    }
}

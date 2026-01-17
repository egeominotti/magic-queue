//! Flows (Parent-Child Jobs).
//!
//! Create job workflows where a parent job waits for all children to complete.

use super::manager::QueueManager;
use super::types::{intern, now_ms, JobLocation};
use crate::protocol::{FlowChild, Job};

impl QueueManager {
    /// Create a flow with parent and children jobs.
    /// The parent job will wait until all children complete before becoming ready.
    pub async fn push_flow(
        &self,
        queue: String,
        parent_data: serde_json::Value,
        children: Vec<FlowChild>,
        priority: i32,
    ) -> Result<(u64, Vec<u64>), String> {
        if children.is_empty() {
            return Err("Flow must have at least one child".to_string());
        }

        let now = now_ms();
        let parent_id = self.next_job_id().await;
        let children_ids = self.next_job_ids(children.len()).await;

        // Create parent job (will wait for children)
        let parent_job = Job {
            id: parent_id,
            queue: queue.clone(),
            data: parent_data,
            priority,
            created_at: now,
            run_at: now,
            started_at: 0,
            attempts: 0,
            max_attempts: 0,
            backoff: 0,
            ttl: 0,
            timeout: 0,
            unique_key: None,
            depends_on: Vec::new(),
            progress: 0,
            progress_msg: None,
            tags: Vec::new(),
            lifo: false,
            remove_on_complete: false,
            remove_on_fail: false,
            last_heartbeat: 0,
            stall_timeout: 0,
            stall_count: 0,
            parent_id: None,
            children_ids: children_ids.clone(),
            children_completed: 0,
            custom_id: None,
            keep_completed_age: 0,
            keep_completed_count: 0,
            completed_at: 0,
        };

        // Create child jobs
        let mut child_jobs = Vec::with_capacity(children.len());
        for (i, child) in children.into_iter().enumerate() {
            let child_job = Job {
                id: children_ids[i],
                queue: child.queue,
                data: child.data,
                priority: child.priority,
                created_at: now,
                run_at: now + child.delay.unwrap_or(0),
                started_at: 0,
                attempts: 0,
                max_attempts: 0,
                backoff: 0,
                ttl: 0,
                timeout: 0,
                unique_key: None,
                depends_on: Vec::new(),
                progress: 0,
                progress_msg: None,
                tags: Vec::new(),
                lifo: false,
                remove_on_complete: false,
                remove_on_fail: false,
                last_heartbeat: 0,
                stall_timeout: 0,
                stall_count: 0,
                parent_id: Some(parent_id),
                children_ids: Vec::new(),
                children_completed: 0,
                custom_id: None,
                keep_completed_age: 0,
                keep_completed_count: 0,
                completed_at: 0,
            };
            child_jobs.push(child_job);
        }

        // Store parent in waiting_children
        let parent_idx = Self::shard_index(&queue);
        // Persist first (needs reference), then insert (moves ownership)
        self.persist_push(&parent_job, "waiting_parent");
        self.index_job(
            parent_id,
            JobLocation::WaitingChildren {
                shard_idx: parent_idx,
            },
        );
        {
            let mut shard = self.shards[parent_idx].write();
            shard.waiting_children.insert(parent_id, parent_job);
        }

        // Push all children to their queues
        // Persist all first (needs references)
        for child_job in &child_jobs {
            self.persist_push(child_job, "waiting");
        }
        // Then insert (moves ownership)
        for child_job in child_jobs {
            let child_idx = Self::shard_index(&child_job.queue);
            let queue_arc = intern(&child_job.queue);
            self.index_job(
                child_job.id,
                JobLocation::Queue {
                    shard_idx: child_idx,
                },
            );
            {
                let mut shard = self.shards[child_idx].write();
                shard.queues.entry(queue_arc).or_default().push(child_job);
            }
            self.notify_shard(child_idx);
        }

        self.metrics.record_push((children_ids.len() + 1) as u64);

        Ok((parent_id, children_ids))
    }

    /// Called when a child job completes - check if parent is ready.
    pub(crate) fn on_child_completed(&self, parent_id: u64) {
        // Find parent in waiting_children across all shards
        for (idx, shard) in self.shards.iter().enumerate() {
            let mut shard_w = shard.write();
            if let Some(parent) = shard_w.waiting_children.get_mut(&parent_id) {
                parent.children_completed += 1;

                // Check if all children completed
                if parent.children_completed >= parent.children_ids.len() as u32 {
                    // Move parent to queue (ready to process)
                    // Safe: get_mut returned Some above
                    let parent_job = shard_w
                        .waiting_children
                        .remove(&parent_id)
                        .expect("parent exists after get_mut");
                    let queue_arc = intern(&parent_job.queue);
                    shard_w
                        .queues
                        .entry(queue_arc)
                        .or_default()
                        .push(parent_job);
                    drop(shard_w);

                    self.index_job(parent_id, JobLocation::Queue { shard_idx: idx });
                    self.notify_shard(idx);

                    // Log the event
                    let _ = self.add_job_log(
                        parent_id,
                        "All children completed, parent job ready".to_string(),
                        "info".to_string(),
                    );
                }
                return;
            }
        }
    }

    /// Get children status for a parent job.
    /// Returns (children jobs, completed count, total count).
    pub fn get_children(&self, parent_id: u64) -> Option<(Vec<Job>, u32, u32)> {
        // Find parent
        for shard in &self.shards {
            let shard_r = shard.read();
            if let Some(parent) = shard_r.waiting_children.get(&parent_id) {
                let children_ids = &parent.children_ids;
                let mut children = Vec::with_capacity(children_ids.len());

                // Collect children jobs
                for child_id in children_ids {
                    let (job, _state) = self.get_job(*child_id);
                    if let Some(j) = job {
                        children.push(j);
                    }
                }

                return Some((
                    children,
                    parent.children_completed,
                    parent.children_ids.len() as u32,
                ));
            }
        }
        None
    }
}

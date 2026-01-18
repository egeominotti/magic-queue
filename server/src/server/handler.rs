//! Command processing for flashQ.
//!
//! Handles all protocol commands for both text and binary protocols.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::protocol::{deserialize_msgpack, Command, JobState, Request, Response, ResponseWithId};
use crate::queue::QueueManager;

use super::connection::ConnectionState;

/// Process command from binary (MessagePack) input
#[inline(always)]
pub async fn process_command_binary(
    data: &[u8],
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> ResponseWithId {
    let request: Request = match deserialize_msgpack(data) {
        Ok(req) => req,
        Err(e) => return ResponseWithId::new(Response::error(e), None),
    };

    process_request(request, queue_manager, state).await
}

#[inline(always)]
fn parse_request(line: &mut String) -> Result<Request, String> {
    // Trim newline in-place
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    // sonic-rs: fastest JSON parser (SIMD-accelerated, 30% faster than simd-json)
    sonic_rs::from_str(line).map_err(|e| format!("Invalid: {}", e))
}

/// Process command from text (JSON) input
#[inline(always)]
pub async fn process_command_text(
    line: &mut String,
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> ResponseWithId {
    let request: Request = match parse_request(line) {
        Ok(req) => req,
        Err(e) => return ResponseWithId::new(Response::error(e), None),
    };

    process_request(request, queue_manager, state).await
}

/// Shared command processing logic for both text and binary protocols
#[inline(always)]
pub async fn process_request(
    request: Request,
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> ResponseWithId {
    let req_id = request.req_id;
    let command = request.command;

    // Handle Auth command first (doesn't require authentication)
    if let Command::Auth { token } = &command {
        if queue_manager.verify_token(token) {
            state.write().authenticated = true;
            return ResponseWithId::new(Response::ok(), req_id);
        } else {
            return ResponseWithId::new(Response::error("Invalid token"), req_id);
        }
    }

    // Check if authentication is required
    if !queue_manager.verify_token("") && !state.read().authenticated {
        return ResponseWithId::new(Response::error("Authentication required"), req_id);
    }

    let response = process_command(command, queue_manager, &req_id).await;
    ResponseWithId::new(response, req_id)
}

/// Process a single command and return the response
async fn process_command(
    command: Command,
    queue_manager: &Arc<QueueManager>,
    _req_id: &Option<String>,
) -> Response {
    match command {
        // === Core Commands ===
        Command::Push {
            queue,
            data,
            priority,
            delay,
            ttl,
            timeout,
            max_attempts,
            backoff,
            unique_key,
            depends_on,
            tags,
            lifo,
            remove_on_complete,
            remove_on_fail,
            stall_timeout,
            debounce_id,
            debounce_ttl,
            job_id,
            keep_completed_age,
            keep_completed_count,
        } => {
            match queue_manager
                .push(
                    queue,
                    data,
                    priority,
                    delay,
                    ttl,
                    timeout,
                    max_attempts,
                    backoff,
                    unique_key,
                    depends_on,
                    tags,
                    lifo,
                    remove_on_complete,
                    remove_on_fail,
                    stall_timeout,
                    debounce_id,
                    debounce_ttl,
                    job_id,
                    keep_completed_age,
                    keep_completed_count,
                )
                .await
            {
                Ok(job) => Response::ok_with_id(job.id),
                Err(e) => Response::error(e),
            }
        }
        Command::Pushb { queue, jobs } => {
            let ids = queue_manager.push_batch(queue, jobs).await;
            Response::batch(ids)
        }
        Command::Pull { queue, timeout } => {
            let timeout_ms = timeout.unwrap_or(60_000);
            if queue_manager.is_distributed_pull() {
                match queue_manager.pull_distributed(&queue, timeout_ms).await {
                    Some(job) => Response::job(job),
                    None => Response::null_job(),
                }
            } else {
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(timeout_ms),
                    queue_manager.pull(&queue),
                )
                .await
                {
                    Ok(job) => Response::job(job),
                    Err(_) => Response::null_job(),
                }
            }
        }
        Command::Pullb {
            queue,
            count,
            timeout,
        } => {
            let timeout_ms = timeout.unwrap_or(60_000);
            if queue_manager.is_distributed_pull() {
                let jobs = queue_manager
                    .pull_distributed_batch(&queue, count, timeout_ms)
                    .await;
                Response::jobs(jobs)
            } else {
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(timeout_ms),
                    queue_manager.pull_batch(&queue, count),
                )
                .await
                {
                    Ok(jobs) => Response::jobs(jobs),
                    Err(_) => Response::jobs(vec![]), // Return empty array on timeout
                }
            }
        }
        Command::Ack { id, result } => match queue_manager.ack(id, result).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::Ackb { ids } => {
            let count = queue_manager.ack_batch(&ids).await;
            Response::batch(vec![count as u64])
        }
        Command::Fail { id, error } => match queue_manager.fail(id, error).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::GetResult { id } => {
            let result = queue_manager.get_result(id).await;
            Response::result(id, result)
        }
        Command::GetJob { id } => {
            let (job, state) = queue_manager.get_job(id);
            Response::job_with_state(job, state)
        }
        Command::GetState { id } => {
            let state = queue_manager.get_state(id);
            Response::state(id, state)
        }
        Command::WaitJob { id, timeout } => match queue_manager.wait_for_job(id, timeout).await {
            Ok(result) => Response::JobResult {
                ok: true,
                result,
                error: None,
            },
            Err(e) => Response::JobResult {
                ok: false,
                result: None,
                error: Some(e),
            },
        },
        Command::GetJobByCustomId { job_id } => match queue_manager.get_job_by_custom_id(&job_id) {
            Some((job, state)) => Response::JobWithState {
                ok: true,
                job: Some(job),
                state,
            },
            None => Response::JobWithState {
                ok: true,
                job: None,
                state: JobState::Unknown,
            },
        },

        // === Job Management Commands ===
        Command::Cancel { id } => match queue_manager.cancel(id).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::Progress {
            id,
            progress,
            message,
        } => match queue_manager.update_progress(id, progress, message).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::GetProgress { id } => match queue_manager.get_progress(id).await {
            Ok((progress, message)) => Response::progress(id, progress, message),
            Err(e) => Response::error(e),
        },
        Command::Dlq { queue, count } => {
            let jobs = queue_manager.get_dlq(&queue, count).await;
            Response::jobs(jobs)
        }
        Command::RetryDlq { queue, id } => {
            let count = queue_manager.retry_dlq(&queue, id).await;
            Response::batch(vec![count as u64])
        }
        Command::PurgeDlq { queue } => {
            let count = queue_manager.purge_dlq(&queue).await;
            Response::count(count)
        }
        Command::GetJobsBatch { ids } => {
            let jobs = queue_manager.get_jobs_batch(&ids).await;
            Response::jobs_batch(jobs)
        }
        Command::Subscribe { queue, events } => {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            queue_manager.subscribe(queue, events, tx);
            Response::ok()
        }
        Command::Unsubscribe { queue } => {
            queue_manager.unsubscribe(&queue);
            Response::ok()
        }
        Command::Metrics => {
            let metrics = queue_manager.get_metrics().await;
            Response::metrics(metrics)
        }
        Command::Stats => {
            let (queued, processing, delayed, dlq) = queue_manager.stats().await;
            Response::stats(queued, processing, delayed, dlq)
        }

        // === Cron Jobs / Repeatable Jobs ===
        Command::Cron {
            name,
            queue,
            data,
            schedule,
            repeat_every,
            priority,
            limit,
        } => {
            match queue_manager
                .add_cron_with_repeat(name, queue, data, schedule, repeat_every, priority, limit)
                .await
            {
                Ok(()) => Response::ok(),
                Err(e) => Response::error(e),
            }
        }
        Command::CronDelete { name } => {
            if queue_manager.delete_cron(&name).await {
                Response::ok()
            } else {
                Response::error("Cron job not found")
            }
        }
        Command::CronList => {
            let crons = queue_manager.list_crons().await;
            Response::cron_list(crons)
        }

        // === Rate Limiting ===
        Command::RateLimit { queue, limit } => {
            queue_manager.set_rate_limit(queue, limit).await;
            Response::ok()
        }
        Command::RateLimitClear { queue } => {
            queue_manager.clear_rate_limit(&queue).await;
            Response::ok()
        }

        // === Queue Control ===
        Command::Pause { queue } => {
            queue_manager.pause(&queue).await;
            Response::ok()
        }
        Command::Resume { queue } => {
            queue_manager.resume(&queue).await;
            Response::ok()
        }
        Command::SetConcurrency { queue, limit } => {
            queue_manager.set_concurrency(queue, limit).await;
            Response::ok()
        }
        Command::ClearConcurrency { queue } => {
            queue_manager.clear_concurrency(&queue).await;
            Response::ok()
        }
        Command::ListQueues => {
            let queues = queue_manager.list_queues().await;
            Response::queues(queues)
        }

        // === Job Logs ===
        Command::Log { id, message, level } => {
            match queue_manager.add_job_log(id, message, level) {
                Ok(()) => Response::ok(),
                Err(e) => Response::error(e),
            }
        }
        Command::GetLogs { id } => {
            let logs = queue_manager.get_job_logs(id);
            Response::logs(id, logs)
        }

        // === Stalled Jobs ===
        Command::Heartbeat { id } => match queue_manager.heartbeat(id) {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },

        // === Flows (Parent-Child) ===
        Command::Flow {
            queue,
            data,
            children,
            priority,
        } => {
            match queue_manager
                .push_flow(queue, data, children, priority)
                .await
            {
                Ok((parent_id, children_ids)) => Response::flow(parent_id, children_ids),
                Err(e) => Response::error(e),
            }
        }
        Command::GetChildren { parent_id } => match queue_manager.get_children(parent_id) {
            Some((children, completed, total)) => {
                Response::children(parent_id, children, completed, total)
            }
            None => Response::error(format!("Parent job {} not found", parent_id)),
        },

        // === BullMQ Advanced Commands ===
        Command::GetJobs {
            queue,
            state,
            limit,
            offset,
        } => {
            let state_filter = state.and_then(|s| match s.to_lowercase().as_str() {
                "waiting" => Some(JobState::Waiting),
                "delayed" => Some(JobState::Delayed),
                "active" => Some(JobState::Active),
                "completed" => Some(JobState::Completed),
                "failed" => Some(JobState::Failed),
                "waiting-children" => Some(JobState::WaitingChildren),
                "waiting-parent" => Some(JobState::WaitingParent),
                "stalled" => Some(JobState::Stalled),
                _ => None,
            });
            let (jobs, total) = queue_manager.get_jobs(
                queue.as_deref(),
                state_filter,
                limit.unwrap_or(100),
                offset.unwrap_or(0),
            );
            Response::jobs_with_total(jobs, total)
        }
        Command::Clean {
            queue,
            grace,
            state,
            limit,
        } => {
            let state_enum = match state.to_lowercase().as_str() {
                "waiting" => JobState::Waiting,
                "delayed" => JobState::Delayed,
                "completed" => JobState::Completed,
                "failed" => JobState::Failed,
                _ => {
                    return Response::error(
                        "Invalid state. Use: waiting, delayed, completed, failed",
                    )
                }
            };
            let count = queue_manager.clean(&queue, grace, state_enum, limit).await;
            Response::count(count)
        }
        Command::Drain { queue } => {
            let count = queue_manager.drain(&queue).await;
            Response::count(count)
        }
        Command::Obliterate { queue } => {
            let count = queue_manager.obliterate(&queue).await;
            Response::count(count)
        }
        Command::ChangePriority { id, priority } => {
            match queue_manager.change_priority(id, priority).await {
                Ok(()) => Response::ok(),
                Err(e) => Response::error(e),
            }
        }
        Command::MoveToDelayed { id, delay } => {
            match queue_manager.move_to_delayed(id, delay).await {
                Ok(()) => Response::ok(),
                Err(e) => Response::error(e),
            }
        }
        Command::Promote { id } => match queue_manager.promote(id).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::UpdateJob { id, data } => match queue_manager.update_job_data(id, data).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::Discard { id } => match queue_manager.discard(id).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::IsPaused { queue } => {
            let paused = queue_manager.is_paused(&queue);
            Response::paused(paused)
        }
        Command::Count { queue } => {
            let count = queue_manager.count(&queue);
            Response::count(count)
        }
        Command::GetJobCounts { queue } => {
            let (waiting, active, delayed, completed, failed) =
                queue_manager.get_job_counts(&queue);
            Response::job_counts(waiting, active, delayed, completed, failed)
        }

        // Already handled in process_request
        Command::Auth { .. } => Response::ok(),

        // === Key-Value Storage Commands ===
        Command::KvSet { key, value, ttl } => match queue_manager.kv_set(key, value, ttl) {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::KvGet { key } => {
            let value = queue_manager.kv_get(&key);
            Response::kv_value(value)
        }
        Command::KvDel { key } => {
            let deleted = queue_manager.kv_del(&key);
            Response::kv_exists(deleted)
        }
        Command::KvMget { keys } => {
            let values = queue_manager.kv_mget(&keys);
            Response::kv_values(values)
        }
        Command::KvMset { entries } => {
            let entries_vec: Vec<_> = entries
                .into_iter()
                .map(|e| (e.key, e.value, e.ttl))
                .collect();
            match queue_manager.kv_mset(entries_vec) {
                Ok(count) => Response::count(count),
                Err(e) => Response::error(e),
            }
        }
        Command::KvExists { key } => {
            let exists = queue_manager.kv_exists(&key);
            Response::kv_exists(exists)
        }
        Command::KvExpire { key, ttl } => {
            let success = queue_manager.kv_expire(&key, ttl);
            Response::kv_exists(success)
        }
        Command::KvTtl { key } => {
            let ttl = queue_manager.kv_ttl(&key);
            Response::kv_ttl(ttl)
        }
        Command::KvKeys { pattern } => {
            let keys = queue_manager.kv_keys(pattern.as_deref());
            Response::kv_keys(keys)
        }
        Command::KvIncr { key, by } => match queue_manager.kv_incr(&key, by) {
            Ok(value) => Response::kv_incr(value),
            Err(e) => Response::error(e),
        },

        // === Pub/Sub Commands ===
        Command::Pub { channel, message } => {
            let receivers = queue_manager.pubsub.publish(&channel, message);
            Response::pub_count(receivers)
        }
        Command::Sub { channels } => {
            // Note: Subscription requires dedicated connection with message loop
            // This registers the subscription but messages are delivered via WebSocket/SSE
            let _rx = queue_manager.pubsub.subscribe(channels.clone());
            Response::pub_subscribed(channels)
        }
        Command::Psub { patterns } => {
            let _rx = queue_manager.pubsub.psubscribe(patterns.clone());
            Response::pub_subscribed(patterns)
        }
        Command::Unsub { channels } => {
            // Note: Proper unsubscribe requires tracking the sender per connection
            // For now this is a no-op acknowledgement
            Response::pub_subscribed(channels)
        }
        Command::Punsub { patterns } => Response::pub_subscribed(patterns),
        Command::PubsubChannels { pattern } => {
            let channels = queue_manager.pubsub.channels(pattern.as_deref());
            Response::pub_channels(channels)
        }
        Command::PubsubNumsub { channels } => {
            let counts = queue_manager.pubsub.numsub(&channels);
            Response::pub_numsub(counts)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::serialize_msgpack;

    fn setup() -> Arc<QueueManager> {
        QueueManager::new(false)
    }

    fn setup_with_auth() -> Arc<QueueManager> {
        QueueManager::with_auth_tokens(false, vec!["secret-token".to_string()])
    }

    #[test]
    fn test_parse_request_valid_json() {
        let mut line = r#"{"cmd":"STATS"}"#.to_string();
        let result = parse_request(&mut line);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_request_with_newline() {
        let mut line = "{\"cmd\":\"STATS\"}\n".to_string();
        let result = parse_request(&mut line);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_request_with_crlf() {
        let mut line = "{\"cmd\":\"STATS\"}\r\n".to_string();
        let result = parse_request(&mut line);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_request_invalid_json() {
        let mut line = "not json".to_string();
        let result = parse_request(&mut line);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid"));
    }

    #[test]
    fn test_parse_request_push_command() {
        let mut line = r#"{"cmd":"PUSH","queue":"test","data":{"foo":"bar"}}"#.to_string();
        let result = parse_request(&mut line);
        assert!(result.is_ok());
        let request = result.unwrap();
        match request.command {
            Command::Push { queue, .. } => assert_eq!(queue, "test"),
            _ => panic!("Expected Push command"),
        }
    }

    #[test]
    fn test_parse_request_with_req_id() {
        let mut line = r#"{"reqId":"abc123","cmd":"STATS"}"#.to_string();
        let result = parse_request(&mut line);
        assert!(result.is_ok());
        let request = result.unwrap();
        assert_eq!(request.req_id, Some("abc123".to_string()));
    }

    #[tokio::test]
    async fn test_process_command_text_valid() {
        let qm = setup();
        let state = Arc::new(RwLock::new(ConnectionState {
            authenticated: false,
        }));

        let mut line = r#"{"cmd":"STATS"}"#.to_string();
        let response = process_command_text(&mut line, &qm, &state).await;

        // Should return stats response
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":true"));
    }

    #[tokio::test]
    async fn test_process_command_text_invalid_json() {
        let qm = setup();
        let state = Arc::new(RwLock::new(ConnectionState {
            authenticated: false,
        }));

        let mut line = "invalid json".to_string();
        let response = process_command_text(&mut line, &qm, &state).await;

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":false"));
        assert!(json.contains("Invalid"));
    }

    #[tokio::test]
    async fn test_process_command_binary_valid() {
        let qm = setup();
        let state = Arc::new(RwLock::new(ConnectionState {
            authenticated: false,
        }));

        let request = Request {
            req_id: None,
            command: Command::Stats,
        };
        let data = serialize_msgpack(&request).unwrap();

        let response = process_command_binary(&data, &qm, &state).await;
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":true"));
    }

    #[tokio::test]
    async fn test_process_command_binary_invalid() {
        let qm = setup();
        let state = Arc::new(RwLock::new(ConnectionState {
            authenticated: false,
        }));

        let data = vec![0xFF, 0xFF, 0xFF]; // Invalid MessagePack
        let response = process_command_binary(&data, &qm, &state).await;

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":false"));
    }

    #[tokio::test]
    async fn test_auth_required_without_token() {
        let qm = setup_with_auth();
        let state = Arc::new(RwLock::new(ConnectionState {
            authenticated: false,
        }));

        let mut line = r#"{"cmd":"STATS"}"#.to_string();
        let response = process_command_text(&mut line, &qm, &state).await;

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":false"));
        assert!(json.contains("Authentication required"));
    }

    #[tokio::test]
    async fn test_auth_success() {
        let qm = setup_with_auth();
        let state = Arc::new(RwLock::new(ConnectionState {
            authenticated: false,
        }));

        // First authenticate
        let mut auth_line = r#"{"cmd":"AUTH","token":"secret-token"}"#.to_string();
        let auth_response = process_command_text(&mut auth_line, &qm, &state).await;
        let json = serde_json::to_string(&auth_response).unwrap();
        assert!(json.contains("\"ok\":true"));

        // Now stats should work
        let mut stats_line = r#"{"cmd":"STATS"}"#.to_string();
        let stats_response = process_command_text(&mut stats_line, &qm, &state).await;
        let json = serde_json::to_string(&stats_response).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("queued"));
    }

    #[tokio::test]
    async fn test_auth_invalid_token() {
        let qm = setup_with_auth();
        let state = Arc::new(RwLock::new(ConnectionState {
            authenticated: false,
        }));

        let mut line = r#"{"cmd":"AUTH","token":"wrong-token"}"#.to_string();
        let response = process_command_text(&mut line, &qm, &state).await;

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"ok\":false"));
        assert!(json.contains("Invalid token"));
    }

    #[tokio::test]
    async fn test_req_id_echoed_in_response() {
        let qm = setup();
        let state = Arc::new(RwLock::new(ConnectionState {
            authenticated: false,
        }));

        let mut line = r#"{"reqId":"test-123","cmd":"STATS"}"#.to_string();
        let response = process_command_text(&mut line, &qm, &state).await;

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"reqId\":\"test-123\""));
    }
}

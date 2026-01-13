mod dashboard;
mod grpc;
mod http;
mod protocol;
mod queue;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};

use protocol::{Command, JobState, Response};
use queue::{generate_node_id, QueueManager};

const DEFAULT_TCP_PORT: u16 = 6789;
const DEFAULT_HTTP_PORT: u16 = 6790;
const DEFAULT_GRPC_PORT: u16 = 6791;
const UNIX_SOCKET_PATH: &str = "/tmp/flashq.sock";

struct ConnectionState {
    authenticated: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let use_unix = std::env::var("UNIX_SOCKET").is_ok();
    let database_url = std::env::var("DATABASE_URL").ok();
    let enable_http = std::env::var("HTTP").is_ok();
    let enable_grpc = std::env::var("GRPC").is_ok();
    let enable_cluster = std::env::var("CLUSTER_MODE").is_ok();

    // Parse auth tokens from environment
    let auth_tokens: Vec<String> = std::env::var("AUTH_TOKENS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // Get HTTP port for cluster registration
    let http_port: i32 = std::env::var("HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_HTTP_PORT as i32);

    // Create QueueManager with optional PostgreSQL persistence and clustering
    let queue_manager = if enable_cluster {
        // Cluster mode requires PostgreSQL
        if let Some(url) = &database_url {
            let node_id = generate_node_id();
            let host = std::env::var("NODE_HOST").unwrap_or_else(|_| "localhost".to_string());
            println!("Starting node {} in cluster mode", node_id);
            QueueManager::with_cluster(url, node_id, host, http_port).await
        } else {
            eprintln!("CLUSTER_MODE requires DATABASE_URL to be set");
            std::process::exit(1);
        }
    } else {
        match (&database_url, auth_tokens.is_empty()) {
            (Some(url), true) => QueueManager::with_postgres(url).await,
            (Some(url), false) => QueueManager::with_postgres_and_auth(url, auth_tokens).await,
            (None, true) => QueueManager::new(false),
            (None, false) => {
                println!("Authentication enabled with {} token(s)", auth_tokens.len());
                QueueManager::with_auth_tokens(false, auth_tokens)
            }
        }
    };

    // Start HTTP server if enabled
    if enable_http {
        let http_port = std::env::var("HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(DEFAULT_HTTP_PORT);

        let qm_http = Arc::clone(&queue_manager);
        tokio::spawn(async move {
            let router = http::create_router(qm_http);
            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", http_port))
                .await
                .expect("Failed to bind HTTP listener");
            println!("HTTP API listening on port {}", http_port);
            println!("Dashboard available at http://localhost:{}", http_port);
            axum::serve(listener, router)
                .await
                .expect("HTTP server error");
        });
    }

    // Start gRPC server if enabled
    if enable_grpc {
        let grpc_port = std::env::var("GRPC_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(DEFAULT_GRPC_PORT);

        let qm_grpc = Arc::clone(&queue_manager);
        tokio::spawn(async move {
            let addr = format!("0.0.0.0:{}", grpc_port).parse().unwrap();
            if let Err(e) = grpc::run_grpc_server(addr, qm_grpc).await {
                eprintln!("gRPC server error: {}", e);
            }
        });
    }

    if use_unix {
        let _ = std::fs::remove_file(UNIX_SOCKET_PATH);
        let listener = UnixListener::bind(UNIX_SOCKET_PATH)?;
        println!("flashQ TCP server listening on {}", UNIX_SOCKET_PATH);

        loop {
            let (socket, _) = listener.accept().await?;
            let qm = Arc::clone(&queue_manager);
            tokio::spawn(async move {
                let (reader, writer) = socket.into_split();
                let _ = handle_connection(reader, writer, qm).await;
            });
        }
    } else {
        let port = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(DEFAULT_TCP_PORT);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        println!("flashQ TCP server listening on port {}", port);

        loop {
            let (socket, _) = listener.accept().await?;
            socket.set_nodelay(true)?;
            let qm = Arc::clone(&queue_manager);
            tokio::spawn(async move {
                let (reader, writer) = socket.into_split();
                let _ = handle_connection(reader, writer, qm).await;
            });
        }
    }
}

async fn handle_connection<R, W>(
    reader: R,
    writer: W,
    queue_manager: Arc<QueueManager>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut reader = BufReader::with_capacity(128 * 1024, reader);
    let mut writer = BufWriter::with_capacity(128 * 1024, writer);
    let mut line = String::with_capacity(8192);
    let state = Arc::new(RwLock::new(ConnectionState {
        authenticated: false,
    }));

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            break;
        }

        let response = process_command(&mut line, &queue_manager, &state).await;
        let response_json = serde_json::to_string(&response)?;
        writer.write_all(response_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        // Pipelining: only flush if no more commands waiting in buffer
        // This allows batching multiple commands without syscall overhead
        if reader.buffer().is_empty() {
            writer.flush().await?;
        }
    }

    Ok(())
}

#[inline(always)]
fn parse_command(line: &mut String) -> Result<Command, String> {
    // Trim newline in-place
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    // sonic-rs: fastest JSON parser (SIMD-accelerated, 30% faster than simd-json)
    sonic_rs::from_str(line).map_err(|e| format!("Invalid: {}", e))
}

#[inline(always)]
async fn process_command(
    line: &mut String,
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> Response {
    let command: Command = match parse_command(line) {
        Ok(cmd) => cmd,
        Err(e) => return Response::error(e),
    };

    // Handle Auth command first (doesn't require authentication)
    if let Command::Auth { token } = &command {
        if queue_manager.verify_token(token) {
            state.write().authenticated = true;
            return Response::ok();
        } else {
            return Response::error("Invalid token");
        }
    }

    // Check if authentication is required
    if !queue_manager.verify_token("") && !state.read().authenticated {
        return Response::error("Authentication required");
    }

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
        Command::Pull { queue } => {
            // Use distributed pull in cluster mode for consistency
            if queue_manager.is_distributed_pull() {
                match queue_manager.pull_distributed(&queue, 30_000).await {
                    Some(job) => Response::job(job),
                    None => {
                        // Fallback to blocking pull if distributed returns nothing
                        let job = queue_manager.pull(&queue).await;
                        Response::job(job)
                    }
                }
            } else {
                let job = queue_manager.pull(&queue).await;
                Response::job(job)
            }
        }
        Command::Pullb { queue, count } => {
            // Use distributed pull in cluster mode for consistency
            if queue_manager.is_distributed_pull() {
                let jobs = queue_manager
                    .pull_distributed_batch(&queue, count, 30_000)
                    .await;
                if jobs.is_empty() {
                    // Fallback to blocking pull if distributed returns nothing
                    let jobs = queue_manager.pull_batch(&queue, count).await;
                    Response::jobs(jobs)
                } else {
                    Response::jobs(jobs)
                }
            } else {
                let jobs = queue_manager.pull_batch(&queue, count).await;
                Response::jobs(jobs)
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

        // === New Commands ===
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
                "waiting" => Some(protocol::JobState::Waiting),
                "delayed" => Some(protocol::JobState::Delayed),
                "active" => Some(protocol::JobState::Active),
                "completed" => Some(protocol::JobState::Completed),
                "failed" => Some(protocol::JobState::Failed),
                "waiting-children" => Some(protocol::JobState::WaitingChildren),
                "waiting-parent" => Some(protocol::JobState::WaitingParent),
                "stalled" => Some(protocol::JobState::Stalled),
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
                "waiting" => protocol::JobState::Waiting,
                "delayed" => protocol::JobState::Delayed,
                "completed" => protocol::JobState::Completed,
                "failed" => protocol::JobState::Failed,
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

        // Already handled above
        Command::Auth { .. } => Response::ok(),
    }
}

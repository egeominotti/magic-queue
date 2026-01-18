mod dashboard;
mod grpc;
mod http;
mod protocol;
mod queue;
mod telemetry;

use mimalloc::MiMalloc;
use tracing::{error, info};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};
use tokio::signal;
use tokio::sync::broadcast;
use tracing::warn;

use protocol::{
    create_binary_frame, deserialize_msgpack, serialize_msgpack, Command, JobState, Request,
    Response, ResponseWithId,
};
use queue::{generate_node_id, QueueManager};

const DEFAULT_TCP_PORT: u16 = 6789;
const DEFAULT_HTTP_PORT: u16 = 6790;
const DEFAULT_GRPC_PORT: u16 = 6791;
const UNIX_SOCKET_PATH: &str = "/tmp/flashq.sock";
const SHUTDOWN_TIMEOUT_SECS: u64 = 30;

struct ConnectionState {
    authenticated: bool,
}

/// Global shutdown flag for graceful shutdown
static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

/// Check if shutdown has been requested
fn is_shutting_down() -> bool {
    SHUTDOWN_FLAG.load(Ordering::Relaxed)
}

/// Create a shutdown signal handler
async fn shutdown_signal(shutdown_tx: broadcast::Sender<()>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, starting graceful shutdown...");
    SHUTDOWN_FLAG.store(true, Ordering::Relaxed);
    let _ = shutdown_tx.send(());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize telemetry (structured logging)
    telemetry::init();

    // Create shutdown channel for graceful shutdown
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let shutdown_tx_signal = shutdown_tx.clone();

    // Spawn shutdown signal handler
    tokio::spawn(async move {
        shutdown_signal(shutdown_tx_signal).await;
    });

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
            info!(node_id = %node_id, "Starting node in cluster mode");
            QueueManager::with_cluster(url, node_id, host, http_port).await
        } else {
            error!("CLUSTER_MODE requires DATABASE_URL to be set");
            std::process::exit(1);
        }
    } else {
        match (&database_url, auth_tokens.is_empty()) {
            (Some(url), true) => QueueManager::with_postgres(url).await,
            (Some(url), false) => QueueManager::with_postgres_and_auth(url, auth_tokens).await,
            (None, true) => QueueManager::new(false),
            (None, false) => {
                info!(token_count = auth_tokens.len(), "Authentication enabled");
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
        let mut shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let router = http::create_router(qm_http);
            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", http_port))
                .await
                .expect("Failed to bind HTTP listener");
            info!(port = http_port, "HTTP API listening");
            info!(url = %format!("http://localhost:{}", http_port), "Dashboard available");
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.recv().await;
                })
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
            let addr = format!("0.0.0.0:{}", grpc_port)
                .parse()
                .expect("valid socket address format");
            info!(port = grpc_port, "gRPC server listening");
            if let Err(e) = grpc::run_grpc_server(addr, qm_grpc).await {
                error!(error = %e, "gRPC server error");
            }
        });
    }

    let mut shutdown_rx = shutdown_tx.subscribe();

    if use_unix {
        let _ = std::fs::remove_file(UNIX_SOCKET_PATH);
        let listener = UnixListener::bind(UNIX_SOCKET_PATH)?;
        info!(
            socket = UNIX_SOCKET_PATH,
            "flashQ TCP server listening (Unix socket)"
        );

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, _)) => {
                            if is_shutting_down() {
                                break;
                            }
                            let qm = Arc::clone(&queue_manager);
                            tokio::spawn(async move {
                                let (reader, writer) = socket.into_split();
                                let _ = handle_connection(reader, writer, qm).await;
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to accept connection");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }
    } else {
        let port = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(DEFAULT_TCP_PORT);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        info!(port = port, "flashQ TCP server listening");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, _)) => {
                            if is_shutting_down() {
                                break;
                            }
                            socket.set_nodelay(true)?;
                            let qm = Arc::clone(&queue_manager);
                            tokio::spawn(async move {
                                let (reader, writer) = socket.into_split();
                                let _ = handle_connection(reader, writer, qm).await;
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to accept connection");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }
    }

    // Graceful shutdown: wait for active jobs to complete
    info!("Stopping new connections, waiting for active jobs to complete...");
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(SHUTDOWN_TIMEOUT_SECS);

    loop {
        let processing = queue_manager.processing_count();
        if processing == 0 {
            info!("All active jobs completed");
            break;
        }

        if start.elapsed() >= timeout {
            warn!(
                remaining_jobs = processing,
                timeout_secs = SHUTDOWN_TIMEOUT_SECS,
                "Shutdown timeout reached, forcing exit"
            );
            break;
        }

        info!(
            processing_jobs = processing,
            elapsed_secs = start.elapsed().as_secs(),
            "Waiting for active jobs to complete..."
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    info!("Shutdown complete");
    Ok(())
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
    let state = Arc::new(RwLock::new(ConnectionState {
        authenticated: false,
    }));

    // Peek first byte to detect protocol
    let first_byte = {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            return Ok(());
        }
        buf[0]
    };

    // Route to appropriate handler based on protocol
    if first_byte == b'{' || first_byte == b'\n' || first_byte == b'\r' {
        // Text protocol (JSON, line-delimited)
        handle_text_protocol(&mut reader, &mut writer, &queue_manager, &state).await
    } else {
        // Binary protocol (MessagePack, length-prefixed)
        handle_binary_protocol(&mut reader, &mut writer, &queue_manager, &state).await
    }
}

/// Handle text protocol (JSON, newline-delimited)
async fn handle_text_protocol<R, W>(
    reader: &mut BufReader<R>,
    writer: &mut BufWriter<W>,
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut line = String::with_capacity(8192);

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            break;
        }

        let response = process_command_text(&mut line, queue_manager, state).await;
        let response_json = serde_json::to_string(&response)?;
        writer.write_all(response_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        // Pipelining: only flush if no more commands waiting in buffer
        if reader.buffer().is_empty() {
            writer.flush().await?;
        }
    }

    Ok(())
}

/// Handle binary protocol (MessagePack, length-prefixed frames)
/// Frame format: [4 bytes length (big-endian u32)] [N bytes MessagePack data]
async fn handle_binary_protocol<R, W>(
    reader: &mut BufReader<R>,
    writer: &mut BufWriter<W>,
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut len_buf = [0u8; 4];
    let mut data_buf = Vec::with_capacity(8192);

    loop {
        // Read 4-byte length prefix
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_be_bytes(len_buf) as usize;

        // Sanity check: max 16MB per message
        if len > 16 * 1024 * 1024 {
            let err_response = ResponseWithId::new(Response::error("Message too large"), None);
            let err_bytes = serialize_msgpack(&err_response)?;
            let frame = create_binary_frame(&err_bytes);
            writer.write_all(&frame).await?;
            writer.flush().await?;
            continue;
        }

        // Read message data
        data_buf.clear();
        data_buf.resize(len, 0);
        reader.read_exact(&mut data_buf).await?;

        // Process command
        let response = process_command_binary(&data_buf, queue_manager, state).await;

        // Serialize and send response
        let response_bytes = serialize_msgpack(&response)?;
        let frame = create_binary_frame(&response_bytes);
        writer.write_all(&frame).await?;

        // Pipelining: only flush if no more commands waiting in buffer
        if reader.buffer().is_empty() {
            writer.flush().await?;
        }
    }

    Ok(())
}

/// Process command from binary (MessagePack) input
#[inline(always)]
async fn process_command_binary(
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
async fn process_command_text(
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
async fn process_request(
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

    let response = match command {
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
            let timeout_ms = timeout.unwrap_or(60_000); // Default 60s
                                                        // Use distributed pull in cluster mode for consistency
            if queue_manager.is_distributed_pull() {
                match queue_manager.pull_distributed(&queue, timeout_ms).await {
                    Some(job) => Response::job(job),
                    None => {
                        // Distributed pull timed out - return null
                        Response::null_job()
                    }
                }
            } else {
                // Use tokio timeout wrapper for non-distributed pull
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(timeout_ms),
                    queue_manager.pull(&queue),
                )
                .await
                {
                    Ok(job) => Response::job(job),
                    Err(_) => Response::null_job(), // Timeout - return null
                }
            }
        }
        Command::Pullb {
            queue,
            count,
            timeout,
        } => {
            let timeout_ms = timeout.unwrap_or(60_000); // Default 60s
                                                        // Use distributed pull in cluster mode for consistency
            if queue_manager.is_distributed_pull() {
                let jobs = queue_manager
                    .pull_distributed_batch(&queue, count, timeout_ms)
                    .await;
                if jobs.is_empty() {
                    Response::null_job() // Timeout or no jobs
                } else {
                    Response::jobs(jobs)
                }
            } else {
                // Use tokio timeout wrapper for non-distributed pull
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(timeout_ms),
                    queue_manager.pull_batch(&queue, count),
                )
                .await
                {
                    Ok(jobs) => Response::jobs(jobs),
                    Err(_) => Response::null_job(), // Timeout - return empty
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
                    return ResponseWithId::new(
                        Response::error("Invalid state. Use: waiting, delayed, completed, failed"),
                        req_id,
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
    };

    ResponseWithId::new(response, req_id)
}

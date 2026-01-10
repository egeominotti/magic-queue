mod dashboard;
mod http;
mod protocol;
mod queue;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::sync::Arc;

use axum;
use parking_lot::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};

use protocol::{Command, Response};
use queue::QueueManager;

const DEFAULT_TCP_PORT: u16 = 6789;
const DEFAULT_HTTP_PORT: u16 = 6790;
const UNIX_SOCKET_PATH: &str = "/tmp/magic-queue.sock";

struct ConnectionState {
    authenticated: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let use_unix = std::env::var("UNIX_SOCKET").is_ok();
    let persistence = std::env::var("PERSIST").is_ok();
    let enable_http = std::env::var("HTTP").is_ok();

    // Parse auth tokens from environment
    let auth_tokens: Vec<String> = std::env::var("AUTH_TOKENS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let queue_manager = if auth_tokens.is_empty() {
        QueueManager::new(persistence)
    } else {
        println!("Authentication enabled with {} token(s)", auth_tokens.len());
        QueueManager::with_auth_tokens(persistence, auth_tokens)
    };

    if persistence {
        println!("Persistence enabled (WAL)");
    }

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
            axum::serve(listener, router).await.expect("HTTP server error");
        });
    }

    if use_unix {
        let _ = std::fs::remove_file(UNIX_SOCKET_PATH);
        let listener = UnixListener::bind(UNIX_SOCKET_PATH)?;
        println!("MagicQueue TCP server listening on {}", UNIX_SOCKET_PATH);

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
        println!("MagicQueue TCP server listening on port {}", port);

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
    let state = Arc::new(RwLock::new(ConnectionState { authenticated: false }));

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            break;
        }

        let response = process_command(&line, &queue_manager, &state).await;
        let response_json = serde_json::to_string(&response)?;
        writer.write_all(response_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    Ok(())
}

#[inline(always)]
async fn process_command(
    line: &str,
    queue_manager: &Arc<QueueManager>,
    state: &Arc<RwLock<ConnectionState>>,
) -> Response {
    let command: Command = match serde_json::from_str(line.trim()) {
        Ok(cmd) => cmd,
        Err(e) => return Response::error(format!("Invalid: {}", e)),
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
        } => {
            match queue_manager
                .push(queue, data, priority, delay, ttl, timeout, max_attempts, backoff, unique_key, depends_on)
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
            let job = queue_manager.pull(&queue).await;
            Response::job(job)
        }
        Command::Pullb { queue, count } => {
            let jobs = queue_manager.pull_batch(&queue, count).await;
            Response::jobs(jobs)
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

        // === New Commands ===
        Command::Cancel { id } => match queue_manager.cancel(id).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e),
        },
        Command::Progress { id, progress, message } => {
            match queue_manager.update_progress(id, progress, message).await {
                Ok(()) => Response::ok(),
                Err(e) => Response::error(e),
            }
        }
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

        // === Cron Jobs ===
        Command::Cron { name, queue, data, schedule, priority } => {
            queue_manager.add_cron(name, queue, data, schedule, priority).await;
            Response::ok()
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

        // Already handled above
        Command::Auth { .. } => Response::ok(),
    }
}

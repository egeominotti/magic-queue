mod dashboard;
mod grpc;
mod http;
mod protocol;
mod queue;
mod runtime;
mod server;
mod telemetry;

use mimalloc::MiMalloc;
use tracing::{error, info, warn};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::net::{TcpListener, UnixListener};
use tokio::signal;
use tokio::sync::broadcast;

use queue::{generate_node_id, QueueManager};
use server::handle_connection;

const DEFAULT_TCP_PORT: u16 = 6789;
const DEFAULT_HTTP_PORT: u16 = 6790;
const DEFAULT_GRPC_PORT: u16 = 6791;
const UNIX_SOCKET_PATH: &str = "/tmp/flashq.sock";
const SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Global shutdown flag for graceful shutdown
static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

/// Check if shutdown has been requested
fn is_shutting_down() -> bool {
    SHUTDOWN_FLAG.load(Ordering::Relaxed)
}

/// Create a shutdown signal handler
async fn shutdown_signal(shutdown_tx: broadcast::Sender<()>) {
    let ctrl_c = async {
        match signal::ctrl_c().await {
            Ok(()) => {}
            Err(e) => {
                warn!(error = %e, "Failed to install Ctrl+C handler, continuing without it");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "Failed to install SIGTERM handler, continuing without it");
                std::future::pending::<()>().await;
            }
        }
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

    // Print runtime information
    runtime::print_runtime_info();

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
    let queue_manager =
        create_queue_manager(enable_cluster, &database_url, &auth_tokens, http_port).await;

    // Start HTTP server if enabled
    if enable_http {
        start_http_server(&queue_manager, &shutdown_tx).await;
    }

    // Start gRPC server if enabled
    if enable_grpc {
        start_grpc_server(&queue_manager).await;
    }

    // Run main TCP server loop
    run_tcp_server(use_unix, &queue_manager, &shutdown_tx).await?;

    // Graceful shutdown
    graceful_shutdown(&queue_manager).await;

    Ok(())
}

/// Create QueueManager based on configuration
async fn create_queue_manager(
    enable_cluster: bool,
    database_url: &Option<String>,
    auth_tokens: &[String],
    http_port: i32,
) -> Arc<QueueManager> {
    if enable_cluster {
        if let Some(url) = database_url {
            let node_id = generate_node_id();
            let host = std::env::var("NODE_HOST").unwrap_or_else(|_| "localhost".to_string());
            info!(node_id = %node_id, "Starting node in cluster mode");
            QueueManager::with_cluster(url, node_id, host, http_port).await
        } else {
            error!("CLUSTER_MODE requires DATABASE_URL to be set");
            std::process::exit(1);
        }
    } else {
        match (database_url, auth_tokens.is_empty()) {
            (Some(url), true) => QueueManager::with_postgres(url).await,
            (Some(url), false) => {
                QueueManager::with_postgres_and_auth(url, auth_tokens.to_vec()).await
            }
            (None, true) => QueueManager::new(false),
            (None, false) => {
                info!(token_count = auth_tokens.len(), "Authentication enabled");
                QueueManager::with_auth_tokens(false, auth_tokens.to_vec())
            }
        }
    }
}

/// Start HTTP server in background
async fn start_http_server(queue_manager: &Arc<QueueManager>, shutdown_tx: &broadcast::Sender<()>) {
    let http_port = std::env::var("HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_HTTP_PORT);

    if http_port == 0 {
        error!(port = http_port, "Invalid HTTP port, must be 1-65535");
        std::process::exit(1);
    }

    let qm_http = Arc::clone(queue_manager);
    let mut shutdown_rx = shutdown_tx.subscribe();

    tokio::spawn(async move {
        let router = http::create_router(qm_http);
        let listener = match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", http_port)).await {
            Ok(l) => l,
            Err(e) => {
                error!(port = http_port, error = %e, "Failed to bind HTTP listener");
                return;
            }
        };
        info!(port = http_port, "HTTP API listening");
        info!(url = %format!("http://localhost:{}", http_port), "Dashboard available");
        if let Err(e) = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
        {
            error!(error = %e, "HTTP server error");
        }
    });
}

/// Start gRPC server in background
async fn start_grpc_server(queue_manager: &Arc<QueueManager>) {
    let grpc_port = std::env::var("GRPC_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_GRPC_PORT);

    if grpc_port == 0 {
        error!(port = grpc_port, "Invalid gRPC port, must be 1-65535");
        std::process::exit(1);
    }

    let qm_grpc = Arc::clone(queue_manager);

    tokio::spawn(async move {
        let addr_str = format!("0.0.0.0:{}", grpc_port);
        let addr = match addr_str.parse() {
            Ok(a) => a,
            Err(e) => {
                error!(addr = %addr_str, error = %e, "Invalid gRPC address format");
                return;
            }
        };
        info!(port = grpc_port, "gRPC server listening");
        if let Err(e) = grpc::run_grpc_server(addr, qm_grpc).await {
            error!(error = %e, "gRPC server error");
        }
    });
}

/// Run main TCP server loop
async fn run_tcp_server(
    use_unix: bool,
    queue_manager: &Arc<QueueManager>,
    shutdown_tx: &broadcast::Sender<()>,
) -> Result<(), Box<dyn std::error::Error>> {
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
                            let qm = Arc::clone(queue_manager);
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
                            let qm = Arc::clone(queue_manager);
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

    Ok(())
}

/// Graceful shutdown: wait for active jobs to complete
async fn graceful_shutdown(queue_manager: &Arc<QueueManager>) {
    queue_manager.shutdown();

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
}

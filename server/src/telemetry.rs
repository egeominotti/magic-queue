//! Telemetry and observability setup.
//!
//! Provides structured logging with tracing.

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize telemetry (tracing).
///
/// Log level can be set via RUST_LOG env var:
/// - RUST_LOG=debug (verbose)
/// - RUST_LOG=info (default)
/// - RUST_LOG=warn (quiet)
/// - RUST_LOG=flashq_server=debug,sqlx=warn (per-crate)
pub fn init() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,hyper=warn,tower=warn"));

    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .compact();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

/// Initialize telemetry with JSON output (for production/log aggregators).
#[allow(dead_code)]
pub fn init_json() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,hyper=warn,tower=warn"));

    let fmt_layer = fmt::layer()
        .json()
        .with_target(true)
        .with_current_span(true);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

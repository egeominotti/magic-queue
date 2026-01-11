# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this codebase.

## Project Overview

MagicQueue is a high-performance job queue server built with Rust.

## Key Commands

### Server (Rust)

```bash
cd server

# Development
cargo run

# Production (optimized)
cargo run --release

# With persistence
PERSIST=1 cargo run --release

# With HTTP API & Dashboard
HTTP=1 cargo run --release

# With Unix socket
UNIX_SOCKET=1 cargo run --release

# Run tests
cargo test
```

### Docker

```bash
# Build and run
docker build -t magic-queue .
docker run -p 6789:6789 magic-queue

# With persistence
docker run -p 6789:6789 -e PERSIST=1 -v $(pwd)/data:/app magic-queue
```

## Architecture

### Server Structure (< 350 lines per file)

```
server/src/
├── main.rs           # TCP/Unix socket server, command routing
├── http.rs           # HTTP REST API (axum)
├── dashboard.rs      # Web dashboard
├── protocol.rs       # Command/Response types, Job struct, JobState enum
└── queue/
    ├── mod.rs        # Module exports
    ├── types.rs      # WalEvent, RateLimiter, Shard, GlobalMetrics
    ├── manager.rs    # QueueManager struct, WAL, get_job, get_state
    ├── core.rs       # Core ops: push, pull, ack, fail
    ├── features.rs   # Advanced: cancel, progress, DLQ, cron, metrics
    ├── background.rs # Background tasks: cleanup, cron runner
    └── tests.rs      # Unit tests
```

### Key Design Decisions

1. **Global Processing Map**: Jobs in processing are stored globally (not sharded) to avoid shard lookup issues on ack/fail
2. **32 Shards**: Queues are sharded by queue name for parallel access
3. **BinaryHeap Priority**: Higher priority = larger in Ord (popped first)
4. **parking_lot Locks**: Faster than std::sync
5. **Implicit Job State**: State is determined by job location (queues, processing, dlq, etc.)

## Protocol Commands

| Command | Description |
|---------|-------------|
| PUSH | Push job with options (priority, delay, ttl, etc.) |
| PUSHB | Batch push |
| PULL | Pull single job (blocking) |
| PULLB | Batch pull |
| ACK | Acknowledge job completion |
| ACKB | Batch acknowledge |
| FAIL | Fail job (retry or DLQ) |
| GETJOB | Get job with its current state |
| GETSTATE | Get job state only |
| GETRESULT | Get job result |
| CANCEL | Cancel pending job |
| PROGRESS | Update job progress |
| GETPROGRESS | Get job progress |
| DLQ | Get dead letter queue jobs |
| RETRYDLQ | Retry DLQ jobs |
| RATELIMIT | Set queue rate limit |
| RATELIMITCLEAR | Clear rate limit |
| SETCONCURRENCY | Set concurrency limit |
| CLEARCONCURRENCY | Clear concurrency limit |
| PAUSE | Pause queue |
| RESUME | Resume queue |
| LISTQUEUES | List all queues |
| CRON | Add cron job |
| CRONDELETE | Delete cron job |
| CRONLIST | List cron jobs |
| STATS | Get queue stats |
| METRICS | Get detailed metrics |

## Job States

| State | Location | Description |
|-------|----------|-------------|
| waiting | queues (BinaryHeap) | Ready to be processed |
| delayed | queues (run_at > now) | Scheduled for future |
| active | processing (HashMap) | Being processed |
| completed | completed_jobs (Set) | Successfully done |
| failed | dlq (VecDeque) | In dead letter queue |
| waiting-children | waiting_deps (HashMap) | Waiting for dependencies |

## Features

### Core
- Batch operations (PUSH/PULL/ACK)
- Job priorities (BinaryHeap)
- Delayed jobs (run_at timestamp)
- Job state tracking (GETJOB/GETSTATE)
- WAL persistence

### Advanced
- **Dead Letter Queue**: max_attempts → DLQ
- **Exponential Backoff**: backoff * 2^attempts
- **Job TTL**: Automatic expiration
- **Unique Jobs**: Deduplication by key
- **Job Dependencies**: depends_on array
- **Rate Limiting**: Token bucket per queue
- **Concurrency Control**: Limit parallel processing
- **Progress Tracking**: 0-100% with message
- **Cron Jobs**: */N second intervals
- **Pause/Resume**: Dynamic queue control

## Common Tasks

### Adding a new command

1. Add variant to `Command` enum in `protocol.rs`
2. Add response type if needed
3. Handle in `process_command()` in `main.rs`
4. Implement in appropriate `queue/*.rs` file

### Adding tests

Add to `server/src/queue/tests.rs`:

```rust
#[tokio::test]
async fn test_feature_name() {
    let qm = setup();
    // Test logic
    assert!(result.is_ok());
}
```

## Performance

| Metric | Throughput |
|--------|------------|
| Push (batch) | 1.9M jobs/sec |
| Processing (no-op) | 280k jobs/sec |
| Processing (CPU work) | 196k jobs/sec |

### Optimizations
- mimalloc allocator
- parking_lot locks
- Atomic u64 IDs
- 32 shards
- LTO build
- Coarse timestamps (cached)
- String interning

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this codebase.

## Project Overview

FlashQ is a high-performance job queue server built with Rust.

## Key Commands

### Server (Rust)

```bash
cd server

# Development
cargo run

# Production (optimized)
cargo run --release

# With PostgreSQL persistence
DATABASE_URL=postgres://user:pass@localhost/flashq cargo run --release

# With HTTP API & Dashboard
HTTP=1 cargo run --release

# With gRPC API
GRPC=1 cargo run --release

# With Unix socket
UNIX_SOCKET=1 cargo run --release

# With Clustering (HA mode)
CLUSTER_MODE=1 NODE_ID=node-1 DATABASE_URL=postgres://user:pass@localhost/flashq HTTP=1 cargo run --release

# Run tests
cargo test
```

### TypeScript SDK (Bun)

```bash
cd sdk/typescript

# Run comprehensive API tests
bun run examples/comprehensive-test.ts

# Run stress tests
bun run examples/stress-test.ts
```

### Docker Compose (Recommended)

```bash
# Start PostgreSQL + FlashQ
docker-compose up -d

# View logs
docker-compose logs -f flashq
```

### Docker (Standalone)

```bash
# Build and run
docker build -t flashq .
docker run -p 6789:6789 flashq
```

## Architecture

### Server Structure (< 350 lines per file)

```
server/src/
├── main.rs           # TCP/Unix socket server, command routing
├── http.rs           # HTTP REST API + WebSocket (axum)
├── grpc.rs           # gRPC API (tonic)
├── dashboard.rs      # Web dashboard
├── protocol.rs       # Command/Response types, Job struct, JobState enum
└── queue/
    ├── mod.rs        # Module exports
    ├── types.rs      # RateLimiter, Shard, GlobalMetrics, JobLocation
    ├── manager.rs    # QueueManager struct, PostgreSQL, get_job, get_state
    ├── postgres.rs   # PostgreSQL storage layer
    ├── cluster.rs    # Clustering and leader election
    ├── core.rs       # Core ops: push, pull, ack, fail
    ├── features.rs   # Advanced: cancel, progress, DLQ, cron, metrics
    ├── background.rs # Background tasks: cleanup, cron runner
    └── tests.rs      # Unit tests (81 tests)
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

## Job Lifecycle Flow

```
PUSH --> [WAITING/DELAYED/WAITING_CHILDREN]
              |
              v (time/deps ready)
           [WAITING]
              |
              v (PULL)
           [ACTIVE]
              |
     +--------+--------+
     |                 |
   ACK               FAIL
     |                 |
     v                 v
[COMPLETED]    attempts < max?
                  |        |
                 YES       NO
                  |        |
                  v        v
              [RETRY]   [DLQ]
                  |
                  v
              [WAITING]
```

## Background Task Intervals

| Task | Interval | Description |
|------|----------|-------------|
| Wakeup | 100ms | Notify workers, check dependencies |
| Timeout | 500ms | Check and fail timed-out jobs |
| Cron | 1s | Execute scheduled cron jobs |
| Metrics | 5s | Collect metrics history |
| Cleanup | 60s | Clean completed jobs, results, index |

## Features

### Core
- Batch operations (PUSH/PULL/ACK)
- Job priorities (BinaryHeap)
- Delayed jobs (run_at timestamp)
- Job state tracking (GETJOB/GETSTATE)
- PostgreSQL persistence

### Advanced
- **Dead Letter Queue**: max_attempts → DLQ
- **Exponential Backoff**: backoff * 2^attempts
- **Job TTL**: Automatic expiration
- **Unique Jobs**: Deduplication by key
- **Job Dependencies**: depends_on array
- **Rate Limiting**: Token bucket per queue
- **Concurrency Control**: Limit parallel processing
- **Progress Tracking**: 0-100% with message
- **Cron Jobs**: Full 6-field cron expressions (sec min hour day month weekday)
- **Pause/Resume**: Dynamic queue control
- **WebSocket**: Real-time events with token auth
- **SSE**: Server-Sent Events for job lifecycle
- **Webhooks**: HTTP callbacks on job events
- **Prometheus Metrics**: `/metrics/prometheus` endpoint
- **Clustering/HA**: Multi-node support with automatic leader election

## Clustering (High Availability)

FlashQ supports clustering for high availability using PostgreSQL as the coordination layer.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `CLUSTER_MODE=1` | Enable cluster mode |
| `NODE_ID=node-1` | Unique node identifier (auto-generated if not set) |
| `NODE_HOST=localhost` | Host address for node registration |
| `DATABASE_URL` | PostgreSQL connection (required for clustering) |

### Architecture

```
┌──────────┐    ┌──────────┐    ┌──────────┐
│  Node 1  │    │  Node 2  │    │  Node 3  │
│ (Leader) │    │(Follower)│    │(Follower)│
└────┬─────┘    └────┬─────┘    └────┬─────┘
     │               │               │
     └───────────────┼───────────────┘
                     │
              ┌──────▼──────┐
              │  PostgreSQL │
              │  (Shared)   │
              └─────────────┘
```

### Leader Election

- Uses PostgreSQL advisory locks (`pg_try_advisory_lock`)
- Only the leader runs background tasks (cron, cleanup, timeout checks)
- All nodes handle client requests (push/pull/ack)
- Automatic failover when leader crashes (within 5 seconds)
- Stale nodes cleaned up after 30 seconds of no heartbeat

### HTTP Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Node health with leader/follower status |
| `GET /cluster/nodes` | List all nodes in cluster |

### Example: Multi-Node Setup

```bash
# Start Node 1 (becomes leader)
CLUSTER_MODE=1 NODE_ID=node-1 DATABASE_URL=postgres://... HTTP=1 HTTP_PORT=6790 PORT=6789 ./flashq-server

# Start Node 2 (becomes follower)
CLUSTER_MODE=1 NODE_ID=node-2 DATABASE_URL=postgres://... HTTP=1 HTTP_PORT=6792 PORT=6793 ./flashq-server

# Check cluster status
curl http://localhost:6790/cluster/nodes
```

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

## Security

### Input Validation
- **Queue names**: Only alphanumeric, underscore, hyphen, dot allowed (max 256 chars)
- **Job data size**: Max 1MB per job to prevent DoS
- **Batch limits**: Max 1000 jobs per batch request (gRPC/HTTP)
- **Cron schedules**: Max 256 chars, validated before saving

### Memory Management
- **Completed jobs**: Cleanup when exceeding 50K entries (removes oldest 25K)
- **Job results**: Cleanup when exceeding 5K entries (removes oldest 2.5K)
- **Job index**: Stale entries cleaned when exceeding 100K
- **Interned strings**: Limited to 10K unique queue names

### Authentication
- Token-based auth via `AUTH_TOKENS` env variable
- WebSocket connections require `?token=xxx` parameter
- HMAC-SHA256 webhook signatures using `hmac` and `sha2` crates

### Prometheus Metrics
- Queue names in labels are escaped to prevent injection attacks

### gRPC Streaming
- Stream connections use timeout-based polling to detect client disconnects
- Prevents resource leaks from abandoned streams

## Stress Test Results

The system has been validated with 33 stress tests:

| Test | Result |
|------|--------|
| Concurrent Push (10 connections) | 59,000 ops/sec |
| Batch Operations (10K jobs) | Push: 14ms, Pull+Ack: 29ms |
| Large Payloads (500KB) | Integrity preserved |
| Many Queues (50 simultaneous) | All processed |
| Rate Limiting | Enforced correctly |
| Concurrency Limit (5) | Max concurrent respected |
| DLQ Flood (100 jobs) | 100% to DLQ, 100% retry |
| Rapid Cancel (100 concurrent) | 100% cancelled |
| Invalid Input (7 attacks) | 100% rejected |
| Connection Churn (50 cycles) | 100% success |
| Unique Key Collision (50 concurrent) | Deduplication works |
| Sustained Load (30s) | 22K push/s, 11K pull/s, 0% errors |

## SDK Structure

```
sdk/typescript/
├── src/
│   ├── index.ts    # Main exports
│   ├── client.ts   # FlashQ client (TCP/HTTP)
│   ├── worker.ts   # Worker class for job processing
│   └── types.ts    # TypeScript type definitions
└── examples/
    ├── comprehensive-test.ts  # 34 API tests
    └── stress-test.ts         # 33 stress tests
```

### SDK Client Methods

| Method | Description |
|--------|-------------|
| `connect()` | Connect to server |
| `close()` | Close connection |
| `push(queue, data, options?)` | Push a job |
| `pushBatch(queue, jobs)` | Push multiple jobs |
| `pull(queue)` | Pull a job (blocking) |
| `pullBatch(queue, count)` | Pull multiple jobs |
| `ack(jobId, result?)` | Acknowledge job |
| `ackBatch(jobIds)` | Acknowledge multiple jobs |
| `fail(jobId, error?)` | Fail a job |
| `cancel(jobId)` | Cancel a pending job |
| `progress(jobId, progress, message?)` | Update progress |
| `getProgress(jobId)` | Get job progress |
| `getJob(jobId)` | Get job with state |
| `getState(jobId)` | Get job state only |
| `getResult(jobId)` | Get job result |
| `getDlq(queue, count?)` | Get DLQ jobs |
| `retryDlq(queue, jobId?)` | Retry DLQ jobs |
| `pause(queue)` | Pause a queue |
| `resume(queue)` | Resume a queue |
| `setRateLimit(queue, limit)` | Set rate limit |
| `clearRateLimit(queue)` | Clear rate limit |
| `setConcurrency(queue, limit)` | Set concurrency limit |
| `clearConcurrency(queue)` | Clear concurrency limit |
| `listQueues()` | List all queues |
| `addCron(name, options)` | Add cron job |
| `deleteCron(name)` | Delete cron job |
| `listCrons()` | List cron jobs |
| `stats()` | Get queue statistics |
| `metrics()` | Get detailed metrics |

<div align="center">

# MagicQueue

**High-Performance Job Queue System**

A blazingly fast, zero-dependency job queue built with Rust.

[![Rust](https://img.shields.io/badge/Rust-000000?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[Features](#features) • [Quick Start](#quick-start) • [Documentation](#documentation) • [Benchmarks](#benchmarks)

</div>

---

## Why MagicQueue?

- **No Redis Required** — Self-contained server with optional persistence
- **Ultra-Low Latency** — Sub-100us P99 latency for job operations
- **High Throughput** — 280,000+ ops/sec for batch operations
- **Production Ready** — Dead letter queues, retries, rate limiting, and more

## Benchmarks

Tested on Apple Silicon M2, single server instance.

| Operation | Throughput | P99 Latency |
|-----------|------------|-------------|
| Sequential Push | 15,000 ops/sec | 93 us |
| Batch Push | 285,000 ops/sec | — |
| Full Cycle (Push->Pull->Ack) | 5,800 ops/sec | — |

### vs BullMQ (Redis)

| Benchmark | MagicQueue | BullMQ | Improvement |
|-----------|------------|--------|-------------|
| Sequential Push | 15,015 ops/s | 4,533 ops/s | **3.3x faster** |
| Batch Push | 294,118 ops/s | 36,232 ops/s | **8.1x faster** |
| P99 Latency | 125 us | 414 us | **3.3x lower** |

---

## Features

### Core
| Feature | Description |
|---------|-------------|
| **Priority Queues** | Higher priority jobs execute first |
| **Delayed Jobs** | Schedule jobs to run in the future |
| **Batch Operations** | Push/pull/ack thousands of jobs in single requests |
| **Job Results** | Store and retrieve job completion results |
| **Job State** | Query job state (waiting, active, completed, failed, delayed) |
| **Persistence** | PostgreSQL backend for full durability |

### Reliability
| Feature | Description |
|---------|-------------|
| **Dead Letter Queue** | Failed jobs moved to DLQ after N attempts |
| **Exponential Backoff** | Configurable retry delays |
| **Job Timeout** | Auto-fail jobs exceeding time limits |
| **Job TTL** | Automatic expiration of stale jobs |
| **Unique Jobs** | Deduplication via custom keys |

### Flow Control
| Feature | Description |
|---------|-------------|
| **Rate Limiting** | Token bucket algorithm per queue |
| **Concurrency Control** | Limit parallel job processing |
| **Pause/Resume** | Dynamic queue control |
| **Job Dependencies** | DAG-style job orchestration |
| **Cron Jobs** | Full 6-field cron expressions (sec min hour day month weekday) |

### Observability
| Feature | Description |
|---------|-------------|
| **Progress Tracking** | Real-time job progress (0-100%) |
| **Metrics API** | Throughput, latency, queue depths |
| **Prometheus Metrics** | `/metrics/prometheus` endpoint |
| **Web Dashboard** | Real-time monitoring UI |
| **SSE Events** | Server-Sent Events for job lifecycle |
| **WebSocket** | Real-time events with token authentication |

---

## Quick Start

### Docker Compose (Recommended)

```bash
# Start with PostgreSQL persistence
docker-compose up -d

# View logs
docker-compose logs -f magicqueue
```

This starts:
- **PostgreSQL** on port 5432
- **MagicQueue** with TCP (6789), HTTP (6790), and gRPC (6791)

### Docker (Standalone)

```bash
# Build and run
docker build -t magic-queue .
docker run -p 6789:6789 magic-queue

# With HTTP API & Dashboard
docker run -p 6789:6789 -p 6790:6790 -e HTTP=1 magic-queue
```

### From Source

```bash
cd server
cargo run --release
```

The server listens on **port 6789** (TCP) by default.

### With PostgreSQL Persistence

```bash
# Set DATABASE_URL to enable PostgreSQL persistence
DATABASE_URL=postgres://user:pass@localhost/magicqueue HTTP=1 cargo run --release
```

### Enable HTTP API & Dashboard

```bash
HTTP=1 cargo run --release
```

- **TCP API:** `localhost:6789`
- **HTTP API:** `localhost:6790`
- **gRPC API:** `localhost:6791`
- **Dashboard:** `http://localhost:6790`

---

## Documentation

### Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `PORT` | TCP server port | `6789` |
| `HTTP` | Enable HTTP API | disabled |
| `HTTP_PORT` | HTTP API port | `6790` |
| `GRPC` | Enable gRPC API | disabled |
| `GRPC_PORT` | gRPC API port | `6791` |
| `DATABASE_URL` | PostgreSQL connection URL | disabled |
| `AUTH_TOKENS` | Comma-separated auth tokens | disabled |
| `UNIX_SOCKET` | Unix socket path | disabled |

### Job States

Jobs can be in one of the following states:

| State | Description |
|-------|-------------|
| `waiting` | In queue, ready to be processed |
| `delayed` | In queue, but scheduled for future execution |
| `active` | Currently being processed by a worker |
| `completed` | Successfully completed |
| `failed` | In DLQ after exceeding max attempts |
| `waiting-children` | Waiting for dependencies to complete |

### HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Dashboard UI |
| `GET` | `/queues` | List all queues |
| `POST` | `/queues/{queue}/jobs` | Push a job |
| `GET` | `/queues/{queue}/jobs` | Pull jobs |
| `POST` | `/jobs/{id}/ack` | Acknowledge job |
| `POST` | `/jobs/{id}/fail` | Fail job |
| `GET` | `/jobs/{id}` | Get job with state |
| `GET` | `/jobs/{id}/state` | Get job state only |
| `GET` | `/stats` | Get statistics |
| `GET` | `/metrics` | Get metrics |
| `GET` | `/metrics/prometheus` | Prometheus format metrics |
| `GET` | `/events` | SSE event stream |
| `GET` | `/ws?token=xxx` | WebSocket event stream |

<details>
<summary>View all endpoints</summary>

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/queues/{queue}/pause` | Pause queue |
| `POST` | `/queues/{queue}/resume` | Resume queue |
| `GET` | `/queues/{queue}/dlq` | Get DLQ jobs |
| `POST` | `/queues/{queue}/dlq/retry` | Retry DLQ jobs |
| `POST` | `/queues/{queue}/rate-limit` | Set rate limit |
| `DELETE` | `/queues/{queue}/rate-limit` | Clear rate limit |
| `POST` | `/queues/{queue}/concurrency` | Set concurrency |
| `DELETE` | `/queues/{queue}/concurrency` | Clear concurrency |
| `POST` | `/jobs/{id}/cancel` | Cancel job |
| `POST` | `/jobs/{id}/progress` | Update progress |
| `GET` | `/jobs/{id}/progress` | Get progress |
| `GET` | `/jobs/{id}/result` | Get result |
| `GET` | `/crons` | List cron jobs |
| `POST` | `/crons/{name}` | Create cron job |
| `DELETE` | `/crons/{name}` | Delete cron job |

</details>

### TCP Protocol

JSON messages over TCP, newline-delimited.

```json
// Push job
{"cmd": "PUSH", "queue": "emails", "data": {"to": "user@test.com"}, "priority": 5}

// Pull job
{"cmd": "PULL", "queue": "emails"}

// Acknowledge
{"cmd": "ACK", "id": 123, "result": {"status": "sent"}}

// Fail (triggers retry)
{"cmd": "FAIL", "id": 123, "error": "Connection timeout"}

// Get job with state
{"cmd": "GETJOB", "id": 123}
// Response: {"ok": true, "job": {...}, "state": "active"}

// Get state only
{"cmd": "GETSTATE", "id": 123}
// Response: {"ok": true, "id": 123, "state": "waiting"}
```

<details>
<summary>View all commands</summary>

```json
// Batch operations
{"cmd": "PUSHB", "queue": "jobs", "jobs": [{"data": {...}}]}
{"cmd": "PULLB", "queue": "jobs", "count": 50}
{"cmd": "ACKB", "ids": [1, 2, 3]}

// Job control
{"cmd": "CANCEL", "id": 123}
{"cmd": "PROGRESS", "id": 123, "progress": 50, "message": "Processing..."}
{"cmd": "GETPROGRESS", "id": 123}
{"cmd": "GETRESULT", "id": 123}
{"cmd": "GETJOB", "id": 123}
{"cmd": "GETSTATE", "id": 123}

// DLQ
{"cmd": "DLQ", "queue": "jobs", "count": 10}
{"cmd": "RETRYDLQ", "queue": "jobs"}

// Queue control
{"cmd": "PAUSE", "queue": "jobs"}
{"cmd": "RESUME", "queue": "jobs"}
{"cmd": "RATELIMIT", "queue": "jobs", "limit": 100}
{"cmd": "SETCONCURRENCY", "queue": "jobs", "limit": 5}
{"cmd": "LISTQUEUES"}

// Cron (supports full 6-field expressions or */N shorthand)
{"cmd": "CRON", "name": "cleanup", "queue": "jobs", "data": {}, "schedule": "0 30 9 * * MON-FRI"}
{"cmd": "CRON", "name": "every-minute", "queue": "jobs", "data": {}, "schedule": "*/60"}
{"cmd": "CRONLIST"}
{"cmd": "CRONDELETE", "name": "cleanup"}

// Stats
{"cmd": "STATS"}
{"cmd": "METRICS"}

// Auth
{"cmd": "AUTH", "token": "secret"}
```

</details>

### Job Options

When pushing a job, you can specify:

| Option | Type | Description |
|--------|------|-------------|
| `priority` | `i32` | Higher = more urgent (default: 0) |
| `delay` | `u64` | Delay in milliseconds |
| `ttl` | `u64` | Time-to-live in milliseconds |
| `timeout` | `u64` | Processing timeout in milliseconds |
| `max_attempts` | `u32` | Max retries before DLQ |
| `backoff` | `u64` | Base backoff (exponential: 1s, 2s, 4s...) |
| `unique_key` | `string` | Deduplication key |
| `depends_on` | `[u64]` | Job dependencies (job IDs) |

---

## Architecture

```
+--------------------------------------------------------------+
|                      MagicQueue Server                       |
+--------------------------------------------------------------+
|                                                              |
|   +-----------+    +-----------+    +-----------+           |
|   | TCP Server|    | HTTP Server|    |  Dashboard |           |
|   |  :6789    |    |   :6790   |    |    (Web)   |           |
|   +-----+-----+    +-----+-----+    +-----------+           |
|         |                |                                   |
|         +-------+--------+                                   |
|                 v                                            |
|   +---------------------------------------------------+     |
|   |              Queue Manager                         |     |
|   |  +---------------------------------------------+  |     |
|   |  |         32 Sharded Priority Queues          |  |     |
|   |  |  (BinaryHeap + FxHashMap + parking_lot)     |  |     |
|   |  +---------------------------------------------+  |     |
|   |                                                    |     |
|   |  +----------+ +----------+ +----------+          |     |
|   |  |   DLQ    | |  Rate    | |  Conc.   |          |     |
|   |  |  Store   | | Limiters | | Limiters |          |     |
|   |  +----------+ +----------+ +----------+          |     |
|   +---------------------------------------------------+     |
|                                                              |
|   +---------------------------------------------------+     |
|   |              Background Tasks                      |     |
|   |  * Delayed Job Processor   * Timeout Checker      |     |
|   |  * Dependency Resolver     * Cron Executor        |     |
|   +---------------------------------------------------+     |
|                                                              |
|   +---------------------------------------------------+     |
|   |         PostgreSQL Storage (Optional)             |     |
|   |  * Jobs, Results, DLQ   * Cron Jobs, Webhooks    |     |
|   +---------------------------------------------------+     |
|                                                              |
+--------------------------------------------------------------+
```

### Performance Optimizations

- **FxHashMap** — 2-3x faster hashing than std HashMap
- **parking_lot** — Faster locks than std RwLock/Mutex
- **mimalloc** — High-performance memory allocator
- **32 Shards** — Minimized lock contention
- **Coarse Timestamps** — Cached time to avoid syscalls
- **String Interning** — Reduced allocations for queue names
- **Lazy TTL Deletion** — Check expiration only during pop
- **LTO + codegen-units=1** — Maximum compiler optimization

---

## Project Structure

```
magic-queue/
├── Dockerfile              # Docker build
├── docker-compose.yml      # PostgreSQL + MagicQueue
├── server/                 # Rust server
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs         # Entry point, TCP server
│       ├── http.rs         # HTTP REST API + WebSocket (axum)
│       ├── grpc.rs         # gRPC API (tonic)
│       ├── dashboard.rs    # Web dashboard
│       ├── protocol.rs     # Commands & responses
│       └── queue/
│           ├── manager.rs  # Queue manager
│           ├── postgres.rs # PostgreSQL storage layer
│           ├── core.rs     # Push/Pull/Ack
│           ├── features.rs # Rate limit, DLQ, etc.
│           ├── background.rs # Background tasks, cron
│           ├── types.rs    # Data structures
│           └── tests.rs    # Test suite (75 tests)
```

---

## Testing

```bash
cd server
cargo test
```

**75 tests** covering:
- Core operations (push, pull, ack, batch)
- Reliability (DLQ, retries, backoff)
- Flow control (rate limiting, concurrency, pause)
- Job features (dependencies, progress, TTL)
- Edge cases (unicode, large payloads, concurrency)

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

**Built with Rust for maximum performance**

</div>

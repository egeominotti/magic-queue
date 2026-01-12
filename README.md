<div align="center">

# FlashQ

**High-Performance Job Queue System**

A blazingly fast, zero-dependency job queue built with Rust.

[![Rust](https://img.shields.io/badge/Rust-000000?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[Features](#features) • [Quick Start](#quick-start) • [Documentation](#documentation) • [Benchmarks](#benchmarks)

</div>

---

## Why FlashQ?

- **No Redis Required** — Self-contained server with optional persistence
- **Ultra-Low Latency** — Sub-100us P99 latency for job operations
- **Insane Throughput** — 2,000,000+ ops/sec for batch operations
- **Production Ready** — Dead letter queues, retries, rate limiting, and more

## Benchmarks

Tested on Apple Silicon M2, single server instance.

| Operation | Throughput | Notes |
|-----------|------------|-------|
| Single Push | 10,000 ops/sec | Sequential |
| **Batch Push** | **2,127,660 ops/sec** | 10 connections × batch |
| **Pull + Ack** | **519,388 ops/sec** | Batch operations |

### Protocol Comparison

| Protocol | Single Push | Batch Push | Pull + Ack |
|----------|-------------|------------|------------|
| **TCP** | 6,000/sec | **667,000/sec** | **185,000/sec** |
| Unix Socket | 10,000/sec | 588,000/sec | 192,000/sec |
| HTTP | 4,000/sec | 20,000/sec | 5,000/sec |

### vs BullMQ (Redis)

| Benchmark | FlashQ | BullMQ | Improvement |
|-----------|------------|--------|-------------|
| Sequential Push | 10,000 ops/s | 4,533 ops/s | **2.2x faster** |
| Batch Push | 2,127,660 ops/s | 36,232 ops/s | **58x faster** |
| Pull + Ack | 519,388 ops/s | ~10,000 ops/s | **52x faster** |

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

### Using Makefile (Recommended)

```bash
# Start PostgreSQL + run server with persistence
make up
make persist

# Or run without persistence
make run

# Open dashboard
make dashboard
```

### Makefile Commands

| Command | Description |
|---------|-------------|
| `make dev` | Run in development mode |
| `make run` | Run with HTTP API |
| `make release` | Run optimized build |
| `make persist` | Run with PostgreSQL persistence |
| `make test` | Run Rust tests |
| `make sdk-test` | Run SDK tests |
| `make stress` | Run stress tests |
| `make up` | Start PostgreSQL container |
| `make down` | Stop containers |
| `make dashboard` | Open dashboard in browser |
| `make restart` | Restart server (with persistence) |
| `make stop` | Stop server |

### Docker Compose

```bash
# Start with PostgreSQL persistence
docker-compose up -d

# View logs
docker-compose logs -f flashq
```

This starts:
- **PostgreSQL** on port 5432
- **FlashQ** with TCP (6789), HTTP (6790), and gRPC (6791)

### Docker (Standalone)

```bash
# Build and run
docker build -t flashq .
docker run -p 6789:6789 flashq

# With HTTP API & Dashboard
docker run -p 6789:6789 -p 6790:6790 -e HTTP=1 flashq
```

### From Source

```bash
cd server
cargo run --release
```

The server listens on **port 6789** (TCP) by default.

### With PostgreSQL Persistence

```bash
DATABASE_URL=postgres://user:pass@localhost/flashq HTTP=1 cargo run --release
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
| `POST` | `/server/restart` | Restart server (exit code 100) |
| `POST` | `/server/shutdown` | Shutdown server |

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
|                      FlashQ Server                       |
+--------------------------------------------------------------+
|                                                              |
|   +-----------+    +-----------+    +-----------+           |
|   | TCP Server|    | HTTP Server|    |gRPC Server|           |
|   |  :6789    |    |   :6790   |    |   :6791   |           |
|   +-----+-----+    +-----+-----+    +-----+-----+           |
|         |                |                |                  |
|         +-------+--------+--------+-------+                  |
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
|   |  * Memory Cleanup          * Metrics Collector    |     |
|   +---------------------------------------------------+     |
|                                                              |
|   +---------------------------------------------------+     |
|   |         PostgreSQL Storage (Optional)             |     |
|   |  * Jobs, Results, DLQ   * Cron Jobs, Webhooks    |     |
|   +---------------------------------------------------+     |
|                                                              |
+--------------------------------------------------------------+
```

### Job Lifecycle Flow

```
                              +------------------+
                              |      PUSH        |
                              +--------+---------+
                                       |
                    +------------------+------------------+
                    |                  |                  |
                    v                  v                  v
            +-------+------+   +------+-------+   +------+-------+
            |   WAITING    |   |   DELAYED    |   |  WAITING     |
            | (ready now)  |   | (future time)|   |  CHILDREN    |
            +-------+------+   +------+-------+   +------+-------+
                    |                  |                  |
                    |      (time passes)      (deps complete)
                    |                  |                  |
                    +--------+---------+------------------+
                             |
                             v
                    +--------+---------+
                    |      PULL        |
                    | (worker fetches) |
                    +--------+---------+
                             |
                             v
                    +--------+---------+
                    |     ACTIVE       |
                    | (processing...)  |
                    +--------+---------+
                             |
              +--------------+--------------+
              |                             |
              v                             v
      +-------+-------+            +--------+--------+
      |      ACK      |            |      FAIL       |
      | (success)     |            | (error)         |
      +-------+-------+            +--------+--------+
              |                             |
              v                    +--------+--------+
      +-------+-------+            |  attempts <     |
      |   COMPLETED   |            |  max_attempts?  |
      | (result saved)|            +--------+--------+
      +---------------+                     |
                                +-----------+-----------+
                                |                       |
                                v                       v
                        +-------+-------+       +-------+-------+
                        |    RETRY      |       |     DLQ       |
                        | (with backoff)|       | (dead letter) |
                        +-------+-------+       +---------------+
                                |
                                v
                        +-------+-------+
                        |   WAITING     |
                        | (queued again)|
                        +---------------+
```

### Processing Flow

```
Producer                    FlashQ                     Worker
   |                            |                            |
   |  PUSH {queue, data}        |                            |
   |--------------------------->|                            |
   |                            |                            |
   |  {ok: true, id: 123}       |                            |
   |<---------------------------|                            |
   |                            |                            |
   |                            |         PULL {queue}       |
   |                            |<---------------------------|
   |                            |                            |
   |                            |    {ok: true, job: {...}}  |
   |                            |--------------------------->|
   |                            |                            |
   |                            |                     [process job]
   |                            |                            |
   |                            |  PROGRESS {id, 50, "..."}  |
   |                            |<---------------------------|
   |                            |                            |
   |                            |         ACK {id, result}   |
   |                            |<---------------------------|
   |                            |                            |
   |                            |        {ok: true}          |
   |                            |--------------------------->|
```

### Retry & DLQ Flow

```
                    Job Failed
                         |
                         v
              +----------+----------+
              |  attempts < max?    |
              +----------+----------+
                    |          |
                   YES         NO
                    |          |
                    v          v
           +--------+---+  +---+--------+
           |   RETRY    |  |    DLQ     |
           +--------+---+  +---+--------+
                    |          |
                    v          v
           +--------+---+  +---+--------+
           | Wait for   |  | Store in   |
           | backoff    |  | dead letter|
           | (exp.)     |  | queue      |
           +--------+---+  +---+--------+
                    |          |
                    v          |
           +--------+---+      |
           | Re-queue   |      |
           | with new   |      |
           | run_at     |      |
           +------------+      |
                               v
                    +----------+----------+
                    |    RETRYDLQ cmd     |
                    | (manual intervention)|
                    +----------+----------+
                               |
                               v
                    +----------+----------+
                    |  Re-queue all jobs  |
                    |  from DLQ           |
                    +---------------------+

Backoff Formula: delay = backoff * 2^(attempts-1)
Example (backoff=1000ms): 1s -> 2s -> 4s -> 8s -> 16s
```

### Background Tasks Flow

```
+------------------------------------------------------------------+
|                    Background Task Loop                           |
+------------------------------------------------------------------+
|                                                                   |
|  Every 100ms: WAKEUP                                             |
|  +----------------------------------------------------------+    |
|  | - Notify all waiting workers                              |    |
|  | - Check job dependencies                                  |    |
|  | - Move ready jobs from waiting_deps to queue             |    |
|  +----------------------------------------------------------+    |
|                                                                   |
|  Every 500ms: TIMEOUT CHECK                                      |
|  +----------------------------------------------------------+    |
|  | - Scan processing jobs                                    |    |
|  | - If job.started_at + timeout < now:                     |    |
|  |   - Release concurrency slot                             |    |
|  |   - Retry or move to DLQ                                 |    |
|  +----------------------------------------------------------+    |
|                                                                   |
|  Every 1s: CRON EXECUTOR                                         |
|  +----------------------------------------------------------+    |
|  | - Check all cron jobs                                     |    |
|  | - If next_run <= now:                                    |    |
|  |   - Push new job to queue                                |    |
|  |   - Calculate next_run from schedule                     |    |
|  +----------------------------------------------------------+    |
|                                                                   |
|  Every 5s: METRICS COLLECTION                                    |
|  +----------------------------------------------------------+    |
|  | - Collect queue depths                                    |    |
|  | - Calculate throughput rates                             |    |
|  | - Update Prometheus metrics                              |    |
|  +----------------------------------------------------------+    |
|                                                                   |
|  Every 60s: CLEANUP                                              |
|  +----------------------------------------------------------+    |
|  | - Remove old completed job IDs (>50K)                    |    |
|  | - Remove old job results (>5K)                           |    |
|  | - Clean stale index entries (>100K)                      |    |
|  | - Cleanup unused interned strings (>10K)                 |    |
|  +----------------------------------------------------------+    |
|                                                                   |
+------------------------------------------------------------------+
```

### Rate Limiting & Concurrency Flow

```
                        PULL Request
                              |
                              v
                    +---------+---------+
                    |   Queue Paused?   |
                    +---------+---------+
                         |         |
                        YES        NO
                         |         |
                         v         v
                    +----+----+   +----+----+
                    | Block   |   | Check   |
                    | waiting |   | Rate    |
                    +---------+   | Limit   |
                                  +----+----+
                                       |
                              +--------+--------+
                              |  Tokens > 0?   |
                              +--------+--------+
                                  |         |
                                 YES        NO
                                  |         |
                                  v         v
                             +----+----+   +----+----+
                             | Check   |   | Wait    |
                             | Concurr.|   | for     |
                             +----+----+   | token   |
                                  |        +---------+
                         +--------+--------+
                         | slots available?|
                         +--------+--------+
                              |         |
                             YES        NO
                              |         |
                              v         v
                         +----+----+   +----+----+
                         | Return  |   | Block   |
                         | Job     |   | until   |
                         +---------+   | slot    |
                                       +---------+

Token Bucket: refills at `limit` tokens/second
Concurrency: max `limit` jobs processing simultaneously
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
flashq/
├── Dockerfile              # Docker build
├── docker-compose.yml      # PostgreSQL + FlashQ
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
│           └── tests.rs    # Test suite (81 tests)
├── sdk/
│   └── typescript/         # TypeScript/Bun SDK
│       ├── src/
│       │   ├── index.ts    # Main exports
│       │   ├── client.ts   # FlashQ client
│       │   ├── worker.ts   # Worker class
│       │   └── types.ts    # Type definitions
│       └── examples/
│           ├── comprehensive-test.ts  # API tests (34)
│           └── stress-test.ts         # Stress tests (33)
```

---

## Security

FlashQ includes multiple security measures:

| Feature | Description |
|---------|-------------|
| **Input Validation** | Queue names validated (alphanumeric, underscore, hyphen, dot only) |
| **Job Size Limits** | Max 1MB job data to prevent DoS attacks |
| **Batch Limits** | Max 1000 jobs per batch request (gRPC/HTTP) |
| **Cron Validation** | Schedule expressions validated and length-limited |
| **HMAC Signatures** | Webhook payloads signed with HMAC-SHA256 |
| **Token Auth** | Optional token-based authentication |
| **Prometheus Safety** | Metric labels sanitized to prevent injection |
| **Memory Bounds** | Automatic cleanup of completed jobs, results, and interned strings |

---

## TypeScript SDK

```bash
cd sdk/typescript
bun install
```

```typescript
import { FlashQ } from 'flashq';

// TCP connection (recommended for performance)
const client = new FlashQ({ host: 'localhost', port: 6789 });

// Unix Socket (for same-machine, +71% faster single ops)
const client = new FlashQ({ socketPath: '/tmp/flashq.sock' });

// HTTP (for simplicity)
const client = new FlashQ({ host: 'localhost', useHttp: true });

await client.connect();

// Push a job
const job = await client.push('emails', { to: 'user@example.com' });

// Batch push (2M+ ops/sec)
await client.pushBatch('emails', [
  { data: { to: 'a@test.com' } },
  { data: { to: 'b@test.com' } },
]);

// Pull and process
const pulled = await client.pull('emails');
await client.ack(pulled.id);

// Batch pull + ack (500k+ ops/sec)
const jobs = await client.pullBatch('emails', 100);
await client.ackBatch(jobs.map(j => j.id));

await client.close();
```

### SDK Examples

| Example | Description |
|---------|-------------|
| `comprehensive-test.ts` | Full API test suite (34 tests) |
| `stress-test.ts` | System resilience tests (33 tests) |
| `cpu-intensive-test.ts` | CPU-bound workload benchmark |
| `benchmark-max.ts` | Maximum throughput test (2M+ ops/sec) |
| `protocol-benchmark.ts` | TCP vs HTTP comparison |
| `unix-socket-benchmark.ts` | Unix Socket vs TCP comparison |
| `concurrency-test.ts` | Parallel worker testing |

Run benchmarks:

```bash
cd sdk/typescript
bun run examples/benchmark-max.ts
bun run examples/cpu-intensive-test.ts
```

---

## Testing

### Server Tests (Rust)

```bash
cd server
cargo test
```

**81 unit tests** covering:
- Core operations (push, pull, ack, batch)
- Reliability (DLQ, retries, backoff)
- Flow control (rate limiting, concurrency, pause)
- Job features (dependencies, progress, TTL)
- Edge cases (unicode, large payloads, concurrency)

### SDK Tests (TypeScript)

```bash
cd sdk/typescript
bun run examples/comprehensive-test.ts  # 34 tests
bun run examples/stress-test.ts         # 33 stress tests
```

### Stress Test Results

| Test | Result |
|------|--------|
| Concurrent Push (10 connections) | **59,000 ops/sec** |
| Batch Operations (10K jobs) | Push: 14ms, Pull+Ack: 29ms |
| Large Payloads (500KB) | Integrity preserved |
| Many Queues (50 simultaneous) | All processed |
| Rate Limiting | Enforced correctly |
| Concurrency Limit (5) | Max concurrent: 5 |
| DLQ Flood (100 jobs) | 100% to DLQ, 100% retry |
| Rapid Cancel (100 concurrent) | 100% cancelled |
| Invalid Input (7 attacks) | 100% rejected |
| Connection Churn (50 cycles) | 100% success |
| Unique Key Collision (50 concurrent) | 1 accepted |
| Sustained Load (30s) | 22K push/s, 11K pull/s, 0% errors |

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

**Built with Rust for maximum performance**

</div>

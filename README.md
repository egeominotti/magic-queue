# MagicQueue

A high-performance job queue system built from scratch with **Rust** and **TypeScript/Bun/Python**.

No Redis. No dependencies. Pure performance.

## Benchmarks

Tested on MacOS Apple Silicon with Node.js client, single MagicQueue server instance.

### Throughput

| Benchmark | Operations | Time | Throughput |
|-----------|------------|------|------------|
| Sequential PUSH | 10,000 | 627ms | **15,949 ops/sec** |
| PUSH → PULL → ACK | 5,000 | 855ms | **5,848 cycles/sec** |
| Batch PUSH (100x100) | 10,000 | 35ms | **285,714 ops/sec** |
| Concurrent (10 conn) | 10,000 | 135ms | **74,074 ops/sec** |

### Latency (PUSH operation)

| Percentile | Latency |
|------------|---------|
| Min | 34 µs |
| P50 | 47 µs |
| P95 | 71 µs |
| P99 | 93 µs |
| Max | 129 µs |

### Optimizations

- **mimalloc** - High-performance memory allocator
- **parking_lot** - Faster locks than std
- **Atomic u64 IDs** - No UUID overhead
- **32 shards** - Minimized lock contention
- **LTO** - Link-Time Optimization for smaller, faster binary
- **Buffered I/O** - 128KB read/write buffers

## Features

### Core
- **Batch Operations** - Push/Pull/Ack thousands of jobs in a single request
- **Job Priorities** - Higher priority jobs execute first
- **Delayed Jobs** - Schedule jobs to run in the future
- **Persistence** - Optional WAL (Write-Ahead Log) for durability
- **Unix Socket** - Lower latency than TCP
- **Sharded Architecture** - 32 shards reduce lock contention

### Advanced
- **Dead Letter Queue** - Jobs that fail N times go to DLQ
- **Retry with Backoff** - Exponential backoff on failures
- **Job TTL** - Jobs expire after a specified time
- **Job Timeout** - Auto-fail jobs after N ms
- **Unique Jobs** - Deduplication based on custom keys
- **Job Cancellation** - Cancel pending jobs
- **Rate Limiting** - Limit processing rate per queue (token bucket)
- **Concurrency Control** - Limit concurrent processing per queue
- **Pause/Resume** - Pause and resume queues dynamically
- **Job Progress** - Track progress of long-running jobs (0-100%)
- **Job Results** - Store and retrieve job results
- **Job Dependencies** - Job B runs only after Job A completes
- **Cron Jobs** - Scheduled recurring jobs
- **Pub/Sub Events** - Subscribe to job events
- **Metrics** - Detailed statistics and throughput metrics

### HTTP API & Dashboard
- **REST API** - Full HTTP API alongside TCP protocol
- **Web Dashboard** - Real-time monitoring UI with Tailwind CSS
- **Authentication** - Optional token-based auth

## SDKs

- **TypeScript/Bun** - `client/` directory
- **Python** - `python/` directory

## Quick Start

### Start the Server

```bash
cd server
cargo run --release
```

Server listens on port `6789` by default.

### With HTTP API & Dashboard

```bash
HTTP=1 cargo run --release
```

- TCP API: `localhost:6789`
- HTTP API: `localhost:6790`
- Dashboard: `http://localhost:6790`

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | TCP server port | 6789 |
| `HTTP` | Enable HTTP API | disabled |
| `HTTP_PORT` | HTTP API port | 6790 |
| `PERSIST` | Enable WAL persistence | disabled |
| `AUTH_TOKENS` | Comma-separated auth tokens | disabled |
| `UNIX_SOCKET` | Use Unix socket instead of TCP | disabled |

## HTTP API

When started with `HTTP=1`, the server exposes a REST API.

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Dashboard UI |
| GET | `/queues` | List all queues |
| POST | `/queues/{queue}/jobs` | Push a job |
| GET | `/queues/{queue}/jobs?count=N` | Pull jobs |
| POST | `/queues/{queue}/pause` | Pause queue |
| POST | `/queues/{queue}/resume` | Resume queue |
| GET | `/queues/{queue}/dlq` | Get DLQ jobs |
| POST | `/queues/{queue}/dlq/retry` | Retry DLQ jobs |
| POST | `/queues/{queue}/rate-limit` | Set rate limit |
| DELETE | `/queues/{queue}/rate-limit` | Clear rate limit |
| POST | `/queues/{queue}/concurrency` | Set concurrency |
| DELETE | `/queues/{queue}/concurrency` | Clear concurrency |
| POST | `/jobs/{id}/ack` | Acknowledge job |
| POST | `/jobs/{id}/fail` | Fail job |
| POST | `/jobs/{id}/cancel` | Cancel job |
| POST | `/jobs/{id}/progress` | Update progress |
| GET | `/jobs/{id}/progress` | Get progress |
| GET | `/jobs/{id}/result` | Get result |
| GET | `/crons` | List cron jobs |
| POST | `/crons/{name}` | Create cron job |
| DELETE | `/crons/{name}` | Delete cron job |
| GET | `/stats` | Get statistics |
| GET | `/metrics` | Get metrics |

### Example: Push a job

```bash
curl -X POST http://localhost:6790/queues/emails/jobs \
  -H "Content-Type: application/json" \
  -d '{"data": {"to": "user@example.com"}, "priority": 5}'
```

### Example: Pull and ack

```bash
# Pull a job
curl http://localhost:6790/queues/emails/jobs

# Acknowledge with result
curl -X POST http://localhost:6790/jobs/1/ack \
  -H "Content-Type: application/json" \
  -d '{"result": {"status": "sent"}}'
```

## TypeScript/Bun

```bash
cd client
bun install
```

### Producer

```typescript
import { Queue } from "./src";

const queue = new Queue("emails");

// Simple push
await queue.push({ to: "user@example.com", subject: "Hello" });

// With all options
await queue.push({ task: "process" }, {
  priority: 10,           // Higher = more urgent
  delay: 5000,            // Run after 5 seconds
  ttl: 60000,             // Expire after 60 seconds
  timeout: 30000,         // Auto-fail after 30 seconds
  maxAttempts: 3,         // Retry up to 3 times before DLQ
  backoff: 1000,          // Exponential backoff (1s, 2s, 4s...)
  uniqueKey: "task-123",  // Deduplication key
  dependsOn: [jobId1],    // Run after these jobs complete
});

// Batch push
await queue.pushBatch([
  { data: { to: "a@test.com" } },
  { data: { to: "b@test.com" }, priority: 5 },
]);
```

### Worker

```typescript
import { Worker } from "./src";

const worker = new Worker(
  "emails",
  async (job, ctx) => {
    console.log("Processing:", job.data);

    // Update progress
    await ctx.updateProgress(50, "Half done");

    // Do work...

    await ctx.updateProgress(100, "Complete");

    // Return result (stored on server)
    return { status: "sent" };
  },
  { batchSize: 50 }
);

await worker.start();
```

## Python

```bash
cd python
pip install -e .
```

### Producer

```python
from magicqueue import Queue

queue = Queue("emails")

# Simple push
job_id = queue.push({"to": "user@example.com", "subject": "Hello"})

# With all options
job_id = queue.push(
    {"task": "process"},
    priority=10,
    delay=5000,
    ttl=60000,
    max_attempts=3,
    backoff=1000,
    unique_key="task-123",
    depends_on=[job_id1],
)

queue.close()
```

### Worker

```python
from magicqueue import Worker, Job, JobContext

def handler(job: Job, ctx: JobContext):
    print(f"Processing: {job.data}")
    ctx.update_progress(50, "Half done")
    # Do work...
    ctx.update_progress(100, "Complete")

worker = Worker("emails", handler, batch_size=50)
worker.start()
```

## Advanced Features

### Dead Letter Queue

```typescript
// Jobs with maxAttempts go to DLQ after N failures
const id = await queue.push({ task: "risky" }, { maxAttempts: 3, backoff: 1000 });

// View DLQ
const dlqJobs = await queue.getDlq();

// Retry all DLQ jobs
const retried = await queue.retryDlq();

// Retry specific job
await queue.retryDlq(jobId);
```

### Job Dependencies

```typescript
// Job B runs only after Job A completes
const jobA = await queue.push({ step: "first" });
const jobB = await queue.push({ step: "second" }, { dependsOn: [jobA] });
```

### Cron Jobs

```typescript
// Run every 60 seconds
await queue.addCron("cleanup", "*/60", { task: "cleanup" });

// List cron jobs
const crons = await queue.listCrons();

// Delete cron
await queue.deleteCron("cleanup");
```

### Rate Limiting & Concurrency

```typescript
// Limit to 100 jobs/second
await queue.setRateLimit(100);

// Limit to 5 concurrent jobs
await queue.setConcurrency(5);

// Pause queue
await queue.pause();

// Resume queue
await queue.resume();
```

### Metrics & Stats

```typescript
// Basic stats
const stats = await queue.stats();
// { queued: 100, processing: 5, delayed: 10, dlq: 2 }

// Detailed metrics
const metrics = await queue.metrics();
// { total_pushed: 1000, total_completed: 950, avg_latency_ms: 5.2, ... }
```

## TCP Protocol

JSON over TCP/Unix socket, newline-delimited.

### Commands

```json
// Push with all options
{"cmd": "PUSH", "queue": "jobs", "data": {...}, "priority": 0, "delay": 1000, "ttl": 60000, "timeout": 30000, "max_attempts": 3, "backoff": 1000, "unique_key": "key", "depends_on": [1, 2]}

// Batch push
{"cmd": "PUSHB", "queue": "jobs", "jobs": [{"data": {...}, "priority": 0}]}

// Pull single job
{"cmd": "PULL", "queue": "jobs"}

// Batch pull
{"cmd": "PULLB", "queue": "jobs", "count": 50}

// Acknowledge job with result
{"cmd": "ACK", "id": 123, "result": {"status": "done"}}

// Batch acknowledge
{"cmd": "ACKB", "ids": [1, 2, 3]}

// Fail job (triggers retry/DLQ)
{"cmd": "FAIL", "id": 123, "error": "reason"}

// Cancel job
{"cmd": "CANCEL", "id": 123}

// Update progress
{"cmd": "PROGRESS", "id": 123, "progress": 50, "message": "Working..."}

// Get progress
{"cmd": "GETPROGRESS", "id": 123}

// Get job result
{"cmd": "GETRESULT", "id": 123}

// Get DLQ jobs
{"cmd": "DLQ", "queue": "jobs", "count": 10}

// Retry DLQ jobs
{"cmd": "RETRYDLQ", "queue": "jobs", "id": 123}

// Queue control
{"cmd": "PAUSE", "queue": "jobs"}
{"cmd": "RESUME", "queue": "jobs"}
{"cmd": "RATELIMIT", "queue": "jobs", "limit": 100}
{"cmd": "RATELIMITCLEAR", "queue": "jobs"}
{"cmd": "SETCONCURRENCY", "queue": "jobs", "limit": 5}
{"cmd": "CLEARCONCURRENCY", "queue": "jobs"}
{"cmd": "LISTQUEUES"}

// Cron jobs
{"cmd": "CRON", "name": "cleanup", "queue": "jobs", "data": {...}, "schedule": "*/60", "priority": 0}
{"cmd": "CRONDELETE", "name": "cleanup"}
{"cmd": "CRONLIST"}

// Stats & Metrics
{"cmd": "STATS"}
{"cmd": "METRICS"}

// Authentication (if enabled)
{"cmd": "AUTH", "token": "secret"}
```

## Architecture

```
                    ┌─────────────────────────────────┐
                    │       MagicQueue Server         │
                    │            (Rust)               │
┌─────────┐        ├─────────────────────────────────┤
│Producer │──TCP──▶│  TCP Server (port 6789)         │
│(Bun/Py) │        │  HTTP Server (port 6790)        │
└─────────┘        ├─────────────────────────────────┤
                   │  QueueManager                    │
┌─────────┐        │  ├─ 32 Sharded Queues           │
│ Worker  │◀─TCP──▶│  ├─ BinaryHeap (priority)       │
│(Bun/Py) │        │  ├─ Dead Letter Queue           │
└─────────┘        │  ├─ Rate Limiters               │
                   │  ├─ Concurrency Limiters        │
┌─────────┐        │  ├─ Job Results                 │
│Dashboard│◀─HTTP─▶│  ├─ Dependencies tracker        │
│(Browser)│        │  └─ Processing tracker          │
└─────────┘        ├─────────────────────────────────┤
                   │  Background Tasks               │
                   │  ├─ Delayed Job Processor       │
                   │  ├─ Dependency Resolver         │
                   │  ├─ Timeout Checker             │
                   │  └─ Cron Executor               │
                   ├─────────────────────────────────┤
                   │  Optional: WAL Persistence      │
                   └─────────────────────────────────┘
```

## Project Structure

```
magic-queue/
├── server/                 # Rust server
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs         # TCP/Unix socket server
│       ├── http.rs         # HTTP REST API (axum)
│       ├── dashboard.rs    # Web dashboard
│       ├── protocol.rs     # Commands & responses
│       └── queue/          # Queue implementation
│           ├── mod.rs
│           ├── manager.rs  # QueueManager
│           ├── core.rs     # Push/Pull/Ack
│           ├── features.rs # Rate limit, pause, etc.
│           └── background.rs
│
├── client/                 # TypeScript/Bun SDK
│   ├── package.json
│   └── src/
│       ├── client.ts       # TCP connection
│       ├── queue.ts        # Producer API
│       └── worker.ts       # Consumer API
│
├── python/                 # Python SDK
│   ├── pyproject.toml
│   └── magicqueue/
│       ├── client.py       # TCP connection
│       ├── queue.py        # Producer API
│       └── worker.py       # Consumer API
│
└── benchmark.js            # Benchmark script
```

## Run Benchmarks

```bash
# Start server
cd server && HTTP=1 cargo run --release

# Run benchmark (another terminal)
node benchmark.js
```

## License

MIT

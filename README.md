# MagicQueue

A high-performance job queue system built from scratch with **Rust** and **TypeScript/Bun/Python**.

No Redis. No dependencies. Pure performance.

## Performance

| Metric | Throughput |
|--------|------------|
| Push (batch) | **1,900,000+ jobs/sec** |
| Processing (no-op) | **280,000+ jobs/sec** |
| Processing (CPU work) | **196,000+ jobs/sec** |

*Benchmarked with 100k jobs on Apple Silicon*

### Optimizations

- **mimalloc** - High-performance memory allocator
- **parking_lot** - Faster locks than std
- **Atomic u64 IDs** - No UUID overhead
- **32 shards** - Minimized lock contention
- **LTO** - Link-Time Optimization for smaller, faster binary

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
- **Unique Jobs** - Deduplication based on custom keys
- **Job Cancellation** - Cancel pending jobs
- **Rate Limiting** - Limit processing rate per queue
- **Job Progress** - Track progress of long-running jobs
- **Job Dependencies** - Job B runs only after Job A completes
- **Cron Jobs** - Scheduled recurring jobs
- **Metrics** - Detailed statistics and throughput metrics

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

### TypeScript/Bun

```bash
cd client
bun install
```

#### Producer

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

#### Worker

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
  },
  { batchSize: 50 }
);

await worker.start();
```

### Python

```bash
cd python
pip install -e .
```

#### Producer

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

#### Worker

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

### Rate Limiting

```typescript
// Limit to 100 jobs/second
await queue.setRateLimit(100);

// Clear rate limit
await queue.clearRateLimit();
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

## Server Options

```bash
# Standard TCP (port 6789)
cargo run --release

# With persistence (WAL)
PERSIST=1 cargo run --release

# Unix socket (lower latency)
UNIX_SOCKET=1 cargo run --release

# Custom port
PORT=7000 cargo run --release
```

## Protocol

JSON over TCP/Unix socket, newline-delimited.

### Commands

```json
// Push with all options
{"cmd": "PUSH", "queue": "jobs", "data": {...}, "priority": 0, "delay": 1000, "ttl": 60000, "max_attempts": 3, "backoff": 1000, "unique_key": "key", "depends_on": [1, 2]}

// Batch push
{"cmd": "PUSHB", "queue": "jobs", "jobs": [{"data": {...}, "priority": 0}]}

// Pull single job (blocking)
{"cmd": "PULL", "queue": "jobs"}

// Batch pull
{"cmd": "PULLB", "queue": "jobs", "count": 50}

// Acknowledge job
{"cmd": "ACK", "id": 123}

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

// Get DLQ jobs
{"cmd": "DLQ", "queue": "jobs", "count": 10}

// Retry DLQ jobs
{"cmd": "RETRYDLQ", "queue": "jobs", "id": 123}

// Set rate limit
{"cmd": "RATELIMIT", "queue": "jobs", "limit": 100}

// Clear rate limit
{"cmd": "RATELIMITCLEAR", "queue": "jobs"}

// Add cron job
{"cmd": "CRON", "name": "cleanup", "queue": "jobs", "data": {...}, "schedule": "*/60", "priority": 0}

// Delete cron job
{"cmd": "CRONDELETE", "name": "cleanup"}

// List cron jobs
{"cmd": "CRONLIST"}

// Get stats
{"cmd": "STATS"}

// Get metrics
{"cmd": "METRICS"}
```

## Architecture

```
                    ┌─────────────────────────────┐
                    │      MagicQueue Server      │
                    │           (Rust)            │
┌─────────┐        ├─────────────────────────────┤
│Producer │──TCP──▶│  32 Sharded Queues          │
│(Bun/Py) │        │  ├─ BinaryHeap (priority)   │
└─────────┘        │  ├─ Dead Letter Queue       │
                   │  ├─ Delayed job support     │
┌─────────┐        │  ├─ Dependencies tracker    │
│ Worker  │◀─TCP──▶│  └─ Processing tracker      │
│(Bun/Py) │        ├─────────────────────────────┤
└─────────┘        │  Optional WAL persistence   │
                   └─────────────────────────────┘
```

## Project Structure

```
magic-queue/
├── server/                 # Rust server
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs         # TCP/Unix socket server
│       ├── protocol.rs     # Commands & responses
│       └── queue.rs        # Sharded queue manager
│
├── client/                 # TypeScript/Bun SDK
│   ├── package.json
│   └── src/
│       ├── client.ts       # TCP connection
│       ├── queue.ts        # Producer API
│       └── worker.ts       # Consumer API
│
└── python/                 # Python SDK
    ├── pyproject.toml
    └── magicqueue/
        ├── client.py       # TCP connection
        ├── queue.py        # Producer API
        └── worker.py       # Consumer API
```

## Benchmarks

```bash
# Terminal 1
cd server && cargo run --release

# Terminal 2 (TypeScript)
cd client && bun run examples/benchmark.ts

# Terminal 2 (Python)
cd python && python examples/benchmark.py
```

## License

MIT

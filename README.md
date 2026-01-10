<div align="center">

# MagicQueue

**High-Performance Job Queue System**

A blazingly fast, zero-dependency job queue built with Rust.

[![Rust](https://img.shields.io/badge/Rust-000000?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-007ACC?style=flat&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[Features](#features) • [Quick Start](#quick-start) • [Documentation](#documentation) • [Benchmarks](#benchmarks)

</div>

---

## Why MagicQueue?

- **No Redis Required** — Self-contained server with optional persistence
- **Ultra-Low Latency** — Sub-100µs P99 latency for job operations
- **High Throughput** — 280,000+ ops/sec for batch operations
- **Production Ready** — Dead letter queues, retries, rate limiting, and more
- **Multi-Language SDKs** — TypeScript/Bun and Python clients included

## Benchmarks

Tested on Apple Silicon M2, single server instance.

| Operation | Throughput | P99 Latency |
|-----------|------------|-------------|
| Sequential Push | 15,000 ops/sec | 93 µs |
| Batch Push | 285,000 ops/sec | — |
| Full Cycle (Push→Pull→Ack) | 5,800 ops/sec | — |

### vs BullMQ (Redis)

| Benchmark | MagicQueue | BullMQ | Improvement |
|-----------|------------|--------|-------------|
| Sequential Push | 15,015 ops/s | 4,533 ops/s | **3.3x faster** |
| Batch Push | 294,118 ops/s | 36,232 ops/s | **8.1x faster** |
| P99 Latency | 125 µs | 414 µs | **3.3x lower** |

---

## Features

### Core
| Feature | Description |
|---------|-------------|
| **Priority Queues** | Higher priority jobs execute first |
| **Delayed Jobs** | Schedule jobs to run in the future |
| **Batch Operations** | Push/pull/ack thousands of jobs in single requests |
| **Job Results** | Store and retrieve job completion results |
| **Persistence** | Optional Write-Ahead Log for durability |

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
| **Cron Jobs** | Scheduled recurring jobs |

### Observability
| Feature | Description |
|---------|-------------|
| **Progress Tracking** | Real-time job progress (0-100%) |
| **Metrics API** | Throughput, latency, queue depths |
| **Web Dashboard** | Real-time monitoring UI |
| **Pub/Sub Events** | Subscribe to job lifecycle events |

---

## Quick Start

### 1. Start the Server

```bash
cd server
cargo run --release
```

The server listens on **port 6789** (TCP) by default.

### 2. Enable HTTP API & Dashboard (Optional)

```bash
HTTP=1 cargo run --release
```

- **TCP API:** `localhost:6789`
- **HTTP API:** `localhost:6790`
- **Dashboard:** `http://localhost:6790`

### 3. Use a Client SDK

#### TypeScript/Bun

```typescript
import { Queue, Worker } from "magic-queue";

// Producer
const queue = new Queue("emails");
await queue.push({ to: "user@example.com", subject: "Hello" });

// Worker
const worker = new Worker("emails", async (job) => {
  console.log("Processing:", job.data);
  return { status: "sent" };
});
await worker.start();
```

#### Python

```python
from magicqueue import Queue, Worker

# Producer
queue = Queue("emails")
queue.push({"to": "user@example.com", "subject": "Hello"})

# Worker
def handler(job, ctx):
    print(f"Processing: {job.data}")

worker = Worker("emails", handler)
worker.start()
```

---

## Documentation

### Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `PORT` | TCP server port | `6789` |
| `HTTP` | Enable HTTP API | disabled |
| `HTTP_PORT` | HTTP API port | `6790` |
| `PERSIST` | Enable WAL persistence | disabled |
| `AUTH_TOKENS` | Comma-separated auth tokens | disabled |
| `UNIX_SOCKET` | Unix socket path | disabled |

### Job Options

```typescript
await queue.push(data, {
  priority: 10,           // Higher = more urgent (default: 0)
  delay: 5000,            // Delay in milliseconds
  ttl: 60000,             // Time-to-live in milliseconds
  timeout: 30000,         // Processing timeout in milliseconds
  maxAttempts: 3,         // Max retries before DLQ
  backoff: 1000,          // Base backoff (exponential: 1s, 2s, 4s...)
  uniqueKey: "user-123",  // Deduplication key
  dependsOn: [jobId],     // Job dependencies
});
```

### HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Dashboard UI |
| `GET` | `/queues` | List all queues |
| `POST` | `/queues/{queue}/jobs` | Push a job |
| `GET` | `/queues/{queue}/jobs` | Pull jobs |
| `POST` | `/jobs/{id}/ack` | Acknowledge job |
| `POST` | `/jobs/{id}/fail` | Fail job |
| `GET` | `/stats` | Get statistics |
| `GET` | `/metrics` | Get metrics |

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

// DLQ
{"cmd": "DLQ", "queue": "jobs", "count": 10}
{"cmd": "RETRYDLQ", "queue": "jobs"}

// Queue control
{"cmd": "PAUSE", "queue": "jobs"}
{"cmd": "RESUME", "queue": "jobs"}
{"cmd": "RATELIMIT", "queue": "jobs", "limit": 100}
{"cmd": "SETCONCURRENCY", "queue": "jobs", "limit": 5}
{"cmd": "LISTQUEUES"}

// Cron
{"cmd": "CRON", "name": "cleanup", "queue": "jobs", "data": {}, "schedule": "*/60"}
{"cmd": "CRONLIST"}
{"cmd": "CRONDELETE", "name": "cleanup"}

// Stats
{"cmd": "STATS"}
{"cmd": "METRICS"}

// Auth
{"cmd": "AUTH", "token": "secret"}
```

</details>

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      MagicQueue Server                       │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│   │ TCP Server  │    │ HTTP Server │    │  Dashboard  │     │
│   │  :6789      │    │   :6790     │    │    (Web)    │     │
│   └──────┬──────┘    └──────┬──────┘    └─────────────┘     │
│          │                  │                                │
│          └────────┬─────────┘                                │
│                   ▼                                          │
│   ┌───────────────────────────────────────────────────┐     │
│   │              Queue Manager                         │     │
│   │  ┌─────────────────────────────────────────────┐  │     │
│   │  │         32 Sharded Priority Queues          │  │     │
│   │  │  (BinaryHeap + FxHashMap + parking_lot)     │  │     │
│   │  └─────────────────────────────────────────────┘  │     │
│   │                                                    │     │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐          │     │
│   │  │   DLQ    │ │  Rate    │ │  Conc.   │          │     │
│   │  │  Store   │ │ Limiters │ │ Limiters │          │     │
│   │  └──────────┘ └──────────┘ └──────────┘          │     │
│   └───────────────────────────────────────────────────┘     │
│                                                              │
│   ┌───────────────────────────────────────────────────┐     │
│   │              Background Tasks                      │     │
│   │  • Delayed Job Processor   • Timeout Checker      │     │
│   │  • Dependency Resolver     • Cron Executor        │     │
│   └───────────────────────────────────────────────────┘     │
│                                                              │
│   ┌───────────────────────────────────────────────────┐     │
│   │         Write-Ahead Log (Optional)                 │     │
│   └───────────────────────────────────────────────────┘     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
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
├── server/                 # Rust server
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs         # Entry point, TCP server
│       ├── http.rs         # HTTP REST API (axum)
│       ├── dashboard.rs    # Web dashboard
│       ├── protocol.rs     # Commands & responses
│       └── queue/
│           ├── manager.rs  # Queue manager, WAL
│           ├── core.rs     # Push/Pull/Ack
│           ├── features.rs # Rate limit, DLQ, etc.
│           ├── background.rs # Background tasks
│           ├── types.rs    # Data structures
│           └── tests.rs    # Test suite (57 tests)
│
├── client/                 # TypeScript SDK
│   └── src/
│       ├── client.ts       # TCP connection
│       ├── queue.ts        # Producer API
│       └── worker.ts       # Consumer API
│
└── python/                 # Python SDK
    └── magicqueue/
        ├── client.py       # TCP connection
        ├── queue.py        # Producer API
        └── worker.py       # Consumer API
```

---

## Testing

```bash
cd server
cargo test
```

**57 tests** covering:
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

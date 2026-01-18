<div align="center">

# ⚡ flashQ

**The fastest open-source job queue.**

Built with Rust. BullMQ-compatible. 600K+ jobs/sec.

[![CI](https://img.shields.io/github/actions/workflow/status/egeominotti/flashq/ci.yml?branch=main&label=CI)](https://github.com/egeominotti/flashq/actions)
[![npm](https://img.shields.io/npm/v/flashq)](https://www.npmjs.com/package/flashq)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)

[Quick Start](#quick-start) • [SDK](#sdk) • [Features](#features) • [Docs](#documentation)

</div>

---

## ⚡ Performance: flashQ vs BullMQ

> **flashQ is 3x to 10x faster than BullMQ** in real-world benchmarks.

### Benchmark Environment

| Component | Version | Configuration |
|-----------|---------|---------------|
| **flashQ Server** | 0.1.0 | Docker with `io_uring`, Rust + tokio async runtime |
| **BullMQ** | 5.66.5 | npm package |
| **Redis** | 7.4.7 | Docker (`redis:7-alpine`), jemalloc allocator |
| **Bun** | 1.3.6 | TypeScript runtime |

### Test Configuration

```
Jobs:                 100,000
Workers:              8
Concurrency/worker:   50
Total concurrency:    400
Batch size:           1,000 jobs
Data integrity:       Verified (input === output)
```

### Results: No-op Jobs (Pure Queue Overhead)

| Metric | flashQ | BullMQ | Speedup |
|--------|-------:|-------:|--------:|
| **Push Rate** | 307,692 jobs/sec | 43,649 jobs/sec | **7.0x** |
| **Process Rate** | 292,398 jobs/sec | 27,405 jobs/sec | **10.7x** |
| **Total Time** | 0.67s | 5.94s | **8.9x** |

### Results: CPU-Bound Jobs (Realistic Workload)

Each job: JSON parse, 10x SHA256, array sort/filter, string ops

| Metric | flashQ | BullMQ | Speedup |
|--------|-------:|-------:|--------:|
| **Push Rate** | 220,751 jobs/sec | 43,422 jobs/sec | **5.1x** |
| **Process Rate** | 62,814 jobs/sec | 23,923 jobs/sec | **2.6x** |
| **Total Time** | 2.04s | 6.48s | **3.2x** |

### 1 Million Jobs (flashQ)

| Scenario | Push | Process | Total | Integrity |
|----------|-----:|--------:|------:|:---------:|
| **No-op** | 266,809/s | 262,536/s | 7.56s | ✅ 100% |
| **CPU-bound** | 257,334/s | 65,240/s | 19.21s | ✅ 100% |

### Why flashQ is Faster

| Optimization | Benefit |
|--------------|---------|
| **Rust + tokio** | Zero-cost abstractions, no GC pauses |
| **io_uring** | Linux kernel async I/O (2x throughput) |
| **32 Shards** | Lock-free concurrent access (DashMap) |
| **MessagePack** | 40% smaller payloads vs JSON |
| **Batch Ops** | Amortized network overhead |
| **No Redis** | Direct TCP, no intermediary |

---

## Quick Start

```bash
# Docker
docker run -p 6789:6789 -p 6790:6790 -e HTTP=1 flashq/flashq

# Docker Compose
git clone https://github.com/egeominotti/flashq.git && cd flashq
docker-compose up -d
```

Dashboard: http://localhost:6790

## SDK

[![npm](https://img.shields.io/npm/v/flashq)](https://www.npmjs.com/package/flashq)

```bash
bun add flashq
# or
npm install flashq
```

```typescript
import { Queue, Worker } from 'flashq';

// Create queue
const queue = new Queue('emails');

// Add job (BullMQ-style)
await queue.add('send', { to: 'user@example.com' }, {
  priority: 10,
  attempts: 3,
  backoff: { type: 'exponential', delay: 1000 }
});

// Process jobs (auto-starts like BullMQ)
const worker = new Worker('emails', async (job) => {
  await sendEmail(job.data);
  return { sent: true };
});

worker.on('completed', (job, result) => {
  console.log('Done:', job.id);
});
```

## Features

| Feature | Description |
|---------|-------------|
| **BullMQ-compatible** | Same API: Queue, Worker, events |
| **Priority Queues** | Higher priority = processed first |
| **Delayed Jobs** | Schedule for future execution |
| **Retry & Backoff** | Automatic retries with exponential backoff |
| **Batch Operations** | Push/pull thousands at once |
| **Rate Limiting** | Control throughput per queue |
| **Concurrency** | Limit parallel processing |
| **Persistence** | Optional PostgreSQL storage |
| **Dashboard** | Built-in monitoring UI |
| **KV Storage** | Redis-like key-value store |
| **Pub/Sub** | Redis-like publish/subscribe messaging |

## Documentation

### Job Options

```typescript
await queue.add('name', data, {
  priority: 10,       // higher = first
  delay: 5000,        // delay in ms
  attempts: 3,        // retry count
  backoff: { type: 'exponential', delay: 1000 },
  timeout: 30000,     // processing timeout
  jobId: 'custom-id', // for deduplication
});
```

### Worker Options

```typescript
const worker = new Worker('queue', handler, {
  concurrency: 10,    // parallel jobs
  autorun: true,      // auto-start (default)
});

worker.on('completed', (job, result) => {});
worker.on('failed', (job, error) => {});
```

### Queue Control

```typescript
await queue.pause();
await queue.resume();
await queue.drain();       // remove waiting
await queue.obliterate();  // remove all
await queue.getJobCounts();
```

### Key-Value Storage

Redis-like in-memory KV store with TTL support.

```typescript
import { FlashQ } from 'flashq';

const client = new FlashQ();

// SET/GET with optional TTL
await client.kvSet('user:123', { name: 'John' });
await client.kvSet('session:abc', { token: 'xyz' }, { ttl: 3600000 });
const user = await client.kvGet('user:123');

// Batch operations (10-100x faster!)
await client.kvMset([
  { key: 'user:1', value: { name: 'Alice' } },
  { key: 'user:2', value: { name: 'Bob' } },
]);
const users = await client.kvMget(['user:1', 'user:2']);

// Pattern matching & counters
const keys = await client.kvKeys('user:*');
await client.kvIncr('page:views');
```

| Operation | Throughput |
|-----------|------------|
| Sequential | ~30K ops/sec |
| **Batch MSET** | **640K ops/sec** |
| **Batch MGET** | **1.2M ops/sec** |

### Pub/Sub

Redis-like publish/subscribe messaging.

```typescript
import { FlashQ } from 'flashq';

const client = new FlashQ();

// Publish messages
const receivers = await client.publish('notifications', { type: 'alert', text: 'Hello!' });

// Subscribe to channels
await client.pubsubSubscribe(['notifications', 'alerts']);

// Pattern subscribe
await client.pubsubPsubscribe(['events:*', 'logs:*']);

// List active channels
const channels = await client.pubsubChannels();
const eventChannels = await client.pubsubChannels('events:*');

// Get subscriber counts
const counts = await client.pubsubNumsub(['notifications', 'alerts']);
```

### Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | TCP port | 6789 |
| `HTTP` | Enable HTTP/Dashboard | disabled |
| `HTTP_PORT` | HTTP port | 6790 |
| `DATABASE_URL` | PostgreSQL URL | in-memory |

## Examples

See [sdk/typescript/examples/](sdk/typescript/examples/):

| File | Description |
|------|-------------|
| **01-basic** | Queue + Worker |
| **02-job-options** | Priority, delay, retry |
| **03-bulk-jobs** | Batch operations |
| **04-events** | Worker events |
| **05-queue-control** | Pause, resume |
| **06-delayed** | Scheduled jobs |
| **07-retry** | Retry with backoff |
| **08-priority** | Priority ordering |
| **09-concurrency** | Parallel processing |
| **10-benchmark** | Basic performance test |
| **heavy-benchmark** | 100K no-op jobs |
| **cpu-benchmark** | 100K CPU-bound jobs |
| **million-benchmark** | 1M jobs with verification |
| **benchmark-full** | Memory + latency + throughput |
| **bullmq-benchmark** | BullMQ comparison (no-op) |
| **bullmq-cpu-benchmark** | BullMQ comparison (CPU) |
| **bullmq-benchmark-full** | BullMQ memory + latency |
| **kv-benchmark** | KV store benchmark |
| **pubsub-example** | Pub/Sub messaging |

## Advanced: io_uring (Linux)

On Linux, flashQ can use `io_uring` for kernel-level async I/O, providing:
- Zero-copy I/O operations
- Batched syscalls
- Reduced context switches

### Build with io_uring

```bash
# Local build
cargo build --release --features io-uring

# Docker (enabled by default)
docker build -t flashq .
```

### Runtime Detection

flashQ automatically detects the optimal I/O backend:

| OS | Backend | Notes |
|----|---------|-------|
| Linux | epoll (default) | Fast, stable |
| Linux + io-uring | io_uring | Fastest, kernel 5.1+ |
| macOS | kqueue | Native, optimal |
| Windows | IOCP | Native, optimal |

Startup logs show the active backend:
```
INFO IO backend runtime="tokio" io_backend="epoll"
INFO IO backend runtime="tokio + io_uring" (with feature)
```

## License

MIT

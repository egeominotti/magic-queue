<div align="center">

# ⚡ flashQ

**The fastest open-source job queue.**

Built with Rust. 600K+ jobs/sec. Sub-millisecond latency.

[![CI](https://img.shields.io/github/actions/workflow/status/egeominotti/flashq/ci.yml?branch=main&label=CI)](https://github.com/egeominotti/flashq/actions)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.75+-orange)](https://www.rust-lang.org/)

[Quick Start](#quick-start) • [Features](#features) • [SDK](#sdk) • [Documentation](#documentation)

</div>

---

## Performance

| Metric | flashQ | BullMQ | Speedup |
|--------|--------|--------|---------|
| Batch Push | 600K/sec | 42K/sec | **14x** |
| Processing | 310K/sec | 15K/sec | **21x** |
| Latency P99 | 0.1ms | 0.6ms | **6x** |

## Quick Start

### Docker

```bash
docker run -p 6789:6789 -p 6790:6790 -e HTTP=1 flashq/flashq
```

### Docker Compose

```bash
git clone https://github.com/egeominotti/flashq.git
cd flashq && docker-compose up -d
```

Dashboard: http://localhost:6790

### From Source

```bash
cd server && cargo build --release
HTTP=1 ./target/release/flashq-server
```

## SDK

```bash
bun add flashq
```

```typescript
import { FlashQ, Worker } from 'flashq';

const client = new FlashQ();

// Add job
await client.add('emails', { to: 'user@example.com' });

// Process jobs
const worker = new Worker('emails', async (job) => {
  await sendEmail(job.data);
  return { sent: true };
});
await worker.start();
```

## Features

| Feature | Description |
|---------|-------------|
| **Priority Queues** | Higher priority jobs process first |
| **Delayed Jobs** | Schedule jobs for future execution |
| **Batch Operations** | Push/pull thousands of jobs at once |
| **Retry & DLQ** | Automatic retries with dead letter queue |
| **Rate Limiting** | Control throughput per queue |
| **Cron Jobs** | Schedule recurring jobs |
| **Job Dependencies** | Parent/child job flows |
| **Progress Tracking** | Real-time job progress |
| **Persistence** | Optional PostgreSQL storage |
| **Dashboard** | Built-in monitoring UI |

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  flashQ Server                   │
├─────────────────────────────────────────────────┤
│  TCP :6789 │ HTTP :6790 │ gRPC :6791 │ WS      │
├─────────────────────────────────────────────────┤
│            32 Sharded Priority Queues            │
│         (parallel processing, zero contention)   │
├─────────────────────────────────────────────────┤
│          PostgreSQL (optional persistence)       │
└─────────────────────────────────────────────────┘
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | TCP port | 6789 |
| `HTTP` | Enable HTTP/Dashboard | disabled |
| `HTTP_PORT` | HTTP port | 6790 |
| `GRPC` | Enable gRPC | disabled |
| `DATABASE_URL` | PostgreSQL URL | in-memory |
| `AUTH_TOKENS` | Auth tokens (comma-separated) | disabled |

## Documentation

### Job Options

```typescript
await client.add('queue', data, {
  priority: 10,       // higher = first
  delay: 5000,        // delay in ms
  max_attempts: 3,    // retry count
  backoff: 1000,      // exponential backoff base
  timeout: 30000,     // processing timeout
  unique_key: 'id',   // deduplication
  jobId: 'custom-id', // custom ID for lookup
});
```

### Worker Options

```typescript
const worker = new Worker('queue', handler, {
  concurrency: 10,    // parallel jobs
});
```

### Queue Control

```typescript
await client.pause('queue');
await client.resume('queue');
await client.drain('queue');       // remove waiting jobs
await client.obliterate('queue');  // remove all data
```

### HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/queues/{queue}/jobs` | Add job |
| GET | `/queues/{queue}/jobs` | Pull jobs |
| POST | `/jobs/{id}/ack` | Acknowledge |
| POST | `/jobs/{id}/fail` | Fail job |
| GET | `/stats` | Statistics |
| GET | `/health` | Health check |

## Examples

See [sdk/typescript/examples/](sdk/typescript/examples/) for 18 complete examples:

- Basic operations, workers, delayed jobs
- Priority, batch, retry, progress
- Cron, stats, rate limiting
- Job flows, deduplication, events

## License

MIT

---

<div align="center">

**[Get Started →](#quick-start)**

</div>

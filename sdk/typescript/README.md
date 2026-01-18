# flashQ TypeScript SDK

BullMQ-compatible API for [flashQ](https://github.com/egeominotti/flashq).

## Installation

```bash
bun add flashq
```

## Quick Start

```typescript
import { Queue, Worker } from 'flashq';

// Create queue
const queue = new Queue('emails');

// Add job
await queue.add('send', { to: 'user@example.com' });

// Process jobs (auto-starts)
const worker = new Worker('emails', async (job) => {
  console.log('Processing:', job.data);
  return { sent: true };
});
```

---

## ⚡ Performance Benchmark: flashQ vs BullMQ

> **flashQ is 3x to 10x faster than BullMQ** in real-world benchmarks.

### Test Environment

| Component | Version | Configuration |
|-----------|---------|---------------|
| **flashQ Server** | 0.1.0 | Docker with `io_uring` enabled, Rust + tokio async runtime |
| **BullMQ** | 5.66.5 | npm package |
| **Redis** | 7.4.7 | Docker (`redis:7-alpine`), jemalloc allocator |
| **Runtime** | Bun 1.x | TypeScript SDK |
| **Platform** | Linux/macOS | Docker containers |

### Benchmark Configuration

```
Workers:              8
Concurrency/worker:   50
Total concurrency:    400
Batch size:           1,000 jobs
Data verification:    Enabled (input === output)
```

### Results: No-op Jobs (100,000 jobs)

Minimal job processing to measure pure queue overhead.

| Metric | flashQ | BullMQ | Speedup |
|--------|-------:|-------:|--------:|
| **Push Rate** | 307,692 jobs/sec | 43,649 jobs/sec | **7.0x** |
| **Process Rate** | 292,398 jobs/sec | 27,405 jobs/sec | **10.7x** |
| **Total Time** | 0.67s | 5.94s | **8.9x** |

### Results: CPU-Bound Jobs (100,000 jobs)

Each job performs realistic CPU work:
- JSON serialize/deserialize
- 10x SHA256 hash rounds
- Array sort/filter/reduce (100 elements)
- String manipulation

| Metric | flashQ | BullMQ | Speedup |
|--------|-------:|-------:|--------:|
| **Push Rate** | 220,751 jobs/sec | 43,422 jobs/sec | **5.1x** |
| **Process Rate** | 62,814 jobs/sec | 23,923 jobs/sec | **2.6x** |
| **Total Time** | 2.04s | 6.48s | **3.2x** |

### Results: 1 Million Jobs (flashQ only)

| Scenario | Push Rate | Process Rate | Total Time | Data Integrity |
|----------|----------:|-------------:|-----------:|:--------------:|
| **No-op** | 266,809/s | 262,536/s | 7.56s | ✅ 100% |
| **CPU-bound** | 257,334/s | 65,240/s | 19.21s | ✅ 100% |

### Why flashQ is Faster

| Optimization | Description |
|--------------|-------------|
| **Rust + tokio** | Zero-cost abstractions, no GC pauses |
| **io_uring** | Linux kernel async I/O (when available) |
| **32 Shards** | Lock-free concurrent access via DashMap |
| **MessagePack** | 40% smaller payloads vs JSON |
| **Batch Operations** | Amortized network overhead |
| **No Redis Dependency** | Direct TCP protocol, no intermediary |

### Run Benchmarks

```bash
# flashQ benchmarks
bun run examples/heavy-benchmark.ts      # No-op 100K
bun run examples/cpu-benchmark.ts        # CPU-bound 100K
bun run examples/million-benchmark.ts    # 1M jobs

# BullMQ comparison (requires Redis)
docker run -d -p 6379:6379 redis:7-alpine
bun run examples/bullmq-benchmark.ts     # No-op 100K
bun run examples/bullmq-cpu-benchmark.ts # CPU-bound 100K
```

---

## Queue

```typescript
const queue = new Queue('emails', {
  host: 'localhost',
  port: 6789,
});

// Add single job
await queue.add('send', data, {
  priority: 10,
  delay: 5000,
  attempts: 3,
  backoff: { type: 'exponential', delay: 1000 },
});

// Add bulk
await queue.addBulk([
  { name: 'send', data: { to: 'a@test.com' } },
  { name: 'send', data: { to: 'b@test.com' }, opts: { priority: 10 } },
]);

// Control
await queue.pause();
await queue.resume();
await queue.drain();      // remove waiting
await queue.obliterate(); // remove all
```

## Worker

```typescript
// Auto-starts by default (like BullMQ)
const worker = new Worker('emails', async (job) => {
  return { done: true };
}, {
  concurrency: 10,
});

// Events
worker.on('completed', (job, result) => {});
worker.on('failed', (job, error) => {});

// Shutdown
await worker.close();
```

## Job Options

| Option | Type | Description |
|--------|------|-------------|
| `priority` | number | Higher = first (default: 0) |
| `delay` | number | Delay in ms |
| `attempts` | number | Retry count |
| `backoff` | number \| object | Backoff config |
| `timeout` | number | Processing timeout |
| `jobId` | string | Custom ID |

## Key-Value Storage

Redis-like KV store with TTL support and batch operations.

```typescript
import { FlashQ } from 'flashq';

const client = new FlashQ();

// Basic operations
await client.kvSet('user:123', { name: 'John', email: 'john@example.com' });
const user = await client.kvGet('user:123');
await client.kvDel('user:123');

// With TTL (milliseconds)
await client.kvSet('session:abc', { token: 'xyz' }, { ttl: 3600000 }); // 1 hour

// TTL operations
await client.kvExpire('user:123', 60000);  // Set TTL
const ttl = await client.kvTtl('user:123'); // Get remaining TTL

// Batch operations (10-100x faster!)
await client.kvMset([
  { key: 'user:1', value: { name: 'Alice' } },
  { key: 'user:2', value: { name: 'Bob' } },
  { key: 'user:3', value: { name: 'Charlie' }, ttl: 60000 },
]);

const users = await client.kvMget(['user:1', 'user:2', 'user:3']);

// Pattern matching
const userKeys = await client.kvKeys('user:*');
const sessionKeys = await client.kvKeys('session:???');

// Atomic counters
await client.kvIncr('page:views');           // +1
await client.kvIncr('user:123:score', 10);   // +10
await client.kvDecr('stock:item:456');       // -1
```

### KV Performance

| Operation | Throughput |
|-----------|------------|
| Sequential SET/GET | ~30K ops/sec |
| **Batch MSET** | **640K ops/sec** |
| **Batch MGET** | **1.2M ops/sec** |

> Use batch operations (MSET/MGET) for best performance!

## Pub/Sub

Redis-like publish/subscribe messaging.

```typescript
import { FlashQ } from 'flashq';

const client = new FlashQ();

// Publish messages to a channel
const receivers = await client.publish('notifications', { type: 'alert', text: 'Hello!' });
console.log(`Message sent to ${receivers} subscribers`);

// Subscribe to channels
await client.pubsubSubscribe(['notifications', 'alerts']);

// Pattern subscribe (e.g., "events:*" matches "events:user:signup")
await client.pubsubPsubscribe(['events:*', 'logs:*']);

// List active channels
const allChannels = await client.pubsubChannels();
const eventChannels = await client.pubsubChannels('events:*');

// Get subscriber counts
const counts = await client.pubsubNumsub(['notifications', 'alerts']);
// [['notifications', 5], ['alerts', 2]]

// Unsubscribe
await client.pubsubUnsubscribe(['notifications']);
await client.pubsubPunsubscribe(['events:*']);
```

## Examples

```bash
bun run examples/01-basic.ts
```

| File | Description |
|------|-------------|
| 01-basic.ts | Queue + Worker basics |
| 02-job-options.ts | Priority, delay, retry |
| 03-bulk-jobs.ts | Add multiple jobs |
| 04-events.ts | Worker events |
| 05-queue-control.ts | Pause, resume, drain |
| 06-delayed.ts | Scheduled jobs |
| 07-retry.ts | Retry with backoff |
| 08-priority.ts | Priority ordering |
| 09-concurrency.ts | Parallel processing |
| 10-benchmark.ts | Basic performance test |
| **heavy-benchmark.ts** | 100K no-op benchmark |
| **cpu-benchmark.ts** | 100K CPU-bound benchmark |
| **million-benchmark.ts** | 1M jobs with verification |
| **bullmq-benchmark.ts** | BullMQ comparison (no-op) |
| **bullmq-cpu-benchmark.ts** | BullMQ comparison (CPU) |
| kv-benchmark.ts | KV store benchmark |
| pubsub-example.ts | Pub/Sub messaging |

## License

MIT

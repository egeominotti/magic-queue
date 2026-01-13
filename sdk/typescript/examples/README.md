# flashQ TypeScript SDK Examples

This folder contains comprehensive examples demonstrating all features of the flashQ TypeScript SDK.

**Runtime: Bun** - This SDK is designed for Bun runtime.

## Prerequisites

1. Install [Bun](https://bun.sh):
   ```bash
   curl -fsSL https://bun.sh/install | bash
   ```

2. Start flashQ server:
   ```bash
   # From project root
   docker-compose up -d
   # Or manually
   cd server && cargo run --release

   # With all features (HTTP Dashboard, gRPC, WebSocket)
   HTTP=1 GRPC=1 cargo run --release
   ```

3. Install dependencies:
   ```bash
   cd sdk/typescript
   bun install
   ```

## Running Examples

```bash
# Run any example with Bun
bun run examples/01-basic-usage.ts
bun run examples/02-worker.ts
bun run examples/13-websocket-events.ts
bun run examples/14-grpc-client.ts
bun run examples/15-idempotency-and-sync-workflow.ts
```

## Examples

| # | File | Description |
|---|------|-------------|
| 01 | `01-basic-usage.ts` | Connect, push, pull, ack basics |
| 02 | `02-worker.ts` | Worker class for job processing |
| 03 | `03-job-options.ts` | All job options (priority, delay, TTL, debounce, idempotency, etc.) |
| 04 | `04-batch-operations.ts` | High-throughput batch push/pull/ack |
| 05 | `05-progress-tracking.ts` | Real-time progress updates |
| 06 | `06-dead-letter-queue.ts` | Handling failed jobs and DLQ |
| 07 | `07-cron-jobs.ts` | Scheduled recurring jobs |
| 08 | `08-queue-control.ts` | Pause/resume, rate limits, concurrency |
| 09 | `09-job-dependencies.ts` | DAG-style job orchestration |
| 10 | `10-authentication.ts` | Token-based authentication |
| 11 | `11-job-state-tracking.ts` | Monitoring job lifecycle |
| 12 | `12-real-world-email-queue.ts` | Complete email queue system |
| 13 | `13-websocket-events.ts` | Real-time events via WebSocket |
| 14 | `14-grpc-client.ts` | High-performance gRPC client |
| 15 | `15-idempotency-and-sync-workflow.ts` | **NEW:** Custom job IDs, finished(), synchronous workflows |

## Features Covered

### Core Operations
- Connect/disconnect
- Push single job
- Push batch
- Pull single job
- Pull batch
- Acknowledge (ack)
- Acknowledge batch
- Fail job

### Job Options
- Priority
- Delay (scheduled jobs)
- TTL (time-to-live)
- Timeout
- Max attempts
- Exponential backoff
- Unique key (deduplication)
- Dependencies
- Tags
- LIFO mode
- Remove on complete/fail
- Stall timeout
- **Debouncing** (debounce_id, debounce_ttl)
- **Custom Job ID** (jobId for idempotency)
- **Retention Policies** (keepCompletedAge, keepCompletedCount)

### Job Management
- Get job with state
- Get state only
- Get result
- Cancel job
- Progress updates
- Get progress
- **Get job by custom ID** (getJobByCustomId)
- **Wait for completion** (finished)
- Update job data
- Change priority
- Move to delayed
- Promote (delayed -> waiting)
- Discard (move to DLQ)

### Dead Letter Queue
- Get DLQ jobs
- Retry all DLQ
- Retry specific job

### Queue Control
- Pause queue
- Resume queue
- Is paused check
- Set rate limit
- Clear rate limit
- Set concurrency
- Clear concurrency
- List queues
- **Count jobs**
- **Get job counts by state**
- **Drain queue** (remove waiting jobs)
- **Obliterate queue** (remove all data)
- **Clean jobs** (by age/state)

### Cron Jobs
- Add cron (6-field expressions)
- Add cron (*/N shorthand)
- Delete cron
- List crons

### Monitoring
- Queue stats
- Detailed metrics
- Job state tracking

### Worker
- Single queue processing
- Multiple queues
- Concurrency control
- Event handling
- Graceful shutdown
- Progress updates from worker

### Authentication
- Token on connect
- Late authentication
- Invalid token handling

### Real-Time Events
- WebSocket connection
- Event subscription (all queues)
- Queue-specific events
- WebSocket authentication

### gRPC (High Performance)
- Unary RPCs (push, pull, ack)
- Batch operations
- Server-side streaming
- Job state via gRPC

## Quick Reference

```typescript
import { FlashQ, Worker } from 'flashq';

// Client
const client = new FlashQ({ host: 'localhost', port: 6789, token: 'secret' });
await client.connect();

// Push with all options
const job = await client.push('queue', { data: 'value' }, {
  priority: 10,
  delay: 5000,
  ttl: 60000,
  timeout: 30000,
  max_attempts: 3,
  backoff: 1000,
  unique_key: 'unique-123',
  tags: ['tag1', 'tag2'],
  lifo: false,
  remove_on_complete: false,
  remove_on_fail: false,
  stall_timeout: 30000,
  debounce_id: 'event-123',
  debounce_ttl: 5000,
  jobId: 'order-12345',           // Custom ID for idempotency
  keepCompletedAge: 86400000,     // Keep result for 24h
  keepCompletedCount: 100,        // Keep in last 100 completed
});

// Idempotent push - returns same job if jobId exists
const sameJob = await client.push('queue', { data: 'value' }, { jobId: 'order-12345' });
console.log(job.id === sameJob.id); // true

// Lookup by custom ID
const found = await client.getJobByCustomId('order-12345');
console.log(found?.state); // 'waiting'

// Wait for job completion (synchronous workflow)
const result = await client.finished(job.id, 30000); // 30s timeout
console.log(result); // { processed: true }

// Pull & Ack
const pulled = await client.pull('queue');
await client.ack(pulled.id, { result: 'data' });

// Advanced queue operations
await client.drain('queue');           // Remove all waiting jobs
await client.obliterate('queue');      // Remove ALL queue data
await client.clean('queue', 3600000, 'completed'); // Clean old completed

// Worker
const worker = new Worker('queue', async (job) => {
  await client.progress(job.id, 50, 'Halfway done');
  return { processed: true };
}, { concurrency: 5 });

await worker.start();

// Graceful shutdown
process.on('SIGTERM', async () => {
  await worker.stop();
  await client.close();
});
```

## Test Files

| File | Description |
|------|-------------|
| `comprehensive-test.ts` | Tests all SDK features (45+ tests) |
| `stress-test.ts` | Load and stress testing (33 tests) |
| `test-all-apis.ts` | Complete API coverage tests |
| `test-advanced-features.ts` | BullMQ-like advanced features tests |

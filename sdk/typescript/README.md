# MagicQueue TypeScript SDK

Official TypeScript client for [MagicQueue](https://github.com/egeominotti/magic-queue) - High-Performance Job Queue.

**Runtime: [Bun](https://bun.sh)** - This SDK is designed for Bun runtime.

## Installation

```bash
bun add magicqueue
```

## Quick Start

```typescript
import { MagicQueue, Worker } from 'magicqueue';

// Create a client
const client = new MagicQueue({
  host: 'localhost',
  port: 6789,
  token: 'your-auth-token', // optional
});

await client.connect();

// Push a job
const job = await client.push('emails', {
  to: 'user@example.com',
  subject: 'Hello',
  body: 'Welcome to MagicQueue!',
});
console.log(`Job created: ${job.id}`);

// Create a worker to process jobs
const worker = new Worker('emails', async (job) => {
  console.log(`Processing job ${job.id}`);
  await sendEmail(job.data);
  return { sent: true };
});

await worker.start();
```

## API Reference

### Client

#### Connection

```typescript
const client = new MagicQueue({
  host: 'localhost',     // Server host (default: "localhost")
  port: 6789,            // TCP port (default: 6789)
  httpPort: 6790,        // HTTP port (default: 6790)
  token: 'secret',       // Auth token (optional)
  timeout: 5000,         // Connection timeout (default: 5000ms)
  useHttp: false,        // Use HTTP instead of TCP (default: false)
});

await client.connect();
await client.close();
```

#### Push Jobs

```typescript
// Simple push
const job = await client.push('queue-name', { any: 'data' });

// With options
const job = await client.push('queue-name', { data: 'value' }, {
  priority: 10,          // Higher = processed first
  delay: 5000,           // Run after 5 seconds
  ttl: 60000,            // Expire after 60 seconds
  timeout: 30000,        // Fail if processing takes > 30s
  max_attempts: 3,       // Retry up to 3 times
  backoff: 1000,         // Backoff: 1s, 2s, 4s...
  unique_key: 'user-123', // Prevent duplicates
  depends_on: [1, 2],    // Wait for jobs 1 and 2
  tags: ['urgent'],      // Categorization
});

// Batch push
const ids = await client.pushBatch('queue-name', [
  { data: { task: 1 } },
  { data: { task: 2 }, priority: 5 },
  { data: { task: 3 }, delay: 10000 },
]);
```

#### Pull Jobs

```typescript
// Pull single job (blocking)
const job = await client.pull('queue-name');

// Pull multiple jobs
const jobs = await client.pullBatch('queue-name', 10);
```

#### Acknowledge/Fail

```typescript
// Acknowledge success
await client.ack(job.id);

// Acknowledge with result
await client.ack(job.id, { processed: true, count: 42 });

// Batch acknowledge
await client.ackBatch([1, 2, 3, 4, 5]);

// Fail job (will retry or move to DLQ)
await client.fail(job.id, 'Connection timeout');
```

#### Job State

```typescript
// Get job with state
const { job, state } = await client.getJob(123);
// state: 'waiting' | 'delayed' | 'active' | 'completed' | 'failed' | 'waiting-children'

// Get state only
const state = await client.getState(123);

// Get result
const result = await client.getResult(123);

// Cancel pending job
await client.cancel(123);
```

#### Progress Tracking

```typescript
// Update progress (0-100)
await client.progress(job.id, 50, 'Processing item 50/100');

// Get progress
const { progress, message } = await client.getProgress(job.id);
```

#### Dead Letter Queue

```typescript
// Get failed jobs
const failedJobs = await client.getDlq('queue-name', 100);

// Retry all DLQ jobs
const count = await client.retryDlq('queue-name');

// Retry specific job
await client.retryDlq('queue-name', 123);
```

#### Queue Control

```typescript
// Pause/Resume
await client.pause('queue-name');
await client.resume('queue-name');

// Rate limiting (jobs per second)
await client.setRateLimit('queue-name', 100);
await client.clearRateLimit('queue-name');

// Concurrency limit
await client.setConcurrency('queue-name', 5);
await client.clearConcurrency('queue-name');

// List queues
const queues = await client.listQueues();
```

#### Cron Jobs

```typescript
// Add cron job (6-field format: sec min hour day month weekday)
await client.addCron('daily-cleanup', {
  queue: 'maintenance',
  data: { task: 'cleanup' },
  schedule: '0 0 3 * * *', // Every day at 3:00 AM
  priority: 0,
});

// Simple interval (every N seconds)
await client.addCron('health-check', {
  queue: 'monitoring',
  data: {},
  schedule: '*/30', // Every 30 seconds
});

// List cron jobs
const crons = await client.listCrons();

// Delete cron job
await client.deleteCron('daily-cleanup');
```

#### Statistics

```typescript
// Queue stats
const stats = await client.stats();
// { queued: 100, processing: 5, delayed: 20, dlq: 2 }

// Detailed metrics
const metrics = await client.metrics();
// { total_pushed, total_completed, total_failed, jobs_per_second, avg_latency_ms, queues }
```

### Worker

```typescript
import { Worker } from 'magicqueue';

const worker = new Worker(
  'queue-name',  // or ['queue1', 'queue2']
  async (job) => {
    // Process job
    console.log('Processing:', job.data);

    // Update progress
    await worker.updateProgress(job.id, 50, 'Halfway done');

    // Return result (stored if using ack with result)
    return { success: true };
  },
  {
    concurrency: 5,          // Process 5 jobs in parallel
    autoAck: true,           // Auto-ack on success (default: true)
    heartbeatInterval: 30000, // Heartbeat every 30s

    // Client options
    host: 'localhost',
    port: 6789,
    token: 'secret',
  }
);

// Event handlers
worker.on('ready', () => console.log('Worker ready'));
worker.on('active', (job, workerId) => console.log(`Job ${job.id} started`));
worker.on('completed', (job, result) => console.log(`Job ${job.id} completed`));
worker.on('failed', (job, error) => console.log(`Job ${job.id} failed:`, error));
worker.on('error', (error) => console.error('Worker error:', error));

// Start processing
await worker.start();

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Shutting down...');
  await worker.stop();
  process.exit(0);
});
```

## TypeScript Types

All types are fully exported:

```typescript
import type {
  Job,
  JobState,
  JobWithState,
  PushOptions,
  QueueInfo,
  QueueStats,
  Metrics,
  CronJob,
  CronOptions,
  WorkerOptions,
  ClientOptions,
  JobProcessor,
} from 'magicqueue';
```

## Examples

### Email Queue

```typescript
import { MagicQueue, Worker } from 'magicqueue';

interface EmailJob {
  to: string;
  subject: string;
  body: string;
}

// Producer
const client = new MagicQueue();
await client.connect();

await client.push<EmailJob>('emails', {
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Hello, welcome to our service.',
}, {
  max_attempts: 3,
  backoff: 5000, // Retry with 5s, 10s, 20s delays
});

// Consumer
const worker = new Worker<EmailJob>('emails', async (job) => {
  await sendEmail(job.data.to, job.data.subject, job.data.body);
  return { sentAt: new Date().toISOString() };
}, { concurrency: 10 });

await worker.start();
```

### Job with Dependencies

```typescript
// Create parent jobs
const job1 = await client.push('process', { step: 1 });
const job2 = await client.push('process', { step: 2 });

// Create dependent job (runs after job1 and job2 complete)
const job3 = await client.push('process', { step: 3, final: true }, {
  depends_on: [job1.id, job2.id],
});
```

### Scheduled Jobs

```typescript
// Process reports every day at midnight
await client.addCron('daily-reports', {
  queue: 'reports',
  data: { type: 'daily' },
  schedule: '0 0 0 * * *',
});

// Weekly cleanup on Sundays at 3 AM
await client.addCron('weekly-cleanup', {
  queue: 'maintenance',
  data: { type: 'weekly' },
  schedule: '0 0 3 * * 0',
});
```

## License

MIT

# flashQ TypeScript SDK

Official TypeScript client for [flashQ](https://github.com/egeominotti/flashq) - High-Performance Job Queue.

## Installation

```bash
bun add flashq
```

## Quick Start

```typescript
import { Queue } from 'flashq';

// Create a queue
const emails = new Queue('emails');

// Add a job (auto-connects!)
await emails.add({ to: 'user@example.com', subject: 'Hello!' });

// Process jobs
emails.process(async (job) => {
  await sendEmail(job.data);
  return { sent: true };
});
```

**That's it.** No `connect()`, no `close()`, no boilerplate.

## API

### Queue (Recommended)

The simplest way to use flashQ.

```typescript
import { Queue } from 'flashq';

// Create queue
const queue = new Queue('my-queue');

// With connection options
const queue = new Queue('my-queue', {
  connection: {
    host: 'localhost',
    port: 6789,
    token: 'secret'
  }
});
```

#### Add Jobs

```typescript
// Simple
await queue.add({ task: 'process-image', url: '...' });

// With options
await queue.add({ task: 'send-email' }, {
  priority: 10,        // Higher = processed first
  delay: 5000,         // Run after 5 seconds
  max_attempts: 3,     // Retry up to 3 times
  backoff: 1000,       // Backoff: 1s, 2s, 4s...
  timeout: 30000,      // Fail if takes > 30s
  unique_key: 'user-123'  // Prevent duplicates
});

// Bulk add
await queue.addBulk([
  { data: { task: 1 } },
  { data: { task: 2 }, priority: 5 },
  { data: { task: 3 }, delay: 10000 }
]);
```

#### Process Jobs

```typescript
// Simple
queue.process(async (job) => {
  console.log('Processing:', job.data);
  await doWork(job.data);
});

// With concurrency
queue.process(async (job) => {
  await doWork(job.data);
}, { concurrency: 10 });

// With return value
queue.process(async (job) => {
  const result = await processImage(job.data.url);
  return { processed: true, result };
});
```

#### Queue Control

```typescript
await queue.pause();           // Pause processing
await queue.resume();          // Resume processing
await queue.setRateLimit(100); // Max 100 jobs/sec
await queue.setConcurrency(5); // Max 5 parallel jobs

// Failed jobs (DLQ)
const failed = await queue.getFailed();
await queue.retryFailed();     // Retry all
await queue.retryFailed(123);  // Retry specific job

// Cleanup
await queue.close();
```

### Low-Level Client

For advanced use cases or when you need more control.

```typescript
import { FlashQ } from 'flashq';

const client = new FlashQ({
  host: 'localhost',
  port: 6789,
  token: 'secret'
});

// Auto-connects on first operation!
await client.add('emails', { to: 'user@example.com' });

// Bulk operations (2M+ ops/sec)
await client.addBulk('emails', [
  { data: { to: 'a@test.com' } },
  { data: { to: 'b@test.com' } }
]);

// Pull and ack
const job = await client.pull('emails');
await client.ack(job.id, { result: 'sent' });

// Batch pull + ack (500k+ ops/sec)
const jobs = await client.pullBatch('emails', 100);
await client.ackBatch(jobs.map(j => j.id));

await client.close();
```

### Worker

For more control over job processing.

```typescript
import { Worker } from 'flashq';

const worker = new Worker('emails', async (job) => {
  await sendEmail(job.data);
  return { sent: true };
}, {
  concurrency: 10,
  host: 'localhost',
  port: 6789
});

worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} completed:`, result);
});

worker.on('failed', (job, error) => {
  console.error(`Job ${job.id} failed:`, error);
});

await worker.start();

// Graceful shutdown
process.on('SIGTERM', async () => {
  await worker.stop();
});
```

## TypeScript

Full TypeScript support with generics:

```typescript
interface EmailJob {
  to: string;
  subject: string;
  body: string;
}

interface EmailResult {
  messageId: string;
  sent: boolean;
}

const emails = new Queue<EmailJob, EmailResult>('emails');

// Type-safe!
await emails.add({
  to: 'user@example.com',
  subject: 'Hello',
  body: 'Welcome!'
});

emails.process(async (job) => {
  // job.data is EmailJob
  const result = await sendEmail(job.data);
  return { messageId: result.id, sent: true }; // Must be EmailResult
});
```

## Comparison with BullMQ

| Feature | flashQ | BullMQ |
|---------|--------|--------|
| Auto-connect | ✅ | ❌ |
| Zero dependencies | ✅ | ❌ (Redis) |
| Batch throughput | 2M+ ops/sec | ~36K ops/sec |
| Setup | `new Queue('x')` | Redis + Queue + Worker |

## License

MIT

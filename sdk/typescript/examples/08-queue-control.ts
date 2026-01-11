/**
 * Queue Control Example
 *
 * Demonstrates pause/resume, rate limiting, and concurrency control.
 *
 * Run: npx ts-node examples/08-queue-control.ts
 */

import { MagicQueue, Worker } from '../src';

async function main() {
  const client = new MagicQueue({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected\n');

  const QUEUE = 'controlled-queue';

  try {
    // Push some jobs first
    console.log('📤 Pushing 20 jobs...');
    for (let i = 0; i < 20; i++) {
      await client.push(QUEUE, { index: i });
    }
    console.log('   Done\n');

    // ===== PAUSE/RESUME =====
    console.log('═══════════════════════════════════════');
    console.log('⏸️  PAUSE / RESUME DEMO');
    console.log('═══════════════════════════════════════\n');

    // Pause the queue
    console.log('Pausing queue...');
    await client.pause(QUEUE);

    // Check queue status
    let queues = await client.listQueues();
    let queueInfo = queues.find((q) => q.name === QUEUE);
    console.log(`   Queue paused: ${queueInfo?.paused}\n`);

    // Start worker (won't process while paused)
    const worker = new Worker(
      QUEUE,
      async (job) => {
        console.log(`   Processing job ${job.data.index}`);
        await sleep(100);
        return { processed: true };
      },
      { host: 'localhost', port: 6789, concurrency: 3 }
    );

    await worker.start();
    console.log('Worker started (queue is paused, no jobs will process)');
    await sleep(2000);

    // Resume the queue
    console.log('\nResuming queue...');
    await client.resume(QUEUE);
    queues = await client.listQueues();
    queueInfo = queues.find((q) => q.name === QUEUE);
    console.log(`   Queue paused: ${queueInfo?.paused}`);
    console.log('   Jobs should start processing now:\n');

    await sleep(3000);
    await worker.stop();

    // ===== RATE LIMITING =====
    console.log('\n═══════════════════════════════════════');
    console.log('🚦 RATE LIMITING DEMO');
    console.log('═══════════════════════════════════════\n');

    // Push more jobs
    console.log('Pushing 10 more jobs...');
    for (let i = 0; i < 10; i++) {
      await client.push(QUEUE, { index: i, batch: 'rate-limited' });
    }

    // Set rate limit: 2 jobs per second
    console.log('Setting rate limit: 2 jobs/second');
    await client.setRateLimit(QUEUE, 2);

    queues = await client.listQueues();
    queueInfo = queues.find((q) => q.name === QUEUE);
    console.log(`   Rate limit: ${queueInfo?.rate_limit} jobs/sec\n`);

    // Create worker to show rate limiting
    const rateLimitedWorker = new Worker(
      QUEUE,
      async (job) => {
        const now = new Date().toISOString().split('T')[1].split('.')[0];
        console.log(`   [${now}] Processing job ${job.data.index}`);
        return { processed: true };
      },
      { host: 'localhost', port: 6789, concurrency: 5 }
    );

    await rateLimitedWorker.start();
    console.log('Worker started (5 concurrency, but rate limited to 2/sec):\n');

    await sleep(6000);
    await rateLimitedWorker.stop();

    // Clear rate limit
    console.log('\nClearing rate limit...');
    await client.clearRateLimit(QUEUE);

    // ===== CONCURRENCY CONTROL =====
    console.log('\n═══════════════════════════════════════');
    console.log('🔢 CONCURRENCY CONTROL DEMO');
    console.log('═══════════════════════════════════════\n');

    // Push more jobs
    console.log('Pushing 10 more jobs...');
    for (let i = 0; i < 10; i++) {
      await client.push(QUEUE, { index: i, batch: 'concurrency-limited' });
    }

    // Set concurrency limit: max 2 jobs processing at once
    console.log('Setting concurrency limit: 2 jobs max');
    await client.setConcurrency(QUEUE, 2);

    queues = await client.listQueues();
    queueInfo = queues.find((q) => q.name === QUEUE);
    console.log(`   Concurrency limit: ${queueInfo?.concurrency_limit}\n`);

    // Create worker
    const concurrencyWorker = new Worker(
      QUEUE,
      async (job) => {
        const now = new Date().toISOString().split('T')[1].split('.')[0];
        console.log(`   [${now}] START job ${job.data.index}`);
        await sleep(1000); // Each job takes 1 second
        console.log(`   [${now}] END job ${job.data.index}`);
        return { processed: true };
      },
      { host: 'localhost', port: 6789, concurrency: 10 } // Worker wants 10, but server limits to 2
    );

    await concurrencyWorker.start();
    console.log('Worker started (wants 10 concurrency, but server limits to 2):\n');

    await sleep(6000);
    await concurrencyWorker.stop();

    // Clear concurrency limit
    console.log('\nClearing concurrency limit...');
    await client.clearConcurrency(QUEUE);

    // ===== QUEUE LIST =====
    console.log('\n═══════════════════════════════════════');
    console.log('📋 QUEUE LIST');
    console.log('═══════════════════════════════════════\n');

    queues = await client.listQueues();
    console.log('All queues:');
    for (const q of queues) {
      console.log(`\n   ${q.name}:`);
      console.log(`      Pending: ${q.pending}`);
      console.log(`      Processing: ${q.processing}`);
      console.log(`      DLQ: ${q.dlq}`);
      console.log(`      Paused: ${q.paused}`);
      console.log(`      Rate limit: ${q.rate_limit ?? 'none'}`);
      console.log(`      Concurrency: ${q.concurrency_limit ?? 'none'}`);
    }

  } finally {
    await client.close();
    console.log('\n👋 Disconnected');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);

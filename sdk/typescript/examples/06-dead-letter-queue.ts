/**
 * Dead Letter Queue (DLQ) Example
 *
 * Demonstrates handling failed jobs with retries and DLQ management.
 *
 * Run: npx ts-node examples/06-dead-letter-queue.ts
 */

import { FlashQ, Worker } from '../src';

interface UnreliableJob {
  taskId: string;
  shouldFail: boolean;
}

async function main() {
  const client = new FlashQ({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected\n');

  const QUEUE = 'dlq-demo';

  try {
    // Push jobs that will fail
    console.log('📤 Pushing jobs (some will fail)...');

    // Job that will succeed
    await client.push<UnreliableJob>(QUEUE, {
      taskId: 'task-1',
      shouldFail: false,
    });

    // Jobs that will always fail (will go to DLQ after max_attempts)
    await client.push<UnreliableJob>(
      QUEUE,
      { taskId: 'task-2', shouldFail: true },
      { max_attempts: 3, backoff: 100 } // Fast backoff for demo
    );

    await client.push<UnreliableJob>(
      QUEUE,
      { taskId: 'task-3', shouldFail: true },
      { max_attempts: 2, backoff: 100 }
    );

    console.log('   Pushed 3 jobs (1 will succeed, 2 will fail)\n');

    // Create worker that sometimes fails
    let processCount = 0;
    const worker = new Worker<UnreliableJob>(
      QUEUE,
      async (job) => {
        processCount++;
        console.log(`⚙️  Processing ${job.data.taskId} (attempt ${job.attempts + 1})`);

        if (job.data.shouldFail) {
          throw new Error(`Simulated failure for ${job.data.taskId}`);
        }

        return { success: true };
      },
      {
        host: 'localhost',
        port: 6789,
        autoAck: true,
        concurrency: 1,
      }
    );

    worker.on('completed', (job) => {
      console.log(`   ✅ ${job.data.taskId} completed\n`);
    });

    worker.on('failed', (job, error) => {
      console.log(`   ❌ ${job.data.taskId} failed: ${(error as Error).message}`);
      console.log(`      Attempts: ${job.attempts + 1}/${job.max_attempts || 'unlimited'}\n`);
    });

    await worker.start();
    console.log('🚀 Worker started\n');

    // Wait for processing
    await sleep(3000);
    await worker.stop();

    // Check DLQ
    console.log('\n📋 Checking Dead Letter Queue...');
    const dlqJobs = await client.getDlq(QUEUE);
    console.log(`   Found ${dlqJobs.length} jobs in DLQ:\n`);

    for (const job of dlqJobs) {
      console.log(`   Job ${job.id}:`);
      console.log(`   - Task: ${(job.data as UnreliableJob).taskId}`);
      console.log(`   - Attempts: ${job.attempts}`);
      console.log(`   - Last error: ${job.progress_msg || 'N/A'}\n`);
    }

    // Retry DLQ jobs
    if (dlqJobs.length > 0) {
      console.log('🔄 Retrying all DLQ jobs...');
      const retried = await client.retryDlq(QUEUE);
      console.log(`   Moved ${retried} jobs back to queue\n`);

      // Check stats
      const stats = await client.stats();
      console.log('📊 Stats after retry:');
      console.log(`   Queued: ${stats.queued}`);
      console.log(`   DLQ: ${stats.dlq}`);
    }

    // You can also retry a specific job:
    // await client.retryDlq(QUEUE, specificJobId);

  } finally {
    await client.close();
    console.log('\n👋 Disconnected');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);

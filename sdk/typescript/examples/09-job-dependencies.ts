/**
 * Job Dependencies Example
 *
 * Demonstrates DAG-style job orchestration where jobs wait
 * for their dependencies to complete before running.
 *
 * Run: npx ts-node examples/09-job-dependencies.ts
 */

import { FlashQ, Worker } from '../src';

interface PipelineJob {
  step: string;
  data?: unknown;
}

async function main() {
  const client = new FlashQ({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected\n');

  const QUEUE = 'pipeline';

  try {
    console.log('═══════════════════════════════════════');
    console.log('🔗 DATA PIPELINE WITH DEPENDENCIES');
    console.log('═══════════════════════════════════════\n');

    /*
     * Pipeline structure:
     *
     *   [fetch-users] ──┬──► [merge-data] ──► [generate-report]
     *   [fetch-orders] ─┘
     *
     * - fetch-users and fetch-orders run in parallel
     * - merge-data waits for both to complete
     * - generate-report waits for merge-data
     */

    // Step 1: Create parallel jobs (no dependencies)
    console.log('📤 Creating pipeline jobs...\n');

    const fetchUsers = await client.push<PipelineJob>(QUEUE, {
      step: 'fetch-users',
      data: { source: 'user-database' },
    });
    console.log(`   Job ${fetchUsers.id}: fetch-users (no dependencies)`);

    const fetchOrders = await client.push<PipelineJob>(QUEUE, {
      step: 'fetch-orders',
      data: { source: 'order-database' },
    });
    console.log(`   Job ${fetchOrders.id}: fetch-orders (no dependencies)`);

    // Step 2: Create dependent job (waits for fetch jobs)
    const mergeData = await client.push<PipelineJob>(
      QUEUE,
      {
        step: 'merge-data',
        data: { operation: 'join' },
      },
      {
        depends_on: [fetchUsers.id, fetchOrders.id],
      }
    );
    console.log(`   Job ${mergeData.id}: merge-data (depends on ${fetchUsers.id}, ${fetchOrders.id})`);

    // Step 3: Create final job (waits for merge)
    const generateReport = await client.push<PipelineJob>(
      QUEUE,
      {
        step: 'generate-report',
        data: { format: 'pdf' },
      },
      {
        depends_on: [mergeData.id],
      }
    );
    console.log(`   Job ${generateReport.id}: generate-report (depends on ${mergeData.id})\n`);

    // Check initial states
    console.log('📊 Initial job states:');
    for (const id of [fetchUsers.id, fetchOrders.id, mergeData.id, generateReport.id]) {
      const state = await client.getState(id);
      console.log(`   Job ${id}: ${state}`);
    }
    console.log('');

    // Create worker to process jobs
    const worker = new Worker<PipelineJob>(
      QUEUE,
      async (job) => {
        console.log(`\n   ⚡ Processing: ${job.data.step} (job ${job.id})`);

        // Simulate work based on step
        switch (job.data.step) {
          case 'fetch-users':
            await sleep(1000);
            return { users: ['user1', 'user2', 'user3'] };

          case 'fetch-orders':
            await sleep(1500);
            return { orders: ['order1', 'order2'] };

          case 'merge-data':
            await sleep(800);
            return { merged: true, records: 5 };

          case 'generate-report':
            await sleep(500);
            return { report: 'report.pdf', generated: true };

          default:
            return { processed: true };
        }
      },
      { host: 'localhost', port: 6789, concurrency: 2 }
    );

    worker.on('completed', async (job, result) => {
      console.log(`   ✅ Completed: ${(job.data as PipelineJob).step}`);
      console.log(`      Result: ${JSON.stringify(result)}`);
    });

    console.log('🚀 Starting worker...\n');
    await worker.start();

    // Monitor pipeline progress
    const monitorPipeline = async () => {
      let allDone = false;
      while (!allDone) {
        await sleep(500);

        const states = await Promise.all([
          client.getState(fetchUsers.id),
          client.getState(fetchOrders.id),
          client.getState(mergeData.id),
          client.getState(generateReport.id),
        ]);

        allDone = states.every((s) => s === 'completed');
      }
    };

    await monitorPipeline();
    await sleep(1000);

    // Final state check
    console.log('\n📊 Final job states:');
    for (const id of [fetchUsers.id, fetchOrders.id, mergeData.id, generateReport.id]) {
      const jobInfo = await client.getJob(id);
      if (jobInfo) {
        console.log(`   Job ${id} (${(jobInfo.job.data as PipelineJob).step}): ${jobInfo.state}`);
      }
    }

    // Get final results
    console.log('\n📄 Final results:');
    const reportResult = await client.getResult(generateReport.id);
    console.log(`   Report: ${JSON.stringify(reportResult)}`);

    await worker.stop();

  } finally {
    await client.close();
    console.log('\n👋 Disconnected');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);

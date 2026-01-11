/**
 * Batch Operations Example
 *
 * Demonstrates batch push, pull, and acknowledge operations
 * for high-throughput scenarios.
 *
 * Run: npx ts-node examples/04-batch-operations.ts
 */

import { MagicQueue } from '../src';

async function main() {
  const client = new MagicQueue({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected\n');

  try {
    const QUEUE = 'batch-queue';
    const BATCH_SIZE = 100;

    // 1. Batch Push
    console.log(`📤 Batch pushing ${BATCH_SIZE} jobs...`);
    const startPush = Date.now();

    const jobs = Array.from({ length: BATCH_SIZE }, (_, i) => ({
      data: {
        index: i,
        message: `Task ${i + 1}`,
        timestamp: Date.now(),
      },
      priority: i % 10, // Varying priorities
    }));

    const ids = await client.pushBatch(QUEUE, jobs);
    const pushTime = Date.now() - startPush;

    console.log(`   ✅ Pushed ${ids.length} jobs in ${pushTime}ms`);
    console.log(`   First ID: ${ids[0]}, Last ID: ${ids[ids.length - 1]}`);
    console.log(`   Throughput: ${Math.round((BATCH_SIZE / pushTime) * 1000)} jobs/sec\n`);

    // 2. Check stats
    const stats = await client.stats();
    console.log(`📊 Stats after push:`);
    console.log(`   Queued: ${stats.queued}\n`);

    // 3. Batch Pull
    console.log(`📥 Batch pulling ${BATCH_SIZE} jobs...`);
    const startPull = Date.now();

    const pulledJobs = await client.pullBatch(QUEUE, BATCH_SIZE);
    const pullTime = Date.now() - startPull;

    console.log(`   ✅ Pulled ${pulledJobs.length} jobs in ${pullTime}ms`);
    console.log(`   Throughput: ${Math.round((pulledJobs.length / pullTime) * 1000)} jobs/sec\n`);

    // 4. Process jobs (simulated)
    console.log(`⚙️  Processing ${pulledJobs.length} jobs...`);
    const processStart = Date.now();

    // Simulate processing
    await Promise.all(
      pulledJobs.map(async (job) => {
        // Simulate some work
        await sleep(Math.random() * 10);
      })
    );

    const processTime = Date.now() - processStart;
    console.log(`   ✅ Processed in ${processTime}ms\n`);

    // 5. Batch Acknowledge
    console.log(`✅ Batch acknowledging ${pulledJobs.length} jobs...`);
    const startAck = Date.now();

    const jobIds = pulledJobs.map((j) => j.id);
    const acked = await client.ackBatch(jobIds);
    const ackTime = Date.now() - startAck;

    console.log(`   ✅ Acknowledged ${acked} jobs in ${ackTime}ms`);
    console.log(`   Throughput: ${Math.round((pulledJobs.length / ackTime) * 1000)} jobs/sec\n`);

    // 6. Final stats
    const finalStats = await client.stats();
    console.log(`📊 Final stats:`);
    console.log(`   Queued: ${finalStats.queued}`);
    console.log(`   Processing: ${finalStats.processing}`);

    // 7. Metrics
    const metrics = await client.metrics();
    console.log(`\n📈 Metrics:`);
    console.log(`   Total Pushed: ${metrics.total_pushed}`);
    console.log(`   Total Completed: ${metrics.total_completed}`);
    console.log(`   Avg Latency: ${metrics.avg_latency_ms.toFixed(2)}ms`);

  } finally {
    await client.close();
    console.log('\n👋 Disconnected');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);

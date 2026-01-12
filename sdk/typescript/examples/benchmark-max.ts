/**
 * MagicQueue Maximum Throughput Benchmark
 *
 * Obiettivo: 10k+ jobs/sec
 */

import { MagicQueue } from '../src/index';

const HOST = process.env.MQ_HOST || 'localhost';
const PORT = parseInt(process.env.MQ_PORT || '6789');

const CONFIG = {
  // Push benchmark
  TOTAL_JOBS: 100_000,
  BATCH_SIZE: 1000,
  CONNECTIONS: 10,

  // Pull benchmark
  PULL_JOBS: 50_000,
  PULL_BATCH: 100,
  PULL_WORKERS: 10,
};

async function benchmarkPush() {
  console.log('\n🚀 PUSH BENCHMARK');
  console.log('─'.repeat(50));

  const queue = `bench-push-${Date.now()}`;
  const clients: MagicQueue[] = [];

  // Create connections
  for (let i = 0; i < CONFIG.CONNECTIONS; i++) {
    const client = new MagicQueue({ host: HOST, port: PORT });
    await client.connect();
    clients.push(client);
  }
  console.log(`  ${CONFIG.CONNECTIONS} connections ready`);

  // Batch push test
  console.log(`\n  Pushing ${CONFIG.TOTAL_JOBS.toLocaleString()} jobs (batch size: ${CONFIG.BATCH_SIZE})...`);

  const startTime = Date.now();
  const batchesPerClient = Math.ceil(CONFIG.TOTAL_JOBS / CONFIG.BATCH_SIZE / CONFIG.CONNECTIONS);

  const promises = clients.map(async (client, clientIdx) => {
    for (let b = 0; b < batchesPerClient; b++) {
      const jobs = Array.from({ length: CONFIG.BATCH_SIZE }, (_, i) => ({
        data: { c: clientIdx, b, i, ts: Date.now() }
      }));
      await client.pushBatch(queue, jobs);
    }
  });

  await Promise.all(promises);
  const elapsed = Date.now() - startTime;

  const totalPushed = batchesPerClient * CONFIG.BATCH_SIZE * CONFIG.CONNECTIONS;
  const throughput = Math.round(totalPushed / (elapsed / 1000));

  console.log(`\n  ✅ Results:`);
  console.log(`     Jobs pushed: ${totalPushed.toLocaleString()}`);
  console.log(`     Time: ${elapsed}ms`);
  console.log(`     Throughput: ${throughput.toLocaleString()} jobs/sec`);

  if (throughput >= 10000) {
    console.log(`\n  🏆 TARGET REACHED! ${throughput.toLocaleString()} > 10,000 jobs/sec`);
  }

  // Cleanup
  for (const client of clients) {
    await client.close();
  }

  return { queue, throughput, totalPushed };
}

async function benchmarkPull(sourceQueue: string) {
  console.log('\n🔄 PULL + ACK BENCHMARK');
  console.log('─'.repeat(50));

  const clients: MagicQueue[] = [];

  // Create worker connections
  for (let i = 0; i < CONFIG.PULL_WORKERS; i++) {
    const client = new MagicQueue({ host: HOST, port: PORT });
    await client.connect();
    clients.push(client);
  }
  console.log(`  ${CONFIG.PULL_WORKERS} workers ready`);

  console.log(`\n  Pulling & acking ${CONFIG.PULL_JOBS.toLocaleString()} jobs (batch size: ${CONFIG.PULL_BATCH})...`);

  let totalProcessed = 0;
  const startTime = Date.now();

  const workerPromises = clients.map(async (client) => {
    let processed = 0;
    while (totalProcessed < CONFIG.PULL_JOBS) {
      const jobs = await client.pullBatch(sourceQueue, CONFIG.PULL_BATCH);
      if (jobs.length === 0) {
        await new Promise(r => setTimeout(r, 5));
        continue;
      }

      const ids = jobs.map(j => j.id);
      await client.ackBatch(ids);

      processed += jobs.length;
      totalProcessed += jobs.length;

      if (totalProcessed % 10000 === 0) {
        process.stdout.write(`\r     Processed: ${totalProcessed.toLocaleString()}`);
      }
    }
    return processed;
  });

  await Promise.all(workerPromises);
  const elapsed = Date.now() - startTime;

  const throughput = Math.round(totalProcessed / (elapsed / 1000));

  console.log(`\n\n  ✅ Results:`);
  console.log(`     Jobs processed: ${totalProcessed.toLocaleString()}`);
  console.log(`     Time: ${elapsed}ms`);
  console.log(`     Throughput: ${throughput.toLocaleString()} jobs/sec`);

  if (throughput >= 10000) {
    console.log(`\n  🏆 TARGET REACHED! ${throughput.toLocaleString()} > 10,000 jobs/sec`);
  }

  // Cleanup
  for (const client of clients) {
    await client.close();
  }

  return { throughput, totalProcessed };
}

async function benchmarkSinglePush() {
  console.log('\n⚡ SINGLE PUSH BENCHMARK (no batch)');
  console.log('─'.repeat(50));

  const client = new MagicQueue({ host: HOST, port: PORT });
  await client.connect();

  const queue = `bench-single-${Date.now()}`;
  const jobs = 10_000;

  console.log(`  Pushing ${jobs.toLocaleString()} jobs one by one...`);

  const startTime = Date.now();

  for (let i = 0; i < jobs; i++) {
    await client.push(queue, { i, ts: Date.now() });
    if (i > 0 && i % 2000 === 0) {
      process.stdout.write(`\r     Progress: ${i.toLocaleString()}`);
    }
  }

  const elapsed = Date.now() - startTime;
  const throughput = Math.round(jobs / (elapsed / 1000));

  console.log(`\n\n  ✅ Results:`);
  console.log(`     Jobs pushed: ${jobs.toLocaleString()}`);
  console.log(`     Time: ${elapsed}ms`);
  console.log(`     Throughput: ${throughput.toLocaleString()} jobs/sec`);

  await client.close();
  return { throughput };
}

async function main() {
  console.log('═'.repeat(60));
  console.log('  MagicQueue Maximum Throughput Benchmark');
  console.log('═'.repeat(60));

  // Single push (baseline)
  const single = await benchmarkSinglePush();

  // Batch push (optimized)
  const push = await benchmarkPush();

  // Pull + Ack
  const pull = await benchmarkPull(push.queue);

  // Summary
  console.log('\n' + '═'.repeat(60));
  console.log('  SUMMARY');
  console.log('═'.repeat(60));
  console.log(`\n  Single Push:     ${single.throughput.toLocaleString()} jobs/sec`);
  console.log(`  Batch Push:      ${push.throughput.toLocaleString()} jobs/sec`);
  console.log(`  Pull + Ack:      ${pull.throughput.toLocaleString()} jobs/sec`);

  const target = 10_000;
  console.log(`\n  Target: ${target.toLocaleString()} jobs/sec`);

  if (push.throughput >= target) {
    console.log(`\n  🏆 PUSH: ✅ PASSED (${(push.throughput / target).toFixed(1)}x target)`);
  } else {
    console.log(`\n  ❌ PUSH: ${((push.throughput / target) * 100).toFixed(0)}% of target`);
  }

  if (pull.throughput >= target) {
    console.log(`  🏆 PULL: ✅ PASSED (${(pull.throughput / target).toFixed(1)}x target)`);
  } else {
    console.log(`  ❌ PULL: ${((pull.throughput / target) * 100).toFixed(0)}% of target`);
  }

  console.log('\n' + '═'.repeat(60));
}

main().catch(console.error);

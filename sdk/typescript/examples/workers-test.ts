/**
 * Parallel Processing Test
 * Tests how throughput scales with concurrent job processing
 */
import { FlashQ } from '../src';

const QUEUE = 'parallel-test';
const JOBS = 1000;
const WORK_DELAY_MS = 10; // Simulated work per job

async function runTest(concurrency: number): Promise<number> {
  const client = new FlashQ();
  await client.connect();

  // Cleanup and push
  await client.drain(QUEUE);
  const jobs = Array.from({ length: JOBS }, (_, i) => ({ data: { id: i } }));
  await client.pushBatch(QUEUE, jobs);

  const startTime = Date.now();
  let processed = 0;

  // Process in batches with parallel execution
  while (processed < JOBS) {
    // Pull a batch
    const batch = await client.pullBatch<{ id: number }>(QUEUE, concurrency);
    if (batch.length === 0) break;

    // Process ALL jobs in batch in parallel (simulated work)
    await Promise.all(batch.map(async (job) => {
      await new Promise(r => setTimeout(r, WORK_DELAY_MS)); // Simulate work
      await client.ack(job.id);
    }));

    processed += batch.length;
  }

  const elapsed = Date.now() - startTime;
  const rate = Math.round(JOBS / elapsed * 1000);

  await client.close();
  return rate;
}

async function main() {
  console.log(`\n=== Parallel Processing Test ===`);
  console.log(`Jobs: ${JOBS}, Work delay: ${WORK_DELAY_MS}ms per job\n`);
  console.log(`Sequential baseline: ${Math.round(1000 / WORK_DELAY_MS)} jobs/sec\n`);

  const configs = [1, 10, 50, 100, 200, 500];
  const results: { concurrency: number; rate: number }[] = [];

  for (const concurrency of configs) {
    process.stdout.write(`Concurrency ${concurrency}... `);
    const rate = await runTest(concurrency);
    results.push({ concurrency, rate });
    console.log(`${rate.toLocaleString()} jobs/sec`);
  }

  console.log('\n=== Results ===');
  console.log('Concurrency | Jobs/sec | vs Sequential');
  console.log('------------|----------|---------------');
  const sequential = 1000 / WORK_DELAY_MS;
  for (const r of results) {
    const speedup = (r.rate / sequential).toFixed(1);
    console.log(`${r.concurrency.toString().padStart(11)} | ${r.rate.toString().padStart(8)} | ${speedup}x`);
  }
}

main().catch(console.error);

/**
 * Bun-Optimized Benchmark
 *
 * Compares performance of:
 * - Old: Node.js net module (FlashQ + Worker)
 * - New: Bun native TCP API (BunFlashQ + BunWorker)
 *
 * Run: bun run examples/bun-benchmark.ts
 */

import { BunFlashQ, BunWorker } from '../src';

const QUEUE_NAME = 'bun-bench';
const TOTAL_JOBS = 50_000;
const BATCH_SIZE = 1000;
const NUM_WORKERS = 4;
const WORKER_CONCURRENCY = 50;

async function main() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   BUN-OPTIMIZED BENCHMARK: 50,000 JOBS                         ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  console.log(`Configuration:`);
  console.log(`  Workers: ${NUM_WORKERS} × ${WORKER_CONCURRENCY} concurrency = ${NUM_WORKERS * WORKER_CONCURRENCY} parallel`);
  console.log(`  Jobs: ${TOTAL_JOBS.toLocaleString()}`);
  console.log(`  Batch size: ${BATCH_SIZE}\n`);

  // Setup client
  const client = new BunFlashQ({ host: 'localhost', port: 6789, timeout: 60000 });
  await client.connect();

  // Cleanup
  console.log('Cleaning up queue...');
  await client.obliterate(QUEUE_NAME);

  // Track results
  let completed = 0;
  let failed = 0;
  const startTime = Date.now();

  // Create Bun-optimized workers
  const workers: BunWorker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new BunWorker(
      QUEUE_NAME,
      async (job) => {
        const { value } = job.data as { value: number };
        // Simple computation
        return { result: value * 2 + 1 };
      },
      {
        host: 'localhost',
        port: 6789,
        concurrency: WORKER_CONCURRENCY,
        batchSize: 100,
        timeout: 60000,
      }
    );

    worker.on('completed', () => completed++);
    worker.on('failed', () => failed++);
    workers.push(worker);
  }

  // Start workers
  console.log('Starting Bun-optimized workers...\n');
  await Promise.all(workers.map((w) => w.start()));

  // Push jobs
  console.log(`Pushing ${TOTAL_JOBS.toLocaleString()} jobs...`);
  const pushStart = Date.now();

  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batch = [];
    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      batch.push({ data: { value: i + j } });
    }
    await client.pushBatch(QUEUE_NAME, batch);

    if ((i + BATCH_SIZE) % 10000 === 0) {
      console.log(`  Pushed: ${Math.min(i + BATCH_SIZE, TOTAL_JOBS).toLocaleString()}`);
    }
  }

  const pushTime = Date.now() - pushStart;
  console.log(`\nPush complete: ${pushTime}ms (${Math.round((TOTAL_JOBS / pushTime) * 1000).toLocaleString()} jobs/sec)\n`);

  // Wait for completion
  console.log('Processing jobs...\n');
  let lastPrint = Date.now();

  while (completed + failed < TOTAL_JOBS) {
    await Bun.sleep(100);

    const now = Date.now();
    if (now - lastPrint >= 1000) {
      const elapsed = ((now - startTime) / 1000).toFixed(1);
      const rate = Math.round(completed / ((now - startTime) / 1000));
      const pct = ((completed / TOTAL_JOBS) * 100).toFixed(1);
      const eta = rate > 0 ? Math.round((TOTAL_JOBS - completed) / rate) : '?';

      console.log(
        `[${elapsed}s] Completed: ${completed.toLocaleString()} (${pct}%) | ` +
          `Rate: ${rate.toLocaleString()}/s | ETA: ${eta}s`
      );
      lastPrint = now;
    }
  }

  const totalTime = Date.now() - startTime;

  // Stop workers
  await Promise.all(workers.map((w) => w.stop()));

  // Results
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║                       RESULTS                                  ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  console.log(`Total Jobs:        ${TOTAL_JOBS.toLocaleString()}`);
  console.log(`Completed:         ${completed.toLocaleString()}`);
  console.log(`Failed:            ${failed}`);
  console.log(`\nPerformance:`);
  console.log(`  Push Rate:       ${Math.round((TOTAL_JOBS / pushTime) * 1000).toLocaleString()} jobs/sec`);
  console.log(`  Process Rate:    ${Math.round((completed / totalTime) * 1000).toLocaleString()} jobs/sec`);
  console.log(`  Total Time:      ${(totalTime / 1000).toFixed(2)}s`);

  await client.obliterate(QUEUE_NAME);
  await client.close();

  const success = completed === TOTAL_JOBS && failed === 0;
  console.log(success ? '\n✅ BENCHMARK PASSED\n' : '\n❌ BENCHMARK FAILED\n');
  process.exit(success ? 0 : 1);
}

main().catch(console.error);

/**
 * CPU-Bound Test: 100,000 Jobs with Real Computation
 *
 * Each job computes Fibonacci(30) = 832040
 * This tests realistic CPU-bound workloads.
 *
 * Run: bun run examples/cpu-bound-100k-test.ts
 */

import { FlashQ, Worker } from '../src';

const QUEUE_NAME = 'cpu-bound-100k';
const TOTAL_JOBS = 100_000;
const BATCH_SIZE = 1000;
const WORKER_CONCURRENCY = 50;
const NUM_WORKERS = 4;

// CPU-intensive Fibonacci calculation (non-memoized)
function fibonacci(n: number): number {
  if (n <= 1) return n;
  return fibonacci(n - 1) + fibonacci(n - 2);
}

// Verify Fibonacci works
const FIB_30 = 832040;
console.log(`Fibonacci(30) = ${fibonacci(30)} (expected ${FIB_30})`);

async function main() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   CPU-BOUND TEST: 100,000 JOBS WITH FIBONACCI(30)             ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const client = new FlashQ({ timeout: 60000 });
  await client.connect();

  console.log('Cleaning up queue...');
  await client.obliterate(QUEUE_NAME);

  // Track results
  const jobExpectations = new Map<number, number>();
  const jobResults = new Map<number, number>();

  let completed = 0;
  let failed = 0;
  const startTime = Date.now();

  // Create workers that compute Fibonacci
  const workers: Worker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new Worker(
      QUEUE_NAME,
      async (job) => {
        const { n, seed } = job.data as { n: number; seed: number };
        // Real CPU work: compute Fibonacci
        const fib = fibonacci(n);
        // Combine with seed for verification
        const result = fib + seed;
        return { jobId: job.id, result };
      },
      {
        host: 'localhost',
        port: 6789,
        concurrency: WORKER_CONCURRENCY,
        timeout: 60000,
      }
    );

    worker.on('completed', (job, returnValue) => {
      completed++;
      if (returnValue && typeof returnValue === 'object') {
        const { jobId, result } = returnValue as { jobId: number; result: number };
        jobResults.set(jobId, result);
      }
    });

    worker.on('failed', (job, error) => {
      failed++;
      console.error(`Job ${job?.id} failed:`, error);
    });

    workers.push(worker);
  }

  console.log(`Starting ${NUM_WORKERS} workers (${NUM_WORKERS * WORKER_CONCURRENCY} parallel)...\n`);
  await Promise.all(workers.map((w) => w.start()));

  // Push jobs
  console.log(`Pushing ${TOTAL_JOBS.toLocaleString()} jobs (each computes Fibonacci(30))...`);
  const pushStart = Date.now();

  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batchData: { n: number; seed: number }[] = [];

    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      const idx = i + j;
      batchData.push({ n: 30, seed: idx });
    }

    const jobIds = await client.pushBatch(
      QUEUE_NAME,
      batchData.map((data) => ({ data }))
    );

    for (let j = 0; j < jobIds.length; j++) {
      const jobId = jobIds[j];
      const expected = FIB_30 + batchData[j].seed;
      jobExpectations.set(jobId, expected);
    }

    if ((i + BATCH_SIZE) % 20000 === 0) {
      console.log(`  Pushed: ${Math.min(i + BATCH_SIZE, TOTAL_JOBS).toLocaleString()}/${TOTAL_JOBS.toLocaleString()}`);
    }
  }

  const pushTime = Date.now() - pushStart;
  console.log(`\nPush complete: ${pushTime}ms (${Math.round((TOTAL_JOBS / pushTime) * 1000).toLocaleString()} jobs/sec)\n`);

  // Wait for completion
  console.log('Processing CPU-bound jobs...\n');
  let lastPrint = Date.now();

  while (completed + failed < TOTAL_JOBS) {
    await new Promise((r) => setTimeout(r, 500));

    const now = Date.now();
    if (now - lastPrint >= 2000) {
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
  await Promise.all(workers.map((w) => w.stop()));

  // Verify results
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('VERIFYING RESULTS...');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  let verified = 0;
  let mismatches = 0;
  let missing = 0;

  for (const [jobId, expected] of jobExpectations) {
    const actual = jobResults.get(jobId);
    if (actual === undefined) {
      missing++;
    } else if (actual !== expected) {
      mismatches++;
      if (mismatches <= 3) {
        console.error(`MISMATCH: Job ${jobId} expected ${expected}, got ${actual}`);
      }
    } else {
      verified++;
    }
  }

  // Results
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║                       FINAL RESULTS                            ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  console.log(`Workload:          Fibonacci(30) per job`);
  console.log(`Total Jobs:        ${TOTAL_JOBS.toLocaleString()}`);
  console.log(`Completed:         ${completed.toLocaleString()}`);
  console.log(`Failed:            ${failed}`);
  console.log(`\nVerification:`);
  console.log(`  ✓ Verified:      ${verified.toLocaleString()}`);
  console.log(`  ✗ Mismatches:    ${mismatches}`);
  console.log(`  ? Missing:       ${missing}`);
  console.log(`\nPerformance:`);
  console.log(`  Push Time:       ${pushTime}ms`);
  console.log(`  Total Time:      ${(totalTime / 1000).toFixed(2)}s`);
  console.log(`  Throughput:      ${Math.round((completed / totalTime) * 1000).toLocaleString()} jobs/sec`);

  await client.obliterate(QUEUE_NAME);
  await client.close();

  const success = verified === TOTAL_JOBS && mismatches === 0 && missing === 0;
  console.log(success ? '\n✅ CPU-BOUND TEST PASSED\n' : '\n❌ TEST FAILED\n');
  process.exit(success ? 0 : 1);
}

main().catch(console.error);

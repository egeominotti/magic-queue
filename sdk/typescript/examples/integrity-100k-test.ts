/**
 * REAL Integrity Test: 100,000 Jobs with Result Verification
 *
 * Verifies that EVERY job returns the correct result matching its input.
 * Uses job IDs for tracking (not indexes) to handle concurrent processing.
 *
 * Run: bun run examples/integrity-100k-test.ts
 */

import { FlashQ, Worker } from '../src';

const QUEUE_NAME = 'integrity-real-100k';
const TOTAL_JOBS = 100_000;
const BATCH_SIZE = 1000;
const WORKER_CONCURRENCY = 50;
const NUM_WORKERS = 4;

async function main() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   REAL INTEGRITY TEST: 100,000 JOBS WITH RESULT VERIFICATION   ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const client = new FlashQ({ timeout: 30000 });
  await client.connect();

  // Cleanup first
  console.log('Cleaning up queue...');
  await client.obliterate(QUEUE_NAME);

  // Track: jobId -> { input, expectedResult }
  const jobExpectations = new Map<number, { a: number; b: number; expected: number }>();
  // Track: jobId -> actualResult
  const jobResults = new Map<number, number>();

  let completed = 0;
  let failed = 0;
  let mismatches = 0;
  const startTime = Date.now();

  // Create workers that compute: result = a * b + a + b
  const workers: Worker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new Worker(
      QUEUE_NAME,
      async (job) => {
        const { a, b } = job.data as { a: number; b: number };
        // Compute deterministic result
        const result = a * b + a + b;
        return { jobId: job.id, result };
      },
      {
        host: 'localhost',
        port: 6789,
        concurrency: WORKER_CONCURRENCY,
        timeout: 30000,
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

  // Start all workers
  console.log(`Starting ${NUM_WORKERS} workers (${NUM_WORKERS * WORKER_CONCURRENCY} parallel)...\n`);
  await Promise.all(workers.map((w) => w.start()));

  // Push ALL jobs first, tracking expectations
  console.log(`Pushing ${TOTAL_JOBS.toLocaleString()} jobs...`);
  const pushStart = Date.now();

  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batchData: { a: number; b: number }[] = [];

    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      const idx = i + j;
      const a = idx * 3 + 7;
      const b = idx * 2 + 13;
      batchData.push({ a, b });
    }

    // Push batch and get job IDs
    const jobIds = await client.pushBatch(
      QUEUE_NAME,
      batchData.map((data) => ({ data }))
    );

    // Store expectations using actual job IDs
    for (let j = 0; j < jobIds.length; j++) {
      const jobId = jobIds[j];
      const { a, b } = batchData[j];
      const expected = a * b + a + b;
      jobExpectations.set(jobId, { a, b, expected });
    }

    if ((i + BATCH_SIZE) % 20000 === 0) {
      console.log(`  Pushed: ${Math.min(i + BATCH_SIZE, TOTAL_JOBS).toLocaleString()}/${TOTAL_JOBS.toLocaleString()}`);
    }
  }

  const pushTime = Date.now() - pushStart;
  console.log(`\nPush complete: ${pushTime}ms (${Math.round((TOTAL_JOBS / pushTime) * 1000).toLocaleString()} jobs/sec)`);
  console.log(`Tracked ${jobExpectations.size.toLocaleString()} job expectations\n`);

  // Wait for all jobs to complete
  console.log('Processing jobs...\n');
  let lastPrint = Date.now();

  while (completed + failed < TOTAL_JOBS) {
    await new Promise((r) => setTimeout(r, 100));

    const now = Date.now();
    if (now - lastPrint >= 1000) {
      const elapsed = ((now - startTime) / 1000).toFixed(1);
      const rate = Math.round(completed / ((now - startTime) / 1000));
      const pct = ((completed / TOTAL_JOBS) * 100).toFixed(1);

      console.log(
        `[${elapsed}s] Completed: ${completed.toLocaleString()} (${pct}%) | ` +
        `Rate: ${rate.toLocaleString()}/s | Failed: ${failed}`
      );
      lastPrint = now;
    }
  }

  const processTime = Date.now() - startTime;

  // Stop workers
  await Promise.all(workers.map((w) => w.stop()));

  // VERIFY ALL RESULTS
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('VERIFYING ALL RESULTS...');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  let verified = 0;
  let missing = 0;
  const mismatchDetails: string[] = [];

  for (const [jobId, expectation] of jobExpectations) {
    const actualResult = jobResults.get(jobId);

    if (actualResult === undefined) {
      missing++;
      if (missing <= 5) {
        console.error(`MISSING: Job ${jobId} has no result`);
      }
    } else if (actualResult !== expectation.expected) {
      mismatches++;
      if (mismatchDetails.length < 5) {
        mismatchDetails.push(
          `Job ${jobId}: expected ${expectation.expected} (${expectation.a}*${expectation.b}+${expectation.a}+${expectation.b}), got ${actualResult}`
        );
      }
    } else {
      verified++;
    }
  }

  if (mismatchDetails.length > 0) {
    console.log('First mismatches:');
    mismatchDetails.forEach((d) => console.error(`  ${d}`));
  }

  // Final Results
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║                       FINAL RESULTS                            ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  console.log(`Total Jobs:        ${TOTAL_JOBS.toLocaleString()}`);
  console.log(`Completed:         ${completed.toLocaleString()}`);
  console.log(`Failed:            ${failed.toLocaleString()}`);
  console.log(`\nVerification:`);
  console.log(`  ✓ Verified:      ${verified.toLocaleString()}`);
  console.log(`  ✗ Mismatches:    ${mismatches}`);
  console.log(`  ? Missing:       ${missing}`);
  console.log(`\nPerformance:`);
  console.log(`  Push Time:       ${pushTime}ms (${Math.round((TOTAL_JOBS / pushTime) * 1000).toLocaleString()} jobs/sec)`);
  console.log(`  Total Time:      ${processTime}ms (${(processTime / 1000).toFixed(2)}s)`);
  console.log(`  Throughput:      ${Math.round((completed / processTime) * 1000).toLocaleString()} jobs/sec`);

  // Cleanup
  await client.obliterate(QUEUE_NAME);
  await client.close();

  const success = completed === TOTAL_JOBS && failed === 0 && mismatches === 0 && missing === 0 && verified === TOTAL_JOBS;

  if (success) {
    console.log('\n✅ INTEGRITY TEST PASSED');
    console.log(`   All ${TOTAL_JOBS.toLocaleString()} jobs verified with correct results!\n`);
  } else {
    console.log('\n❌ INTEGRITY TEST FAILED\n');
    process.exit(1);
  }
}

main().catch(console.error);

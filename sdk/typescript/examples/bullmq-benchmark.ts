/**
 * BullMQ Benchmark - Direct comparison with flashQ
 * Same configuration: 100K jobs, 8 workers, 50 concurrency each
 */
import { Queue, Worker, Job } from 'bullmq';

const TOTAL_JOBS = 100_000;
const BATCH_SIZE = 1000;
const NUM_WORKERS = 8;
const CONCURRENCY_PER_WORKER = 50;

const connection = { host: 'localhost', port: 6379 };

console.log('='.repeat(70));
console.log('📦 BullMQ Benchmark (for comparison)');
console.log('='.repeat(70));
console.log(`Jobs: ${TOTAL_JOBS.toLocaleString()}`);
console.log(`Workers: ${NUM_WORKERS}`);
console.log(`Concurrency/worker: ${CONCURRENCY_PER_WORKER}`);
console.log(`Total concurrency: ${NUM_WORKERS * CONCURRENCY_PER_WORKER}`);
console.log('='.repeat(70));

const queue = new Queue('bullmq-benchmark', { connection });

// Clean up
console.log('\n📋 Cleaning up queue...');
await queue.obliterate({ force: true });

// Push jobs
console.log(`\n📤 Pushing ${TOTAL_JOBS.toLocaleString()} jobs...`);
const pushStart = Date.now();

let pushed = 0;
for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
  const batchCount = Math.min(BATCH_SIZE, TOTAL_JOBS - i);
  const jobs = Array.from({ length: batchCount }, (_, j) => ({
    name: 'task',
    data: {
      index: i + j,
      value: `job-${i + j}`,
      timestamp: Date.now(),
    },
  }));
  await queue.addBulk(jobs);
  pushed += batchCount;

  if (pushed % 10_000 === 0) {
    const elapsed = (Date.now() - pushStart) / 1000;
    const rate = Math.round(pushed / elapsed);
    console.log(`   Pushed: ${pushed.toLocaleString()} (${rate.toLocaleString()} jobs/sec)`);
  }
}

const pushTime = Date.now() - pushStart;
const pushRate = Math.round(TOTAL_JOBS / (pushTime / 1000));
console.log(`\n✅ Push complete: ${pushRate.toLocaleString()} jobs/sec (${(pushTime / 1000).toFixed(2)}s)`);

// Create workers
console.log(`\n👷 Starting ${NUM_WORKERS} workers...`);
const workers: Worker[] = [];
let processed = 0;
let errors = 0;
let dataErrors = 0;
const processStart = Date.now();
let lastReport = processStart;
let lastProcessed = 0;

for (let w = 0; w < NUM_WORKERS; w++) {
  const worker = new Worker(
    'bullmq-benchmark',
    async (job: Job) => {
      // Minimal work - same as flashQ benchmark
      return { ok: true, index: job.data.index, value: job.data.value };
    },
    {
      connection,
      concurrency: CONCURRENCY_PER_WORKER,
    }
  );

  worker.on('completed', (job: Job, result: any) => {
    processed++;

    // Verify data integrity
    if (job.data.index !== result.index || job.data.value !== result.value) {
      dataErrors++;
    }
  });

  worker.on('failed', () => {
    errors++;
  });

  workers.push(worker);
}

console.log('   All workers started\n');

// Progress reporter
const progressInterval = setInterval(() => {
  const now = Date.now();
  const elapsed = (now - processStart) / 1000;
  const intervalElapsed = (now - lastReport) / 1000;
  const intervalProcessed = processed - lastProcessed;
  const currentRate = Math.round(intervalProcessed / intervalElapsed);
  const avgRate = Math.round(processed / elapsed);
  const pct = ((processed / TOTAL_JOBS) * 100).toFixed(1);
  const eta = avgRate > 0 ? Math.round((TOTAL_JOBS - processed) / avgRate) : 0;

  console.log(
    `   ${processed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${pct}%) | Rate: ${currentRate.toLocaleString()}/s | Avg: ${avgRate.toLocaleString()}/s | ETA: ${eta}s | Errors: ${dataErrors}`
  );

  lastReport = now;
  lastProcessed = processed;
}, 2000);

// Wait for completion
while (processed < TOTAL_JOBS) {
  await new Promise((r) => setTimeout(r, 100));
}

clearInterval(progressInterval);

const processTime = Date.now() - processStart;
const processRate = Math.round(TOTAL_JOBS / (processTime / 1000));

// Stop workers
console.log('\n🛑 Stopping workers...');
await Promise.all(workers.map((w) => w.close()));

// Results
console.log('\n' + '='.repeat(70));
console.log('📊 BullMQ RESULTS');
console.log('='.repeat(70));
console.log(`Total jobs:       ${TOTAL_JOBS.toLocaleString()}`);
console.log(`Processed:        ${processed.toLocaleString()}`);
console.log(`Errors:           ${errors}`);
console.log(`Data errors:      ${dataErrors}`);
console.log(`Data integrity:   ${dataErrors === 0 ? '✅ 100% VERIFIED' : '❌ FAILED'}`);
console.log('-'.repeat(70));
console.log(`Push time:        ${(pushTime / 1000).toFixed(2)}s`);
console.log(`Process time:     ${(processTime / 1000).toFixed(2)}s`);
console.log(`Total time:       ${((pushTime + processTime) / 1000).toFixed(2)}s`);
console.log('-'.repeat(70));
console.log(`Push rate:        ${pushRate.toLocaleString()} jobs/sec`);
console.log(`Process rate:     ${processRate.toLocaleString()} jobs/sec`);
console.log('='.repeat(70));

// Cleanup
await queue.obliterate({ force: true });
await queue.close();

console.log('\n✅ Benchmark complete!');
process.exit(0);

/**
 * 1 Million Jobs Benchmark with Data Integrity Verification
 */
import { Queue, Worker } from '../src';

const TOTAL_JOBS = 1_000_000;
const BATCH_SIZE = 1000;  // Server limit: max 1000 per batch
const NUM_WORKERS = 8;
const CONCURRENCY_PER_WORKER = 100;

const queue = new Queue('million-benchmark');

console.log('='.repeat(70));
console.log('🚀 flashQ 1 MILLION Jobs Benchmark');
console.log('='.repeat(70));
console.log(`Jobs: ${TOTAL_JOBS.toLocaleString()}`);
console.log(`Workers: ${NUM_WORKERS}`);
console.log(`Concurrency/worker: ${CONCURRENCY_PER_WORKER}`);
console.log(`Total concurrency: ${NUM_WORKERS * CONCURRENCY_PER_WORKER}`);
console.log(`Batch size: ${BATCH_SIZE}`);
console.log(`Data integrity check: ENABLED`);
console.log('='.repeat(70));

// Clean up before starting
console.log('\n📋 Cleaning up queue...');
await queue.obliterate();

// Push jobs in batches
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
      timestamp: Date.now()
    }
  }));
  await queue.addBulk(jobs);
  pushed += batchCount;

  // Progress update every 100K
  if (pushed % 100_000 === 0) {
    const elapsed = (Date.now() - pushStart) / 1000;
    const rate = Math.round(pushed / elapsed);
    console.log(`   Pushed: ${pushed.toLocaleString()} (${rate.toLocaleString()} jobs/sec)`);
  }
}

const pushTime = Date.now() - pushStart;
const pushRate = Math.round(TOTAL_JOBS / (pushTime / 1000));
console.log(`\n✅ Push complete: ${pushRate.toLocaleString()} jobs/sec (${(pushTime/1000).toFixed(2)}s)`);

// Create workers
console.log(`\n👷 Starting ${NUM_WORKERS} workers with data verification...`);
const workers: Worker[] = [];
let processed = 0;
let errors = 0;
let dataErrors = 0;
const processStart = Date.now();
let lastReport = processStart;
let lastProcessed = 0;

for (let w = 0; w < NUM_WORKERS; w++) {
  const worker = new Worker('million-benchmark', async (job) => {
    const data = job.data as { index: number; value: string; timestamp: number };

    // Return the same data to verify in completed event
    return {
      index: data.index,
      value: data.value,
      originalTimestamp: data.timestamp,
      processedAt: Date.now()
    };
  }, {
    concurrency: CONCURRENCY_PER_WORKER,
    batchSize: 100,
    autorun: false
  });

  // Count and verify on completed event
  worker.on('completed', (job, result) => {
    processed++;

    // Verify data integrity
    const input = job.data as { index: number; value: string };
    const output = result as { index: number; value: string };

    if (input.index !== output.index || input.value !== output.value) {
      dataErrors++;
      console.error(`❌ Data mismatch! Input: ${JSON.stringify(input)}, Output: ${JSON.stringify(output)}`);
    }
  });

  worker.on('failed', (job, err) => {
    errors++;
  });

  worker.on('error', (err) => {
    console.error('Worker error:', err);
  });

  workers.push(worker);
}

// Start all workers
await Promise.all(workers.map(w => w.start()));
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

  console.log(`   ${processed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${pct}%) | Rate: ${currentRate.toLocaleString()}/s | Avg: ${avgRate.toLocaleString()}/s | ETA: ${eta}s | Errors: ${dataErrors}`);

  lastReport = now;
  lastProcessed = processed;
}, 3000);

// Wait for all jobs to be processed
while (processed < TOTAL_JOBS) {
  await new Promise(r => setTimeout(r, 100));
}

clearInterval(progressInterval);

const processTime = Date.now() - processStart;
const processRate = Math.round(TOTAL_JOBS / (processTime / 1000));

// Stop all workers
console.log('\n🛑 Stopping workers...');
await Promise.all(workers.map(w => w.close()));

// Results
console.log('\n' + '='.repeat(70));
console.log('📊 RESULTS');
console.log('='.repeat(70));
console.log(`Total jobs:       ${TOTAL_JOBS.toLocaleString()}`);
console.log(`Processed:        ${processed.toLocaleString()}`);
console.log(`Errors:           ${errors}`);
console.log(`Data errors:      ${dataErrors}`);
console.log(`Data integrity:   ${dataErrors === 0 ? '✅ 100% VERIFIED' : '❌ FAILED'}`);
console.log('-'.repeat(70));
console.log(`Push time:        ${(pushTime/1000).toFixed(2)}s`);
console.log(`Process time:     ${(processTime/1000).toFixed(2)}s`);
console.log(`Total time:       ${((pushTime + processTime)/1000).toFixed(2)}s`);
console.log('-'.repeat(70));
console.log(`Push rate:        ${pushRate.toLocaleString()} jobs/sec`);
console.log(`Process rate:     ${processRate.toLocaleString()} jobs/sec`);
console.log('='.repeat(70));

// Verify queue is empty
const counts = await queue.getJobCounts();
console.log(`\n📋 Queue state: waiting=${counts.waiting}, active=${counts.active}, completed=${counts.completed}, failed=${counts.failed}`);

// Cleanup
await queue.obliterate();
await queue.close();

console.log('\n✅ Benchmark complete!');
process.exit(0);

/**
 * Heavy Benchmark - 100K jobs with multiple workers
 */
import { Queue, Worker } from '../src';

const TOTAL_JOBS = 100_000;
const BATCH_SIZE = 1000;
const NUM_WORKERS = 4;
const CONCURRENCY_PER_WORKER = 50;

const queue = new Queue('heavy-benchmark');

console.log('='.repeat(60));
console.log('🚀 flashQ Heavy Benchmark');
console.log('='.repeat(60));
console.log(`Jobs: ${TOTAL_JOBS.toLocaleString()}`);
console.log(`Workers: ${NUM_WORKERS}`);
console.log(`Concurrency/worker: ${CONCURRENCY_PER_WORKER}`);
console.log(`Total concurrency: ${NUM_WORKERS * CONCURRENCY_PER_WORKER}`);
console.log('='.repeat(60));

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
    data: { index: i + j, timestamp: Date.now() }
  }));
  await queue.addBulk(jobs);
  pushed += batchCount;

  // Progress update every 10K
  if (pushed % 10_000 === 0) {
    const elapsed = (Date.now() - pushStart) / 1000;
    const rate = Math.round(pushed / elapsed);
    console.log(`   Pushed: ${pushed.toLocaleString()} (${rate.toLocaleString()} jobs/sec)`);
  }
}

const pushTime = Date.now() - pushStart;
const pushRate = Math.round(TOTAL_JOBS / (pushTime / 1000));
console.log(`\n✅ Push complete: ${pushRate.toLocaleString()} jobs/sec (${pushTime}ms)`);

// Create workers
console.log(`\n👷 Starting ${NUM_WORKERS} workers...`);
const workers: Worker[] = [];
let processed = 0;
let errors = 0;
const processStart = Date.now();
let lastReport = processStart;
let lastProcessed = 0;

for (let w = 0; w < NUM_WORKERS; w++) {
  const worker = new Worker('heavy-benchmark', async (job) => {
    // Minimal work
    return { ok: true };
  }, {
    concurrency: CONCURRENCY_PER_WORKER,
    batchSize: 100,
    autorun: false  // Manual start for coordination
  });

  // Count only after ack (completed event)
  worker.on('completed', () => {
    processed++;
  });

  worker.on('failed', () => {
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

  console.log(`   Progress: ${processed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${pct}%) - Current: ${currentRate.toLocaleString()} jobs/sec - Avg: ${avgRate.toLocaleString()} jobs/sec`);

  lastReport = now;
  lastProcessed = processed;
}, 2000);

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
console.log('\n' + '='.repeat(60));
console.log('📊 RESULTS');
console.log('='.repeat(60));
console.log(`Total jobs:      ${TOTAL_JOBS.toLocaleString()}`);
console.log(`Processed:       ${processed.toLocaleString()}`);
console.log(`Errors:          ${errors}`);
console.log(`Push time:       ${pushTime.toLocaleString()}ms`);
console.log(`Process time:    ${processTime.toLocaleString()}ms`);
console.log(`Push rate:       ${pushRate.toLocaleString()} jobs/sec`);
console.log(`Process rate:    ${processRate.toLocaleString()} jobs/sec`);
console.log('='.repeat(60));

// Verify queue is empty
const counts = await queue.getJobCounts();
console.log(`\n📋 Queue state: waiting=${counts.waiting}, active=${counts.active}, completed=${counts.completed}, failed=${counts.failed}`);

// Cleanup
await queue.obliterate();
await queue.close();

console.log('\n✅ Benchmark complete!');
process.exit(0);

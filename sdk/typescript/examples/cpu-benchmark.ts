/**
 * CPU-Bound Realistic Benchmark
 * Simulates real-world CPU-intensive jobs
 */
import { Queue, Worker } from '../src';
import { createHash } from 'crypto';

const TOTAL_JOBS = 100_000;
const BATCH_SIZE = 1000;
const NUM_WORKERS = 8;
const CONCURRENCY_PER_WORKER = 50;

// Simulate realistic CPU work
function cpuIntensiveWork(data: { payload: string; iterations: number }): {
  hash: string;
  computed: number;
  length: number;
} {
  // 1. JSON serialization/deserialization (common in APIs)
  const jsonStr = JSON.stringify(data);
  const parsed = JSON.parse(jsonStr);

  // 2. Multiple SHA256 hash rounds (crypto operations)
  let hash = parsed.payload;
  for (let i = 0; i < data.iterations; i++) {
    hash = createHash('sha256').update(hash).digest('hex');
  }

  // 3. Array manipulation (data processing)
  const numbers = Array.from({ length: 100 }, (_, i) => i * data.iterations);
  const sum = numbers.reduce((a, b) => a + b, 0);
  const sorted = [...numbers].sort((a, b) => b - a);
  const filtered = sorted.filter((n) => n % 2 === 0);

  // 4. String operations
  const upper = data.payload.toUpperCase();
  const reversed = upper.split('').reverse().join('');
  const repeated = reversed.repeat(3);

  return {
    hash: hash.substring(0, 16),
    computed: sum + filtered.length,
    length: repeated.length,
  };
}

const queue = new Queue('cpu-benchmark');

console.log('='.repeat(70));
console.log('🔥 flashQ CPU-Bound Realistic Benchmark');
console.log('='.repeat(70));
console.log(`Jobs: ${TOTAL_JOBS.toLocaleString()}`);
console.log(`Workers: ${NUM_WORKERS}`);
console.log(`Concurrency/worker: ${CONCURRENCY_PER_WORKER}`);
console.log(`Total concurrency: ${NUM_WORKERS * CONCURRENCY_PER_WORKER}`);
console.log('');
console.log('Each job performs:');
console.log('  - JSON serialize/deserialize');
console.log('  - 10x SHA256 hash rounds');
console.log('  - Array sort/filter/reduce');
console.log('  - String manipulation');
console.log('='.repeat(70));

// Clean up
console.log('\n📋 Cleaning up queue...');
await queue.obliterate();

// Push jobs
console.log(`\n📤 Pushing ${TOTAL_JOBS.toLocaleString()} jobs...`);
const pushStart = Date.now();

let pushed = 0;
for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
  const batchCount = Math.min(BATCH_SIZE, TOTAL_JOBS - i);
  const jobs = Array.from({ length: batchCount }, (_, j) => ({
    name: 'cpu-task',
    data: {
      index: i + j,
      payload: `job-payload-${i + j}-${Date.now()}`,
      iterations: 10, // 10 SHA256 rounds per job
    },
  }));
  await queue.addBulk(jobs);
  pushed += batchCount;

  if (pushed % 20_000 === 0) {
    const elapsed = (Date.now() - pushStart) / 1000;
    const rate = Math.round(pushed / elapsed);
    console.log(`   Pushed: ${pushed.toLocaleString()} (${rate.toLocaleString()} jobs/sec)`);
  }
}

const pushTime = Date.now() - pushStart;
const pushRate = Math.round(TOTAL_JOBS / (pushTime / 1000));
console.log(`\n✅ Push complete: ${pushRate.toLocaleString()} jobs/sec (${(pushTime / 1000).toFixed(2)}s)`);

// Workers
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
    'cpu-benchmark',
    async (job) => {
      const data = job.data as { index: number; payload: string; iterations: number };

      // Do real CPU work
      const result = cpuIntensiveWork(data);

      return {
        index: data.index,
        ...result,
      };
    },
    {
      concurrency: CONCURRENCY_PER_WORKER,
      batchSize: 50,
      autorun: false,
    }
  );

  worker.on('completed', (job, result) => {
    processed++;

    // Verify data integrity
    const input = job.data as { index: number };
    const output = result as { index: number };

    if (input.index !== output.index) {
      dataErrors++;
    }
  });

  worker.on('failed', () => {
    errors++;
  });

  workers.push(worker);
}

// Start all workers
await Promise.all(workers.map((w) => w.start()));
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
    `   ${processed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${pct}%) | Rate: ${currentRate.toLocaleString()}/s | Avg: ${avgRate.toLocaleString()}/s | ETA: ${eta}s`
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
console.log('📊 RESULTS');
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
await queue.obliterate();
await queue.close();

console.log('\n✅ Benchmark complete!');
process.exit(0);

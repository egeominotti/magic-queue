/**
 * Comparison Benchmark: Node.js-style vs Bun-native
 *
 * Compares:
 * - FlashQ + Worker (uses Node.js net module)
 * - BunFlashQ + BunWorker (uses Bun.connect() native API)
 *
 * Run: bun run examples/comparison-benchmark.ts
 */

import { FlashQ, Worker } from '../src';
import { BunFlashQ, BunWorker } from '../src';

const TOTAL_JOBS = 20_000;
const BATCH_SIZE = 500;
const NUM_WORKERS = 4;
const WORKER_CONCURRENCY = 50;

interface TestResult {
  name: string;
  pushTime: number;
  pushRate: number;
  processTime: number;
  processRate: number;
  totalTime: number;
  completed: number;
}

// ============== TEST 1: Node.js-style (FlashQ + Worker) ==============
async function testNodeStyle(): Promise<TestResult> {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   TEST 1: Node.js-style (FlashQ + Worker)                       ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const QUEUE = 'compare-node';
  const client = new FlashQ({ timeout: 60000 });
  await client.connect();
  await client.obliterate(QUEUE);

  let completed = 0;
  const startTime = Date.now();

  // Create workers
  const workers: Worker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new Worker(
      QUEUE,
      async (job) => {
        const { value } = job.data as { value: number };
        return { result: value * 2 };
      },
      { host: 'localhost', port: 6789, concurrency: WORKER_CONCURRENCY, timeout: 60000 }
    );
    worker.on('completed', () => completed++);
    workers.push(worker);
  }

  await Promise.all(workers.map((w) => w.start()));

  // Push jobs
  console.log('Pushing jobs...');
  const pushStart = Date.now();
  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batch = [];
    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      batch.push({ data: { value: i + j } });
    }
    await client.pushBatch(QUEUE, batch);
  }
  const pushTime = Date.now() - pushStart;
  const pushRate = Math.round((TOTAL_JOBS / pushTime) * 1000);
  console.log(`Push: ${pushTime}ms (${pushRate.toLocaleString()} jobs/sec)`);

  // Wait for completion
  console.log('Processing...');
  const processStart = Date.now();
  while (completed < TOTAL_JOBS) {
    await new Promise((r) => setTimeout(r, 50));
  }
  const processTime = Date.now() - processStart;
  const processRate = Math.round((completed / processTime) * 1000);

  await Promise.all(workers.map((w) => w.stop()));
  const totalTime = Date.now() - startTime;

  console.log(`Process: ${processTime}ms (${processRate.toLocaleString()} jobs/sec)`);
  console.log(`Total: ${totalTime}ms`);

  await client.obliterate(QUEUE);
  await client.close();

  return {
    name: 'Node.js-style',
    pushTime,
    pushRate,
    processTime,
    processRate,
    totalTime,
    completed,
  };
}

// ============== TEST 2: Bun-native (BunFlashQ + BunWorker) ==============
async function testBunNative(): Promise<TestResult> {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   TEST 2: Bun-native (BunFlashQ + BunWorker)                    ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const QUEUE = 'compare-bun';
  const client = new BunFlashQ({ timeout: 60000 });
  await client.connect();
  await client.obliterate(QUEUE);

  let completed = 0;
  const startTime = Date.now();

  // Create Bun workers
  const workers: BunWorker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new BunWorker(
      QUEUE,
      async (job) => {
        const { value } = job.data as { value: number };
        return { result: value * 2 };
      },
      { host: 'localhost', port: 6789, concurrency: WORKER_CONCURRENCY, timeout: 60000 }
    );
    worker.on('completed', () => completed++);
    workers.push(worker);
  }

  await Promise.all(workers.map((w) => w.start()));

  // Push jobs
  console.log('Pushing jobs...');
  const pushStart = Date.now();
  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batch = [];
    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      batch.push({ data: { value: i + j } });
    }
    await client.pushBatch(QUEUE, batch);
  }
  const pushTime = Date.now() - pushStart;
  const pushRate = Math.round((TOTAL_JOBS / pushTime) * 1000);
  console.log(`Push: ${pushTime}ms (${pushRate.toLocaleString()} jobs/sec)`);

  // Wait for completion
  console.log('Processing...');
  const processStart = Date.now();
  while (completed < TOTAL_JOBS) {
    await Bun.sleep(50);
  }
  const processTime = Date.now() - processStart;
  const processRate = Math.round((completed / processTime) * 1000);

  await Promise.all(workers.map((w) => w.stop()));
  const totalTime = Date.now() - startTime;

  console.log(`Process: ${processTime}ms (${processRate.toLocaleString()} jobs/sec)`);
  console.log(`Total: ${totalTime}ms`);

  await client.obliterate(QUEUE);
  await client.close();

  return {
    name: 'Bun-native',
    pushTime,
    pushRate,
    processTime,
    processRate,
    totalTime,
    completed,
  };
}

// ============== MAIN ==============
async function main() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║   COMPARISON: Node.js-style vs Bun-native                      ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log(`\nJobs: ${TOTAL_JOBS.toLocaleString()}`);
  console.log(`Workers: ${NUM_WORKERS} × ${WORKER_CONCURRENCY} = ${NUM_WORKERS * WORKER_CONCURRENCY} parallel\n`);

  const nodeResult = await testNodeStyle();
  const bunResult = await testBunNative();

  // Comparison
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║                    COMPARISON RESULTS                          ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  console.log('┌─────────────────┬─────────────────┬─────────────────┬──────────┐');
  console.log('│ Metric          │ Node.js-style   │ Bun-native      │ Speedup  │');
  console.log('├─────────────────┼─────────────────┼─────────────────┼──────────┤');

  const pushSpeedup = (bunResult.pushRate / nodeResult.pushRate).toFixed(2);
  console.log(
    `│ Push Rate       │ ${nodeResult.pushRate.toLocaleString().padStart(12)}/s │ ${bunResult.pushRate.toLocaleString().padStart(12)}/s │ ${pushSpeedup.padStart(6)}x │`
  );

  const processSpeedup = (bunResult.processRate / nodeResult.processRate).toFixed(2);
  console.log(
    `│ Process Rate    │ ${nodeResult.processRate.toLocaleString().padStart(12)}/s │ ${bunResult.processRate.toLocaleString().padStart(12)}/s │ ${processSpeedup.padStart(6)}x │`
  );

  const totalSpeedup = (nodeResult.totalTime / bunResult.totalTime).toFixed(2);
  console.log(
    `│ Total Time      │ ${(nodeResult.totalTime + 'ms').padStart(15)} │ ${(bunResult.totalTime + 'ms').padStart(15)} │ ${totalSpeedup.padStart(6)}x │`
  );

  console.log('└─────────────────┴─────────────────┴─────────────────┴──────────┘');

  console.log('\n✅ Comparison complete!\n');
}

main().catch(console.error);

/**
 * Comparison: Node.js-style vs Bun-native with REALISTIC workloads
 *
 * Tests SHA256 hashing (CPU-bound) to see real performance difference.
 *
 * Run: bun run examples/comparison-realistic.ts
 */

import { FlashQ, Worker } from '../src';
import { BunFlashQ, BunWorker } from '../src';

const TOTAL_JOBS = 10_000;
const BATCH_SIZE = 500;
const NUM_WORKERS = 4;
const WORKER_CONCURRENCY = 50;

interface TestResult {
  name: string;
  completed: number;
  time: number;
  rate: number;
}

// SHA256 hash processor
async function sha256Hash(input: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(input);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
}

// ============== TEST 1: Node.js-style ==============
async function testNodeStyle(): Promise<TestResult> {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   Node.js-style (FlashQ + Worker) - SHA256 CPU-bound           ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const QUEUE = 'compare-node-cpu';
  const client = new FlashQ({ timeout: 60000 });
  await client.connect();
  await client.obliterate(QUEUE);

  let completed = 0;
  const startTime = Date.now();

  const workers: Worker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new Worker(
      QUEUE,
      async (job) => {
        const { data } = job.data as { data: string };
        const hash = await sha256Hash(data);
        return { hash };
      },
      { host: 'localhost', port: 6789, concurrency: WORKER_CONCURRENCY, timeout: 60000 }
    );
    worker.on('completed', () => completed++);
    workers.push(worker);
  }

  await Promise.all(workers.map((w) => w.start()));

  // Push jobs
  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batch = [];
    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      batch.push({ data: { data: `job-data-${i + j}-${Date.now()}` } });
    }
    await client.pushBatch(QUEUE, batch);
  }
  console.log(`Pushed ${TOTAL_JOBS.toLocaleString()} jobs`);

  // Wait for completion
  while (completed < TOTAL_JOBS) {
    await new Promise((r) => setTimeout(r, 100));
    const rate = Math.round(completed / ((Date.now() - startTime) / 1000));
    process.stdout.write(`\r  Progress: ${completed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${rate}/s)`);
  }
  console.log();

  await Promise.all(workers.map((w) => w.stop()));
  const totalTime = Date.now() - startTime;

  await client.obliterate(QUEUE);
  await client.close();

  return {
    name: 'Node.js-style',
    completed,
    time: totalTime,
    rate: Math.round((completed / totalTime) * 1000),
  };
}

// ============== TEST 2: Bun-native ==============
async function testBunNative(): Promise<TestResult> {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   Bun-native (BunFlashQ + BunWorker) - SHA256 CPU-bound        ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const QUEUE = 'compare-bun-cpu';
  const client = new BunFlashQ({ timeout: 60000 });
  await client.connect();
  await client.obliterate(QUEUE);

  let completed = 0;
  const startTime = Date.now();

  const workers: BunWorker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new BunWorker(
      QUEUE,
      async (job) => {
        const { data } = job.data as { data: string };
        const hash = await sha256Hash(data);
        return { hash };
      },
      { host: 'localhost', port: 6789, concurrency: WORKER_CONCURRENCY, timeout: 60000 }
    );
    worker.on('completed', () => completed++);
    workers.push(worker);
  }

  await Promise.all(workers.map((w) => w.start()));

  // Push jobs
  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batch = [];
    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      batch.push({ data: { data: `job-data-${i + j}-${Date.now()}` } });
    }
    await client.pushBatch(QUEUE, batch);
  }
  console.log(`Pushed ${TOTAL_JOBS.toLocaleString()} jobs`);

  // Wait for completion
  while (completed < TOTAL_JOBS) {
    await Bun.sleep(100);
    const rate = Math.round(completed / ((Date.now() - startTime) / 1000));
    process.stdout.write(`\r  Progress: ${completed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${rate}/s)`);
  }
  console.log();

  await Promise.all(workers.map((w) => w.stop()));
  const totalTime = Date.now() - startTime;

  await client.obliterate(QUEUE);
  await client.close();

  return {
    name: 'Bun-native',
    completed,
    time: totalTime,
    rate: Math.round((completed / totalTime) * 1000),
  };
}

// ============== MAIN ==============
async function main() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║   COMPARISON: Node.js vs Bun with SHA256 CPU-bound workload    ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log(`\nJobs: ${TOTAL_JOBS.toLocaleString()} (each computes SHA256 hash)`);
  console.log(`Workers: ${NUM_WORKERS} × ${WORKER_CONCURRENCY} = ${NUM_WORKERS * WORKER_CONCURRENCY} parallel\n`);

  const nodeResult = await testNodeStyle();
  const bunResult = await testBunNative();

  // Comparison
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║                REALISTIC COMPARISON RESULTS                    ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  console.log('┌─────────────────┬─────────────────┬─────────────────┬──────────┐');
  console.log('│ Implementation  │ Time            │ Throughput      │ Speedup  │');
  console.log('├─────────────────┼─────────────────┼─────────────────┼──────────┤');

  const speedup = (nodeResult.time / bunResult.time).toFixed(2);
  console.log(
    `│ Node.js-style   │ ${(nodeResult.time / 1000).toFixed(2).padStart(13)}s │ ${nodeResult.rate.toLocaleString().padStart(12)}/s │   1.00x │`
  );
  console.log(
    `│ Bun-native      │ ${(bunResult.time / 1000).toFixed(2).padStart(13)}s │ ${bunResult.rate.toLocaleString().padStart(12)}/s │ ${speedup.padStart(6)}x │`
  );
  console.log('└─────────────────┴─────────────────┴─────────────────┴──────────┘');

  const winner = bunResult.time < nodeResult.time ? 'Bun-native' : 'Node.js-style';
  const improvement = Math.abs(((bunResult.time - nodeResult.time) / nodeResult.time) * 100).toFixed(1);
  console.log(`\n🏆 Winner: ${winner} (${improvement}% ${bunResult.time < nodeResult.time ? 'faster' : 'slower'})\n`);
}

main().catch(console.error);

/**
 * Bun-Optimized Realistic Workload Test
 *
 * Uses Bun's native TCP API for 30% faster performance.
 * Tests CPU-bound, I/O-bound, and Memory-bound workloads.
 *
 * Run: bun run examples/bun-realistic-test.ts
 */

import { BunFlashQ, BunWorker } from '../src';

const TOTAL_JOBS = 10_000;
const BATCH_SIZE = 500;
const WORKER_CONCURRENCY = 50;
const NUM_WORKERS = 4;

// ============== TEST 1: CPU-BOUND (Hash Computation) ==============
async function testCpuBound() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   TEST 1: CPU-BOUND (SHA256 Hash Computation) - BUN NATIVE     ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const QUEUE = 'bun-test-cpu';
  const client = new BunFlashQ({ timeout: 60000 });
  await client.connect();
  await client.obliterate(QUEUE);

  const results = new Map<number, string>();
  let completed = 0;
  let failed = 0;
  const startTime = Date.now();

  const workers: BunWorker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new BunWorker(
      QUEUE,
      async (job) => {
        const { data: inputData } = job.data as { data: string };
        // Real CPU work: compute SHA256 hash
        const encoder = new TextEncoder();
        const dataBuffer = encoder.encode(inputData);
        const hashBuffer = await crypto.subtle.digest('SHA-256', dataBuffer);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        const hash = hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
        return { jobId: job.id, hash };
      },
      { host: 'localhost', port: 6789, concurrency: WORKER_CONCURRENCY, timeout: 60000 }
    );

    worker.on('completed', (job, ret) => {
      completed++;
      if (ret) results.set((ret as { jobId: number; hash: string }).jobId, (ret as { jobId: number; hash: string }).hash);
    });
    worker.on('failed', () => failed++);
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

  console.log(`Pushed ${TOTAL_JOBS.toLocaleString()} jobs, waiting for completion...`);

  while (completed + failed < TOTAL_JOBS) {
    await Bun.sleep(500);
    const rate = Math.round(completed / ((Date.now() - startTime) / 1000));
    console.log(`  Progress: ${completed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${rate}/s)`);
  }

  await Promise.all(workers.map((w) => w.stop()));
  const totalTime = Date.now() - startTime;

  // Verify all hashes are valid
  let validHashes = 0;
  for (const [, hash] of results) {
    if (hash && hash.length === 64 && /^[a-f0-9]+$/.test(hash)) validHashes++;
  }

  console.log(`\n✓ Completed: ${completed.toLocaleString()}`);
  console.log(`✓ Valid Hashes: ${validHashes.toLocaleString()}`);
  console.log(`⏱ Time: ${(totalTime / 1000).toFixed(2)}s`);
  console.log(`📊 Throughput: ${Math.round((completed / totalTime) * 1000).toLocaleString()} jobs/sec`);

  await client.obliterate(QUEUE);
  await client.close();

  return { name: 'CPU-bound', completed, time: totalTime, rate: Math.round((completed / totalTime) * 1000) };
}

// ============== TEST 2: I/O-BOUND (Async Delay) ==============
async function testIoBound() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   TEST 2: I/O-BOUND (Simulated 10-50ms Async Delay) - BUN      ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const QUEUE = 'bun-test-io';
  const client = new BunFlashQ({ timeout: 60000 });
  await client.connect();
  await client.obliterate(QUEUE);

  const results = new Map<number, number>();
  let completed = 0;
  let failed = 0;
  const startTime = Date.now();

  const workers: BunWorker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new BunWorker(
      QUEUE,
      async (job) => {
        const { delay, value } = job.data as { delay: number; value: number };
        // Simulate I/O with Bun.sleep (more efficient than setTimeout)
        await Bun.sleep(delay);
        return { jobId: job.id, result: value * 2 };
      },
      { host: 'localhost', port: 6789, concurrency: WORKER_CONCURRENCY, timeout: 60000 }
    );

    worker.on('completed', (job, ret) => {
      completed++;
      if (ret) results.set((ret as { jobId: number; result: number }).jobId, (ret as { jobId: number; result: number }).result);
    });
    worker.on('failed', () => failed++);
    workers.push(worker);
  }

  await Promise.all(workers.map((w) => w.start()));

  // Push jobs with random delays 10-50ms
  const expectations = new Map<number, number>();
  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batch = [];
    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      const idx = i + j;
      const delay = 10 + Math.floor(Math.random() * 40);
      batch.push({ data: { delay, value: idx } });
    }
    const ids = await client.pushBatch(QUEUE, batch);
    for (let j = 0; j < ids.length; j++) {
      expectations.set(ids[j], (i + j) * 2);
    }
  }

  console.log(`Pushed ${TOTAL_JOBS.toLocaleString()} jobs (each waits 10-50ms), waiting...`);

  while (completed + failed < TOTAL_JOBS) {
    await Bun.sleep(1000);
    const rate = Math.round(completed / ((Date.now() - startTime) / 1000));
    console.log(`  Progress: ${completed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${rate}/s)`);
  }

  await Promise.all(workers.map((w) => w.stop()));
  const totalTime = Date.now() - startTime;

  // Verify results
  let verified = 0;
  for (const [id, expected] of expectations) {
    if (results.get(id) === expected) verified++;
  }

  console.log(`\n✓ Completed: ${completed.toLocaleString()}`);
  console.log(`✓ Verified: ${verified.toLocaleString()}`);
  console.log(`⏱ Time: ${(totalTime / 1000).toFixed(2)}s`);
  console.log(`📊 Throughput: ${Math.round((completed / totalTime) * 1000).toLocaleString()} jobs/sec`);

  await client.obliterate(QUEUE);
  await client.close();

  return { name: 'I/O-bound', completed, time: totalTime, rate: Math.round((completed / totalTime) * 1000) };
}

// ============== TEST 3: MEMORY-BOUND (Array Manipulation) ==============
async function testMemoryBound() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║   TEST 3: MEMORY-BOUND (Array Sort & Reduce) - BUN NATIVE      ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const QUEUE = 'bun-test-mem';
  const client = new BunFlashQ({ timeout: 60000 });
  await client.connect();
  await client.obliterate(QUEUE);

  const results = new Map<number, number>();
  let completed = 0;
  let failed = 0;
  const startTime = Date.now();

  const workers: BunWorker[] = [];
  for (let w = 0; w < NUM_WORKERS; w++) {
    const worker = new BunWorker(
      QUEUE,
      async (job) => {
        const { size, seed } = job.data as { size: number; seed: number };
        // Memory work: create array, sort, reduce
        const arr = Array.from({ length: size }, (_, i) => (seed + i) % 1000);
        arr.sort((a, b) => a - b);
        const sum = arr.reduce((acc, val) => acc + val, 0);
        return { jobId: job.id, sum };
      },
      { host: 'localhost', port: 6789, concurrency: WORKER_CONCURRENCY, timeout: 60000 }
    );

    worker.on('completed', (job, ret) => {
      completed++;
      if (ret) results.set((ret as { jobId: number; sum: number }).jobId, (ret as { jobId: number; sum: number }).sum);
    });
    worker.on('failed', () => failed++);
    workers.push(worker);
  }

  await Promise.all(workers.map((w) => w.start()));

  // Push jobs with 1000-element arrays
  const expectations = new Map<number, number>();
  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batch = [];
    for (let j = 0; j < Math.min(BATCH_SIZE, TOTAL_JOBS - i); j++) {
      batch.push({ data: { size: 1000, seed: i + j } });
    }
    const ids = await client.pushBatch(QUEUE, batch);
    for (let j = 0; j < ids.length; j++) {
      const idx = i + j;
      const arr = Array.from({ length: 1000 }, (_, k) => (idx + k) % 1000);
      arr.sort((a, b) => a - b);
      expectations.set(ids[j], arr.reduce((acc, val) => acc + val, 0));
    }
  }

  console.log(`Pushed ${TOTAL_JOBS.toLocaleString()} jobs (each processes 1000-element array), waiting...`);

  while (completed + failed < TOTAL_JOBS) {
    await Bun.sleep(500);
    const rate = Math.round(completed / ((Date.now() - startTime) / 1000));
    console.log(`  Progress: ${completed.toLocaleString()}/${TOTAL_JOBS.toLocaleString()} (${rate}/s)`);
  }

  await Promise.all(workers.map((w) => w.stop()));
  const totalTime = Date.now() - startTime;

  // Verify results
  let verified = 0;
  for (const [id, expected] of expectations) {
    if (results.get(id) === expected) verified++;
  }

  console.log(`\n✓ Completed: ${completed.toLocaleString()}`);
  console.log(`✓ Verified: ${verified.toLocaleString()}`);
  console.log(`⏱ Time: ${(totalTime / 1000).toFixed(2)}s`);
  console.log(`📊 Throughput: ${Math.round((completed / totalTime) * 1000).toLocaleString()} jobs/sec`);

  await client.obliterate(QUEUE);
  await client.close();

  return { name: 'Memory-bound', completed, time: totalTime, rate: Math.round((completed / totalTime) * 1000) };
}

// ============== MAIN ==============
async function main() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║   BUN-NATIVE REALISTIC WORKLOAD TESTS - 10,000 JOBS EACH       ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log(`\nConfiguration: ${NUM_WORKERS} workers × ${WORKER_CONCURRENCY} concurrency = ${NUM_WORKERS * WORKER_CONCURRENCY} parallel\n`);

  const results = [];

  results.push(await testCpuBound());
  results.push(await testIoBound());
  results.push(await testMemoryBound());

  // Summary
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║                      SUMMARY                                   ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  console.log('┌─────────────────┬────────────┬────────────┬────────────────┐');
  console.log('│ Workload Type   │ Completed  │ Time       │ Throughput     │');
  console.log('├─────────────────┼────────────┼────────────┼────────────────┤');
  for (const r of results) {
    console.log(
      `│ ${r.name.padEnd(15)} │ ${r.completed.toLocaleString().padStart(10)} │ ${(r.time / 1000).toFixed(2).padStart(8)}s │ ${r.rate.toLocaleString().padStart(12)}/s │`
    );
  }
  console.log('└─────────────────┴────────────┴────────────┴────────────────┘');

  console.log('\n✅ All tests completed!\n');
}

main().catch(console.error);

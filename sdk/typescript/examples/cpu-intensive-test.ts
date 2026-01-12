/**
 * FlashQ CPU Intensive Test
 *
 * Tests system performance with CPU-heavy workloads
 */

import { FlashQ } from '../src/index';
import * as crypto from 'crypto';

const HOST = process.env.MQ_HOST || 'localhost';
const PORT = parseInt(process.env.MQ_PORT || '6789');

const CONFIG = {
  TOTAL_JOBS: 100,
  PRIME_LIMIT: 50000,
  FIBONACCI_N: 38,
  HASH_ITERATIONS: 5000,
  MATRIX_SIZE: 150,
};

// CPU Intensive Functions
function isPrime(n: number): boolean {
  if (n <= 1) return false;
  for (let i = 2; i * i <= n; i++) {
    if (n % i === 0) return false;
  }
  return true;
}

function countPrimes(limit: number): number {
  let count = 0;
  for (let i = 2; i <= limit; i++) {
    if (isPrime(i)) count++;
  }
  return count;
}

function fibonacci(n: number): number {
  if (n <= 1) return n;
  return fibonacci(n - 1) + fibonacci(n - 2);
}

function hashIterations(data: string, iterations: number): string {
  let hash = data;
  for (let i = 0; i < iterations; i++) {
    hash = crypto.createHash('sha256').update(hash).digest('hex');
  }
  return hash.slice(0, 16);
}

function matrixMultiply(size: number): number {
  const a: number[][] = Array(size).fill(0).map(() => Array(size).fill(0).map(() => Math.random()));
  const b: number[][] = Array(size).fill(0).map(() => Array(size).fill(0).map(() => Math.random()));
  let sum = 0;
  for (let i = 0; i < size; i++) {
    for (let j = 0; j < size; j++) {
      let val = 0;
      for (let k = 0; k < size; k++) {
        val += a[i][k] * b[k][j];
      }
      sum += val;
    }
  }
  return sum;
}

function processTask(taskType: string, params: any): { result: any; timeMs: number } {
  const start = Date.now();
  let result: any;

  switch (taskType) {
    case 'prime':
      result = countPrimes(params.limit || CONFIG.PRIME_LIMIT);
      break;
    case 'fibonacci':
      result = fibonacci(params.n || CONFIG.FIBONACCI_N);
      break;
    case 'hash':
      result = hashIterations(params.data || 'test', params.iterations || CONFIG.HASH_ITERATIONS);
      break;
    case 'matrix':
      result = matrixMultiply(params.size || CONFIG.MATRIX_SIZE);
      break;
    default:
      result = 0;
  }

  return { result, timeMs: Date.now() - start };
}

async function main() {
  console.log('═'.repeat(60));
  console.log('  FlashQ CPU Intensive Test');
  console.log('═'.repeat(60));
  console.log(`\nConfig: ${CONFIG.TOTAL_JOBS} jobs\n`);

  const client = new FlashQ({ host: HOST, port: PORT });
  await client.connect();
  console.log('✓ Connected\n');

  const queue = `cpu-test-${Date.now()}`;
  const taskTypes = ['prime', 'fibonacci', 'hash', 'matrix'];

  // Phase 1: Push jobs
  console.log('Phase 1: Pushing jobs...');
  const pushStart = Date.now();

  for (let i = 0; i < CONFIG.TOTAL_JOBS; i++) {
    await client.push(queue, {
      taskType: taskTypes[i % taskTypes.length],
      index: i,
      params: {
        limit: CONFIG.PRIME_LIMIT,
        n: CONFIG.FIBONACCI_N,
        data: `data-${i}`,
        iterations: CONFIG.HASH_ITERATIONS,
        size: CONFIG.MATRIX_SIZE,
      }
    });
  }

  console.log(`  ✓ Pushed ${CONFIG.TOTAL_JOBS} jobs in ${Date.now() - pushStart}ms\n`);

  // Phase 2: Process jobs sequentially
  console.log('Phase 2: Processing jobs...');
  const processStart = Date.now();

  const stats: Record<string, { count: number; totalTime: number }> = {};
  for (const t of taskTypes) stats[t] = { count: 0, totalTime: 0 };

  let processed = 0;

  while (processed < CONFIG.TOTAL_JOBS) {
    const job = await client.pull(queue);
    if (!job?.id) {
      await new Promise(r => setTimeout(r, 10));
      continue;
    }

    const { taskType, params } = job.data as any;
    const { result, timeMs } = processTask(taskType, params);

    await client.ack(job.id, { result, timeMs });

    stats[taskType].count++;
    stats[taskType].totalTime += timeMs;
    processed++;

    if (processed % 10 === 0) {
      process.stdout.write(`\r  Progress: ${processed}/${CONFIG.TOTAL_JOBS}`);
    }
  }

  const totalTime = Date.now() - processStart;
  console.log(`\r  ✓ Processed ${processed} jobs in ${totalTime}ms`);
  console.log(`  ✓ Throughput: ${(processed / (totalTime / 1000)).toFixed(1)} jobs/sec\n`);

  // Phase 3: Stats
  console.log('Task Breakdown:');
  console.log('─'.repeat(50));
  let totalCpuTime = 0;

  for (const [type, s] of Object.entries(stats)) {
    const avg = s.count > 0 ? Math.round(s.totalTime / s.count) : 0;
    console.log(`  ${type.padEnd(12)} ${s.count.toString().padStart(4)} jobs  ${s.totalTime.toString().padStart(6)}ms total  ${avg.toString().padStart(4)}ms avg`);
    totalCpuTime += s.totalTime;
  }

  console.log('─'.repeat(50));
  console.log(`  Total CPU time: ${totalCpuTime}ms`);
  console.log(`  Avg per job: ${Math.round(totalCpuTime / processed)}ms\n`);

  // Final stats
  const serverStats = await client.stats();
  console.log(`Server: queued=${serverStats.queued} processing=${serverStats.processing}`);

  await client.close();
  console.log('\n✓ Test complete!');
}

main().catch(console.error);

/**
 * Throughput Benchmark
 */
import { FlashQ } from '../src';

const QUEUE = 'bench';
const JOBS = 50_000;
const BATCH = 1000;

async function main() {
  const client = new FlashQ();
  await client.connect();

  // Cleanup first
  await client.drain(QUEUE);

  console.log(`\n=== Throughput Test: ${JOBS.toLocaleString()} jobs ===\n`);

  // 1. PUSH throughput
  console.log('1. PUSH benchmark...');
  const pushStart = Date.now();

  for (let i = 0; i < JOBS; i += BATCH) {
    const jobs = Array.from({ length: BATCH }, (_, j) => ({ data: { n: i + j } }));
    await client.pushBatch(QUEUE, jobs);
  }

  const pushMs = Date.now() - pushStart;
  const pushRate = Math.round(JOBS / pushMs * 1000);
  console.log(`   ${JOBS.toLocaleString()} jobs in ${pushMs}ms`);
  console.log(`   ⚡ ${pushRate.toLocaleString()} jobs/sec\n`);

  // 2. PULL + ACK throughput
  console.log('2. PULL + ACK benchmark...');
  const processStart = Date.now();
  let processed = 0;

  while (processed < JOBS) {
    const batch = await client.pullBatch<{ n: number }>(QUEUE, BATCH);
    // Parallel acks
    await Promise.all(batch.map(j => client.ack(j.id)));
    processed += batch.length;
  }

  const processMs = Date.now() - processStart;
  const processRate = Math.round(JOBS / processMs * 1000);
  console.log(`   ${JOBS.toLocaleString()} jobs in ${processMs}ms`);
  console.log(`   ⚡ ${processRate.toLocaleString()} jobs/sec\n`);

  // 3. End-to-end (push + process)
  const totalMs = pushMs + processMs;
  const e2eRate = Math.round(JOBS / totalMs * 1000);

  console.log('=== Results ===');
  console.log(`Push:        ${pushRate.toLocaleString()} jobs/sec`);
  console.log(`Process:     ${processRate.toLocaleString()} jobs/sec`);
  console.log(`End-to-end:  ${e2eRate.toLocaleString()} jobs/sec`);
  console.log(`Total time:  ${totalMs}ms`);

  await client.close();
}

main().catch(console.error);

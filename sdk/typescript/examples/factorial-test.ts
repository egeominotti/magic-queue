/**
 * Real CPU Work Test: Factorial Calculations
 * 100,000 jobs computing factorials
 */
import { FlashQ } from '../src';

const QUEUE = 'factorial';
const JOBS = 100_000;
const MAX_NUMBER = 1000; // Calculate factorial for numbers 1-1000
const BATCH_SIZE = 1000;

// Factorial calculation (real CPU work)
function factorial(n: number): bigint {
  let result = 1n;
  for (let i = 2n; i <= BigInt(n); i++) {
    result *= i;
  }
  return result;
}

async function main() {
  console.log(`\n=== Factorial Calculation Test ===`);
  console.log(`Jobs: ${JOBS.toLocaleString()}`);
  console.log(`Numbers: 1 to ${MAX_NUMBER}`);
  console.log(`Batch size: ${BATCH_SIZE}\n`);

  const client = new FlashQ();
  await client.connect();

  // Cleanup
  await client.drain(QUEUE);

  // 1. Push 100k jobs
  console.log('1. Pushing jobs...');
  const pushStart = Date.now();

  for (let i = 0; i < JOBS; i += BATCH_SIZE) {
    const jobs = Array.from({ length: BATCH_SIZE }, (_, j) => ({
      data: { n: ((i + j) % MAX_NUMBER) + 1 }, // Numbers 1-1000 cycling
    }));
    await client.pushBatch(QUEUE, jobs);
  }

  const pushTime = Date.now() - pushStart;
  console.log(`   ✓ Pushed ${JOBS.toLocaleString()} jobs in ${pushTime}ms`);
  console.log(`   ✓ Rate: ${Math.round(JOBS / pushTime * 1000).toLocaleString()} jobs/sec\n`);

  // 2. Process with factorial calculation
  console.log('2. Processing (calculating factorials)...');
  const processStart = Date.now();
  let processed = 0;
  let totalDigits = 0n;

  while (processed < JOBS) {
    const batch = await client.pullBatch<{ n: number }>(QUEUE, BATCH_SIZE);
    if (batch.length === 0) break;

    // Process batch - calculate factorials
    for (const job of batch) {
      const result = factorial(job.data.n);
      totalDigits += BigInt(result.toString().length);
      await client.ack(job.id, { digits: result.toString().length });
    }

    processed += batch.length;

    // Progress
    if (processed % 10000 === 0) {
      const elapsed = Date.now() - processStart;
      const rate = Math.round(processed / elapsed * 1000);
      console.log(`   ... ${processed.toLocaleString()}/${JOBS.toLocaleString()} (${rate.toLocaleString()} jobs/sec)`);
    }
  }

  const processTime = Date.now() - processStart;
  const totalTime = pushTime + processTime;

  console.log(`   ✓ Processed ${processed.toLocaleString()} jobs in ${processTime}ms`);
  console.log(`   ✓ Rate: ${Math.round(JOBS / processTime * 1000).toLocaleString()} jobs/sec\n`);

  // 3. Summary
  console.log('=== Summary ===');
  console.log(`Push:          ${Math.round(JOBS / pushTime * 1000).toLocaleString()} jobs/sec`);
  console.log(`Process:       ${Math.round(JOBS / processTime * 1000).toLocaleString()} jobs/sec`);
  console.log(`Total time:    ${totalTime}ms (${(totalTime / 1000).toFixed(2)}s)`);
  console.log(`Total digits:  ${totalDigits.toLocaleString()} computed`);

  const stats = await client.stats();
  console.log('\nFinal stats:', stats);

  await client.close();
  console.log('\n=== Done ===');
}

main().catch(console.error);

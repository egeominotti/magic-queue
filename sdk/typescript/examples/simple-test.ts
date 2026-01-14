/**
 * Simple Push + Worker Test
 * Demonstrates basic queue workflow with minimal code
 */
import { FlashQ } from '../src';

const QUEUE = 'tasks';
const NUM_JOBS = 10_000;
const BATCH_SIZE = 500;

async function main() {
  console.log('=== Simple Push + Worker Test ===\n');
  console.log(`Jobs: ${NUM_JOBS}, Batch size: ${BATCH_SIZE}\n`);

  // Producer client
  const producer = new FlashQ();
  await producer.connect();

  // Worker client (separate connection)
  const worker = new FlashQ();
  await worker.connect();

  // 1. Push all jobs in batches
  console.log(`1. Pushing ${NUM_JOBS} jobs...`);
  const pushStart = Date.now();

  const allIds: number[] = [];
  for (let i = 0; i < NUM_JOBS; i += BATCH_SIZE) {
    const batchJobs = Array.from({ length: Math.min(BATCH_SIZE, NUM_JOBS - i) }, (_, j) => ({
      data: { id: i + j, value: (i + j) * 2 },
    }));
    const ids = await producer.pushBatch(QUEUE, batchJobs);
    allIds.push(...ids);
  }

  const pushTime = Date.now() - pushStart;
  console.log(`   ✓ Pushed ${allIds.length} jobs in ${pushTime}ms`);
  console.log(`   ✓ Rate: ${Math.round(NUM_JOBS / pushTime * 1000).toLocaleString()} jobs/sec\n`);

  // 2. Process all jobs
  console.log(`2. Processing ${NUM_JOBS} jobs...`);
  const processStart = Date.now();
  let processed = 0;
  let totalValue = 0;

  while (processed < NUM_JOBS) {
    // Pull batch
    const batch = await worker.pullBatch<{ id: number; value: number }>(QUEUE, BATCH_SIZE);

    // Process and ack in parallel (multiplexing!)
    const ackPromises = batch.map(async (job) => {
      const result = job.data.value * 2;
      totalValue += result;
      await worker.ack(job.id, { result });
    });

    await Promise.all(ackPromises);
    processed += batch.length;

    // Progress every 2000
    if (processed % 2000 === 0) {
      console.log(`   ... ${processed}/${NUM_JOBS} processed`);
    }
  }

  const processTime = Date.now() - processStart;
  const totalTime = Date.now() - pushStart;

  console.log(`   ✓ Processed ${processed} jobs in ${processTime}ms`);
  console.log(`   ✓ Rate: ${Math.round(NUM_JOBS / processTime * 1000).toLocaleString()} jobs/sec\n`);

  // 3. Summary
  console.log('=== Summary ===');
  console.log(`Total time: ${totalTime}ms`);
  console.log(`Push rate:  ${Math.round(NUM_JOBS / pushTime * 1000).toLocaleString()} jobs/sec`);
  console.log(`Process rate: ${Math.round(NUM_JOBS / processTime * 1000).toLocaleString()} jobs/sec`);
  console.log(`Computed sum: ${totalValue.toLocaleString()}`);

  // 4. Final stats
  const stats = await producer.stats();
  console.log('\nFinal stats:', stats);

  // Cleanup
  await producer.close();
  await worker.close();

  console.log('\n=== Test Complete ===');
}

main().catch(console.error);

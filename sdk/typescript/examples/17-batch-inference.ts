/**
 * Batch Inference Example
 *
 * Demonstrates high-throughput AI inference:
 * - Bulk job submission for batch processing
 * - Concurrency control for GPU utilization
 * - Worker event-based result collection
 *
 * Run: bun run examples/17-batch-inference.ts
 */

import { Worker, FlashQ } from '../src';

const QUEUE_NAME = 'batch-inference';
const BATCH_SIZE = 20;
const CONCURRENCY = 3;

// Simulated embedding function
async function generateEmbedding(_text: string): Promise<number[]> {
  await new Promise(r => setTimeout(r, 20)); // 20ms inference
  return Array(1536).fill(0).map(() => Math.random() * 2 - 1);
}

async function main() {
  const client = new FlashQ();
  await client.connect();
  await client.obliterate(QUEUE_NAME);

  console.log('=== Batch Inference Example ===\n');
  console.log(`Batch size: ${BATCH_SIZE} documents`);
  console.log(`Concurrency: ${CONCURRENCY} parallel jobs\n`);

  // Track results
  const results: Map<number, number[]> = new Map();
  let completed = 0;

  // Create worker
  const worker = new Worker(QUEUE_NAME, async (job) => {
    const { text, index } = job.data as { text: string; index: number };
    const embedding = await generateEmbedding(text);
    return { index, embedding };
  }, { concurrency: CONCURRENCY });

  worker.on('completed', (_job, result) => {
    const { index, embedding } = result as { index: number; embedding: number[] };
    results.set(index, embedding);
    completed++;
    if (completed % 10 === 0) {
      console.log(`Progress: ${completed}/${BATCH_SIZE}`);
    }
  });

  worker.on('failed', (_job, error) => {
    console.error(`Job failed: ${error.message}`);
  });

  // Wait for worker to start
  await new Promise(r => setTimeout(r, 300));

  // Generate documents
  const documents = Array(BATCH_SIZE).fill(null).map((_, i) => ({
    text: `Document ${i}: Sample text for embedding.`,
    index: i
  }));

  console.log('Submitting batch...');
  const startTime = Date.now();

  // Submit all jobs
  const jobIds = await client.pushBatch(QUEUE_NAME, documents.map(doc => ({
    data: doc
  })));

  console.log(`Submitted ${jobIds.length} jobs\n`);
  console.log('Processing...');

  // Wait for all jobs to complete
  while (completed < BATCH_SIZE) {
    await new Promise(r => setTimeout(r, 100));
  }

  const totalTime = Date.now() - startTime;
  const throughput = (BATCH_SIZE / (totalTime / 1000)).toFixed(1);

  console.log(`\n=== Results ===`);
  console.log(`Total time: ${totalTime}ms`);
  console.log(`Throughput: ${throughput} embeddings/sec`);
  console.log(`Results collected: ${results.size}`);
  console.log(`Sample embedding dims: ${results.get(0)?.length}`);
  console.log(`All results valid: ${results.size === BATCH_SIZE ? 'YES' : 'NO'}`);

  await worker.close();
  await client.obliterate(QUEUE_NAME);
  await client.close();

  console.log('\n=== Batch Complete ===');
}

main().catch(console.error);

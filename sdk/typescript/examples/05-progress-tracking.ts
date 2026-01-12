/**
 * Progress Tracking Example
 *
 * Demonstrates real-time progress updates for long-running jobs.
 *
 * Run: npx ts-node examples/05-progress-tracking.ts
 */

import { FlashQ, Worker } from '../src';

interface ProcessingJob {
  items: string[];
  totalItems: number;
}

async function main() {
  const client = new FlashQ({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected\n');

  // Push a job with many items to process
  const items = Array.from({ length: 10 }, (_, i) => `Item ${i + 1}`);
  const job = await client.push<ProcessingJob>('progress-queue', {
    items,
    totalItems: items.length,
  });
  console.log(`📤 Pushed job ${job.id} with ${items.length} items to process\n`);

  // Start progress monitor in background
  const monitorInterval = setInterval(async () => {
    try {
      const { progress, message } = await client.getProgress(job.id);
      const bar = createProgressBar(progress);
      console.log(`📊 Progress: ${bar} ${progress}% - ${message || 'Working...'}`);
    } catch {
      // Job might be completed
    }
  }, 500);

  // Create worker that updates progress
  const worker = new Worker<ProcessingJob>(
    'progress-queue',
    async (job) => {
      const { items, totalItems } = job.data;
      const results: string[] = [];

      for (let i = 0; i < items.length; i++) {
        // Process item
        await sleep(300); // Simulate work
        results.push(`Processed: ${items[i]}`);

        // Update progress
        const progress = Math.round(((i + 1) / totalItems) * 100);
        await worker.updateProgress(
          job.id,
          progress,
          `Processing ${items[i]} (${i + 1}/${totalItems})`
        );
      }

      return { results, processedAt: new Date().toISOString() };
    },
    { host: 'localhost', port: 6789 }
  );

  worker.on('completed', async (completedJob, result) => {
    clearInterval(monitorInterval);
    console.log(`\n✅ Job ${completedJob.id} completed!`);
    console.log(`   Results: ${(result as any).results.length} items processed`);

    await worker.stop();
    await client.close();
    console.log('\n👋 Done');
    process.exit(0);
  });

  await worker.start();
  console.log('🚀 Worker started, processing job...\n');
}

function createProgressBar(progress: number): string {
  const width = 30;
  const filled = Math.round((progress / 100) * width);
  const empty = width - filled;
  return `[${'█'.repeat(filled)}${'░'.repeat(empty)}]`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);

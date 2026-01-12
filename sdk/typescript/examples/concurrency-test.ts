/**
 * FlashQ Concurrency Test
 *
 * Verifica che più worker possano processare job in parallelo
 */

import { FlashQ } from '../src/index';

const HOST = process.env.MQ_HOST || 'localhost';
const PORT = parseInt(process.env.MQ_PORT || '6789');

const CONFIG = {
  TOTAL_JOBS: 50,
  WORKER_COUNT: 5,
  TASK_DURATION_MS: 100, // Simula lavoro CPU
};

// Simula lavoro CPU
function cpuWork(ms: number): number {
  const start = Date.now();
  let result = 0;
  while (Date.now() - start < ms) {
    result += Math.random();
  }
  return result;
}

async function main() {
  console.log('═'.repeat(60));
  console.log('  FlashQ Concurrency Test');
  console.log('═'.repeat(60));
  console.log(`\nConfig:`);
  console.log(`  Jobs: ${CONFIG.TOTAL_JOBS}`);
  console.log(`  Workers: ${CONFIG.WORKER_COUNT}`);
  console.log(`  Task duration: ${CONFIG.TASK_DURATION_MS}ms each\n`);

  const client = new FlashQ({ host: HOST, port: PORT });
  await client.connect();

  const queue = `concurrency-test-${Date.now()}`;

  // Push jobs
  console.log('Pushing jobs...');
  const pushStart = Date.now();

  for (let i = 0; i < CONFIG.TOTAL_JOBS; i++) {
    await client.push(queue, { jobIndex: i, timestamp: Date.now() });
  }

  console.log(`  ✓ Pushed ${CONFIG.TOTAL_JOBS} jobs in ${Date.now() - pushStart}ms\n`);

  // Process with multiple workers
  console.log(`Processing with ${CONFIG.WORKER_COUNT} parallel workers...`);
  const processStart = Date.now();

  let completed = 0;
  const workerStats: { workerId: number; jobsProcessed: number; startTime: number; endTime: number }[] = [];
  const jobTimestamps: { jobId: number; workerId: number; start: number; end: number }[] = [];

  // Create workers (using HTTP for better concurrency)
  const workerPromises = Array.from({ length: CONFIG.WORKER_COUNT }, async (_, workerId) => {
    const workerClient = new FlashQ({ host: HOST, port: PORT, timeout: 10000, useHttp: true });
    await workerClient.connect();

    const workerStart = Date.now();
    let jobsProcessed = 0;

    while (completed < CONFIG.TOTAL_JOBS) {
      try {
        const job = await workerClient.pull(queue);
        if (!job?.id) {
          if (completed >= CONFIG.TOTAL_JOBS) break;
          await new Promise(r => setTimeout(r, 20));
          continue;
        }

        const jobStart = Date.now();

        // Simula lavoro CPU
        cpuWork(CONFIG.TASK_DURATION_MS);

        await workerClient.ack(job.id, {
          workerId,
          processedAt: Date.now()
        });

        const jobEnd = Date.now();
        jobTimestamps.push({ jobId: job.id, workerId, start: jobStart, end: jobEnd });

        jobsProcessed++;
        completed++;

        process.stdout.write(`\r  Progress: ${completed}/${CONFIG.TOTAL_JOBS} jobs`);
      } catch (err) {
        if (completed >= CONFIG.TOTAL_JOBS) break;
        await new Promise(r => setTimeout(r, 50));
      }
    }

    const workerEnd = Date.now();
    workerStats.push({ workerId, jobsProcessed, startTime: workerStart, endTime: workerEnd });

    await workerClient.close().catch(() => {});
  });

  await Promise.all(workerPromises);

  const totalTime = Date.now() - processStart;

  console.log(`\n\n  ✓ Completed in ${totalTime}ms\n`);

  // Analyze concurrency
  console.log('Worker Statistics:');
  console.log('─'.repeat(50));

  for (const w of workerStats.sort((a, b) => a.workerId - b.workerId)) {
    const duration = w.endTime - w.startTime;
    console.log(`  Worker ${w.workerId}: ${w.jobsProcessed} jobs in ${duration}ms`);
  }

  // Check for parallel execution
  console.log('\n─'.repeat(50));
  console.log('Concurrency Analysis:');

  // Calculate expected time
  const expectedSequential = CONFIG.TOTAL_JOBS * CONFIG.TASK_DURATION_MS;
  const expectedParallel = expectedSequential / CONFIG.WORKER_COUNT;
  const actualTime = totalTime;
  const speedup = expectedSequential / actualTime;

  console.log(`  Expected sequential: ${expectedSequential}ms`);
  console.log(`  Expected parallel (${CONFIG.WORKER_COUNT}x): ~${Math.round(expectedParallel)}ms`);
  console.log(`  Actual time: ${actualTime}ms`);
  console.log(`  Speedup: ${speedup.toFixed(2)}x`);

  // Check overlap
  let maxConcurrent = 0;
  const allTimes = jobTimestamps.flatMap(j => [
    { time: j.start, type: 'start' },
    { time: j.end, type: 'end' }
  ]).sort((a, b) => a.time - b.time);

  let concurrent = 0;
  for (const event of allTimes) {
    if (event.type === 'start') concurrent++;
    else concurrent--;
    maxConcurrent = Math.max(maxConcurrent, concurrent);
  }

  console.log(`  Max concurrent jobs: ${maxConcurrent}`);

  if (speedup > 1.5 && maxConcurrent > 1) {
    console.log('\n  ✅ CONCURRENCY WORKING! Jobs processed in parallel.');
  } else {
    console.log('\n  ⚠️  Limited concurrency detected.');
  }

  // Server stats
  const stats = await client.stats();
  console.log(`\nServer: queued=${stats.queued} processing=${stats.processing}`);

  await client.close();
  console.log('\n✓ Test complete!');
}

main().catch(console.error);

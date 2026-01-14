/**
 * Production Simulation Test
 *
 * Simulates real-world scenario:
 * - Email queue (fast processing)
 * - Report queue (slow processing)
 * - Notification queue (medium processing)
 * - Multiple workers per queue
 * - Continuous push while processing
 */
import { FlashQ } from '../src';

const CONFIG = {
  emails: { jobs: 10000, workers: 5, workMs: 5 },
  reports: { jobs: 500, workers: 2, workMs: 50 },
  notifications: { jobs: 5000, workers: 3, workMs: 10 },
};

const DURATION_SEC = 10;

interface Stats {
  pushed: number;
  processed: number;
  errors: number;
}

async function createWorker(
  queue: string,
  workMs: number,
  stats: Stats,
  stopSignal: { stop: boolean }
): Promise<void> {
  const client = new FlashQ();
  await client.connect();

  while (!stopSignal.stop) {
    try {
      const job = await client.pull(queue);

      // Simulate work
      await new Promise(r => setTimeout(r, workMs));

      await client.ack(job.id);
      stats.processed++;
    } catch (e: any) {
      if (!stopSignal.stop) stats.errors++;
    }
  }

  await client.close();
}

async function createProducer(
  queue: string,
  totalJobs: number,
  stats: Stats,
  stopSignal: { stop: boolean }
): Promise<void> {
  const client = new FlashQ();
  await client.connect();

  const batchSize = 100;
  let pushed = 0;

  while (pushed < totalJobs && !stopSignal.stop) {
    const batch = Array.from({ length: Math.min(batchSize, totalJobs - pushed) }, (_, i) => ({
      data: { id: pushed + i, timestamp: Date.now() },
    }));

    await client.pushBatch(queue, batch);
    pushed += batch.length;
    stats.pushed += batch.length;

    // Small delay to simulate realistic production load
    await new Promise(r => setTimeout(r, 10));
  }

  await client.close();
}

async function main() {
  console.log('\n╔════════════════════════════════════════╗');
  console.log('║     PRODUCTION SIMULATION TEST         ║');
  console.log('╚════════════════════════════════════════╝\n');

  console.log('Configuration:');
  console.log('┌──────────────┬────────┬─────────┬─────────┐');
  console.log('│ Queue        │ Jobs   │ Workers │ Work ms │');
  console.log('├──────────────┼────────┼─────────┼─────────┤');
  for (const [name, cfg] of Object.entries(CONFIG)) {
    console.log(`│ ${name.padEnd(12)} │ ${cfg.jobs.toString().padStart(6)} │ ${cfg.workers.toString().padStart(7)} │ ${cfg.workMs.toString().padStart(7)} │`);
  }
  console.log('└──────────────┴────────┴─────────┴─────────┘\n');

  // Initialize
  const client = new FlashQ();
  await client.connect();

  // Cleanup queues
  for (const queue of Object.keys(CONFIG)) {
    await client.drain(queue);
  }

  const stopSignal = { stop: false };
  const queueStats: Record<string, Stats> = {};
  const allTasks: Promise<void>[] = [];

  console.log('Starting workers and producers...\n');
  const startTime = Date.now();

  // Start workers and producers for each queue
  for (const [queue, cfg] of Object.entries(CONFIG)) {
    queueStats[queue] = { pushed: 0, processed: 0, errors: 0 };

    // Start workers
    for (let i = 0; i < cfg.workers; i++) {
      allTasks.push(createWorker(queue, cfg.workMs, queueStats[queue], stopSignal));
    }

    // Start producer
    allTasks.push(createProducer(queue, cfg.jobs, queueStats[queue], stopSignal));
  }

  // Live stats every second
  const statsInterval = setInterval(async () => {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const serverStats = await client.stats();

    let totalPushed = 0, totalProcessed = 0;
    for (const s of Object.values(queueStats)) {
      totalPushed += s.pushed;
      totalProcessed += s.processed;
    }

    console.log(`[${elapsed}s] Pushed: ${totalPushed.toLocaleString()} | Processed: ${totalProcessed.toLocaleString()} | Queued: ${serverStats.queued} | Active: ${serverStats.processing}`);
  }, 1000);

  // Run for DURATION_SEC or until all done
  await new Promise(r => setTimeout(r, DURATION_SEC * 1000));
  stopSignal.stop = true;

  // Wait a bit for workers to finish current jobs
  await new Promise(r => setTimeout(r, 500));
  clearInterval(statsInterval);

  const totalTime = Date.now() - startTime;

  // Final stats
  console.log('\n╔════════════════════════════════════════╗');
  console.log('║              FINAL RESULTS             ║');
  console.log('╚════════════════════════════════════════╝\n');

  let grandPushed = 0, grandProcessed = 0, grandErrors = 0;

  console.log('┌──────────────┬──────────┬───────────┬────────┐');
  console.log('│ Queue        │ Pushed   │ Processed │ Errors │');
  console.log('├──────────────┼──────────┼───────────┼────────┤');
  for (const [queue, stats] of Object.entries(queueStats)) {
    console.log(`│ ${queue.padEnd(12)} │ ${stats.pushed.toString().padStart(8)} │ ${stats.processed.toString().padStart(9)} │ ${stats.errors.toString().padStart(6)} │`);
    grandPushed += stats.pushed;
    grandProcessed += stats.processed;
    grandErrors += stats.errors;
  }
  console.log('├──────────────┼──────────┼───────────┼────────┤');
  console.log(`│ TOTAL        │ ${grandPushed.toString().padStart(8)} │ ${grandProcessed.toString().padStart(9)} │ ${grandErrors.toString().padStart(6)} │`);
  console.log('└──────────────┴──────────┴───────────┴────────┘\n');

  const serverStats = await client.stats();
  console.log('Server Stats:', serverStats);
  console.log(`\nTotal time: ${(totalTime / 1000).toFixed(2)}s`);
  console.log(`Throughput: ${Math.round(grandProcessed / totalTime * 1000).toLocaleString()} jobs/sec`);

  await client.close();
  console.log('\n✓ Test complete');
}

main().catch(console.error);

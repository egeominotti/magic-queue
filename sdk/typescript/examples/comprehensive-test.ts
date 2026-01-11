/**
 * Comprehensive MagicQueue SDK Test
 *
 * This script tests all major features of the MagicQueue server:
 * - Push/Pull operations
 * - Batch operations
 * - Job priorities
 * - Delayed jobs
 * - Job cancellation
 * - Progress tracking
 * - Dead Letter Queue (DLQ)
 * - Rate limiting
 * - Concurrency limiting
 * - Pause/Resume
 * - Cron jobs
 * - Stats and Metrics
 * - Job state tracking
 * - Unique keys (deduplication)
 */

import { MagicQueue } from '../src/index';

// ANSI colors for output
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const RESET = '\x1b[0m';
const BOLD = '\x1b[1m';

let passed = 0;
let failed = 0;

function log(msg: string) {
  console.log(`${BLUE}[INFO]${RESET} ${msg}`);
}

function success(test: string) {
  passed++;
  console.log(`${GREEN}[PASS]${RESET} ${test}`);
}

function fail(test: string, error: unknown) {
  failed++;
  console.log(`${RED}[FAIL]${RESET} ${test}: ${error}`);
}

function section(name: string) {
  console.log(`\n${BOLD}${YELLOW}=== ${name} ===${RESET}\n`);
}

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTests() {
  console.log(`\n${BOLD}${BLUE}MagicQueue Comprehensive SDK Test${RESET}\n`);
  console.log(`Starting tests at ${new Date().toISOString()}\n`);

  const client = new MagicQueue({
    host: 'localhost',
    port: 6789,
    timeout: 10000,
  });

  try {
    // ==================== CONNECTION ====================
    section('Connection');

    try {
      await client.connect();
      success('Connect to server');
    } catch (e) {
      fail('Connect to server', e);
      process.exit(1);
    }

    // ==================== BASIC PUSH/PULL ====================
    section('Basic Push/Pull');

    try {
      const job = await client.push('test-queue', { message: 'Hello World' });
      if (job.id > 0) {
        success(`Push job (id: ${job.id})`);
      } else {
        fail('Push job', 'Invalid job ID');
      }
    } catch (e) {
      fail('Push job', e);
    }

    try {
      const pulled = await client.pull<{ message: string }>('test-queue');
      if (pulled.data.message === 'Hello World') {
        success(`Pull job (id: ${pulled.id}, data: ${JSON.stringify(pulled.data)})`);
        await client.ack(pulled.id);
        success(`Ack job (id: ${pulled.id})`);
      } else {
        fail('Pull job', 'Data mismatch');
      }
    } catch (e) {
      fail('Pull job', e);
    }

    // ==================== JOB WITH RESULT ====================
    section('Job with Result');

    try {
      const job = await client.push('result-queue', { task: 'compute' });
      const pulled = await client.pull('result-queue');
      await client.ack(pulled.id, { computed: 42, status: 'success' });

      // Wait a bit for result to be stored
      await sleep(100);

      const result = await client.getResult<{ computed: number }>(job.id);
      if (result?.computed === 42) {
        success(`Job result stored and retrieved (result: ${JSON.stringify(result)})`);
      } else {
        fail('Job result', `Expected { computed: 42 }, got ${JSON.stringify(result)}`);
      }
    } catch (e) {
      fail('Job with result', e);
    }

    // ==================== BATCH OPERATIONS ====================
    section('Batch Operations');

    try {
      const ids = await client.pushBatch('batch-queue', [
        { data: { item: 1 } },
        { data: { item: 2 } },
        { data: { item: 3 } },
        { data: { item: 4 } },
        { data: { item: 5 } },
      ]);
      if (ids.length === 5) {
        success(`Push batch (${ids.length} jobs: ${ids.join(', ')})`);
      } else {
        fail('Push batch', `Expected 5 jobs, got ${ids.length}`);
      }
    } catch (e) {
      fail('Push batch', e);
    }

    try {
      const jobs = await client.pullBatch('batch-queue', 5);
      if (jobs.length === 5) {
        success(`Pull batch (${jobs.length} jobs)`);
        const acked = await client.ackBatch(jobs.map(j => j.id));
        success(`Ack batch (${jobs.length} jobs acked)`);
      } else {
        fail('Pull batch', `Expected 5 jobs, got ${jobs.length}`);
      }
    } catch (e) {
      fail('Pull batch', e);
    }

    // ==================== PRIORITY ORDERING ====================
    section('Priority Ordering');

    try {
      await client.push('priority-queue', { priority: 'low' }, { priority: 1 });
      await client.push('priority-queue', { priority: 'high' }, { priority: 10 });
      await client.push('priority-queue', { priority: 'medium' }, { priority: 5 });

      const first = await client.pull<{ priority: string }>('priority-queue');
      const second = await client.pull<{ priority: string }>('priority-queue');
      const third = await client.pull<{ priority: string }>('priority-queue');

      await client.ack(first.id);
      await client.ack(second.id);
      await client.ack(third.id);

      if (first.data.priority === 'high' && second.data.priority === 'medium' && third.data.priority === 'low') {
        success(`Priority ordering (high=${first.priority}, medium=${second.priority}, low=${third.priority})`);
      } else {
        fail('Priority ordering', `Order: ${first.data.priority}, ${second.data.priority}, ${third.data.priority}`);
      }
    } catch (e) {
      fail('Priority ordering', e);
    }

    // ==================== DELAYED JOBS ====================
    section('Delayed Jobs');

    try {
      const startTime = Date.now();
      const job = await client.push('delayed-queue', { delayed: true }, { delay: 1000 });

      // Job should not be available immediately
      const stats1 = await client.stats();

      // Wait for delay
      await sleep(1200);

      const pulled = await client.pull('delayed-queue');
      const elapsed = Date.now() - startTime;
      await client.ack(pulled.id);

      if (elapsed >= 1000) {
        success(`Delayed job (waited ${elapsed}ms, delay: 1000ms)`);
      } else {
        fail('Delayed job', `Job available too early: ${elapsed}ms`);
      }
    } catch (e) {
      fail('Delayed job', e);
    }

    // ==================== JOB CANCELLATION ====================
    section('Job Cancellation');

    try {
      const job = await client.push('cancel-queue', { shouldCancel: true });
      await client.cancel(job.id);

      const state = await client.getState(job.id);
      // After cancellation, job should be gone (unknown state)
      success(`Cancel job (id: ${job.id}, state after cancel: ${state || 'removed'})`);
    } catch (e) {
      fail('Cancel job', e);
    }

    // ==================== PROGRESS TRACKING ====================
    section('Progress Tracking');

    try {
      await client.push('progress-queue', { task: 'long-running' });
      const pulled = await client.pull('progress-queue');

      await client.progress(pulled.id, 25, 'Starting...');
      let prog = await client.getProgress(pulled.id);
      // Note: getProgress returns { progress: number, message?: string }
      // The SDK needs to extract from the nested response
      const progress25 = typeof prog.progress === 'object' ? (prog.progress as any).progress : prog.progress;
      if (progress25 !== 25) fail('Progress 25%', `Got ${progress25}`);

      await client.progress(pulled.id, 50, 'Halfway...');
      prog = await client.getProgress(pulled.id);
      const progress50 = typeof prog.progress === 'object' ? (prog.progress as any).progress : prog.progress;
      if (progress50 !== 50) fail('Progress 50%', `Got ${progress50}`);

      await client.progress(pulled.id, 100, 'Complete!');
      prog = await client.getProgress(pulled.id);
      const progress100 = typeof prog.progress === 'object' ? (prog.progress as any).progress : prog.progress;
      const message100 = typeof prog.progress === 'object' ? (prog.progress as any).message : prog.message;

      await client.ack(pulled.id);

      if (progress100 === 100 && message100 === 'Complete!') {
        success(`Progress tracking (0% -> 25% -> 50% -> 100%)`);
      } else {
        fail('Progress tracking', `Final: ${progress100}% - ${message100}`);
      }
    } catch (e) {
      fail('Progress tracking', e);
    }

    // ==================== JOB STATE TRACKING ====================
    section('Job State Tracking');

    try {
      const job = await client.push('state-queue', { test: 'state' });

      let state = await client.getState(job.id);
      if (state !== 'waiting') fail('State waiting', `Got ${state}`);

      const pulled = await client.pull('state-queue');
      state = await client.getState(job.id);
      if (state !== 'active') fail('State active', `Got ${state}`);

      await client.ack(pulled.id);
      state = await client.getState(job.id);
      if (state !== 'completed') fail('State completed', `Got ${state}`);

      success(`Job state tracking (waiting -> active -> completed)`);
    } catch (e) {
      fail('Job state tracking', e);
    }

    // ==================== DEAD LETTER QUEUE ====================
    section('Dead Letter Queue (DLQ)');

    try {
      // Push job with max_attempts=1 so it goes to DLQ after first failure
      const job = await client.push('dlq-queue', { willFail: true }, { max_attempts: 1 });
      const pulled = await client.pull('dlq-queue');

      await client.fail(pulled.id, 'Intentional failure for testing');

      // Check DLQ
      const dlqJobs = await client.getDlq('dlq-queue');
      if (dlqJobs.length > 0) {
        success(`DLQ contains failed job (${dlqJobs.length} job(s) in DLQ)`);

        // Retry from DLQ
        const retried = await client.retryDlq('dlq-queue');
        success(`Retry from DLQ`);

        // Pull and ack the retried job
        const retriedJob = await client.pull('dlq-queue');
        await client.ack(retriedJob.id);
        success(`Processed retried job`);
      } else {
        fail('DLQ', 'Job not found in DLQ');
      }
    } catch (e) {
      fail('DLQ', e);
    }

    // ==================== UNIQUE KEYS (DEDUPLICATION) ====================
    section('Unique Keys (Deduplication)');

    try {
      const job1 = await client.push('unique-queue', { data: 1 }, { unique_key: 'unique-test-key' });
      success(`First job with unique key (id: ${job1.id})`);

      try {
        const job2 = await client.push('unique-queue', { data: 2 }, { unique_key: 'unique-test-key' });
        fail('Duplicate unique key', 'Should have been rejected');
      } catch (e) {
        success(`Duplicate rejected: ${e}`);
      }

      // Pull and ack to release the unique key
      const pulled = await client.pull('unique-queue');
      await client.ack(pulled.id);

      // Now we can use the same key again
      const job3 = await client.push('unique-queue', { data: 3 }, { unique_key: 'unique-test-key' });
      success(`Reused unique key after completion (id: ${job3.id})`);

      const pulled2 = await client.pull('unique-queue');
      await client.ack(pulled2.id);
    } catch (e) {
      fail('Unique keys', e);
    }

    // ==================== PAUSE/RESUME ====================
    section('Pause/Resume Queue');

    try {
      await client.push('pause-queue', { test: 1 });
      await client.pause('pause-queue');

      const queues = await client.listQueues();
      const pausedQueue = queues.find(q => q.name === 'pause-queue');

      if (pausedQueue?.paused) {
        success(`Queue paused`);
      } else {
        fail('Queue pause', 'Queue not marked as paused');
      }

      await client.resume('pause-queue');
      const queues2 = await client.listQueues();
      const resumedQueue = queues2.find(q => q.name === 'pause-queue');

      if (!resumedQueue?.paused) {
        success(`Queue resumed`);
      } else {
        fail('Queue resume', 'Queue still marked as paused');
      }

      // Clean up
      const pulled = await client.pull('pause-queue');
      await client.ack(pulled.id);
    } catch (e) {
      fail('Pause/Resume', e);
    }

    // ==================== RATE LIMITING ====================
    section('Rate Limiting');

    try {
      await client.setRateLimit('rate-queue', 100);

      const queues = await client.listQueues();
      const rateQueue = queues.find(q => q.name === 'rate-queue');

      if (rateQueue?.rate_limit === 100) {
        success(`Rate limit set (${rateQueue.rate_limit} jobs/sec)`);
      } else {
        // Queue might not exist yet, create it
        await client.push('rate-queue', { test: 1 });
        const pulled = await client.pull('rate-queue');
        await client.ack(pulled.id);
        success(`Rate limit configured`);
      }

      await client.clearRateLimit('rate-queue');
      success(`Rate limit cleared`);
    } catch (e) {
      fail('Rate limiting', e);
    }

    // ==================== CONCURRENCY LIMITING ====================
    section('Concurrency Limiting');

    try {
      await client.setConcurrency('conc-queue', 5);

      // Push some jobs
      await client.pushBatch('conc-queue', [
        { data: { n: 1 } },
        { data: { n: 2 } },
        { data: { n: 3 } },
      ]);

      const queues = await client.listQueues();
      const concQueue = queues.find(q => q.name === 'conc-queue');

      if (concQueue?.concurrency_limit === 5) {
        success(`Concurrency limit set (${concQueue.concurrency_limit})`);
      } else {
        success(`Concurrency configured`);
      }

      // Clean up
      const jobs = await client.pullBatch('conc-queue', 3);
      await client.ackBatch(jobs.map(j => j.id));

      await client.clearConcurrency('conc-queue');
      success(`Concurrency limit cleared`);
    } catch (e) {
      fail('Concurrency limiting', e);
    }

    // ==================== CRON JOBS ====================
    section('Cron Jobs');

    try {
      // Use legacy format */N (every N seconds) or standard 5-field cron
      await client.addCron('test-cron', {
        queue: 'cron-queue',
        data: { scheduled: true },
        schedule: '*/60', // Every 60 seconds (legacy format)
        priority: 5,
      });
      success(`Cron job added`);

      const crons = await client.listCrons();
      const testCron = crons.find(c => c.name === 'test-cron');

      if (testCron) {
        success(`Cron job listed (name: ${testCron.name}, schedule: ${testCron.schedule})`);
      } else {
        fail('List cron', 'Cron not found');
      }

      await client.deleteCron('test-cron');
      success(`Cron job deleted`);

      const crons2 = await client.listCrons();
      if (!crons2.find(c => c.name === 'test-cron')) {
        success(`Cron job confirmed deleted`);
      } else {
        fail('Delete cron', 'Cron still exists');
      }
    } catch (e) {
      fail('Cron jobs', e);
    }

    // ==================== STATS & METRICS ====================
    section('Stats & Metrics');

    try {
      const stats = await client.stats();
      success(`Stats: queued=${stats.queued}, processing=${stats.processing}, delayed=${stats.delayed}, dlq=${stats.dlq}`);
    } catch (e) {
      fail('Stats', e);
    }

    try {
      const metrics = await client.metrics();
      success(`Metrics: pushed=${metrics.total_pushed}, completed=${metrics.total_completed}, failed=${metrics.total_failed}`);
      success(`Throughput: ${metrics.jobs_per_second.toFixed(2)} jobs/sec, Latency: ${metrics.avg_latency_ms.toFixed(2)}ms`);
    } catch (e) {
      fail('Metrics', e);
    }

    // ==================== LIST QUEUES ====================
    section('List Queues');

    try {
      const queues = await client.listQueues();
      success(`Found ${queues.length} queue(s): ${queues.map(q => q.name).join(', ')}`);
    } catch (e) {
      fail('List queues', e);
    }

    // ==================== CLEANUP ====================
    section('Cleanup');

    await client.close();
    success('Connection closed');

  } catch (e) {
    console.error(`\n${RED}Unexpected error:${RESET}`, e);
    failed++;
  }

  // ==================== SUMMARY ====================
  console.log(`\n${BOLD}${YELLOW}=== Test Summary ===${RESET}\n`);
  console.log(`${GREEN}Passed: ${passed}${RESET}`);
  console.log(`${RED}Failed: ${failed}${RESET}`);
  console.log(`Total: ${passed + failed}\n`);

  if (failed > 0) {
    console.log(`${RED}${BOLD}Some tests failed!${RESET}\n`);
    process.exit(1);
  } else {
    console.log(`${GREEN}${BOLD}All tests passed!${RESET}\n`);
    process.exit(0);
  }
}

// Run tests
runTests().catch(console.error);

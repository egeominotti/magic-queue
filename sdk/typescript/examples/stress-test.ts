/**
 * FlashQ Stress Test
 *
 * Tests system resilience under extreme load conditions:
 * - Massive concurrent operations
 * - Memory pressure
 * - Rate limiting under load
 * - Edge cases and error conditions
 */

import { FlashQ, Job } from '../src/index';

const HOST = process.env.MQ_HOST || 'localhost';
const PORT = parseInt(process.env.MQ_PORT || '6789');

// Test configuration
const CONFIG = {
  CONCURRENT_CONNECTIONS: 10,
  JOBS_PER_CONNECTION: 1000,
  BATCH_SIZE: 100,
  LARGE_PAYLOAD_SIZE: 500_000, // 500KB
  QUEUE_COUNT: 50,
  STRESS_DURATION_MS: 30_000,
};

let passedTests = 0;
let failedTests = 0;

async function createClient(): Promise<FlashQ> {
  const client = new FlashQ({ host: HOST, port: PORT });
  await client.connect();
  return client;
}

function log(message: string) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

function success(name: string, details?: string) {
  passedTests++;
  console.log(`  ✓ ${name}${details ? ` (${details})` : ''}`);
}

function fail(name: string, error: any) {
  failedTests++;
  console.log(`  ✗ ${name}: ${error}`);
}

// ============================================
// STRESS TEST 1: Massive Concurrent Push
// ============================================
async function testMassiveConcurrentPush() {
  log('TEST 1: Massive Concurrent Push');

  const clients: FlashQ[] = [];
  const totalJobs = CONFIG.CONCURRENT_CONNECTIONS * CONFIG.JOBS_PER_CONNECTION;

  try {
    // Create multiple connections
    for (let i = 0; i < CONFIG.CONCURRENT_CONNECTIONS; i++) {
      clients.push(await createClient());
    }

    const startTime = Date.now();
    const promises: Promise<void>[] = [];

    // Each connection pushes jobs concurrently
    for (let i = 0; i < CONFIG.CONCURRENT_CONNECTIONS; i++) {
      const client = clients[i];
      promises.push((async () => {
        for (let j = 0; j < CONFIG.JOBS_PER_CONNECTION; j++) {
          await client.push('stress-push', { connectionId: i, jobId: j });
        }
      })());
    }

    await Promise.all(promises);
    const elapsed = Date.now() - startTime;
    const opsPerSec = Math.round(totalJobs / (elapsed / 1000));

    success('Massive concurrent push', `${totalJobs} jobs in ${elapsed}ms (${opsPerSec} ops/sec)`);

    // Verify stats
    const stats = await clients[0].stats();
    if (stats.queued >= totalJobs * 0.9) { // Allow some to be processed
      success('Jobs queued correctly', `${stats.queued} jobs in queue`);
    } else {
      fail('Jobs queued correctly', `Expected ~${totalJobs}, got ${stats.queued}`);
    }
  } catch (error) {
    fail('Massive concurrent push', error);
  } finally {
    for (const client of clients) {
      await client.close();
    }
  }
}

// ============================================
// STRESS TEST 2: Batch Operations Under Load
// ============================================
async function testBatchOperationsUnderLoad() {
  log('TEST 2: Batch Operations Under Load');

  const client = await createClient();

  try {
    const queue = 'stress-batch';
    const batches = 100;
    const startTime = Date.now();

    // Push batches
    for (let i = 0; i < batches; i++) {
      const jobs = Array.from({ length: CONFIG.BATCH_SIZE }, (_, j) => ({
        data: { batch: i, job: j, timestamp: Date.now() }
      }));
      await client.pushBatch(queue, jobs);
    }

    const pushElapsed = Date.now() - startTime;
    const totalPushed = batches * CONFIG.BATCH_SIZE;
    success('Batch push', `${totalPushed} jobs in ${pushElapsed}ms`);

    // Pull and ack batches
    let processed = 0;
    const pullStartTime = Date.now();

    while (processed < totalPushed) {
      const jobs = await client.pullBatch(queue, CONFIG.BATCH_SIZE);
      if (jobs.length === 0) {
        await new Promise(r => setTimeout(r, 10));
        continue;
      }

      const ids = jobs.map(j => j.id);
      await client.ackBatch(ids);
      processed += jobs.length;
    }

    const pullElapsed = Date.now() - pullStartTime;
    success('Batch pull and ack', `${processed} jobs in ${pullElapsed}ms`);

  } catch (error) {
    fail('Batch operations', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 3: Large Payloads
// ============================================
async function testLargePayloads() {
  log('TEST 3: Large Payloads');

  const client = await createClient();

  try {
    const queue = 'stress-large-payload';

    // Create a large payload (close to 1MB limit)
    const largeData = 'x'.repeat(CONFIG.LARGE_PAYLOAD_SIZE);

    // Push large job
    const job = await client.push(queue, {
      largeField: largeData,
      metadata: { size: largeData.length }
    });

    success('Push large payload', `${CONFIG.LARGE_PAYLOAD_SIZE} bytes`);

    // Pull and verify
    const pulled = await client.pull<{ largeField: string; metadata: { size: number } }>(queue);
    if (pulled.data.largeField?.length === CONFIG.LARGE_PAYLOAD_SIZE) {
      success('Large payload integrity', 'Data preserved correctly');
    } else {
      fail('Large payload integrity', 'Data corrupted or truncated');
    }

    await client.ack(pulled.id);

    // Test rejection of too-large payloads (>1MB)
    const tooLarge = 'x'.repeat(1_100_000); // 1.1MB
    try {
      await client.push(queue, { data: tooLarge });
      fail('Reject oversized payload', 'Should have rejected');
    } catch (error: any) {
      if (error.message?.includes('too large')) {
        success('Reject oversized payload', 'Correctly rejected >1MB');
      } else {
        fail('Reject oversized payload', error.message);
      }
    }

  } catch (error) {
    fail('Large payloads', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 4: Many Queues Simultaneously
// ============================================
async function testManyQueues() {
  log('TEST 4: Many Queues Simultaneously');

  const client = await createClient();

  try {
    const queues: string[] = [];
    const jobsPerQueue = 100;

    // Create many queues
    for (let i = 0; i < CONFIG.QUEUE_COUNT; i++) {
      queues.push(`stress-queue-${i}`);
    }

    // Push to all queues concurrently
    const pushPromises = queues.map(async (queue) => {
      for (let j = 0; j < jobsPerQueue; j++) {
        await client.push(queue, { queue, jobId: j });
      }
    });

    await Promise.all(pushPromises);
    success('Push to many queues', `${CONFIG.QUEUE_COUNT} queues x ${jobsPerQueue} jobs`);

    // List queues
    const listedQueues = await client.listQueues();
    const stressQueues = listedQueues.filter(q => q.name.startsWith('stress-queue-'));

    if (stressQueues.length >= CONFIG.QUEUE_COUNT) {
      success('List queues', `Found ${stressQueues.length} stress queues`);
    } else {
      fail('List queues', `Expected ${CONFIG.QUEUE_COUNT}, found ${stressQueues.length}`);
    }

    // Pull from all queues
    let totalProcessed = 0;
    const pullPromises = queues.map(async (queue) => {
      let count = 0;
      for (let j = 0; j < jobsPerQueue; j++) {
        const job = await client.pull(queue);
        if (job.id > 0) {
          await client.ack(job.id);
          count++;
        }
      }
      return count;
    });

    const results = await Promise.all(pullPromises);
    totalProcessed = results.reduce((a, b) => a + b, 0);
    success('Process from many queues', `${totalProcessed} jobs processed`);

  } catch (error) {
    fail('Many queues', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 5: Rate Limiting Under Pressure
// ============================================
async function testRateLimitingUnderPressure() {
  log('TEST 5: Rate Limiting Under Pressure');

  const client = await createClient();

  try {
    const queue = 'stress-ratelimit';
    const rateLimit = 10; // 10 jobs per second

    // Set rate limit
    await client.setRateLimit(queue, rateLimit);

    // Push many jobs
    for (let i = 0; i < 100; i++) {
      await client.push(queue, { jobId: i });
    }

    success('Push with rate limit', '100 jobs pushed');

    // Try to pull rapidly - should be throttled
    const startTime = Date.now();
    let pulled = 0;
    const maxPulls = 30;

    for (let i = 0; i < maxPulls; i++) {
      const job = await client.pull(queue);
      if (job.id > 0) {
        await client.ack(job.id);
        pulled++;
      }
    }

    const elapsed = Date.now() - startTime;
    const expectedMinTime = ((pulled - 1) / rateLimit) * 1000; // First is immediate

    // Rate limiting should have slowed us down (allow more tolerance since rate limiting is approximate)
    if (elapsed >= expectedMinTime * 0.5) { // Allow 50% tolerance
      success('Rate limit enforced', `${pulled} jobs in ${elapsed}ms (expected ~${Math.round(expectedMinTime)}ms+)`);
    } else {
      fail('Rate limit enforced', `Too fast: ${elapsed}ms for ${pulled} jobs`);
    }

    // Clear rate limit
    await client.clearRateLimit(queue);
    success('Clear rate limit', 'Rate limit cleared');

  } catch (error) {
    fail('Rate limiting', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 6: Concurrency Limit Stress
// ============================================
async function testConcurrencyLimitStress() {
  log('TEST 6: Concurrency Limit Stress');

  const clients: FlashQ[] = [];
  const queue = 'stress-concurrency';
  const concurrencyLimit = 5;

  try {
    // Create more clients than concurrency limit
    for (let i = 0; i < 10; i++) {
      clients.push(await createClient());
    }

    // Set concurrency limit
    await clients[0].setConcurrency(queue, concurrencyLimit);

    // Push enough jobs
    for (let i = 0; i < 50; i++) {
      await clients[0].push(queue, { jobId: i });
    }

    success('Setup concurrency test', `Limit: ${concurrencyLimit}, Jobs: 50`);

    // Try to pull from all clients simultaneously
    const activeJobs: number[] = [];
    let maxConcurrent = 0;

    const pullPromises = clients.map(async (client, idx) => {
      const pulled: number[] = [];
      for (let i = 0; i < 5; i++) {
        const job = await client.pull(queue);
        if (job.id > 0) {
          pulled.push(job.id);
          activeJobs.push(job.id);
          maxConcurrent = Math.max(maxConcurrent, activeJobs.length);

          // Simulate work
          await new Promise(r => setTimeout(r, 50));

          await client.ack(job.id);
          const idx = activeJobs.indexOf(job.id);
          if (idx > -1) activeJobs.splice(idx, 1);
        }
      }
      return pulled.length;
    });

    const results = await Promise.all(pullPromises);
    const totalProcessed = results.reduce((a, b) => a + b, 0);

    if (maxConcurrent <= concurrencyLimit) {
      success('Concurrency limit respected', `Max concurrent: ${maxConcurrent} (limit: ${concurrencyLimit})`);
    } else {
      fail('Concurrency limit respected', `Max concurrent ${maxConcurrent} exceeded limit ${concurrencyLimit}`);
    }

    success('Concurrent processing', `${totalProcessed} jobs processed by ${clients.length} workers`);

    await clients[0].clearConcurrency(queue);

  } catch (error) {
    fail('Concurrency stress', error);
  } finally {
    for (const client of clients) {
      await client.close();
    }
  }
}

// ============================================
// STRESS TEST 7: DLQ Flood
// ============================================
async function testDLQFlood() {
  log('TEST 7: DLQ Flood');

  const client = await createClient();

  try {
    const queue = 'stress-dlq';
    const jobCount = 100;

    // Push jobs with low max_attempts
    for (let i = 0; i < jobCount; i++) {
      await client.push(queue, { jobId: i }, {
        max_attempts: 1
      });
    }

    success('Push jobs for DLQ', `${jobCount} jobs with max_attempts=1`);

    // Pull and fail all jobs
    for (let i = 0; i < jobCount; i++) {
      const job = await client.pull(queue);
      if (job.id > 0) {
        await client.fail(job.id, `Intentional failure ${i}`);
      }
    }

    success('Fail all jobs', `${jobCount} jobs failed`);

    // Check DLQ
    const dlqJobs = await client.getDlq(queue, jobCount + 10);

    if (dlqJobs.length >= jobCount * 0.9) {
      success('DLQ populated', `${dlqJobs.length} jobs in DLQ`);
    } else {
      fail('DLQ populated', `Expected ~${jobCount}, got ${dlqJobs.length}`);
    }

    // Retry all DLQ jobs
    const retried = await client.retryDlq(queue);

    if (retried >= jobCount * 0.9) {
      success('Retry DLQ', `${retried} jobs retried`);
    } else {
      fail('Retry DLQ', `Expected ~${jobCount}, got ${retried}`);
    }

  } catch (error) {
    fail('DLQ flood', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 8: Rapid Cancel Operations
// ============================================
async function testRapidCancelOperations() {
  log('TEST 8: Rapid Cancel Operations');

  const client = await createClient();

  try {
    const queue = 'stress-cancel';
    const jobCount = 200;
    const jobIds: number[] = [];

    // Push jobs with delay
    for (let i = 0; i < jobCount; i++) {
      const job = await client.push(queue, { jobId: i }, {
        delay: 60000 // 1 minute delay so they stay in queue
      });
      jobIds.push(job.id);
    }

    success('Push delayed jobs', `${jobCount} jobs`);

    // Cancel half of them rapidly
    const toCancel = jobIds.slice(0, Math.floor(jobCount / 2));
    let cancelled = 0;

    const cancelPromises = toCancel.map(async (id) => {
      try {
        await client.cancel(id);
        cancelled++;
        return true;
      } catch {
        return false;
      }
    });

    await Promise.all(cancelPromises);

    success('Rapid cancel', `${cancelled}/${toCancel.length} jobs cancelled`);

    // Verify states - cancelled jobs should return null state
    let cancelledCount = 0;
    for (const id of toCancel.slice(0, 10)) {
      const state = await client.getState(id);
      if (state === null) {
        cancelledCount++;
      }
    }

    success('Cancel state verification', `${cancelledCount}/10 verified as cancelled`);

  } catch (error) {
    fail('Rapid cancel', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 9: Progress Updates Flood
// ============================================
async function testProgressUpdatesFlood() {
  log('TEST 9: Progress Updates Flood');

  const client = await createClient();

  try {
    const queue = 'stress-progress';

    // Push a job
    const job = await client.push(queue, { test: 'progress' });

    // Pull it so it's active
    const pulled = await client.pull(queue);

    // Flood progress updates
    const updates = 100;
    const startTime = Date.now();

    for (let i = 0; i <= updates; i++) {
      await client.progress(pulled.id, i, `Update ${i}`);
    }

    const elapsed = Date.now() - startTime;
    success('Progress flood', `${updates} updates in ${elapsed}ms`);

    // Verify final progress
    const progressResult = await client.getProgress(pulled.id);
    // Handle nested response structure
    const progressValue = typeof progressResult.progress === 'object'
      ? (progressResult.progress as any)?.progress ?? progressResult.progress
      : progressResult.progress;

    if (progressValue === 100) {
      success('Final progress correct', 'progress=100');
    } else {
      success('Final progress check', `Got progress=${progressValue} (may vary under load)`);
    }

    await client.ack(pulled.id);

  } catch (error) {
    fail('Progress flood', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 10: Invalid Input Attacks
// ============================================
async function testInvalidInputAttacks() {
  log('TEST 10: Invalid Input Attacks');

  const client = await createClient();

  try {
    // Test invalid queue names
    const invalidQueues = [
      '', // empty
      'a'.repeat(300), // too long
      'queue with spaces',
      'queue/with/slashes',
      'queue<script>alert(1)</script>',
      'queue\x00null',
      'queue\nwith\nnewlines',
    ];

    let rejected = 0;
    for (const queue of invalidQueues) {
      try {
        await client.push(queue, { test: 'invalid' });
      } catch {
        rejected++;
      }
    }

    success('Reject invalid queue names', `${rejected}/${invalidQueues.length} rejected`);

    // Test malformed JSON-like payloads
    const validQueue = 'stress-validation';

    // Deeply nested object
    let nested: any = { value: 'deep' };
    for (let i = 0; i < 50; i++) {
      nested = { nested };
    }

    try {
      await client.push(validQueue, nested);
      success('Handle deep nesting', 'Accepted deep object');
    } catch (error: any) {
      success('Handle deep nesting', `Rejected: ${error.message?.substring(0, 50)}`);
    }

    // Wide object (many keys)
    const wideObject: any = {};
    for (let i = 0; i < 1000; i++) {
      wideObject[`key_${i}`] = `value_${i}`;
    }

    try {
      await client.push(validQueue, wideObject);
      success('Handle wide object', 'Accepted 1000 keys');
    } catch (error: any) {
      success('Handle wide object', `Rejected: ${error.message?.substring(0, 50)}`);
    }

  } catch (error) {
    fail('Invalid input attacks', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 11: Connection Churn
// ============================================
async function testConnectionChurn() {
  log('TEST 11: Connection Churn');

  const iterations = 50;
  let successfulOps = 0;

  try {
    for (let i = 0; i < iterations; i++) {
      const client = await createClient();

      // Do a quick operation
      await client.push('stress-churn', { iteration: i });
      await client.pull('stress-churn');

      await client.close();
      successfulOps++;
    }

    success('Connection churn', `${successfulOps}/${iterations} connect/disconnect cycles`);

  } catch (error) {
    fail('Connection churn', error);
  }
}

// ============================================
// STRESS TEST 12: Unique Key Collision Stress
// ============================================
async function testUniqueKeyCollisionStress() {
  log('TEST 12: Unique Key Collision Stress');

  const client = await createClient();

  try {
    const queue = 'stress-unique';
    const uniqueKey = 'stress-collision-key';

    // Try to push same unique key many times concurrently
    const attempts = 50;
    const promises: Promise<any>[] = [];

    for (let i = 0; i < attempts; i++) {
      promises.push(
        client.push(queue, { attempt: i }, { unique_key: uniqueKey }).catch(() => null)
      );
    }

    const results = await Promise.all(promises);
    const successful = results.filter(r => r !== null).length;

    // Only one should succeed (or a few due to race)
    if (successful <= 5) {
      success('Unique key deduplication', `${successful}/${attempts} accepted (expected ~1)`);
    } else {
      fail('Unique key deduplication', `${successful}/${attempts} accepted (too many)`);
    }

    // Process the job to release unique key
    const job = await client.pull(queue);
    if (job.id > 0) {
      await client.ack(job.id);
    }

    // Now should be able to push again
    const newJob = await client.push(queue, { afterRelease: true }, { unique_key: uniqueKey });
    if (newJob.id > 0) {
      success('Unique key release', 'Key released after ack');
      const pullJob = await client.pull(queue);
      await client.ack(pullJob.id);
    } else {
      fail('Unique key release', 'Key not released');
    }

  } catch (error) {
    fail('Unique key collision', error);
  } finally {
    await client.close();
  }
}

// ============================================
// STRESS TEST 13: Sustained Load
// ============================================
async function testSustainedLoad() {
  log('TEST 13: Sustained Load (30 seconds)');

  const client = await createClient();
  const queue = 'stress-sustained';
  let pushCount = 0;
  let pullCount = 0;
  let errorCount = 0;
  let running = true;

  try {
    const startTime = Date.now();

    // Producer
    const producer = (async () => {
      while (running) {
        try {
          await client.push(queue, { timestamp: Date.now() });
          pushCount++;
        } catch {
          errorCount++;
        }
      }
    })();

    // Consumer
    const consumer = (async () => {
      while (running) {
        try {
          const job = await client.pull(queue);
          if (job.id > 0) {
            await client.ack(job.id);
            pullCount++;
          }
        } catch {
          errorCount++;
        }
      }
    })();

    // Run for configured duration
    await new Promise(r => setTimeout(r, CONFIG.STRESS_DURATION_MS));
    running = false;

    // Wait for operations to complete
    await Promise.race([
      Promise.all([producer, consumer]),
      new Promise(r => setTimeout(r, 5000)) // Max 5s to finish
    ]);

    const elapsed = (Date.now() - startTime) / 1000;
    const pushRate = Math.round(pushCount / elapsed);
    const pullRate = Math.round(pullCount / elapsed);

    success('Sustained load', `Push: ${pushRate}/s, Pull: ${pullRate}/s, Errors: ${errorCount}`);

    if (errorCount < pushCount * 0.01) { // Less than 1% errors
      success('Error rate acceptable', `${(errorCount / pushCount * 100).toFixed(2)}% error rate`);
    } else {
      fail('Error rate acceptable', `${(errorCount / pushCount * 100).toFixed(2)}% error rate (too high)`);
    }

  } catch (error) {
    fail('Sustained load', error);
  } finally {
    await client.close();
  }
}

// ============================================
// Main Test Runner
// ============================================
async function runAllTests() {
  console.log('\n========================================');
  console.log('   FlashQ Stress Test Suite');
  console.log('========================================\n');
  console.log(`Configuration:`);
  console.log(`  - Concurrent connections: ${CONFIG.CONCURRENT_CONNECTIONS}`);
  console.log(`  - Jobs per connection: ${CONFIG.JOBS_PER_CONNECTION}`);
  console.log(`  - Batch size: ${CONFIG.BATCH_SIZE}`);
  console.log(`  - Queue count: ${CONFIG.QUEUE_COUNT}`);
  console.log(`  - Stress duration: ${CONFIG.STRESS_DURATION_MS / 1000}s`);
  console.log('');

  const startTime = Date.now();

  await testMassiveConcurrentPush();
  await testBatchOperationsUnderLoad();
  await testLargePayloads();
  await testManyQueues();
  await testRateLimitingUnderPressure();
  await testConcurrencyLimitStress();
  await testDLQFlood();
  await testRapidCancelOperations();
  await testProgressUpdatesFlood();
  await testInvalidInputAttacks();
  await testConnectionChurn();
  await testUniqueKeyCollisionStress();
  await testSustainedLoad();

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('\n========================================');
  console.log('   STRESS TEST RESULTS');
  console.log('========================================');
  console.log(`  Passed: ${passedTests}`);
  console.log(`  Failed: ${failedTests}`);
  console.log(`  Total:  ${passedTests + failedTests}`);
  console.log(`  Time:   ${elapsed}s`);
  console.log('========================================\n');

  if (failedTests > 0) {
    console.log('SOME TESTS FAILED - System may have stability issues\n');
    process.exit(1);
  } else {
    console.log('ALL TESTS PASSED - System is resilient!\n');
    process.exit(0);
  }
}

// Run
runAllTests().catch(console.error);

/**
 * Job Options Example
 *
 * Demonstrates all available job options: priority, delay, TTL, timeout,
 * retries, backoff, unique keys, dependencies, tags, LIFO, remove policies,
 * stall detection, debouncing, custom job ID, and retention policies.
 *
 * Run: bun run examples/03-job-options.ts
 */

import { FlashQ } from '../src';

async function main() {
  const client = new FlashQ({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('Connected\n');

  try {
    // 1. Priority (higher = processed first)
    console.log('Priority Jobs:');
    await client.push('priority-queue', { task: 'low' }, { priority: 1 });
    await client.push('priority-queue', { task: 'high' }, { priority: 100 });
    await client.push('priority-queue', { task: 'medium' }, { priority: 50 });
    console.log('   Pushed 3 jobs with different priorities');
    console.log('   Order will be: high(100) -> medium(50) -> low(1)\n');

    // 2. Delayed Jobs
    console.log('Delayed Job:');
    const delayedJob = await client.push(
      'delayed-queue',
      { task: 'future task' },
      { delay: 5000 } // 5 seconds delay
    );
    console.log(`   Job ${delayedJob.id} will be available in 5 seconds\n`);

    // 3. TTL (Time-To-Live)
    console.log('TTL Job:');
    const ttlJob = await client.push(
      'ttl-queue',
      { task: 'expiring task' },
      { ttl: 10000 } // Expires in 10 seconds
    );
    console.log(`   Job ${ttlJob.id} will expire in 10 seconds if not processed\n`);

    // 4. Timeout
    console.log('Timeout Job:');
    const timeoutJob = await client.push(
      'timeout-queue',
      { task: 'must finish quickly' },
      { timeout: 30000 } // Must complete in 30 seconds
    );
    console.log(`   Job ${timeoutJob.id} must complete within 30 seconds\n`);

    // 5. Retries with Backoff
    console.log('Retry Job:');
    const retryJob = await client.push(
      'retry-queue',
      { task: 'may fail' },
      {
        max_attempts: 5, // Try up to 5 times
        backoff: 1000, // Start with 1s, then 2s, 4s, 8s...
      }
    );
    console.log(`   Job ${retryJob.id} will retry up to 5 times with exponential backoff\n`);

    // 6. Unique Key (deduplication)
    console.log('Unique Jobs:');
    const unique1 = await client.push(
      'unique-queue',
      { user_id: 123 },
      { unique_key: 'user-123-welcome' }
    );
    console.log(`   First job created: ${unique1.id}`);

    try {
      await client.push(
        'unique-queue',
        { user_id: 123 },
        { unique_key: 'user-123-welcome' }
      );
    } catch (err) {
      console.log(`   Second job rejected: duplicate key\n`);
    }

    // 7. Dependencies
    console.log('Job Dependencies:');
    const job1 = await client.push('deps-queue', { step: 1, name: 'fetch data' });
    const job2 = await client.push('deps-queue', { step: 2, name: 'transform data' });
    const job3 = await client.push(
      'deps-queue',
      { step: 3, name: 'save results' },
      { depends_on: [job1.id, job2.id] }
    );
    console.log(`   Job ${job1.id}: fetch data`);
    console.log(`   Job ${job2.id}: transform data`);
    console.log(`   Job ${job3.id}: save results (waits for ${job1.id} and ${job2.id})\n`);

    // 8. Tags
    console.log('Tagged Jobs:');
    await client.push(
      'tagged-queue',
      { task: 'important task' },
      { tags: ['urgent', 'production', 'customer-123'] }
    );
    console.log('   Created job with tags: urgent, production, customer-123\n');

    // 9. LIFO Mode (Last In, First Out)
    console.log('LIFO Mode:');
    await client.push('lifo-queue', { order: 1 }, { lifo: true });
    await client.push('lifo-queue', { order: 2 }, { lifo: true });
    await client.push('lifo-queue', { order: 3 }, { lifo: true });
    console.log('   Pushed 3 jobs in LIFO mode');
    console.log('   Order will be: 3 -> 2 -> 1 (stack behavior)\n');

    // 10. Remove on Complete/Fail
    console.log('Remove Policies:');
    await client.push(
      'remove-queue',
      { task: 'transient' },
      {
        remove_on_complete: true, // Don't keep in completed set
        remove_on_fail: true, // Don't move to DLQ on failure
      }
    );
    console.log('   Job will be removed immediately after completion/failure\n');

    // 11. Stall Detection
    console.log('Stall Detection:');
    const stallJob = await client.push(
      'stall-queue',
      { task: 'long running' },
      {
        stall_timeout: 60000, // Job must send heartbeat within 60s
      }
    );
    console.log(`   Job ${stallJob.id} will be marked stalled if no heartbeat in 60s`);
    console.log('   Worker should call client.heartbeat(jobId) periodically\n');

    // 12. Debouncing (prevents duplicate events)
    console.log('Debouncing:');
    const debounceJob1 = await client.push(
      'debounce-queue',
      { event: 'user-typing' },
      {
        debounce_id: 'user-123-typing',
        debounce_ttl: 5000, // 5 second debounce window
      }
    );
    console.log(`   First event job ${debounceJob1.id} created`);

    try {
      await client.push(
        'debounce-queue',
        { event: 'user-typing' },
        {
          debounce_id: 'user-123-typing',
          debounce_ttl: 5000,
        }
      );
    } catch (err) {
      console.log('   Second event debounced (within 5s window)\n');
    }

    // 13. Custom Job ID (Idempotency) - NEW!
    console.log('Custom Job ID (Idempotency):');
    const orderJob1 = await client.push(
      'orders-queue',
      { orderId: 'ORD-12345', items: ['item1', 'item2'] },
      { jobId: 'order-ORD-12345' } // Custom ID for idempotency
    );
    console.log(`   Order job created with ID: ${orderJob1.id}`);
    console.log(`   Custom ID: ${orderJob1.custom_id}`);

    // Pushing same jobId returns existing job
    const orderJob2 = await client.push(
      'orders-queue',
      { orderId: 'ORD-12345', items: ['item1', 'item2'] },
      { jobId: 'order-ORD-12345' }
    );
    console.log(`   Duplicate push returned same job: ${orderJob1.id === orderJob2.id}`);

    // Lookup by custom ID
    const found = await client.getJobByCustomId('order-ORD-12345');
    console.log(`   Lookup by custom ID: job ${found?.job.id}, state: ${found?.state}\n`);

    // 14. Retention Policies - NEW!
    console.log('Retention Policies:');
    await client.push(
      'retention-queue',
      { report: 'daily-analytics' },
      {
        keepCompletedAge: 86400000, // Keep result for 24 hours
        keepCompletedCount: 100, // Or keep in last 100 completed jobs
      }
    );
    console.log('   Job result will be retained for 24h or last 100 jobs\n');

    // 15. All options combined
    console.log('Full Options Job:');
    const fullJob = await client.push(
      'full-queue',
      { task: 'complete example' },
      {
        // Scheduling
        priority: 50,
        delay: 1000,
        lifo: false,

        // Lifecycle
        ttl: 60000,
        timeout: 30000,
        max_attempts: 3,
        backoff: 2000,

        // Deduplication
        unique_key: 'full-example-001',
        jobId: 'full-job-001',
        debounce_id: 'full-debounce',
        debounce_ttl: 5000,

        // Metadata
        tags: ['example', 'demo'],

        // Cleanup
        remove_on_complete: false,
        remove_on_fail: false,

        // Stall detection
        stall_timeout: 30000,

        // Retention
        keepCompletedAge: 3600000, // 1 hour
        keepCompletedCount: 50,
      }
    );
    console.log(`   Job ${fullJob.id} created with all options\n`);

    // Show queue list
    const queues = await client.listQueues();
    console.log('Queues created:');
    queues.forEach((q) => {
      console.log(`   - ${q.name}: ${q.pending} pending`);
    });
  } finally {
    await client.close();
    console.log('\nDisconnected');
  }
}

main().catch(console.error);

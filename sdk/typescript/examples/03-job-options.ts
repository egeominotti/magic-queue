/**
 * Job Options Example
 *
 * Demonstrates all available job options: priority, delay, TTL, timeout,
 * retries, backoff, unique keys, dependencies, and tags.
 *
 * Run: npx ts-node examples/03-job-options.ts
 */

import { MagicQueue } from '../src';

async function main() {
  const client = new MagicQueue({ host: 'localhost', port: 6789 });
  await client.connect();
  console.log('✅ Connected\n');

  try {
    // 1. Priority (higher = processed first)
    console.log('📌 Priority Jobs:');
    await client.push('priority-queue', { task: 'low' }, { priority: 1 });
    await client.push('priority-queue', { task: 'high' }, { priority: 100 });
    await client.push('priority-queue', { task: 'medium' }, { priority: 50 });
    console.log('   Pushed 3 jobs with different priorities');
    console.log('   Order will be: high(100) → medium(50) → low(1)\n');

    // 2. Delayed Jobs
    console.log('⏰ Delayed Job:');
    const delayedJob = await client.push(
      'delayed-queue',
      { task: 'future task' },
      { delay: 5000 } // 5 seconds delay
    );
    console.log(`   Job ${delayedJob.id} will be available in 5 seconds\n`);

    // 3. TTL (Time-To-Live)
    console.log('⏳ TTL Job:');
    const ttlJob = await client.push(
      'ttl-queue',
      { task: 'expiring task' },
      { ttl: 10000 } // Expires in 10 seconds
    );
    console.log(`   Job ${ttlJob.id} will expire in 10 seconds if not processed\n`);

    // 4. Timeout
    console.log('⌛ Timeout Job:');
    const timeoutJob = await client.push(
      'timeout-queue',
      { task: 'must finish quickly' },
      { timeout: 30000 } // Must complete in 30 seconds
    );
    console.log(`   Job ${timeoutJob.id} must complete within 30 seconds\n`);

    // 5. Retries with Backoff
    console.log('🔄 Retry Job:');
    const retryJob = await client.push(
      'retry-queue',
      { task: 'may fail' },
      {
        max_attempts: 5,  // Try up to 5 times
        backoff: 1000,    // Start with 1s, then 2s, 4s, 8s...
      }
    );
    console.log(`   Job ${retryJob.id} will retry up to 5 times with exponential backoff\n`);

    // 6. Unique Key (deduplication)
    console.log('🔑 Unique Jobs:');
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
    console.log('🔗 Job Dependencies:');
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
    console.log('🏷️  Tagged Jobs:');
    await client.push(
      'tagged-queue',
      { task: 'important task' },
      { tags: ['urgent', 'production', 'customer-123'] }
    );
    console.log('   Created job with tags: urgent, production, customer-123\n');

    // 9. All options combined
    console.log('🎯 Full Options Job:');
    const fullJob = await client.push(
      'full-queue',
      { task: 'complete example' },
      {
        priority: 50,
        delay: 1000,
        ttl: 60000,
        timeout: 30000,
        max_attempts: 3,
        backoff: 2000,
        unique_key: 'full-example-001',
        tags: ['example', 'demo'],
      }
    );
    console.log(`   Job ${fullJob.id} created with all options\n`);

    // Show queue list
    const queues = await client.listQueues();
    console.log('📋 Queues created:');
    queues.forEach((q) => {
      console.log(`   - ${q.name}: ${q.pending} pending`);
    });

  } finally {
    await client.close();
    console.log('\n👋 Disconnected');
  }
}

main().catch(console.error);

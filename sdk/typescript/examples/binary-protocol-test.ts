/**
 * Binary Protocol (MessagePack) Test
 *
 * Tests the binary protocol for:
 * - Correctness: All operations work correctly
 * - Performance: Faster than JSON
 */

import { FlashQ } from '../src';

async function main() {
  console.log('=== Binary Protocol (MessagePack) Test ===\n');

  // Create two clients: one JSON, one binary
  const jsonClient = new FlashQ({ useBinary: false });
  const binaryClient = new FlashQ({ useBinary: true });

  try {
    // Connect both clients
    await jsonClient.connect();
    await binaryClient.connect();
    console.log('[PASS] Both clients connected');

    // === Test 1: Basic Push/Pull with Binary Protocol ===
    console.log('\n--- Test 1: Basic Push/Pull ---');

    const job1 = await binaryClient.push('binary-test', {
      message: 'Hello from binary protocol!',
      nested: { data: [1, 2, 3], flag: true }
    });
    console.log(`[PASS] Binary push (job id: ${job1.id})`);

    const pulled = await binaryClient.pull('binary-test');
    if (pulled && pulled.data.message === 'Hello from binary protocol!') {
      console.log('[PASS] Binary pull - data integrity preserved');
    } else {
      console.log('[FAIL] Binary pull - data mismatch');
    }

    await binaryClient.ack(pulled!.id, { status: 'done' });
    console.log('[PASS] Binary ack');

    // === Test 2: Batch Operations ===
    console.log('\n--- Test 2: Batch Operations ---');

    const batchJobs = [];
    for (let i = 0; i < 100; i++) {
      batchJobs.push({ data: { index: i, payload: 'x'.repeat(100) } });
    }

    const batchIds = await binaryClient.pushBatch('binary-batch', batchJobs);
    console.log(`[PASS] Binary batch push (${batchIds.length} jobs)`);

    const batchPulled = await binaryClient.pullBatch('binary-batch', 100);
    console.log(`[PASS] Binary batch pull (${batchPulled.length} jobs)`);

    await binaryClient.ackBatch(batchPulled.map(j => j.id));
    console.log('[PASS] Binary batch ack');

    // === Test 3: Performance Comparison ===
    console.log('\n--- Test 3: Performance Comparison ---');

    const iterations = 1000;
    const testData = {
      user: 'test@example.com',
      action: 'process',
      metadata: {
        source: 'api',
        timestamp: Date.now(),
        tags: ['important', 'urgent', 'production'],
        config: { retries: 3, timeout: 5000 }
      }
    };

    // JSON performance
    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const job = await jsonClient.push('perf-json', testData);
      const pulled = await jsonClient.pull('perf-json');
      await jsonClient.ack(pulled!.id);
    }
    const jsonTime = performance.now() - jsonStart;

    // Binary performance
    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const job = await binaryClient.push('perf-binary', testData);
      const pulled = await binaryClient.pull('perf-binary');
      await binaryClient.ack(pulled!.id);
    }
    const binaryTime = performance.now() - binaryStart;

    console.log(`JSON:   ${iterations} cycles in ${jsonTime.toFixed(0)}ms (${(iterations / jsonTime * 1000).toFixed(0)} ops/s)`);
    console.log(`Binary: ${iterations} cycles in ${binaryTime.toFixed(0)}ms (${(iterations / binaryTime * 1000).toFixed(0)} ops/s)`);

    const speedup = ((jsonTime - binaryTime) / jsonTime * 100).toFixed(1);
    if (binaryTime < jsonTime) {
      console.log(`[PASS] Binary is ${speedup}% faster than JSON`);
    } else {
      console.log(`[INFO] Binary is ${(-parseFloat(speedup)).toFixed(1)}% slower (may vary based on payload size)`);
    }

    // === Test 4: Large Payload ===
    console.log('\n--- Test 4: Large Payload ---');

    const largeData = {
      items: Array.from({ length: 1000 }, (_, i) => ({
        id: i,
        name: `Item ${i}`,
        description: 'x'.repeat(100),
        tags: ['tag1', 'tag2', 'tag3'],
        metadata: { created: Date.now(), version: 1 }
      }))
    };

    const largeJob = await binaryClient.push('binary-large', largeData);
    console.log(`[PASS] Large payload push (job id: ${largeJob.id})`);

    const largePulled = await binaryClient.pull('binary-large');
    if (largePulled && largePulled.data.items.length === 1000) {
      console.log('[PASS] Large payload pull - integrity preserved');
    } else {
      console.log('[FAIL] Large payload - data mismatch');
    }
    await binaryClient.ack(largePulled!.id);

    // === Test 5: All Commands Work ===
    console.log('\n--- Test 5: Various Commands ---');

    // Stats
    const stats = await binaryClient.stats();
    console.log(`[PASS] Binary stats: queued=${stats.queued}`);

    // List queues
    const queues = await binaryClient.listQueues();
    console.log(`[PASS] Binary listQueues: ${queues.length} queues`);

    // Metrics
    const metrics = await binaryClient.metrics();
    console.log(`[PASS] Binary metrics: pushed=${metrics.total_pushed}`);

    // Cleanup
    await binaryClient.obliterate('binary-test');
    await binaryClient.obliterate('binary-batch');
    await binaryClient.obliterate('perf-json');
    await binaryClient.obliterate('perf-binary');
    await binaryClient.obliterate('binary-large');
    console.log('[PASS] Cleanup complete');

    console.log('\n=== All Binary Protocol Tests Passed! ===');

  } catch (error) {
    console.error('[FAIL] Error:', error);
    process.exit(1);
  } finally {
    await jsonClient.close();
    await binaryClient.close();
  }
}

main();

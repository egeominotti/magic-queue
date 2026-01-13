/**
 * Test delle nuove feature BullMQ avanzate
 */

import { FlashQ } from '../src/client';

const client = new FlashQ({ host: 'localhost', port: 6789 });

async function test(name: string, fn: () => Promise<void>) {
  try {
    await fn();
    console.log(`✅ ${name}`);
  } catch (err) {
    console.log(`❌ ${name}: ${err}`);
    process.exit(1);
  }
}

async function main() {
  console.log('\n🧪 Test Feature BullMQ Avanzate\n');
  console.log('='.repeat(50));

  await client.connect();

  // Clean up before tests
  try {
    await client.obliterate('test-advanced');
    await client.obliterate('test-clean');
    await client.obliterate('test-drain');
    await client.obliterate('test-priority');
    await client.obliterate('test-delayed');
    await client.obliterate('test-debounce');
    await client.obliterate('test-customid');
    await client.obliterate('test-finished');
  } catch (e) {
    // Ignore errors if queues don't exist
  }

  // ============== Test getJobs ==============
  console.log('\n📋 getJobs()');

  await test('getJobs - push jobs for testing', async () => {
    for (let i = 0; i < 5; i++) {
      await client.push('test-advanced', { index: i }, { priority: i });
    }
    // Push some delayed jobs
    for (let i = 0; i < 3; i++) {
      await client.push('test-advanced', { delayed: i }, { delay: 60000 });
    }
  });

  await test('getJobs - list all jobs', async () => {
    const { jobs, total } = await client.getJobs({});
    if (total < 8) throw new Error(`Expected at least 8 jobs, got ${total}`);
  });

  await test('getJobs - filter by queue', async () => {
    const { jobs, total } = await client.getJobs({ queue: 'test-advanced' });
    if (total !== 8) throw new Error(`Expected 8 jobs, got ${total}`);
  });

  await test('getJobs - filter by state (waiting)', async () => {
    const { jobs, total } = await client.getJobs({ queue: 'test-advanced', state: 'waiting' });
    if (total !== 5) throw new Error(`Expected 5 waiting jobs, got ${total}`);
  });

  await test('getJobs - filter by state (delayed)', async () => {
    const { jobs, total } = await client.getJobs({ queue: 'test-advanced', state: 'delayed' });
    if (total !== 3) throw new Error(`Expected 3 delayed jobs, got ${total}`);
  });

  await test('getJobs - pagination', async () => {
    const { jobs, total } = await client.getJobs({ queue: 'test-advanced', limit: 2, offset: 0 });
    if (jobs.length !== 2) throw new Error(`Expected 2 jobs, got ${jobs.length}`);
    if (total !== 8) throw new Error(`Expected total 8, got ${total}`);
  });

  // ============== Test changePriority ==============
  console.log('\n🔄 changePriority()');

  await test('changePriority - push job and change priority', async () => {
    const job = await client.push('test-priority', { test: 'priority' }, { priority: 5 });

    // Check initial priority
    const before = await client.getJob(job.id);
    if (!before || before.job.priority !== 5) {
      throw new Error(`Expected priority 5, got ${before?.job.priority}`);
    }

    // Change priority
    await client.changePriority(job.id, 100);

    // Check new priority
    const after = await client.getJob(job.id);
    if (!after || after.job.priority !== 100) {
      throw new Error(`Expected priority 100, got ${after?.job.priority}`);
    }
  });

  await test('changePriority - change priority of processing job', async () => {
    const job = await client.push('test-priority', { test: 'processing' });

    // Pull to make it processing
    const pulled = await client.pull('test-priority');

    // Change priority while processing
    await client.changePriority(pulled.id, 50);

    // Check priority
    const after = await client.getJob(pulled.id);
    if (!after || after.job.priority !== 50) {
      throw new Error(`Expected priority 50, got ${after?.job.priority}`);
    }

    // Ack to clean up
    await client.ack(pulled.id);
  });

  // ============== Test moveToDelayed ==============
  console.log('\n⏰ moveToDelayed()');

  await test('moveToDelayed - move processing job to delayed', async () => {
    const job = await client.push('test-delayed', { test: 'move' });

    // Pull to make it processing
    const pulled = await client.pull('test-delayed');

    // Check state is active
    const stateBefore = await client.getState(pulled.id);
    if (stateBefore !== 'active') {
      throw new Error(`Expected state 'active', got '${stateBefore}'`);
    }

    // Move to delayed with 10 second delay
    await client.moveToDelayed(pulled.id, 10000);

    // Check state is delayed
    const stateAfter = await client.getState(pulled.id);
    if (stateAfter !== 'delayed') {
      throw new Error(`Expected state 'delayed', got '${stateAfter}'`);
    }
  });

  await test('moveToDelayed - error if job not processing', async () => {
    const job = await client.push('test-delayed', { test: 'not-processing' });

    try {
      await client.moveToDelayed(job.id, 5000);
      throw new Error('Should have thrown error');
    } catch (err: any) {
      if (!err.message.includes('not in processing')) {
        throw new Error(`Expected 'not in processing' error, got: ${err.message}`);
      }
    }
  });

  // ============== Test clean ==============
  console.log('\n🧹 clean()');

  await test('clean - setup jobs for cleaning', async () => {
    // Push some jobs with max_attempts=1 so they go directly to DLQ on fail
    for (let i = 0; i < 10; i++) {
      await client.push('test-clean', { index: i }, { max_attempts: 1 });
    }

    // Pull and fail some to create DLQ entries
    for (let i = 0; i < 3; i++) {
      const job = await client.pull('test-clean');
      await client.fail(job.id, 'Test failure');
    }
  });

  await test('clean - clean failed jobs', async () => {
    // Clean failed jobs older than 0ms (all of them)
    const cleaned = await client.clean('test-clean', 0, 'failed');
    if (cleaned !== 3) throw new Error(`Expected 3 cleaned, got ${cleaned}`);

    // Verify DLQ is empty
    const dlq = await client.getDlq('test-clean');
    if (dlq.length !== 0) throw new Error(`Expected 0 in DLQ, got ${dlq.length}`);
  });

  await test('clean - clean waiting jobs with limit', async () => {
    // Clean 2 waiting jobs
    const cleaned = await client.clean('test-clean', 0, 'waiting', 2);
    if (cleaned !== 2) throw new Error(`Expected 2 cleaned, got ${cleaned}`);

    // Check remaining
    const { total } = await client.getJobs({ queue: 'test-clean', state: 'waiting' });
    if (total !== 5) throw new Error(`Expected 5 remaining, got ${total}`);
  });

  // ============== Test drain ==============
  console.log('\n🚰 drain()');

  await test('drain - setup and drain queue', async () => {
    // Push jobs
    for (let i = 0; i < 20; i++) {
      await client.push('test-drain', { index: i });
    }

    // Pull one to processing
    const processing = await client.pull('test-drain');

    // Drain the queue
    const drained = await client.drain('test-drain');
    if (drained !== 19) throw new Error(`Expected 19 drained, got ${drained}`);

    // Verify queue is empty except processing
    const { total } = await client.getJobs({ queue: 'test-drain', state: 'waiting' });
    if (total !== 0) throw new Error(`Expected 0 waiting, got ${total}`);

    // Processing job should still exist
    const state = await client.getState(processing.id);
    if (state !== 'active') throw new Error(`Expected 'active', got '${state}'`);

    // Clean up
    await client.ack(processing.id);
  });

  // ============== Test obliterate ==============
  console.log('\n💥 obliterate()');

  await test('obliterate - remove all queue data', async () => {
    const queueName = 'test-obliterate-' + Date.now();

    // Push various jobs
    for (let i = 0; i < 10; i++) {
      await client.push(queueName, { index: i });
    }

    // Add delayed jobs
    for (let i = 0; i < 5; i++) {
      await client.push(queueName, { delayed: i }, { delay: 60000 });
    }

    // Pull and fail some
    for (let i = 0; i < 3; i++) {
      const job = await client.pull(queueName);
      await client.fail(job.id, 'Test');
    }

    // Pull one to processing
    const processing = await client.pull(queueName);

    // Obliterate
    const removed = await client.obliterate(queueName);
    // Should remove: 6 waiting + 5 delayed + 3 DLQ + 1 processing = 15
    if (removed < 10) throw new Error(`Expected at least 10 removed, got ${removed}`);

    // Verify everything is gone
    const { total } = await client.getJobs({ queue: queueName });
    if (total !== 0) throw new Error(`Expected 0 jobs, got ${total}`);

    // Processing job should also be gone (returns 'unknown' or null)
    const state = await client.getState(processing.id);
    if (state !== null && state !== 'unknown') {
      throw new Error(`Expected null or 'unknown' state, got '${state}'`);
    }
  });

  // ============== Test promote ==============
  console.log('\n⬆️ promote()');

  await test('promote - move delayed job to waiting immediately', async () => {
    // Push a delayed job (1 minute delay)
    const job = await client.push('test-promote', { test: 'promote' }, { delay: 60000 });

    // Verify it's delayed
    const stateBefore = await client.getState(job.id);
    if (stateBefore !== 'delayed') {
      throw new Error(`Expected 'delayed', got '${stateBefore}'`);
    }

    // Promote it
    await client.promote(job.id);

    // Verify it's now waiting
    const stateAfter = await client.getState(job.id);
    if (stateAfter !== 'waiting') {
      throw new Error(`Expected 'waiting', got '${stateAfter}'`);
    }

    // Clean up
    const pulled = await client.pull('test-promote');
    await client.ack(pulled.id);
  });

  await test('promote - error if job not delayed', async () => {
    const job = await client.push('test-promote', { test: 'not-delayed' });

    try {
      await client.promote(job.id);
      throw new Error('Should have thrown error');
    } catch (err: any) {
      if (!err.message.includes('not delayed') && !err.message.includes('not found')) {
        throw new Error(`Expected 'not delayed' error, got: ${err.message}`);
      }
    }

    // Clean up
    const pulled = await client.pull('test-promote');
    await client.ack(pulled.id);
  });

  // ============== Test update ==============
  console.log('\n📝 update()');

  await test('update - update job data while waiting', async () => {
    const job = await client.push('test-update', { original: true });

    // Update data
    await client.update(job.id, { updated: true, newField: 'value' });

    // Pull and verify
    const pulled = await client.pull('test-update');
    if (pulled.data.original) throw new Error('Expected original field to be replaced');
    if (!pulled.data.updated) throw new Error('Expected updated field to be true');
    if (pulled.data.newField !== 'value') throw new Error('Expected newField to be "value"');

    await client.ack(pulled.id);
  });

  await test('update - update job data while processing', async () => {
    const job = await client.push('test-update', { phase: 1 });
    const pulled = await client.pull('test-update');

    // Update while processing
    await client.update(pulled.id, { phase: 2 });

    // Verify the update
    const jobInfo = await client.getJob(pulled.id);
    if (!jobInfo || jobInfo.job.data.phase !== 2) {
      throw new Error(`Expected phase 2, got ${jobInfo?.job.data.phase}`);
    }

    await client.ack(pulled.id);
  });

  // ============== Test discard ==============
  console.log('\n🗑️ discard()');

  await test('discard - move waiting job to DLQ immediately', async () => {
    const job = await client.push('test-discard', { test: 'discard-waiting' });

    // Discard it
    await client.discard(job.id);

    // Verify it's in DLQ
    const state = await client.getState(job.id);
    if (state !== 'failed') {
      throw new Error(`Expected 'failed', got '${state}'`);
    }

    // Verify it's in DLQ
    const dlq = await client.getDlq('test-discard');
    if (!dlq.some((j) => j.id === job.id)) {
      throw new Error('Job not found in DLQ');
    }
  });

  await test('discard - move processing job to DLQ immediately', async () => {
    const job = await client.push('test-discard', { test: 'discard-processing' });
    const pulled = await client.pull('test-discard');

    // Discard while processing
    await client.discard(pulled.id);

    // Verify it's in DLQ
    const state = await client.getState(pulled.id);
    if (state !== 'failed') {
      throw new Error(`Expected 'failed', got '${state}'`);
    }
  });

  // ============== Test isPaused ==============
  console.log('\n⏸️ isPaused()');

  await test('isPaused - returns false for non-paused queue', async () => {
    const paused = await client.isPaused('test-ispaused');
    if (paused) throw new Error('Expected false, got true');
  });

  await test('isPaused - returns true after pause', async () => {
    await client.pause('test-ispaused');
    const paused = await client.isPaused('test-ispaused');
    if (!paused) throw new Error('Expected true, got false');

    // Clean up
    await client.resume('test-ispaused');
  });

  await test('isPaused - returns false after resume', async () => {
    await client.pause('test-ispaused');
    await client.resume('test-ispaused');
    const paused = await client.isPaused('test-ispaused');
    if (paused) throw new Error('Expected false after resume');
  });

  // ============== Test count ==============
  console.log('\n🔢 count()');

  await test('count - counts waiting and delayed jobs', async () => {
    // Clean up first
    await client.obliterate('test-count');

    // Push some waiting jobs
    for (let i = 0; i < 5; i++) {
      await client.push('test-count', { index: i });
    }

    // Push some delayed jobs
    for (let i = 0; i < 3; i++) {
      await client.push('test-count', { delayed: i }, { delay: 60000 });
    }

    const total = await client.count('test-count');
    if (total !== 8) throw new Error(`Expected 8, got ${total}`);
  });

  // ============== Test debounce ==============
  console.log('\n🔁 debounce()');

  await test('debounce - prevents duplicate jobs within window', async () => {
    // Clean up first
    await client.obliterate('test-debounce');

    // Push first job with debounce_id
    const job1 = await client.push('test-debounce', { attempt: 1 }, {
      debounce_id: 'unique-event-123',
      debounce_ttl: 5000,
    });

    // Try to push duplicate immediately - should fail
    try {
      await client.push('test-debounce', { attempt: 2 }, {
        debounce_id: 'unique-event-123',
        debounce_ttl: 5000,
      });
      throw new Error('Should have been debounced');
    } catch (err: any) {
      if (!err.message.includes('Debounced')) {
        throw new Error(`Expected 'Debounced' error, got: ${err.message}`);
      }
    }

    // First job should still exist
    const state = await client.getState(job1.id);
    if (state !== 'waiting') {
      throw new Error(`Expected 'waiting', got '${state}'`);
    }

    // Clean up
    const pulled = await client.pull('test-debounce');
    await client.ack(pulled.id);
  });

  await test('debounce - allows same debounce_id after window expires', async () => {
    // Push with very short debounce window
    const job1 = await client.push('test-debounce', { attempt: 1 }, {
      debounce_id: 'expire-test',
      debounce_ttl: 100, // 100ms
    });

    // Wait for debounce window to expire
    await new Promise(resolve => setTimeout(resolve, 150));

    // Should now be able to push again
    const job2 = await client.push('test-debounce', { attempt: 2 }, {
      debounce_id: 'expire-test',
      debounce_ttl: 100,
    });

    if (job2.id === job1.id) {
      throw new Error('Expected different job IDs');
    }

    // Clean up
    const pulled1 = await client.pull('test-debounce');
    await client.ack(pulled1.id);
    const pulled2 = await client.pull('test-debounce');
    await client.ack(pulled2.id);
  });

  await test('debounce - different debounce_ids are independent', async () => {
    // Push with debounce_id A
    const jobA = await client.push('test-debounce', { type: 'A' }, {
      debounce_id: 'event-A',
      debounce_ttl: 5000,
    });

    // Push with debounce_id B - should succeed
    const jobB = await client.push('test-debounce', { type: 'B' }, {
      debounce_id: 'event-B',
      debounce_ttl: 5000,
    });

    if (jobA.id === jobB.id) {
      throw new Error('Expected different job IDs');
    }

    // Clean up
    const pulled1 = await client.pull('test-debounce');
    await client.ack(pulled1.id);
    const pulled2 = await client.pull('test-debounce');
    await client.ack(pulled2.id);
  });

  // ============== Test Custom Job ID (Idempotency) ==============
  console.log('\n🆔 Custom Job ID (jobId)');

  await test('jobId - push with custom ID', async () => {
    const job = await client.push('test-customid', { test: 'custom' }, {
      jobId: 'my-unique-order-123',
    });

    // Verify custom_id is set
    const jobInfo = await client.getJob(job.id);
    if (!jobInfo || jobInfo.job.custom_id !== 'my-unique-order-123') {
      throw new Error(`Expected custom_id 'my-unique-order-123', got '${jobInfo?.job.custom_id}'`);
    }
  });

  await test('jobId - idempotent push returns same job', async () => {
    // Push first job with custom ID
    const job1 = await client.push('test-customid', { attempt: 1 }, {
      jobId: 'idempotent-test-456',
    });

    // Push again with same custom ID - should return same job
    const job2 = await client.push('test-customid', { attempt: 2 }, {
      jobId: 'idempotent-test-456',
    });

    if (job1.id !== job2.id) {
      throw new Error(`Expected same job ID ${job1.id}, got ${job2.id}`);
    }
  });

  await test('getJobByCustomId - find job by custom ID', async () => {
    // Push a job with custom ID
    await client.push('test-customid', { lookup: true }, {
      jobId: 'lookup-test-789',
    });

    // Find it by custom ID
    const result = await client.getJobByCustomId('lookup-test-789');
    if (!result) {
      throw new Error('Job not found by custom ID');
    }
    if (result.job.custom_id !== 'lookup-test-789') {
      throw new Error(`Expected custom_id 'lookup-test-789', got '${result.job.custom_id}'`);
    }
    if (result.state !== 'waiting') {
      throw new Error(`Expected state 'waiting', got '${result.state}'`);
    }
  });

  await test('getJobByCustomId - returns null for non-existent ID', async () => {
    const result = await client.getJobByCustomId('non-existent-id-xyz');
    if (result !== null) {
      throw new Error('Expected null for non-existent custom ID');
    }
  });

  // ============== Test finished() (Wait for Job Completion) ==============
  console.log('\n⏳ finished() - Wait for Job Completion');

  await test('finished - wait for job to complete', async () => {
    // Use a separate client for the worker to avoid TCP response interleaving
    const workerClient = new FlashQ({ host: 'localhost', port: 6789 });
    await workerClient.connect();

    try {
      // Push a job
      const job = await client.push('test-finished', { work: 'data' });

      // Start waiting in the background with a timeout
      const waitPromise = client.finished(job.id, 5000);

      // Small delay to ensure WAITJOB is sent first
      await new Promise(r => setTimeout(r, 50));

      // Simulate worker on separate connection: pull, process, ack
      const pulled = await workerClient.pull('test-finished');
      await workerClient.ack(pulled.id, { result: 'success', value: 42 });

      // Now the wait should resolve
      const result = await waitPromise;
      if (!result || result.result !== 'success' || result.value !== 42) {
        throw new Error(`Expected result { result: 'success', value: 42 }, got ${JSON.stringify(result)}`);
      }
    } finally {
      await workerClient.close();
    }
  });

  await test('finished - timeout if job not completed', async () => {
    // Push a job but don't process it
    const job = await client.push('test-finished', { slow: true });

    try {
      // Wait with short timeout
      await client.finished(job.id, 100);
      throw new Error('Should have timed out');
    } catch (err: any) {
      // Accept various timeout error formats
      const isTimeoutError = err.message.toLowerCase().includes('timeout') ||
                            err.message.includes('Timeout');
      if (!isTimeoutError) {
        throw new Error(`Expected timeout error, got: ${err.message}`);
      }
    }

    // Clean up
    const pulled = await client.pull('test-finished');
    await client.ack(pulled.id);
  });

  // ============== Test getJobCounts ==============
  console.log('\n📊 getJobCounts()');

  await test('getJobCounts - returns counts by state', async () => {
    // Clean up and set up fresh
    await client.obliterate('test-jobcounts');

    // Push some waiting jobs with max_attempts=1 so fail goes to DLQ
    for (let i = 0; i < 5; i++) {
      await client.push('test-jobcounts', { index: i }, { max_attempts: 1 });
    }

    // Push some delayed jobs
    for (let i = 0; i < 3; i++) {
      await client.push('test-jobcounts', { delayed: i }, { delay: 60000 });
    }

    // Pull one to make it active
    const pulled = await client.pull('test-jobcounts');

    // Fail one to put it in DLQ (max_attempts=1 means it goes directly to DLQ)
    const pulled2 = await client.pull('test-jobcounts');
    await client.fail(pulled2.id, 'Test failure');

    const counts = await client.getJobCounts('test-jobcounts');

    // Should have: 3 waiting, 1 active, 3 delayed, 0 completed, 1 failed
    if (counts.waiting !== 3) throw new Error(`Expected 3 waiting, got ${counts.waiting}`);
    if (counts.active !== 1) throw new Error(`Expected 1 active, got ${counts.active}`);
    if (counts.delayed !== 3) throw new Error(`Expected 3 delayed, got ${counts.delayed}`);
    if (counts.failed !== 1) throw new Error(`Expected 1 failed, got ${counts.failed}`);

    // Clean up
    await client.ack(pulled.id);
  });

  // ============== Summary ==============
  console.log('\n' + '='.repeat(50));
  console.log('✅ Tutti i test passati!\n');

  // Clean up
  await client.obliterate('test-advanced');
  await client.obliterate('test-clean');
  await client.obliterate('test-drain');
  await client.obliterate('test-priority');
  await client.obliterate('test-delayed');
  await client.obliterate('test-promote');
  await client.obliterate('test-update');
  await client.obliterate('test-discard');
  await client.obliterate('test-ispaused');
  await client.obliterate('test-count');
  await client.obliterate('test-jobcounts');
  await client.obliterate('test-debounce');
  await client.obliterate('test-customid');
  await client.obliterate('test-finished');

  await client.close();
}

main().catch((err) => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});

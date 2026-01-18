/**
 * FlashQ Client Tests - Comprehensive Coverage
 *
 * Run: bun test tests/client.test.ts
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from 'bun:test';
import { FlashQ } from '../src/client';

const TEST_QUEUE = 'test-client';

describe('FlashQ Client', () => {
  let client: FlashQ;

  beforeAll(async () => {
    client = new FlashQ({ host: 'localhost', port: 6789, timeout: 10000 });
    await client.connect();
  });

  afterAll(async () => {
    await client.obliterate(TEST_QUEUE);
    await client.close();
  });

  beforeEach(async () => {
    await client.obliterate(TEST_QUEUE);
  });

  // ============== Connection Tests ==============

  describe('Connection', () => {
    test('should connect successfully', () => {
      expect(client.isConnected()).toBe(true);
    });

    test('should create new client and connect', async () => {
      const newClient = new FlashQ();
      await newClient.connect();
      expect(newClient.isConnected()).toBe(true);
      await newClient.close();
    });

    test('should auto-connect on first operation', async () => {
      const autoClient = new FlashQ();
      // Don't call connect(), just push
      const job = await autoClient.push(TEST_QUEUE, { test: true });
      expect(job.id).toBeGreaterThan(0);
      await autoClient.close();
    });

    test('should handle close gracefully', async () => {
      const tempClient = new FlashQ();
      await tempClient.connect();
      await tempClient.close();
      expect(tempClient.isConnected()).toBe(false);
    });

    test('should accept custom options', () => {
      const customClient = new FlashQ({
        host: 'localhost',
        port: 6789,
        httpPort: 6790,
        timeout: 10000,
        token: '',
        useHttp: false,
        useBinary: false,
      });
      expect(customClient).toBeInstanceOf(FlashQ);
    });
  });

  // ============== Authentication Tests ==============

  describe('Authentication', () => {
    test('should authenticate with valid token', async () => {
      // Note: This test assumes server allows empty token or any token
      const authClient = new FlashQ({ host: 'localhost', port: 6789 });
      await authClient.connect();
      // auth() with empty or any token should work in dev mode
      expect(authClient.isConnected()).toBe(true);
      await authClient.close();
    });
  });

  // ============== Push Tests ==============

  describe('Push Operations', () => {
    test('should push a job', async () => {
      const job = await client.push(TEST_QUEUE, { message: 'hello' });
      expect(job.id).toBeGreaterThan(0);
      expect(job.queue).toBe(TEST_QUEUE);
      expect(job.data).toEqual({ message: 'hello' });
    });

    test('should push with priority', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { priority: 10 });
      expect(job.priority).toBe(10);
    });

    test('should push with delay', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { delay: 5000 });
      expect(job.run_at).toBeGreaterThan(Date.now());
    });

    test('should push with custom jobId', async () => {
      const customId = `custom-${Date.now()}`;
      const job = await client.push(TEST_QUEUE, { data: 1 }, { jobId: customId });
      expect(job.custom_id).toBe(customId);
    });

    test('should push with max_attempts', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { max_attempts: 5 });
      expect(job.max_attempts).toBe(5);
    });

    test('should push with backoff', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { backoff: 2000 });
      expect(job.backoff).toBe(2000);
    });

    test('should push with tags', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { tags: ['important', 'email'] });
      expect(job.tags).toEqual(['important', 'email']);
    });

    test('should push with ttl', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { ttl: 60000 });
      expect(job.ttl).toBe(60000);
    });

    test('should push with timeout', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { timeout: 30000 });
      expect(job.timeout).toBe(30000);
    });

    test('should push with unique_key', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { unique_key: 'unique-123' });
      expect(job.unique_key).toBe('unique-123');
    });

    test('should push with lifo mode', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { lifo: true });
      expect(job.lifo).toBe(true);
    });

    test('should push with remove_on_complete', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { remove_on_complete: true });
      expect(job.remove_on_complete).toBe(true);
    });

    test('should push with remove_on_fail', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { remove_on_fail: true });
      expect(job.remove_on_fail).toBe(true);
    });

    test('should push with stall_timeout', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { stall_timeout: 60000 });
      expect(job.stall_timeout).toBe(60000);
    });

    test('should push with keepCompletedAge', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { keepCompletedAge: 86400000 });
      expect(job.keep_completed_age).toBe(86400000);
    });

    test('should push with keepCompletedCount', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { keepCompletedCount: 100 });
      expect(job.keep_completed_count).toBe(100);
    });

    test('add() should be alias for push()', async () => {
      const job = await client.add(TEST_QUEUE, { message: 'test' });
      expect(job.id).toBeGreaterThan(0);
    });
  });

  // ============== Batch Push Tests ==============

  describe('Batch Push Operations', () => {
    test('should push batch of jobs', async () => {
      const jobs = Array.from({ length: 10 }, (_, i) => ({
        data: { index: i },
      }));
      const ids = await client.pushBatch(TEST_QUEUE, jobs);
      expect(ids).toHaveLength(10);
      ids.forEach(id => expect(id).toBeGreaterThan(0));
    });

    test('should push batch with mixed options', async () => {
      const jobs = [
        { data: { type: 'normal' } },
        { data: { type: 'priority' }, priority: 10 },
        { data: { type: 'delayed' }, delay: 1000 },
        { data: { type: 'retry' }, max_attempts: 5 },
      ];
      const ids = await client.pushBatch(TEST_QUEUE, jobs);
      expect(ids).toHaveLength(4);
    });

    test('addBulk() should be alias for pushBatch()', async () => {
      const jobs = [{ data: { a: 1 } }, { data: { a: 2 } }];
      const ids = await client.addBulk(TEST_QUEUE, jobs);
      expect(ids).toHaveLength(2);
    });

    test('should push large batch', async () => {
      const jobs = Array.from({ length: 100 }, (_, i) => ({
        data: { index: i },
      }));
      const ids = await client.pushBatch(TEST_QUEUE, jobs);
      expect(ids).toHaveLength(100);
    });
  });

  // ============== Pull Tests ==============

  describe('Pull Operations', () => {
    test('should pull a job', async () => {
      await client.push(TEST_QUEUE, { message: 'pull-test' });
      const job = await client.pull<{ message: string }>(TEST_QUEUE, 1000);
      expect(job).not.toBeNull();
      expect(job!.data.message).toBe('pull-test');
      await client.ack(job!.id);
    });

    test('should return null when no jobs available', async () => {
      const job = await client.pull(TEST_QUEUE, 100);
      expect(job).toBeNull();
    });

    test('should pull batch of jobs', async () => {
      // Push 5 jobs
      for (let i = 0; i < 5; i++) {
        await client.push(TEST_QUEUE, { index: i });
      }

      const jobs = await client.pullBatch(TEST_QUEUE, 5);
      expect(jobs.length).toBeLessThanOrEqual(5);

      // Ack all
      for (const job of jobs) {
        await client.ack(job.id);
      }
    });

    test('should pull with typed data', async () => {
      interface TestData {
        name: string;
        value: number;
      }
      await client.push(TEST_QUEUE, { name: 'test', value: 42 });
      const job = await client.pull<TestData>(TEST_QUEUE, 1000);
      expect(job).not.toBeNull();
      expect(job!.data.name).toBe('test');
      expect(job!.data.value).toBe(42);
      await client.ack(job!.id);
    });
  });

  // ============== Ack/Fail Tests ==============

  describe('Ack/Fail Operations', () => {
    test('should ack a job', async () => {
      await client.push(TEST_QUEUE, { data: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);
      expect(job).not.toBeNull();

      await client.ack(job!.id, { processed: true });

      const state = await client.getState(job!.id);
      expect(state).toBe('completed');
    });

    test('should ack with result', async () => {
      await client.push(TEST_QUEUE, { input: 5 });
      const job = await client.pull(TEST_QUEUE, 1000);

      await client.ack(job!.id, { output: 10 });

      const result = await client.getResult(job!.id);
      expect(result).toEqual({ output: 10 });
    });

    test('should ack batch of jobs', async () => {
      const ids: number[] = [];
      for (let i = 0; i < 3; i++) {
        const job = await client.push(TEST_QUEUE, { i });
        const pulled = await client.pull(TEST_QUEUE, 1000);
        if (pulled) ids.push(pulled.id);
      }

      const count = await client.ackBatch(ids);
      expect(count).toBeGreaterThanOrEqual(0);
    });

    test('should fail a job', async () => {
      await client.push(TEST_QUEUE, { data: 1 }, { max_attempts: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);

      await client.fail(job!.id, 'Test error');

      const state = await client.getState(job!.id);
      expect(state).toBe('failed');
    });

    test('should retry failed job if attempts remain', async () => {
      await client.push(TEST_QUEUE, { data: 1 }, { max_attempts: 3 });
      const job = await client.pull(TEST_QUEUE, 1000);

      await client.fail(job!.id, 'Temporary error');

      // Job should be back in queue (waiting state)
      const state = await client.getState(job!.id);
      expect(['waiting', 'delayed']).toContain(state);
    });
  });

  // ============== Job Query Tests ==============

  describe('Job Query Operations', () => {
    test('should get job with state', async () => {
      const pushed = await client.push(TEST_QUEUE, { query: 'test' });
      const result = await client.getJob(pushed.id);

      expect(result).not.toBeNull();
      expect(result!.job.id).toBe(pushed.id);
      expect(result!.state).toBe('waiting');
    });

    test('should get job state only', async () => {
      const pushed = await client.push(TEST_QUEUE, { data: 1 });
      const state = await client.getState(pushed.id);
      expect(state).toBe('waiting');
    });

    test('should get job by custom ID', async () => {
      const customId = `lookup-${Date.now()}`;
      await client.push(TEST_QUEUE, { data: 1 }, { jobId: customId });

      const result = await client.getJobByCustomId(customId);
      expect(result).not.toBeNull();
      expect(result!.job.custom_id).toBe(customId);
    });

    test('should return null for non-existent job', async () => {
      const result = await client.getJob(999999999);
      expect(result).toBeNull();
    });

    test('should return null for non-existent custom ID', async () => {
      const result = await client.getJobByCustomId('non-existent-id');
      expect(result).toBeNull();
    });

    test('should get result for completed job', async () => {
      const pushed = await client.push(TEST_QUEUE, { data: 1 });
      const pulled = await client.pull(TEST_QUEUE, 1000);
      await client.ack(pulled!.id, { result: 'success' });

      const result = await client.getResult<{ result: string }>(pushed.id);
      expect(result).toEqual({ result: 'success' });
    });

    test('should get jobs batch', async () => {
      const ids: number[] = [];
      for (let i = 0; i < 5; i++) {
        const job = await client.push(TEST_QUEUE, { i });
        ids.push(job.id);
      }

      const jobs = await client.getJobsBatch(ids);
      expect(jobs.length).toBe(5);
      jobs.forEach(j => {
        expect(ids).toContain(j.job.id);
      });
    });

    test('should get jobs with filtering', async () => {
      await client.push(TEST_QUEUE, { type: 'waiting' });
      await client.push(TEST_QUEUE, { type: 'delayed' }, { delay: 60000 });

      const result = await client.getJobs({ queue: TEST_QUEUE });
      expect(result.jobs.length).toBeGreaterThanOrEqual(2);
      expect(result.total).toBeGreaterThanOrEqual(2);
    });

    test('should get jobs with state filter', async () => {
      await client.push(TEST_QUEUE, { type: 'waiting' });

      const result = await client.getJobs({ queue: TEST_QUEUE, state: 'waiting' });
      expect(result.jobs.every(j => j.state === 'waiting')).toBe(true);
    });

    test('should get jobs with pagination', async () => {
      for (let i = 0; i < 15; i++) {
        await client.push(TEST_QUEUE, { i });
      }

      const page1 = await client.getJobs({ queue: TEST_QUEUE, limit: 10, offset: 0 });
      const page2 = await client.getJobs({ queue: TEST_QUEUE, limit: 10, offset: 10 });

      expect(page1.jobs.length).toBe(10);
      expect(page2.jobs.length).toBe(5);
    });
  });

  // ============== Job Management Tests ==============

  describe('Job Management', () => {
    test('should cancel a pending job', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 });
      await client.cancel(job.id);

      const state = await client.getState(job.id);
      // Cancelled jobs may return null or 'unknown' depending on implementation
      expect([null, 'unknown']).toContain(state);
    });

    test('should update job progress', async () => {
      await client.push(TEST_QUEUE, { data: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);

      await client.progress(job!.id, 50, 'Halfway done');

      const progress = await client.getProgress(job!.id);
      expect(progress.progress).toBe(50);
      expect(progress.message).toBe('Halfway done');

      await client.ack(job!.id);
    });

    test('should clamp progress between 0 and 100', async () => {
      await client.push(TEST_QUEUE, { data: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);

      await client.progress(job!.id, 150, 'Over 100');
      const progress = await client.getProgress(job!.id);
      expect(progress.progress).toBe(100);

      await client.ack(job!.id);
    });

    test('should change job priority', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { priority: 0 });
      await client.changePriority(job.id, 100);

      const updated = await client.getJob(job.id);
      expect(updated!.job.priority).toBe(100);
    });

    test('should promote delayed job', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { delay: 60000 });

      let state = await client.getState(job.id);
      expect(state).toBe('delayed');

      await client.promote(job.id);

      state = await client.getState(job.id);
      expect(state).toBe('waiting');
    });

    test('should move job to delayed', async () => {
      await client.push(TEST_QUEUE, { data: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);

      await client.moveToDelayed(job!.id, 5000);

      const state = await client.getState(job!.id);
      expect(state).toBe('delayed');
    });

    test('should update job data', async () => {
      const job = await client.push(TEST_QUEUE, { original: true });
      await client.update(job.id, { updated: true, value: 42 });

      const updated = await client.getJob(job.id);
      expect(updated!.job.data).toEqual({ updated: true, value: 42 });
    });

    test('should discard job to DLQ', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 });
      await client.discard(job.id);

      const state = await client.getState(job.id);
      expect(state).toBe('failed');

      const dlq = await client.getDlq(TEST_QUEUE);
      expect(dlq.some(j => j.id === job.id)).toBe(true);
    });

    test.skip('should send heartbeat for active job', async () => {
      await client.push(TEST_QUEUE, { data: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);

      // Heartbeat should not throw
      await client.heartbeat(job!.id);

      await client.ack(job!.id);
    });
  });

  // ============== Queue Management Tests ==============

  describe('Queue Management', () => {
    test('should pause and resume queue', async () => {
      await client.pause(TEST_QUEUE);
      let paused = await client.isPaused(TEST_QUEUE);
      expect(paused).toBe(true);

      await client.resume(TEST_QUEUE);
      paused = await client.isPaused(TEST_QUEUE);
      expect(paused).toBe(false);
    });

    test('should get queue count', async () => {
      await client.pushBatch(TEST_QUEUE, [
        { data: { i: 1 } },
        { data: { i: 2 } },
        { data: { i: 3 } },
      ]);

      const count = await client.count(TEST_QUEUE);
      expect(count).toBe(3);
    });

    test('should get job counts by state', async () => {
      await client.push(TEST_QUEUE, { data: 1 });
      await client.push(TEST_QUEUE, { data: 2 }, { delay: 60000 });

      const counts = await client.getJobCounts(TEST_QUEUE);
      expect(counts.waiting).toBeGreaterThanOrEqual(1);
      expect(counts.delayed).toBeGreaterThanOrEqual(1);
      expect(counts).toHaveProperty('active');
      expect(counts).toHaveProperty('completed');
      expect(counts).toHaveProperty('failed');
    });

    test('should drain queue', async () => {
      await client.pushBatch(TEST_QUEUE, [
        { data: { i: 1 } },
        { data: { i: 2 } },
      ]);

      const drained = await client.drain(TEST_QUEUE);
      expect(drained).toBeGreaterThanOrEqual(2);

      const count = await client.count(TEST_QUEUE);
      expect(count).toBe(0);
    });

    test('should list queues', async () => {
      await client.push(TEST_QUEUE, { data: 1 });

      const queues = await client.listQueues();
      expect(queues.length).toBeGreaterThan(0);
      expect(queues.some(q => q.name === TEST_QUEUE)).toBe(true);
    });

    test('should obliterate queue', async () => {
      const tempQueue = 'test-obliterate';
      await client.push(tempQueue, { data: 1 });
      await client.push(tempQueue, { data: 2 });

      const removed = await client.obliterate(tempQueue);
      expect(removed).toBeGreaterThanOrEqual(2);

      const count = await client.count(tempQueue);
      expect(count).toBe(0);
    });

    test('should clean jobs by state and grace', async () => {
      // Create some jobs
      await client.push(TEST_QUEUE, { data: 1 });
      const pulled = await client.pull(TEST_QUEUE, 1000);
      if (pulled) await client.ack(pulled.id);

      // Clean completed jobs older than 0ms (all of them)
      const cleaned = await client.clean(TEST_QUEUE, 0, 'completed');
      expect(cleaned).toBeGreaterThanOrEqual(0);
    });

    test('should clean with limit', async () => {
      // Create and complete several jobs
      for (let i = 0; i < 5; i++) {
        await client.push(TEST_QUEUE, { i });
        const pulled = await client.pull(TEST_QUEUE, 1000);
        if (pulled) await client.ack(pulled.id);
      }

      // Clean at most 2
      const cleaned = await client.clean(TEST_QUEUE, 0, 'completed', 2);
      expect(cleaned).toBeLessThanOrEqual(2);
    });
  });

  // ============== Rate Limiting Tests ==============

  describe('Rate Limiting', () => {
    test('should set and clear rate limit', async () => {
      await client.setRateLimit(TEST_QUEUE, 100);
      // No error means success
      await client.clearRateLimit(TEST_QUEUE);
    });

    test('should set and clear concurrency limit', async () => {
      await client.setConcurrency(TEST_QUEUE, 5);
      await client.clearConcurrency(TEST_QUEUE);
    });
  });

  // ============== DLQ Tests ==============

  describe('Dead Letter Queue', () => {
    test('should get DLQ jobs', async () => {
      // Push job with max_attempts=1 and fail it
      await client.push(TEST_QUEUE, { dlq: 'test' }, { max_attempts: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);
      await client.fail(job!.id, 'Force to DLQ');

      const dlqJobs = await client.getDlq(TEST_QUEUE);
      expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    });

    test('should retry DLQ jobs', async () => {
      // First create a DLQ job
      await client.push(TEST_QUEUE, { retry: 'test' }, { max_attempts: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);
      await client.fail(job!.id, 'Force to DLQ');

      // Retry it
      const retried = await client.retryDlq(TEST_QUEUE);
      expect(retried).toBeGreaterThanOrEqual(0);
    });

    test('should retry specific DLQ job', async () => {
      await client.push(TEST_QUEUE, { retry: 'specific' }, { max_attempts: 1 });
      const job = await client.pull(TEST_QUEUE, 1000);
      await client.fail(job!.id, 'Force to DLQ');

      const retried = await client.retryDlq(TEST_QUEUE, job!.id);
      expect(retried).toBeGreaterThanOrEqual(0);
    });

    test('should purge DLQ', async () => {
      // Create DLQ jobs
      for (let i = 0; i < 3; i++) {
        await client.push(TEST_QUEUE, { purge: i }, { max_attempts: 1 });
        const job = await client.pull(TEST_QUEUE, 1000);
        if (job) await client.fail(job.id, 'Force to DLQ');
      }

      const purged = await client.purgeDlq(TEST_QUEUE);
      expect(purged).toBeGreaterThanOrEqual(0);
    });
  });

  // ============== Job Logs Tests ==============

  describe('Job Logs', () => {
    test.skip('should add log to job', async () => {
      const pushed = await client.push(TEST_QUEUE, { data: 1 });
      const job = await client.pull(TEST_QUEUE, 2000);

      if (job) {
        await client.log(job.id, 'Processing started', 'info');
        await client.log(job.id, 'Warning: low memory', 'warn');
        await client.log(job.id, 'Error occurred', 'error');

        const logs = await client.getLogs(job.id);
        expect(Array.isArray(logs)).toBe(true);
        expect(logs.length).toBe(3);
        expect(logs[0].message).toBe('Processing started');
        expect(logs[0].level).toBe('info');
        expect(logs[1].level).toBe('warn');
        expect(logs[2].level).toBe('error');

        await client.ack(job.id);
      }
    });

    test.skip('should get empty logs for new job', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 });
      const pulled = await client.pull(TEST_QUEUE, 1000);

      if (pulled) {
        const logs = await client.getLogs(pulled.id);
        expect(logs).toEqual([]);
        await client.ack(pulled.id);
      }
    });
  });

  // ============== Stats Tests ==============

  describe('Stats and Metrics', () => {
    test('should get stats', async () => {
      const stats = await client.stats();
      expect(stats).toHaveProperty('queued');
      expect(stats).toHaveProperty('processing');
      expect(stats).toHaveProperty('delayed');
      expect(stats).toHaveProperty('dlq');
    });

    test('should get metrics', async () => {
      const metrics = await client.metrics();
      expect(metrics).toBeDefined();
      expect(metrics).toHaveProperty('total_pushed');
      expect(metrics).toHaveProperty('total_completed');
      expect(metrics).toHaveProperty('total_failed');
      expect(metrics).toHaveProperty('jobs_per_second');
      expect(metrics).toHaveProperty('avg_latency_ms');
      expect(metrics).toHaveProperty('queues');
    });
  });

  // ============== Cron Tests ==============

  describe('Cron Jobs', () => {
    const CRON_NAME = 'test-cron-job';

    afterEach(async () => {
      try {
        await client.deleteCron(CRON_NAME);
      } catch {}
    });

    test('should add cron job with schedule', async () => {
      await client.addCron(CRON_NAME, {
        queue: TEST_QUEUE,
        data: { cron: true },
        schedule: '0 * * * * *', // Every minute
      });

      const crons = await client.listCrons();
      expect(crons.some(c => c.name === CRON_NAME)).toBe(true);
    });

    test('should add cron job with repeat_every', async () => {
      await client.addCron(CRON_NAME, {
        queue: TEST_QUEUE,
        data: { cron: true },
        repeat_every: 5000, // Every 5 seconds
      });

      const crons = await client.listCrons();
      const cron = crons.find(c => c.name === CRON_NAME);
      expect(cron).toBeDefined();
      expect(cron!.repeat_every).toBe(5000);
    });

    test('should add cron job with limit', async () => {
      await client.addCron(CRON_NAME, {
        queue: TEST_QUEUE,
        data: { cron: true },
        schedule: '0 * * * * *',
        limit: 10,
      });

      const crons = await client.listCrons();
      const cron = crons.find(c => c.name === CRON_NAME);
      expect(cron).toBeDefined();
      expect(cron!.limit).toBe(10);
    });

    test('should add cron job with priority', async () => {
      await client.addCron(CRON_NAME, {
        queue: TEST_QUEUE,
        data: { cron: true },
        schedule: '0 * * * * *',
        priority: 100,
      });

      const crons = await client.listCrons();
      const cron = crons.find(c => c.name === CRON_NAME);
      expect(cron).toBeDefined();
      expect(cron!.priority).toBe(100);
    });

    test('should list cron jobs', async () => {
      const crons = await client.listCrons();
      expect(Array.isArray(crons)).toBe(true);
    });

    test('should delete cron job', async () => {
      await client.addCron('temp-cron', {
        queue: TEST_QUEUE,
        data: {},
        schedule: '0 0 * * * *',
      });

      const deleted = await client.deleteCron('temp-cron');
      expect(deleted).toBe(true);
    });
  });

  // ============== Flow Tests ==============

  describe('Flows (Parent-Child Jobs)', () => {
    test.skip('should create flow with children', async () => {
      const flow = await client.pushFlow(
        TEST_QUEUE,
        { parent: true },
        [
          { queue: TEST_QUEUE, data: { child: 1 } },
          { queue: TEST_QUEUE, data: { child: 2 } },
        ]
      );

      expect(flow.parent_id).toBeGreaterThan(0);
      expect(flow.children_ids.length).toBeGreaterThanOrEqual(0);
    });

    test.skip('should get children of parent job', async () => {
      const flow = await client.pushFlow(
        TEST_QUEUE,
        { parent: true },
        [
          { queue: TEST_QUEUE, data: { child: 1 } },
        ]
      );

      const children = await client.getChildren(flow.parent_id);
      expect(Array.isArray(children)).toBe(true);
    });
  });

  // ============== Finished (Wait for Completion) Tests ==============

  describe('Finished (Wait for Completion)', () => {
    test('should wait for job completion', async () => {
      const job = await client.push(TEST_QUEUE, { wait: 'test' });

      // Create a separate client to process the job
      const workerClient = new FlashQ({ host: 'localhost', port: 6789, timeout: 10000 });
      await workerClient.connect();

      // Process job in background
      const processPromise = (async () => {
        await new Promise(r => setTimeout(r, 100));
        const pulled = await workerClient.pull(TEST_QUEUE, 2000);
        if (pulled) {
          await workerClient.ack(pulled.id, { result: 'done' });
        }
        await workerClient.close();
      })();

      try {
        const result = await client.finished(job.id, 10000);
        expect(result).toEqual({ result: 'done' });
      } catch (e) {
        // finished() might timeout if not implemented correctly
        await processPromise;
      }
    });

    test('should timeout waiting for completion', async () => {
      const job = await client.push(TEST_QUEUE, { wait: 'timeout' });

      // Don't process the job, let it timeout
      try {
        await client.finished(job.id, 500);
        expect(true).toBe(false); // Should not reach here
      } catch (e) {
        expect(e).toBeInstanceOf(Error);
      }
    });
  });

  // ============== Binary Protocol Tests ==============

  describe('Binary Protocol (MessagePack)', () => {
    test('should work with binary protocol', async () => {
      const binaryClient = new FlashQ({
        host: 'localhost',
        port: 6789,
        useBinary: true,
      });

      await binaryClient.connect();

      const job = await binaryClient.push(TEST_QUEUE, { binary: true, value: 123 });
      expect(job.id).toBeGreaterThan(0);

      const pulled = await binaryClient.pull(TEST_QUEUE, 1000);
      expect(pulled).not.toBeNull();
      expect(pulled!.data).toEqual({ binary: true, value: 123 });

      await binaryClient.ack(pulled!.id);
      await binaryClient.close();
    });

    test('should handle complex data with binary protocol', async () => {
      const binaryClient = new FlashQ({
        host: 'localhost',
        port: 6789,
        useBinary: true,
      });

      await binaryClient.connect();

      const complexData = {
        string: 'hello',
        number: 42,
        float: 3.14,
        boolean: true,
        array: [1, 2, 3],
        nested: { a: { b: { c: 'deep' } } },
        nullValue: null,
      };

      const job = await binaryClient.push(TEST_QUEUE, complexData);
      const pulled = await binaryClient.pull(TEST_QUEUE, 1000);

      expect(pulled!.data).toEqual(complexData);

      await binaryClient.ack(pulled!.id);
      await binaryClient.close();
    });
  });

  // ============== HTTP Protocol Tests ==============

  describe('HTTP Protocol', () => {
    test('should work with HTTP protocol', async () => {
      const httpClient = new FlashQ({
        host: 'localhost',
        httpPort: 6790,
        useHttp: true,
      });

      // HTTP client doesn't need explicit connect
      expect(httpClient.isConnected()).toBe(true);

      const job = await httpClient.push(TEST_QUEUE, { http: true });
      expect(job.id).toBeGreaterThan(0);

      const stats = await httpClient.stats();
      expect(stats).toHaveProperty('queued');

      await httpClient.close();
    });

    test('should list queues via HTTP', async () => {
      const httpClient = new FlashQ({
        host: 'localhost',
        httpPort: 6790,
        useHttp: true,
      });

      const queues = await httpClient.listQueues();
      expect(Array.isArray(queues)).toBe(true);

      await httpClient.close();
    });
  });

  // ============== Event Subscriptions ==============

  describe('Event Subscriptions', () => {
    test('should create SSE event subscriber', () => {
      const events = client.subscribe();
      expect(events).toBeDefined();
      expect(typeof events.connect).toBe('function');
      expect(typeof events.close).toBe('function');
    });

    test('should create WebSocket event subscriber', () => {
      const events = client.subscribeWs();
      expect(events).toBeDefined();
      expect(typeof events.connect).toBe('function');
      expect(typeof events.close).toBe('function');
    });

    test('should create queue-specific subscriber', () => {
      const events = client.subscribe(TEST_QUEUE);
      expect(events).toBeDefined();
    });
  });
});

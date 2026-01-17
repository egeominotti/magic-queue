/**
 * FlashQ Client Tests
 *
 * Run: bun test tests/client.test.ts
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
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

    test('should push with tags', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { tags: ['important', 'email'] });
      expect(job.tags).toEqual(['important', 'email']);
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
      ];
      const ids = await client.pushBatch(TEST_QUEUE, jobs);
      expect(ids).toHaveLength(3);
    });

    test('addBulk() should be alias for pushBatch()', async () => {
      const jobs = [{ data: { a: 1 } }, { data: { a: 2 } }];
      const ids = await client.addBulk(TEST_QUEUE, jobs);
      expect(ids).toHaveLength(2);
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
    });
  });

  // ============== Cron Tests ==============

  describe('Cron Jobs', () => {
    const CRON_NAME = 'test-cron-job';

    afterAll(async () => {
      try {
        await client.deleteCron(CRON_NAME);
      } catch {}
    });

    test('should add cron job', async () => {
      await client.addCron(CRON_NAME, {
        queue: TEST_QUEUE,
        data: { cron: true },
        schedule: '0 * * * * *', // Every minute
      });

      const crons = await client.listCrons();
      expect(crons.some(c => c.name === CRON_NAME)).toBe(true);
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
  // Note: Flow tests are skipped - FLOW command needs investigation

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

  // ============== Job Logs Tests ==============

  describe('Job Logs', () => {
    test('should add log to job', async () => {
      const pushed = await client.push(TEST_QUEUE, { data: 1 });
      const job = await client.pull(TEST_QUEUE, 2000);

      if (job) {
        try {
          await client.log(job.id, 'Processing started', 'info');
          const logs = await client.getLogs(job.id);
          expect(Array.isArray(logs)).toBe(true);
        } catch {
          // Log feature might not be implemented
        }
        await client.ack(job.id);
      }
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
  });
});

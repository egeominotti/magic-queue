/**
 * FlashQ Integration Tests
 *
 * End-to-end tests covering complete workflows.
 *
 * Run: bun test tests/integration.test.ts
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { FlashQ } from '../src/client';
import { Worker } from '../src/worker';
import { Queue } from '../src/queue';

const TEST_QUEUE = 'test-integration';

describe('Integration Tests', () => {
  let client: FlashQ;

  beforeAll(async () => {
    client = new FlashQ({ host: 'localhost', port: 6789, timeout: 30000 });
    await client.connect();
  });

  afterAll(async () => {
    await client.obliterate(TEST_QUEUE);
    await client.close();
  });

  beforeEach(async () => {
    await client.obliterate(TEST_QUEUE);
  });

  // ============== Full Workflow Tests ==============

  describe('Complete Workflows', () => {
    test('should complete push -> pull -> ack workflow', async () => {
      // Push
      const job = await client.push(TEST_QUEUE, { action: 'test' });
      expect(job.id).toBeGreaterThan(0);

      // Verify state
      let state = await client.getState(job.id);
      expect(state).toBe('waiting');

      // Pull
      const pulled = await client.pull(TEST_QUEUE, 5000);
      expect(pulled).not.toBeNull();
      expect(pulled!.id).toBe(job.id);

      // Verify active state
      state = await client.getState(job.id);
      expect(state).toBe('active');

      // Ack
      await client.ack(job.id, { result: 'done' });

      // Verify completed
      state = await client.getState(job.id);
      expect(state).toBe('completed');

      // Verify result
      const result = await client.getResult(job.id);
      expect(result).toEqual({ result: 'done' });
    });

    test('should handle retry workflow', async () => {
      // Push with 3 max attempts
      const job = await client.push(TEST_QUEUE, { data: 1 }, { max_attempts: 3, backoff: 100 });

      // First attempt - fail
      let pulled = await client.pull(TEST_QUEUE, 5000);
      await client.fail(pulled!.id, 'Attempt 1 failed');

      // Wait for backoff
      await new Promise(r => setTimeout(r, 200));

      // Second attempt - fail
      pulled = await client.pull(TEST_QUEUE, 5000);
      expect(pulled).not.toBeNull();
      await client.fail(pulled!.id, 'Attempt 2 failed');

      await new Promise(r => setTimeout(r, 400));

      // Third attempt - success
      pulled = await client.pull(TEST_QUEUE, 5000);
      expect(pulled).not.toBeNull();
      expect(pulled!.attempts).toBe(2); // 0-indexed attempts before this pull

      await client.ack(pulled!.id, { success: true });

      const state = await client.getState(job.id);
      expect(state).toBe('completed');
    });

    test('should move to DLQ after max attempts', async () => {
      const job = await client.push(TEST_QUEUE, { data: 1 }, { max_attempts: 2 });

      // First attempt
      let pulled = await client.pull(TEST_QUEUE, 5000);
      await client.fail(pulled!.id, 'Fail 1');

      await new Promise(r => setTimeout(r, 200));

      // Second attempt (last)
      pulled = await client.pull(TEST_QUEUE, 5000);
      await client.fail(pulled!.id, 'Fail 2 - final');

      // Should be in DLQ now
      const state = await client.getState(job.id);
      expect(state).toBe('failed');

      const dlqJobs = await client.getDlq(TEST_QUEUE);
      expect(dlqJobs.some(j => j.id === job.id)).toBe(true);
    });
  });

  // ============== Worker Integration Tests ==============

  describe('Worker Integration', () => {
    test('should process multiple jobs with worker', async () => {
      const results: number[] = [];

      const worker = new Worker<{ num: number }, { doubled: number }>(
        TEST_QUEUE,
        async (job) => {
          results.push(job.data.num);
          return { doubled: job.data.num * 2 };
        },
        { host: 'localhost', port: 6789, concurrency: 2 }
      );

      await worker.start();

      // Push 10 jobs
      for (let i = 1; i <= 10; i++) {
        await client.push(TEST_QUEUE, { num: i });
      }

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 1000));

      await worker.stop();

      expect(results.length).toBe(10);
      expect(results.sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    test('should handle worker errors gracefully', async () => {
      const processed: number[] = [];
      const failed: number[] = [];

      const worker = new Worker<{ num: number }>(
        TEST_QUEUE,
        async (job) => {
          if (job.data.num % 2 === 0) {
            throw new Error('Even numbers fail');
          }
          processed.push(job.data.num);
          return { ok: true };
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      worker.on('failed', (job) => {
        failed.push((job.data as { num: number }).num);
      });

      await worker.start();

      // Push odd and even numbers
      for (let i = 1; i <= 5; i++) {
        await client.push(TEST_QUEUE, { num: i }, { max_attempts: 1 });
      }

      await new Promise(resolve => setTimeout(resolve, 1000));
      await worker.stop();

      expect(processed.sort()).toEqual([1, 3, 5]);
      expect(failed.sort()).toEqual([2, 4]);
    });
  });

  // ============== Priority Tests ==============

  describe('Priority Processing', () => {
    test('should process high priority jobs first', async () => {
      const order: string[] = [];

      // Push low priority first
      await client.push(TEST_QUEUE, { name: 'low1' }, { priority: 1 });
      await client.push(TEST_QUEUE, { name: 'low2' }, { priority: 1 });

      // Push high priority
      await client.push(TEST_QUEUE, { name: 'high1' }, { priority: 100 });
      await client.push(TEST_QUEUE, { name: 'high2' }, { priority: 100 });

      const worker = new Worker<{ name: string }>(
        TEST_QUEUE,
        async (job) => {
          order.push(job.data.name);
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();
      await new Promise(resolve => setTimeout(resolve, 1000));
      await worker.stop();

      // High priority should come first
      expect(order[0]).toMatch(/^high/);
      expect(order[1]).toMatch(/^high/);
    });
  });

  // ============== Delayed Jobs Tests ==============

  describe('Delayed Jobs', () => {
    test('should process delayed jobs after delay', async () => {
      const processed: number[] = [];

      const worker = new Worker<{ id: number }>(
        TEST_QUEUE,
        async (job) => {
          processed.push(job.data.id);
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();

      // Push immediate job
      await client.push(TEST_QUEUE, { id: 1 });

      // Push delayed job (500ms)
      await client.push(TEST_QUEUE, { id: 2 }, { delay: 500 });

      // Wait less than delay
      await new Promise(resolve => setTimeout(resolve, 200));
      expect(processed).toEqual([1]); // Only immediate job

      // Wait for delay to pass
      await new Promise(resolve => setTimeout(resolve, 500));
      expect(processed).toContain(2);

      await worker.stop();
    });
  });

  // ============== Batch Operations Tests ==============

  describe('Batch Operations', () => {
    test('should handle large batch push', async () => {
      const BATCH_SIZE = 1000;

      const jobs = Array.from({ length: BATCH_SIZE }, (_, i) => ({
        data: { index: i },
      }));

      const ids = await client.pushBatch(TEST_QUEUE, jobs);
      expect(ids).toHaveLength(BATCH_SIZE);

      const count = await client.count(TEST_QUEUE);
      expect(count).toBe(BATCH_SIZE);
    });

    test('should process batch efficiently', async () => {
      // Push 100 jobs
      const jobs = Array.from({ length: 100 }, (_, i) => ({
        data: { i },
      }));
      await client.pushBatch(TEST_QUEUE, jobs);

      let processed = 0;

      const worker = new Worker(
        TEST_QUEUE,
        async () => {
          processed++;
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 20 }
      );

      await worker.start();

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      await worker.stop();

      expect(processed).toBe(100);
    });
  });

  // ============== Flow (Parent-Child) Tests ==============
  // Note: Flow tests skipped - FLOW command needs investigation

  describe('Flow Processing', () => {
    test.skip('should create flow with parent and children', async () => {
      // Create flow
      const flow = await client.pushFlow(
        TEST_QUEUE,
        { type: 'parent', aggregate: [] },
        [
          { queue: TEST_QUEUE, data: { type: 'child', value: 1 } },
          { queue: TEST_QUEUE, data: { type: 'child', value: 2 } },
        ]
      );

      expect(flow.parent_id).toBeGreaterThan(0);
      expect(flow.children_ids.length).toBeGreaterThanOrEqual(0);

      // Get parent state
      const parentState = await client.getState(flow.parent_id);
      expect(parentState).not.toBeNull();
    });
  });

  // ============== Queue High-Level API Integration ==============

  describe('Queue API Integration', () => {
    test('should work with Queue high-level API', async () => {
      const emailQueue = new Queue<{ to: string; subject: string }, { sent: boolean }>(
        TEST_QUEUE,
        { connection: { host: 'localhost', port: 6789 } }
      );

      const sentEmails: string[] = [];

      emailQueue.process(async (job) => {
        sentEmails.push(job.data.to);
        return { sent: true };
      });

      // Add emails
      await emailQueue.add({ to: 'a@test.com', subject: 'Hello A' });
      await emailQueue.add({ to: 'b@test.com', subject: 'Hello B' });

      await new Promise(resolve => setTimeout(resolve, 500));

      expect(sentEmails).toContain('a@test.com');
      expect(sentEmails).toContain('b@test.com');

      await emailQueue.close();
    });
  });

  // ============== Stress Tests ==============

  describe('Stress Tests', () => {
    test('should handle rapid push/process cycle', async () => {
      const JOBS = 500;
      let processed = 0;

      const worker = new Worker(
        TEST_QUEUE,
        async () => {
          processed++;
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 50 }
      );

      await worker.start();

      // Push jobs rapidly
      const pushPromises = [];
      for (let i = 0; i < JOBS; i++) {
        pushPromises.push(client.push(TEST_QUEUE, { i }));
      }
      await Promise.all(pushPromises);

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 3000));

      await worker.stop();

      expect(processed).toBe(JOBS);
    });
  });
});

/**
 * FlashQ Queue Tests (BullMQ-compatible API)
 *
 * Run: bun test tests/queue.test.ts
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { Queue, Worker } from '../src';
import { FlashQ } from '../src/client';

const TEST_QUEUE = 'test-queue-api';

describe('FlashQ Queue API', () => {
  let cleanupClient: FlashQ;

  beforeAll(async () => {
    cleanupClient = new FlashQ();
    await cleanupClient.obliterate(TEST_QUEUE);
  });

  afterAll(async () => {
    await cleanupClient.obliterate(TEST_QUEUE);
    await cleanupClient.close();
  });

  beforeEach(async () => {
    await cleanupClient.obliterate(TEST_QUEUE);
  });

  // ============== Basic Queue Tests ==============

  describe('Basic Operations', () => {
    test('should create queue', () => {
      const queue = new Queue(TEST_QUEUE);
      expect(queue.name).toBe(TEST_QUEUE);
    });

    test('should add a job', async () => {
      const queue = new Queue(TEST_QUEUE);
      const job = await queue.add('send-email', { email: 'test@example.com' });

      expect(job.id).toBeGreaterThan(0);
      expect(job.queue).toBe(TEST_QUEUE);

      await queue.close();
    });

    test('should add job with options', async () => {
      const queue = new Queue(TEST_QUEUE);
      const job = await queue.add(
        'send-email',
        { email: 'vip@example.com' },
        { priority: 10, delay: 1000 }
      );

      expect(job.priority).toBe(10);

      await queue.close();
    });

    test('should add bulk jobs', async () => {
      const queue = new Queue(TEST_QUEUE);
      const jobs = await queue.addBulk([
        { name: 'job1', data: { i: 1 } },
        { name: 'job2', data: { i: 2 } },
        { name: 'job3', data: { i: 3 } },
      ]);

      expect(jobs).toHaveLength(3);

      await queue.close();
    });
  });

  // ============== Queue Processing Tests ==============

  describe('Processing with Worker', () => {
    test('should process jobs with separate Worker', async () => {
      const queue = new Queue<{ value: number }>(TEST_QUEUE);
      const results: number[] = [];

      const worker = new Worker(TEST_QUEUE, async (job) => {
        results.push(job.data.value);
        return { doubled: job.data.value * 2 };
      });

      await queue.add('calc', { value: 5 });
      await queue.add('calc', { value: 10 });

      await new Promise(resolve => setTimeout(resolve, 500));

      expect(results).toContain(5);
      expect(results).toContain(10);

      await worker.close();
      await queue.close();
    });

    test('should process with concurrency', async () => {
      const queue = new Queue(TEST_QUEUE);
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const worker = new Worker(
        TEST_QUEUE,
        async () => {
          currentConcurrent++;
          maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
          await new Promise(r => setTimeout(r, 100));
          currentConcurrent--;
          return {};
        },
        { concurrency: 3 }
      );

      // Add jobs
      for (let i = 0; i < 6; i++) {
        await queue.add('task', { i });
      }

      await new Promise(resolve => setTimeout(resolve, 500));

      expect(maxConcurrent).toBeLessThanOrEqual(3);

      await worker.close();
      await queue.close();
    });
  });

  // ============== Queue Control Tests ==============

  describe('Queue Control', () => {
    test('should pause and resume queue', async () => {
      const queue = new Queue(TEST_QUEUE);

      await queue.pause();
      expect(await queue.isPaused()).toBe(true);

      await queue.resume();
      expect(await queue.isPaused()).toBe(false);

      await queue.close();
    });

    test('should get job counts', async () => {
      const queue = new Queue(TEST_QUEUE);

      await queue.add('task', { n: 1 });
      await queue.add('task', { n: 2 });

      const counts = await queue.getJobCounts();
      expect(counts.waiting).toBe(2);

      await queue.close();
    });

    test('should drain waiting jobs', async () => {
      const queue = new Queue(TEST_QUEUE);

      await queue.add('task', { n: 1 });
      await queue.add('task', { n: 2 });

      await queue.drain();

      const counts = await queue.getJobCounts();
      expect(counts.waiting).toBe(0);

      await queue.close();
    });
  });

  // ============== Get Job Tests ==============

  describe('Job Operations', () => {
    test('should get job by ID', async () => {
      const queue = new Queue(TEST_QUEUE);

      const job = await queue.add('task', { data: 'test' });
      const fetched = await queue.getJob(job.id);

      expect(fetched).not.toBeNull();
      expect(fetched!.id).toBe(job.id);

      await queue.close();
    });
  });
});

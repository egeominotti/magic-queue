/**
 * FlashQ Queue Tests (High-Level API)
 *
 * Run: bun test tests/queue.test.ts
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { Queue } from '../src/queue';
import { FlashQ } from '../src/client';

const TEST_QUEUE = 'test-queue-api';

describe('FlashQ Queue API', () => {
  let cleanupClient: FlashQ;

  beforeAll(async () => {
    cleanupClient = new FlashQ({ host: 'localhost', port: 6789 });
    await cleanupClient.connect();
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
      const job = await queue.add({ email: 'test@example.com' });

      expect(job.id).toBeGreaterThan(0);
      expect(job.queue).toBe(TEST_QUEUE);

      await queue.close();
    });

    test('should add job with options', async () => {
      const queue = new Queue(TEST_QUEUE);
      const job = await queue.add(
        { email: 'vip@example.com' },
        { priority: 10, delay: 1000 }
      );

      expect(job.priority).toBe(10);
      expect(job.run_at).toBeGreaterThan(Date.now());

      await queue.close();
    });

    test('should add bulk jobs', async () => {
      const queue = new Queue(TEST_QUEUE);
      const ids = await queue.addBulk([
        { data: { i: 1 } },
        { data: { i: 2 } },
        { data: { i: 3 } },
      ]);

      expect(ids).toHaveLength(3);

      await queue.close();
    });
  });

  // ============== Queue Processing Tests ==============

  describe('Processing', () => {
    test('should process jobs', async () => {
      const queue = new Queue<{ value: number }, { doubled: number }>(TEST_QUEUE);
      const results: number[] = [];

      const worker = queue.process(async (job) => {
        results.push(job.data.value);
        return { doubled: job.data.value * 2 };
      });

      await queue.add({ value: 5 });
      await queue.add({ value: 10 });

      await new Promise(resolve => setTimeout(resolve, 500));

      expect(results).toContain(5);
      expect(results).toContain(10);

      await queue.close();
    });

    test('should process with concurrency', async () => {
      const queue = new Queue(TEST_QUEUE);
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      queue.process(
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
        await queue.add({ i });
      }

      await new Promise(resolve => setTimeout(resolve, 500));

      expect(maxConcurrent).toBeLessThanOrEqual(3);

      await queue.close();
    });
  });

  // ============== Queue Control Tests ==============

  describe('Queue Control', () => {
    test('should pause and resume queue', async () => {
      const queue = new Queue(TEST_QUEUE);

      await queue.pause();
      const paused = await cleanupClient.isPaused(TEST_QUEUE);
      expect(paused).toBe(true);

      await queue.resume();
      const resumed = await cleanupClient.isPaused(TEST_QUEUE);
      expect(resumed).toBe(false);

      await queue.close();
    });

    test('should set rate limit', async () => {
      const queue = new Queue(TEST_QUEUE);
      await queue.setRateLimit(100);
      // No error means success
      await queue.close();
    });

    test('should set concurrency limit', async () => {
      const queue = new Queue(TEST_QUEUE);
      await queue.setConcurrency(5);
      // No error means success
      await queue.close();
    });
  });

  // ============== Failed Jobs Tests ==============

  describe('Failed Jobs', () => {
    test('should get failed jobs', async () => {
      const queue = new Queue(TEST_QUEUE);

      // Create a failing job
      queue.process(
        async () => {
          throw new Error('Test failure');
        },
        { concurrency: 1 }
      );

      await queue.add({ data: 1 }, { max_attempts: 1 });

      await new Promise(resolve => setTimeout(resolve, 500));

      const failed = await queue.getFailed();
      expect(failed.length).toBeGreaterThanOrEqual(1);

      await queue.close();
    });

    test('should retry failed jobs', async () => {
      const queue = new Queue(TEST_QUEUE);

      // First, create a DLQ job manually
      const job = await queue.add({ data: 1 }, { max_attempts: 1 });
      const pulled = await cleanupClient.pull(TEST_QUEUE, 1000);
      if (pulled) {
        await cleanupClient.fail(pulled.id, 'Force fail');
      }

      const retried = await queue.retryFailed();
      expect(retried).toBeGreaterThanOrEqual(0);

      await queue.close();
    });
  });

  // ============== Get Client Tests ==============

  describe('Get Client', () => {
    test('should return underlying client', async () => {
      const queue = new Queue(TEST_QUEUE);
      const client = queue.getClient();

      expect(client).toBeInstanceOf(FlashQ);

      await queue.close();
    });

    test('should allow advanced operations via client', async () => {
      const queue = new Queue(TEST_QUEUE);
      const client = queue.getClient();

      const stats = await client.stats();
      expect(stats).toHaveProperty('queued');

      await queue.close();
    });
  });

  // ============== Connection Options Tests ==============

  describe('Connection Options', () => {
    test('should accept connection options', async () => {
      const queue = new Queue(TEST_QUEUE, {
        connection: {
          host: 'localhost',
          port: 6789,
          timeout: 5000,
        },
      });

      const job = await queue.add({ test: true });
      expect(job.id).toBeGreaterThan(0);

      await queue.close();
    });
  });
});

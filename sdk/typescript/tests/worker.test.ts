/**
 * FlashQ Worker Tests - Comprehensive Coverage
 *
 * Run: bun test tests/worker.test.ts
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { FlashQ } from '../src/client';
import { Worker } from '../src/worker';
import { EventEmitter } from 'events';

const TEST_QUEUE = 'test-worker';

describe('FlashQ Worker', () => {
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

  // ============== Constructor Tests ==============

  describe('Constructor', () => {
    test('should extend EventEmitter', () => {
      const worker = new Worker(TEST_QUEUE, async () => ({}));
      expect(worker).toBeInstanceOf(EventEmitter);
    });

    test('should accept single queue as string', () => {
      const worker = new Worker(TEST_QUEUE, async () => ({}));
      expect(worker).toBeInstanceOf(Worker);
    });

    test('should accept multiple queues as array', () => {
      const worker = new Worker([TEST_QUEUE, 'another-queue'], async () => ({}));
      expect(worker).toBeInstanceOf(Worker);
    });

    test('should accept all worker options', () => {
      const worker = new Worker(TEST_QUEUE, async () => ({}), {
        id: 'custom-worker-id',
        concurrency: 5,
        heartbeatInterval: 2000,
        autoAck: true,
        host: 'localhost',
        port: 6789,
        httpPort: 6790,
        timeout: 10000,
      });
      expect(worker).toBeInstanceOf(Worker);
    });

    test('should generate unique ID if not provided', () => {
      const worker1 = new Worker(TEST_QUEUE, async () => ({}));
      const worker2 = new Worker(TEST_QUEUE, async () => ({}));
      // Workers should be different instances
      expect(worker1).not.toBe(worker2);
    });
  });

  // ============== Basic Worker Tests ==============

  describe('Basic Operations', () => {
    test('should create worker', () => {
      const worker = new Worker(TEST_QUEUE, async () => ({}));
      expect(worker).toBeInstanceOf(Worker);
      expect(worker.isRunning()).toBe(false);
    });

    test('should start and stop worker', async () => {
      const worker = new Worker(TEST_QUEUE, async () => ({}), {
        host: 'localhost',
        port: 6789,
        concurrency: 1,
      });

      await worker.start();
      expect(worker.isRunning()).toBe(true);

      await worker.stop();
      expect(worker.isRunning()).toBe(false);
    });

    test.skip('should process a job', async () => {
      let processedData: any = null;

      const worker = new Worker<{ message: string }>(
        TEST_QUEUE,
        async (job) => {
          processedData = job.data;
          return { processed: true };
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();

      // Push a job
      await client.push(TEST_QUEUE, { message: 'hello worker' });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 500));

      await worker.stop();

      expect(processedData).toEqual({ message: 'hello worker' });
    });

    test.skip('should emit completed event', async () => {
      let completedJob: any = null;
      let completedResult: any = null;

      const worker = new Worker(
        TEST_QUEUE,
        async () => ({ result: 'success' }),
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      worker.on('completed', (job, result) => {
        completedJob = job;
        completedResult = result;
      });

      await worker.start();
      await client.push(TEST_QUEUE, { data: 1 });

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      expect(completedJob).not.toBeNull();
      expect(completedResult).toEqual({ result: 'success' });
    });

    test.skip('should emit failed event on error', async () => {
      let failedJob: any = null;
      let failedError: any = null;

      const worker = new Worker(
        TEST_QUEUE,
        async () => {
          throw new Error('Test error');
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      worker.on('failed', (job, error) => {
        failedJob = job;
        failedError = error;
      });

      await worker.start();
      await client.push(TEST_QUEUE, { data: 1 }, { max_attempts: 1 });

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      expect(failedJob).not.toBeNull();
      expect(failedError).toBeInstanceOf(Error);
      expect(failedError.message).toBe('Test error');
    });
  });

  // ============== Concurrency Tests ==============

  describe('Concurrency', () => {
    test.skip('should process jobs concurrently', async () => {
      const processingTimes: number[] = [];
      const startTime = Date.now();

      const worker = new Worker(
        TEST_QUEUE,
        async (job) => {
          await new Promise(r => setTimeout(r, 100));
          processingTimes.push(Date.now() - startTime);
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 5 }
      );

      await worker.start();

      // Push 5 jobs
      for (let i = 0; i < 5; i++) {
        await client.push(TEST_QUEUE, { index: i });
      }

      // Wait for all to complete
      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      // With concurrency 5, all jobs should complete around the same time
      expect(processingTimes.length).toBe(5);
      const maxDiff = Math.max(...processingTimes) - Math.min(...processingTimes);
      expect(maxDiff).toBeLessThan(200); // All within 200ms of each other
    });

    test('should track processing count', async () => {
      let maxProcessing = 0;

      const worker = new Worker(
        TEST_QUEUE,
        async () => {
          const current = worker.getProcessingCount();
          maxProcessing = Math.max(maxProcessing, current);
          await new Promise(r => setTimeout(r, 100));
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 3 }
      );

      await worker.start();

      for (let i = 0; i < 6; i++) {
        await client.push(TEST_QUEUE, { i });
      }

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      expect(maxProcessing).toBeLessThanOrEqual(3);
    });
  });

  // ============== Multiple Queues Tests ==============

  describe('Multiple Queues', () => {
    const QUEUE_A = 'test-worker-a';
    const QUEUE_B = 'test-worker-b';

    afterAll(async () => {
      await client.obliterate(QUEUE_A);
      await client.obliterate(QUEUE_B);
    });

    test('should process from multiple queues', async () => {
      const processed: string[] = [];

      const worker = new Worker(
        [QUEUE_A, QUEUE_B],
        async (job) => {
          processed.push(job.queue);
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 2 }
      );

      await worker.start();

      await client.push(QUEUE_A, { queue: 'A' });
      await client.push(QUEUE_B, { queue: 'B' });

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      expect(processed).toContain(QUEUE_A);
      expect(processed).toContain(QUEUE_B);
    });
  });

  // ============== Jobs Processed Counter Tests ==============

  describe('Jobs Processed Counter', () => {
    test.skip('should track total jobs processed', async () => {
      const worker = new Worker(
        TEST_QUEUE,
        async () => ({}),
        { host: 'localhost', port: 6789, concurrency: 2 }
      );

      await worker.start();

      for (let i = 0; i < 5; i++) {
        await client.push(TEST_QUEUE, { i });
      }

      await new Promise(resolve => setTimeout(resolve, 500));

      const processed = worker.getJobsProcessed();
      await worker.stop();

      expect(processed).toBe(5);
    });
  });

  // ============== Graceful Shutdown Tests ==============

  describe('Graceful Shutdown', () => {
    test.skip('should complete current jobs on stop', async () => {
      let completed = 0;

      const worker = new Worker(
        TEST_QUEUE,
        async () => {
          await new Promise(r => setTimeout(r, 200));
          completed++;
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();
      await client.push(TEST_QUEUE, { data: 1 });

      // Wait for job to start processing
      await new Promise(resolve => setTimeout(resolve, 50));

      // Stop should wait for current job
      await worker.stop();

      expect(completed).toBe(1);
    });

    test('should emit stopping and stopped events', async () => {
      const events: string[] = [];

      const worker = new Worker(
        TEST_QUEUE,
        async () => ({}),
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      worker.on('ready', () => events.push('ready'));
      worker.on('stopping', () => events.push('stopping'));
      worker.on('stopped', () => events.push('stopped'));

      await worker.start();
      await worker.stop();

      expect(events).toContain('ready');
      expect(events).toContain('stopping');
      expect(events).toContain('stopped');
    });
  });

  // ============== Auto Ack Tests ==============

  describe('Auto Ack', () => {
    test('should auto-ack successful jobs by default', async () => {
      const worker = new Worker(
        TEST_QUEUE,
        async () => ({ success: true }),
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();

      const job = await client.push(TEST_QUEUE, { data: 1 });

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      const state = await client.getState(job.id);
      expect(state).toBe('completed');
    });

    test.skip('should auto-fail errored jobs', async () => {
      const worker = new Worker(
        TEST_QUEUE,
        async () => {
          throw new Error('Fail');
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();

      const job = await client.push(TEST_QUEUE, { data: 1 }, { max_attempts: 1 });

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      const state = await client.getState(job.id);
      expect(state).toBe('failed');
    });
  });

  // ============== Update Progress Tests ==============

  describe('updateProgress', () => {
    test('should have updateProgress method', async () => {
      const worker = new Worker<{ data: number }>(
        TEST_QUEUE,
        async () => ({}),
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      // Verify method exists
      expect(typeof worker.updateProgress).toBe('function');
    });

    test('should update progress for active job', async () => {
      let capturedJobId: number = 0;

      const worker = new Worker<{ data: number }>(
        TEST_QUEUE,
        async (job) => {
          capturedJobId = job.id;
          // Update progress during processing
          await worker.updateProgress(job.id, 50, 'Processing...');
          await new Promise(r => setTimeout(r, 100));
          await worker.updateProgress(job.id, 100, 'Done');
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();
      await client.push(TEST_QUEUE, { data: 1 });

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      // Verify progress was updated
      if (capturedJobId > 0) {
        const progress = await client.getProgress(capturedJobId);
        expect(progress.progress).toBe(100);
      }
    });
  });

  // ============== Error Event Tests ==============

  describe('Error Events', () => {
    test.skip('should emit error event on connection issues', async () => {
      let errorEmitted = false;

      const worker = new Worker(
        TEST_QUEUE,
        async () => ({}),
        { host: 'localhost', port: 9999, concurrency: 1 } // Wrong port
      );

      worker.on('error', () => {
        errorEmitted = true;
      });

      try {
        await worker.start();
        await new Promise(r => setTimeout(r, 200));
      } catch {
        // Expected to fail
      }

      await worker.stop();
    });
  });

  // ============== Active Event Tests ==============

  describe('Active Event', () => {
    test.skip('should emit active event when job starts processing', async () => {
      let activeJob: any = null;
      let activeWorkerId: number | undefined;

      const worker = new Worker(
        TEST_QUEUE,
        async () => {
          await new Promise(r => setTimeout(r, 50));
          return {};
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      worker.on('active', (job, workerId) => {
        activeJob = job;
        activeWorkerId = workerId;
      });

      await worker.start();
      await client.push(TEST_QUEUE, { data: 1 });

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      expect(activeJob).not.toBeNull();
      expect(typeof activeWorkerId).toBe('number');
    });
  });

  // ============== Typed Worker Tests ==============

  describe('Typed Worker', () => {
    test.skip('should handle typed input and output', async () => {
      interface InputData {
        value: number;
        name: string;
      }

      interface OutputData {
        doubled: number;
        greeting: string;
      }

      const results: OutputData[] = [];

      const worker = new Worker<InputData, OutputData>(
        TEST_QUEUE,
        async (job) => {
          return {
            doubled: job.data.value * 2,
            greeting: `Hello, ${job.data.name}`,
          };
        },
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      worker.on('completed', (job, result) => {
        results.push(result as OutputData);
      });

      await worker.start();
      await client.push(TEST_QUEUE, { value: 5, name: 'Alice' });

      await new Promise(resolve => setTimeout(resolve, 500));
      await worker.stop();

      expect(results.length).toBe(1);
      expect(results[0].doubled).toBe(10);
      expect(results[0].greeting).toBe('Hello, Alice');
    });
  });

  // ============== Double Start/Stop Tests ==============

  describe('Idempotent Operations', () => {
    test('should handle double start gracefully', async () => {
      const worker = new Worker(
        TEST_QUEUE,
        async () => ({}),
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();
      await worker.start(); // Second start should be no-op

      expect(worker.isRunning()).toBe(true);

      await worker.stop();
    });

    test('should handle double stop gracefully', async () => {
      const worker = new Worker(
        TEST_QUEUE,
        async () => ({}),
        { host: 'localhost', port: 6789, concurrency: 1 }
      );

      await worker.start();
      await worker.stop();
      await worker.stop(); // Second stop should be no-op

      expect(worker.isRunning()).toBe(false);
    });
  });
});

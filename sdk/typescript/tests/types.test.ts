/**
 * FlashQ Types Tests
 *
 * Tests to verify all types are properly exported and usable.
 *
 * Run: bun test tests/types.test.ts
 */

import { describe, test, expect } from 'bun:test';
import type {
  Job,
  JobState,
  PushOptions,
  WorkerOptions,
  ClientOptions,
} from '../src/types';

describe('Type Definitions', () => {
  // ============== Job Types ==============

  describe('Job Types', () => {
    test('Job interface should be valid', () => {
      const job: Job = {
        id: 1,
        queue: 'test',
        data: { message: 'hello' },
        priority: 0,
        created_at: Date.now(),
        run_at: Date.now(),
        started_at: 0,
        attempts: 0,
        max_attempts: 3,
        backoff: 1000,
        ttl: 0,
        timeout: 30000,
        progress: 0,
        tags: [],
        depends_on: [],
        children_ids: [],
        children_completed: 0,
        lifo: false,
        remove_on_complete: false,
        remove_on_fail: false,
        last_heartbeat: 0,
        stall_timeout: 0,
        stall_count: 0,
        keep_completed_age: 0,
        keep_completed_count: 0,
        completed_at: 0,
      };

      expect(job.id).toBe(1);
    });

    test('JobState should include all states', () => {
      const states: JobState[] = [
        'waiting',
        'delayed',
        'active',
        'completed',
        'failed',
      ];

      expect(states.length).toBe(5);
    });

    test('PushOptions should accept all fields', () => {
      const options: PushOptions = {
        priority: 10,
        delay: 5000,
        ttl: 60000,
        timeout: 30000,
        max_attempts: 5,
        backoff: 2000,
        unique_key: 'unique-123',
        depends_on: [1, 2, 3],
        tags: ['important'],
        lifo: false,
        remove_on_complete: true,
        remove_on_fail: false,
        stall_timeout: 60000,
        debounce_id: 'debounce-key',
        debounce_ttl: 5000,
        jobId: 'custom-id',
        keepCompletedAge: 86400000,
        keepCompletedCount: 100,
      };

      expect(options.priority).toBe(10);
    });
  });

  // ============== Worker Types ==============

  describe('Worker Types', () => {
    test('WorkerOptions should have all fields', () => {
      const options: WorkerOptions = {
        id: 'worker-1',
        concurrency: 5,
        heartbeatInterval: 1000,
        autoAck: true,
      };

      expect(options.concurrency).toBe(5);
    });

    test('ClientOptions should have all fields', () => {
      const options: ClientOptions = {
        host: 'localhost',
        port: 6789,
        httpPort: 6790,
        socketPath: '/tmp/flashq.sock',
        token: 'secret',
        timeout: 5000,
        useHttp: false,
        useBinary: true,
      };

      expect(options.host).toBe('localhost');
    });
  });
});

// ============== Export Tests ==============

describe('Module Exports', () => {
  test('should export all classes from main index', async () => {
    const exports = await import('../src/index');

    // Classes
    expect(exports.FlashQ).toBeDefined();
    expect(exports.Worker).toBeDefined();
    expect(exports.Queue).toBeDefined();
    expect(exports.EventSubscriber).toBeDefined();
  });

  test('should have default export as FlashQ', async () => {
    const exports = await import('../src/index');
    expect(exports.default).toBe(exports.FlashQ);
  });
});

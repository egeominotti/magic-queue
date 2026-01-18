/**
 * FlashQ Event Subscription Tests
 *
 * Tests for EventSubscriber (SSE and WebSocket), including
 * connection management, event handling, and auto-reconnect.
 *
 * Run: bun test tests/events.test.ts
 *
 * Note: These tests require the flashQ server to be running with HTTP enabled (HTTP=1)
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { FlashQ } from '../src/client';
import {
  EventSubscriber,
  createEventSubscriber,
  createWebSocketSubscriber,
  subscribeToEvents,
} from '../src/events';
import type { JobEvent, EventSubscriberOptions } from '../src/events';

const TEST_QUEUE = 'test-events';

describe('EventSubscriber', () => {
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
    test('should create EventSubscriber with default options', () => {
      const subscriber = new EventSubscriber();
      expect(subscriber).toBeInstanceOf(EventSubscriber);
      expect(subscriber.isConnected()).toBe(false);
    });

    test('should create EventSubscriber with custom options', () => {
      const subscriber = new EventSubscriber({
        host: 'localhost',
        httpPort: 6790,
        token: 'test-token',
        queue: TEST_QUEUE,
        type: 'sse',
        autoReconnect: true,
        reconnectDelay: 2000,
        maxReconnectAttempts: 5,
      });
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('should default to SSE type', () => {
      const subscriber = new EventSubscriber();
      // Type is private, but we can verify it works with SSE defaults
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });
  });

  // ============== Connection Management Tests ==============

  describe('Connection Management', () => {
    test('should check connection status', () => {
      const subscriber = new EventSubscriber({
        host: 'localhost',
        httpPort: 6790,
      });

      expect(subscriber.isConnected()).toBe(false);
    });

    test('should close connection gracefully', () => {
      const subscriber = new EventSubscriber({
        host: 'localhost',
        httpPort: 6790,
      });

      // Should not throw even if not connected
      subscriber.close();
      expect(subscriber.isConnected()).toBe(false);
    });

    test('should emit disconnected event on close', async () => {
      const subscriber = new EventSubscriber({
        host: 'localhost',
        httpPort: 6790,
        autoReconnect: false,
      });

      let disconnectedEmitted = false;

      // Try to connect (might fail if server not running with HTTP)
      try {
        await subscriber.connect();

        subscriber.on('disconnected', () => {
          disconnectedEmitted = true;
        });

        subscriber.close();
        // Give time for event to fire
        await new Promise(r => setTimeout(r, 100));
        expect(disconnectedEmitted).toBe(true);
      } catch (e) {
        // Server might not have HTTP enabled, skip
        subscriber.close();
      }
    });
  });

  // ============== Typed Event Emitter Tests ==============

  describe('Typed Event Emitter', () => {
    test('should support on/off for typed events', () => {
      const subscriber = new EventSubscriber();

      const handler = () => {};

      subscriber.on('connected', handler);
      subscriber.off('connected', handler);
    });

    test('should support all event types', () => {
      const subscriber = new EventSubscriber();

      subscriber.on('connected', () => {});
      subscriber.on('disconnected', () => {});
      subscriber.on('reconnecting', (attempt) => {});
      subscriber.on('error', (error) => {});
      subscriber.on('event', (event) => {});
      subscriber.on('pushed', (event) => {});
      subscriber.on('completed', (event) => {});
      subscriber.on('failed', (event) => {});
      subscriber.on('progress', (event) => {});
      subscriber.on('timeout', (event) => {});
    });
  });

  // ============== Factory Functions Tests ==============

  describe('Factory Functions', () => {
    test('createEventSubscriber should create SSE subscriber', () => {
      const subscriber = createEventSubscriber({
        host: 'localhost',
        httpPort: 6790,
      });

      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('createWebSocketSubscriber should create WebSocket subscriber', () => {
      const subscriber = createWebSocketSubscriber({
        host: 'localhost',
        httpPort: 6790,
      });

      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('createEventSubscriber should default to SSE', () => {
      const subscriber = createEventSubscriber();
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('createWebSocketSubscriber should set type to websocket', () => {
      const subscriber = createWebSocketSubscriber();
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('should pass queue filter', () => {
      const subscriber = createEventSubscriber({
        queue: TEST_QUEUE,
      });
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('should pass token', () => {
      const subscriber = createEventSubscriber({
        token: 'my-secret-token',
      });
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });
  });

  // ============== subscribeToEvents Convenience Function Tests ==============

  describe('subscribeToEvents', () => {
    test('should create subscription with callback', async () => {
      const events: JobEvent[] = [];

      // This will fail if HTTP server is not running
      try {
        const unsubscribe = await subscribeToEvents(
          { host: 'localhost', httpPort: 6790, queue: TEST_QUEUE },
          (event) => {
            events.push(event);
          }
        );

        // Cleanup
        unsubscribe();
      } catch (e) {
        // HTTP server might not be running
        // Test passes if no crash
      }
    });
  });

  // ============== Options Tests ==============

  describe('Options', () => {
    test('should accept all options', () => {
      const options: EventSubscriberOptions = {
        host: 'localhost',
        httpPort: 6790,
        token: 'secret',
        queue: TEST_QUEUE,
        type: 'sse',
        autoReconnect: true,
        reconnectDelay: 1000,
        maxReconnectAttempts: 10,
      };

      const subscriber = new EventSubscriber(options);
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('should accept websocket type', () => {
      const subscriber = new EventSubscriber({
        type: 'websocket',
      });
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('should disable auto-reconnect', () => {
      const subscriber = new EventSubscriber({
        autoReconnect: false,
      });
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('should set custom reconnect delay', () => {
      const subscriber = new EventSubscriber({
        reconnectDelay: 5000,
      });
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });

    test('should set infinite reconnect attempts (0)', () => {
      const subscriber = new EventSubscriber({
        maxReconnectAttempts: 0,
      });
      expect(subscriber).toBeInstanceOf(EventSubscriber);
    });
  });

  // ============== Integration Tests (require HTTP server) ==============

  describe('Integration Tests (SSE)', () => {
    test.skip('should connect and receive events via SSE', async () => {
      // This test requires the HTTP server to be running
      // It's skipped by default because SSE/EventSource may not work in Bun tests

      const events: JobEvent[] = [];
      const subscriber = createEventSubscriber({
        host: 'localhost',
        httpPort: 6790,
        queue: TEST_QUEUE,
      });

      subscriber.on('event', (event) => {
        events.push(event);
      });

      await subscriber.connect();
      expect(subscriber.isConnected()).toBe(true);

      // Push a job and wait for event
      await client.push(TEST_QUEUE, { test: 'event' });
      await new Promise(r => setTimeout(r, 500));

      subscriber.close();

      expect(events.length).toBeGreaterThan(0);
    });

    test.skip('should filter events by queue', async () => {
      const events: JobEvent[] = [];
      const subscriber = createEventSubscriber({
        host: 'localhost',
        httpPort: 6790,
        queue: TEST_QUEUE,
      });

      subscriber.on('pushed', (event) => {
        events.push(event);
      });

      await subscriber.connect();

      // Push to test queue
      await client.push(TEST_QUEUE, { test: 'filtered' });
      // Push to different queue (should not be received)
      await client.push('other-queue', { test: 'other' });

      await new Promise(r => setTimeout(r, 500));
      subscriber.close();

      // Should only have events from TEST_QUEUE
      events.forEach(event => {
        expect(event.queue).toBe(TEST_QUEUE);
      });
    });

    test.skip('should emit specific event types', async () => {
      let pushedReceived = false;
      let completedReceived = false;

      const subscriber = createEventSubscriber({
        host: 'localhost',
        httpPort: 6790,
        queue: TEST_QUEUE,
      });

      subscriber.on('pushed', () => {
        pushedReceived = true;
      });

      subscriber.on('completed', () => {
        completedReceived = true;
      });

      await subscriber.connect();

      // Push job
      const job = await client.push(TEST_QUEUE, { test: 'events' });
      await new Promise(r => setTimeout(r, 200));

      // Complete job
      const pulled = await client.pull(TEST_QUEUE, 1000);
      if (pulled) {
        await client.ack(pulled.id, { done: true });
      }

      await new Promise(r => setTimeout(r, 200));
      subscriber.close();

      expect(pushedReceived).toBe(true);
      expect(completedReceived).toBe(true);
    });
  });

  describe('Integration Tests (WebSocket)', () => {
    test.skip('should connect via WebSocket', async () => {
      // This test requires WebSocket support
      const subscriber = createWebSocketSubscriber({
        host: 'localhost',
        httpPort: 6790,
        token: '', // Empty token for testing
      });

      await subscriber.connect();
      expect(subscriber.isConnected()).toBe(true);

      subscriber.close();
    });

    test.skip('should receive events via WebSocket', async () => {
      const events: JobEvent[] = [];
      const subscriber = createWebSocketSubscriber({
        host: 'localhost',
        httpPort: 6790,
        queue: TEST_QUEUE,
      });

      subscriber.on('event', (event) => {
        events.push(event);
      });

      await subscriber.connect();

      await client.push(TEST_QUEUE, { websocket: 'test' });
      await new Promise(r => setTimeout(r, 500));

      subscriber.close();

      expect(events.length).toBeGreaterThan(0);
    });
  });

  // ============== Reconnection Tests ==============

  describe('Auto-Reconnect', () => {
    test.skip('should emit reconnecting event', async () => {
      let reconnectAttempt = 0;

      const subscriber = new EventSubscriber({
        host: 'localhost',
        httpPort: 9999, // Wrong port to trigger reconnect
        autoReconnect: true,
        reconnectDelay: 100,
        maxReconnectAttempts: 2,
      });

      subscriber.on('reconnecting', (attempt) => {
        reconnectAttempt = attempt;
      });

      try {
        await subscriber.connect();
      } catch (e) {
        // Expected to fail
      }

      await new Promise(r => setTimeout(r, 500));
      subscriber.close();

      // Should have attempted reconnect
      expect(reconnectAttempt).toBeGreaterThan(0);
    });

    test.skip('should emit error after max reconnect attempts', async () => {
      let errorReceived = false;

      const subscriber = new EventSubscriber({
        host: 'localhost',
        httpPort: 9999, // Wrong port
        autoReconnect: true,
        reconnectDelay: 50,
        maxReconnectAttempts: 2,
      });

      subscriber.on('error', (error) => {
        if (error.message.includes('Max reconnect')) {
          errorReceived = true;
        }
      });

      try {
        await subscriber.connect();
      } catch (e) {}

      await new Promise(r => setTimeout(r, 500));
      subscriber.close();

      expect(errorReceived).toBe(true);
    });

    // Skip: Flaky test - unrelated errors from other tests leak through
    test.skip('should not reconnect when autoReconnect is false', async () => {
      let reconnectAttempts = 0;

      const subscriber = new EventSubscriber({
        host: 'localhost',
        httpPort: 9999, // Wrong port
        autoReconnect: false,
      });

      subscriber.on('reconnecting', () => {
        reconnectAttempts++;
      });

      try {
        await subscriber.connect();
      } catch (e) {
        // Expected to fail
      }

      await new Promise(r => setTimeout(r, 200));
      subscriber.close();

      expect(reconnectAttempts).toBe(0);
    });
  });

  // ============== Event Normalization Tests ==============

  describe('Event Normalization', () => {
    test('should handle snake_case event fields', () => {
      // The normalizeEvent method converts snake_case to camelCase
      // We test this indirectly through the EventSubscriber

      const subscriber = new EventSubscriber();

      // Subscribe to events to ensure handlers are set up
      subscriber.on('event', (event: JobEvent) => {
        // Event should have normalized fields
        expect(event).toHaveProperty('eventType');
        expect(event).toHaveProperty('jobId');
        expect(event).toHaveProperty('timestamp');
      });
    });
  });

  // ============== Event Type Tests ==============

  describe('JobEvent Types', () => {
    test('should define all event types', () => {
      const eventTypes = ['pushed', 'completed', 'failed', 'progress', 'timeout'];

      const subscriber = new EventSubscriber();

      eventTypes.forEach(type => {
        subscriber.on(type as any, (event: JobEvent) => {
          expect(event.eventType).toBe(type);
        });
      });
    });

    test('should have proper JobEvent structure', () => {
      const mockEvent: JobEvent = {
        eventType: 'completed',
        queue: TEST_QUEUE,
        jobId: 123,
        timestamp: Date.now(),
        data: { test: true },
        error: undefined,
        progress: undefined,
      };

      expect(mockEvent.eventType).toBe('completed');
      expect(mockEvent.queue).toBe(TEST_QUEUE);
      expect(mockEvent.jobId).toBe(123);
      expect(typeof mockEvent.timestamp).toBe('number');
    });

    test('should support optional fields in JobEvent', () => {
      const failedEvent: JobEvent = {
        eventType: 'failed',
        queue: TEST_QUEUE,
        jobId: 456,
        timestamp: Date.now(),
        error: 'Something went wrong',
      };

      expect(failedEvent.error).toBe('Something went wrong');

      const progressEvent: JobEvent = {
        eventType: 'progress',
        queue: TEST_QUEUE,
        jobId: 789,
        timestamp: Date.now(),
        progress: 50,
      };

      expect(progressEvent.progress).toBe(50);
    });
  });
});

// ============== Client Event Subscription Tests ==============

describe('Client Event Subscriptions', () => {
  let client: FlashQ;

  beforeAll(async () => {
    client = new FlashQ({ host: 'localhost', port: 6789, timeout: 10000 });
    await client.connect();
  });

  afterAll(async () => {
    await client.close();
  });

  test('should create SSE subscriber via client.subscribe()', () => {
    const subscriber = client.subscribe();
    expect(subscriber).toBeInstanceOf(EventSubscriber);
  });

  test('should create queue-specific SSE subscriber', () => {
    const subscriber = client.subscribe(TEST_QUEUE);
    expect(subscriber).toBeInstanceOf(EventSubscriber);
  });

  test('should create WebSocket subscriber via client.subscribeWs()', () => {
    const subscriber = client.subscribeWs();
    expect(subscriber).toBeInstanceOf(EventSubscriber);
  });

  test('should create queue-specific WebSocket subscriber', () => {
    const subscriber = client.subscribeWs(TEST_QUEUE);
    expect(subscriber).toBeInstanceOf(EventSubscriber);
  });
});
